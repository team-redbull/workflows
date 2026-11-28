"""create-upi-cluster and add-server-to-upi: the REAL workflows (and the real allocate-segment
child), mock activities on every queue they dispatch to, time-skipping env.

Pinned: what each limb received, the segment types allocated per kind, the MCE-only day1
step, and classification — a permanent error fails the run with its type after one attempt,
a workflow-raised refusal gives the server's lock back first.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    ADD_SERVER_TO_UPI_WORKFLOW_QUEUE,
    ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
    CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
    CREATE_UPI_CLUSTER_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    UPI_PROVISIONING_ACTIVITY_QUEUE,
    upi_server_activity_queue,
)
from shared.exceptions import InstallerJobFailedError, NodeJoinTimeoutError
from shared.models.cluster_lifecycle import MergeRequestRef, MergeRequestRequest, MergeRequestState
from shared.models.segment_lifecycle import (
    ClusterAllocationLookup,
    ClusterAllocationLookupRequest,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
)
from shared.models.server_lifecycle import AcquiredServer, BmcEndpoint, LinkState, ServerInterface
from shared.models.upi_lifecycle import (
    AddServerToUpiInput,
    AddServerToUpiRunArgs,
    AllocateUpiServerRequest,
    ArgoRegistration,
    ArgoRegistrationRequest,
    ClusterReadyRequest,
    CreateUpiClusterInput,
    CreateUpiClusterRunArgs,
    FinishedUpiNode,
    FinishUpiNodeRequest,
    InstallerStepRequest,
    InstallerStepResult,
    MceFolderPublishRequest,
    MceFolderRef,
    NodeIso,
    NodeIsoRequest,
    NodeJoined,
    NodeJoinRequest,
    UpiClusterKind,
    UpiClusterStatus,
    UpiNode,
    UpiNodeInspectRequest,
    UpiNodeState,
    UpiServerLockRequest,
    VirtualMediaBoot,
    VirtualMediaBootRequest,
)
from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow
from workflow_domains.upi_lifecycle.add_server_to_upi import AddServerToUpiWorkflow
from workflow_domains.upi_lifecycle.create_upi_cluster import CreateUpiClusterWorkflow

CLUSTER = "ocp4-prod-mce-tlv-a"
SITE = "tlv"
MR_URL = "https://gitlab.example/p/-/merge_requests/7"
SEGMENTS = {"MCE": ("10.1.5.0/24", 105), "INVENTORY": ("10.1.6.0/24", 106), "UPI": ("10.1.7.0/24", 107)}


class _Harness:
    def __init__(self, queues: dict[str, list]) -> None:
        self._queues = queues

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=CREATE_UPI_CLUSTER_WORKFLOW_QUEUE, workflows=[CreateUpiClusterWorkflow]),
            Worker(client, task_queue=ADD_SERVER_TO_UPI_WORKFLOW_QUEUE, workflows=[AddServerToUpiWorkflow]),
            Worker(client, task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE, workflows=[AllocateSegmentWorkflow]),
            *(Worker(client, task_queue=queue, activities=acts) for queue, acts in self._queues.items()),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


def _error(exc: WorkflowFailureError) -> ApplicationError:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


# --- create-upi-cluster --------------------------------------------------------------------


def _segment_mocks(calls: dict):
    @activity.defn
    async def get_valid_sites() -> list[str]:
        return [SITE]

    @activity.defn
    async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
        calls.setdefault("allocate", []).append(request.type.value)
        segment, vlan = SEGMENTS[request.type.value]
        return SegmentAllocation(vlan_id=vlan, segment=segment, epg_name=f"EPG_{vlan}")

    @activity.defn
    async def get_segment(segment: str) -> SegmentEntry:
        kind = next(t for t, (s, _) in SEGMENTS.items() if s == segment)
        holder: str | list[str] = [CLUSTER] if kind == "UPI" else CLUSTER
        return SegmentEntry(segment=segment, site=SITE, vlan_id=SEGMENTS[kind][1], status="Allocated", type=kind,
                            cluster_name=holder)

    return [get_valid_sites, allocate_segment, get_segment]


def _create_mocks(calls: dict, *, install_error: Exception | None = None):
    @activity.defn
    async def run_installer_step(request: InstallerStepRequest) -> InstallerStepResult:
        calls.setdefault("steps", []).append((request.step.value, request.segment, request.vlan_id, request.run_token))
        if install_error is not None and request.step.value == "install":
            raise install_error
        return InstallerStepResult(step=request.step, job_name=f"upi-{request.cluster}-{request.step.value}")

    @activity.defn
    async def await_upi_cluster_ready(request: ClusterReadyRequest) -> UpiClusterStatus:
        calls.setdefault("ready", []).append(request.cluster)
        return UpiClusterStatus(api_url=f"https://api.{request.cluster}.example:6443", version="4.20.8",
                                nodes=[UpiNode(name=f"m{i}", internal_ip=f"10.1.5.{i}", ready=True) for i in range(1, 7)])

    @activity.defn
    async def register_upi_cluster_in_argocd(request: ArgoRegistrationRequest) -> ArgoRegistration:
        calls.setdefault("argo", []).append(request.kind.value)
        return ArgoRegistration(secret_name=f"cluster-{request.cluster}", namespace="openshift-gitops",
                                name=request.cluster, server="https://api", changed=True)

    @activity.defn
    async def publish_mce_folder(request: MceFolderPublishRequest) -> MceFolderRef:
        calls.setdefault("folder", []).append((request.site, request.mce, request.mastertag, request.branch))
        return MceFolderRef(folder=f"sites/{request.site}/mces/{request.mce}", branch=request.branch, changed=True)

    @activity.defn
    async def ensure_merge_request(request: MergeRequestRequest) -> MergeRequestRef:
        calls.setdefault("mr", []).append(request.branch)
        return MergeRequestRef(url=MR_URL, identifier="7", created=True)

    @activity.defn
    async def get_merge_request_state(merge_request: MergeRequestRef) -> MergeRequestState:
        polls = calls.setdefault("polls", [])
        polls.append(1)
        return MergeRequestState(state="merged" if len(polls) >= 2 else "open", merged_sha="abc123")

    return {
        UPI_PROVISIONING_ACTIVITY_QUEUE: [run_installer_step, await_upi_cluster_ready, register_upi_cluster_in_argocd],
        CLUSTER_LIFECYCLE_ACTIVITY_QUEUE: [publish_mce_folder, ensure_merge_request, get_merge_request_state],
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE: _segment_mocks(calls),
    }


async def _create(client: Client, kind: UpiClusterKind = UpiClusterKind.MCE):
    handle = await client.start_workflow(
        CreateUpiClusterWorkflow.run,
        CreateUpiClusterRunArgs(input=CreateUpiClusterInput(cluster=CLUSTER, site=SITE, kind=kind)),
        id=f"test-{uuid.uuid4()}",
        task_queue=CREATE_UPI_CLUSTER_WORKFLOW_QUEUE,
    )
    return await asyncio.wait_for(handle.result(), timeout=60)


async def test_an_mce_is_built_registered_and_added_to_day1_after_the_merge():
    calls: dict = {}
    async with _Harness(_create_mocks(calls)) as client:
        result = await _create(client)
    assert calls["allocate"] == ["MCE", "INVENTORY"]
    assert [s[:3] for s in calls["steps"]] == [("prepare", "10.1.5.0/24", 105), ("install", "10.1.5.0/24", 105)]
    assert len({s[3] for s in calls["steps"]}) == 1, "both Jobs carry the run's one token"
    assert calls["folder"] == [(SITE, CLUSTER, "4.20.8", f"create-upi-cluster/{CLUSTER}")]
    assert len(calls["polls"]) == 2
    assert result.inventory_vlan_id == 106 and result.merge_request_url == MR_URL and result.merged_sha == "abc123"
    assert result.argo_secret == f"cluster-{CLUSTER}" and result.version == "4.20.8"


async def test_a_standalone_cluster_takes_a_upi_segment_and_no_day1_step():
    calls: dict = {}
    async with _Harness(_create_mocks(calls)) as client:
        result = await _create(client, UpiClusterKind.STANDALONE)
    assert calls["allocate"] == ["UPI"]
    assert "folder" not in calls and "mr" not in calls
    assert result.vlan_id == 107 and result.inventory_segment is None and result.mce_folder is None


async def test_a_failed_installer_job_fails_the_run_after_one_attempt():
    calls: dict = {}
    async with _Harness(_create_mocks(calls, install_error=InstallerJobFailedError("terraform: boom"))) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _create(client)
    assert _error(exc_info.value).type == "InstallerJobFailedError"
    assert [s[0] for s in calls["steps"]] == ["prepare", "install"]
    assert "ready" not in calls and "argo" not in calls


# --- add-server-to-upi ---------------------------------------------------------------------


def _server(vendor: str = "DELL") -> AcquiredServer:
    return AcquiredServer(
        id="srv_1", name="ocp-r760-tlv-abc", vendor=vendor.lower(), bmc_vendor=vendor,
        bmc=BmcEndpoint(host="10.9.9.9"), health_overall="HEALTHY",
        interfaces=[
            ServerInterface(name="NIC.1", mac="aa:00:00:00:00:01", location="1/1/1", link_state=LinkState.UP),
            ServerInterface(name="NIC.2", mac="aa:00:00:00:00:02", location="1/2/1", link_state=LinkState.UP),
        ],
    )


def _add_mocks(calls: dict, *, vendor: str = "DELL", dhcp: bool | None = True, join_error: Exception | None = None):
    @activity.defn
    async def find_cluster_allocation(request: ClusterAllocationLookupRequest) -> ClusterAllocationLookup:
        calls.setdefault("lookup", []).append(request.type.value)
        if request.type.value != "MCE":
            return ClusterAllocationLookup(found=False)
        return ClusterAllocationLookup(found=True, entry=SegmentEntry(
            segment="10.1.5.0/24", site=SITE, vlan_id=105, status="Allocated", type="MCE", cluster_name=CLUSTER, dhcp=dhcp))

    @activity.defn
    async def allocate_upi_server(request: AllocateUpiServerRequest) -> AcquiredServer:
        calls.setdefault("allocate", []).append(request.server_name)
        return _server(vendor)

    @activity.defn
    async def release_upi_server(request: UpiServerLockRequest) -> None:
        calls.setdefault("release", []).append(request.server.id)

    @activity.defn
    async def extend_upi_server_lock(request: UpiServerLockRequest) -> None:
        calls.setdefault("extend", []).append(request.ttl_seconds)

    @activity.defn
    async def inspect_upi_node(request: UpiNodeInspectRequest) -> UpiNodeState:
        return UpiNodeState(exists=False)

    @activity.defn
    async def build_node_iso(request: NodeIsoRequest) -> NodeIso:
        calls.setdefault("iso", []).append((request.hostname, request.vlan_id, [m.mac for m in request.members]))
        return NodeIso(file_name=f"{request.hostname}.iso", url=f"http://isos/{request.hostname}.iso",
                       config_sha256="x", size_bytes=1, built=True)

    @activity.defn
    async def boot_from_virtual_media(request: VirtualMediaBootRequest) -> VirtualMediaBoot:
        calls.setdefault("boot", []).append(request.iso_url)
        return VirtualMediaBoot(system_path="/redfish/v1/Systems/1", media_path="/m", reset_type="On")

    @activity.defn
    async def await_node_joined(request: NodeJoinRequest) -> NodeJoined:
        if join_error is not None:
            raise join_error
        return NodeJoined(node=UpiNode(name=request.hostname, internal_ip="10.1.5.40", ready=True),
                          csrs_approved=["csr-a", "csr-b"])

    @activity.defn
    async def finish_upi_node(request: FinishUpiNodeRequest) -> FinishedUpiNode:
        calls.setdefault("finish", []).append(request.node_labels)
        return FinishedUpiNode(labelled=True, media_ejected=True, iso_removed=True)

    return {
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE: [find_cluster_allocation],
        upi_server_activity_queue(CLUSTER): [
            allocate_upi_server, release_upi_server, extend_upi_server_lock, inspect_upi_node, build_node_iso,
            boot_from_virtual_media, await_node_joined, finish_upi_node,
        ],
    }


async def _add(client: Client):
    handle = await client.start_workflow(
        AddServerToUpiWorkflow.run,
        AddServerToUpiRunArgs(input=AddServerToUpiInput(cluster=CLUSTER, server_name="OCP-R760-TLV-ABC",
                                                        node_labels={"node-role.kubernetes.io/storage": ""})),
        id=f"test-{uuid.uuid4()}",
        task_queue=ADD_SERVER_TO_UPI_WORKFLOW_QUEUE,
    )
    return await asyncio.wait_for(handle.result(), timeout=60)


async def test_a_server_joins_with_a_bond_on_the_clusters_vlan():
    calls: dict = {}
    async with _Harness(_add_mocks(calls)) as client:
        result = await _add(client)
    assert calls["lookup"] == ["MCE"]
    assert calls["iso"] == [("ocp-r760-tlv-abc", 105, ["aa:00:00:00:00:01", "aa:00:00:00:00:02"])]
    assert calls["boot"] == ["http://isos/ocp-r760-tlv-abc.iso"]
    assert calls["finish"] == [{"node-role.kubernetes.io/storage": ""}] and calls["extend"] == [86_400]
    assert "release" not in calls
    assert result.node.ready and result.csrs_approved == ["csr-a", "csr-b"] and not result.already_joined


async def test_an_ipmi_bmc_is_refused_and_its_lock_given_back():
    calls: dict = {}
    async with _Harness(_add_mocks(calls, vendor="CISCO")) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _add(client)
    assert _error(exc_info.value).type == "VirtualMediaUnsupportedError"
    assert calls["release"] == ["srv_1"] and "iso" not in calls


async def test_a_segment_without_dhcp_is_refused_before_anything_is_allocated():
    calls: dict = {}
    async with _Harness(_add_mocks(calls, dhcp=False)) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _add(client)
    assert _error(exc_info.value).type == "SegmentWithoutDhcpError"
    assert "allocate" not in calls


async def test_a_join_timeout_fails_the_run_and_keeps_the_lock():
    calls: dict = {}
    async with _Harness(_add_mocks(calls, join_error=NodeJoinTimeoutError("no Node"))) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _add(client)
    assert _error(exc_info.value).type == "NodeJoinTimeoutError"
    assert "release" not in calls and "finish" not in calls
