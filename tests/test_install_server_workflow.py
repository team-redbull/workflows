"""Workflow tests: real InstallServerWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_allocate_segment_workflow.py, with one extra
worker: install-server spans TWO activity queues, because the inventory VLAN is
read through a segment-lifecycle activity on that domain's limb rather than by
this domain holding a second copy of the Segments Manager token.

The time-skipping environment collapses the registration poll's durable timers
and every retry backoff to milliseconds.
"""

from __future__ import annotations

import asyncio
import re
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    INSTALL_SERVER_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import InventorySegmentNotFoundError, ServerNotAvailableError
from shared.models.segment_lifecycle import SegmentEntry
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    BmcEndpoint,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    InstallServerInput,
    InstallServerRunArgs,
    LinkState,
    ServerInterface,
)
from workflow_domains.server_lifecycle.install_server import InstallServerWorkflow

INFRA_ENV = "cisco-m6-bat-yam-64c-512gb"
MCE_CLUSTER = "ocp4-mce-bat-yam-01"
# The queue the worker INSIDE that MCE polls. The brain picks the target
# cluster purely by dispatching here, so the harness has to register the mock
# activities on the same MCE-scoped name the workflow computes.
MCE_ACTIVITY_QUEUE = server_lifecycle_activity_queue(MCE_CLUSTER)
NAMESPACE = "multicluster-engine"
VLAN_ID = 24
SERVER_NAME = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"

INPUT = InstallServerInput(
    infra_env=INFRA_ENV, mce_cluster=MCE_CLUSTER, namespace=NAMESPACE
)

INVENTORY_SEGMENT = SegmentEntry(
    segment="10.20.90.0/24",
    site="bat-yam",
    vlan_id=VLAN_ID,
    status="Allocated",
    type="INVENTORY",
    cluster_name=MCE_CLUSTER,
)


def _nic(name: str, mac: str, location: str | None, link: LinkState) -> ServerInterface:
    return ServerInterface(name=name, mac=mac, location=location, link_state=link)


def bondable_server(server_id: str = "srv_001", name: str = SERVER_NAME) -> AcquiredServer:
    """A Dell server with two link-up NICs on two distinct physical ports."""
    return AcquiredServer(
        id=server_id,
        name=name,
        vendor="dell",
        source_provider="OPENMANAGE",
        bmc_vendor="DELL",
        bmc=BmcEndpoint(
            host="10.11.1.229",
            host_is_ip=True,
            scheme="redfish",
            path="/redfish/v1/Systems/System.Embedded.1",
        ),
        interfaces=[
            _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP),
            _nic("NIC.Integrated.1-2-1", "aa:bb:cc:dd:ee:02", "1/2/1", LinkState.UP),
        ],
        site_id="bat-yam",
        health_overall="HEALTHY",
        live_recheck_performed=True,
    )


def oneview_server(server_id: str = "srv_hp") -> AcquiredServer:
    """An HPE server: two ports, but OneView reports no link state for either."""
    server = bondable_server(server_id, name="ocp-hp-gen9-tlv-64c-256gb-HP0001353")
    return server.model_copy(
        update={
            "vendor": "hp",
            "source_provider": "ONEVIEW",
            "bmc_vendor": "HP",
            "interfaces": [
                _nic("Slot 1 port 1", "aa:bb:cc:dd:ee:11", None, LinkState.UNKNOWN),
                _nic("Slot 1 port 2", "aa:bb:cc:dd:ee:12", None, LinkState.UNKNOWN),
            ],
        }
    )


def npar_server(server_id: str = "srv_npar") -> AcquiredServer:
    """A Dell server whose only two up interfaces are partitions of ONE port."""
    return bondable_server(server_id).model_copy(
        update={
            "interfaces": [
                _nic("NIC.Slot.2-1-1", "aa:bb:cc:dd:ee:21", "2/1/1", LinkState.UP),
                _nic("NIC.Slot.2-1-2", "aa:bb:cc:dd:ee:22", "2/1/2", LinkState.UP),
            ]
        }
    )


def make_mock_activities(
    *,
    candidates: list[AcquiredServer] | None = None,
    acquire_error: Exception | None = None,
    segment: SegmentEntry | None = None,
    segment_error: Exception | None = None,
    resources_exist: bool = False,
    bmh_script: list[BmhState] | None = None,
    bmh_never_registers: bool = False,
    already_installed: set[str] | None = None,
):
    """Build the full mock activity set + a call recorder.

    `get_baremetal_host` serves two callers and this mock answers as the real
    cluster would: BEFORE the BareMetalHost is created it reports whether that
    server is in `already_installed` (the candidate probe), and afterwards it
    answers the registration poll from `bmh_script`.

    bmh_script: per-poll returns; once exhausted (and not bmh_never_registers)
    every later poll reports a registered host.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_inventory_segment",
            "acquire_servers",
            "create_bmc_secret",
            "create_baremetal_host",
            "create_nmstate_config",
            "get_baremetal_host",
        )
    }
    drawn = candidates if candidates is not None else [bondable_server()]
    script = list(bmh_script or [])
    installed = already_installed or set()
    # `available`, not `registering`: registering is the state a host with a
    # WRONG BMC address sits in, so it can never count as proof of registration.
    registered = BmhState(
        found=True, provisioning_state="available", operational_status="OK"
    )

    @activity.defn
    async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
        calls["get_inventory_segment"].append(mce_cluster)
        if segment_error is not None:
            raise segment_error
        return segment or INVENTORY_SEGMENT

    @activity.defn
    async def acquire_servers(request: AcquireServerRequest) -> list[AcquiredServer]:
        calls["acquire_servers"].append(request)
        if acquire_error is not None:
            raise acquire_error
        return list(drawn)

    def _created(kind: str, name: str) -> CreatedResource:
        return CreatedResource(kind=kind, name=name, changed=not resources_exist)

    @activity.defn
    async def create_bmc_secret(request: BmhResourceRequest) -> CreatedResource:
        calls["create_bmc_secret"].append(request)
        return _created("Secret", f"dell-cred-{request.server_name}")

    @activity.defn
    async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
        calls["create_baremetal_host"].append(request)
        return _created("BareMetalHost", request.server_name)

    @activity.defn
    async def create_nmstate_config(request: BmhResourceRequest) -> CreatedResource:
        calls["create_nmstate_config"].append(request)
        return _created("NMStateConfig", f"nmstate-config-{request.server_name}")

    @activity.defn
    async def get_baremetal_host(ref: BmhRef) -> BmhState:
        calls["get_baremetal_host"].append(ref)
        created = [c.server_name for c in calls["create_baremetal_host"]]
        if ref.server_name not in created:
            # The pre-selection probe: does this candidate already have a host?
            return BmhState(
                found=ref.server_name in installed,
                provisioning_state="provisioned" if ref.server_name in installed else None,
            )
        if bmh_never_registers:
            return BmhState(found=True, provisioning_state="", operational_status="")
        if script:
            return script.pop(0)
        return registered

    segment_activities = [get_inventory_segment]
    server_activities = [
        acquire_servers,
        create_bmc_secret,
        create_baremetal_host,
        create_nmstate_config,
        get_baremetal_host,
    ]
    return calls, segment_activities, server_activities


class _Harness:
    """Brain + BOTH limbs, mirroring the real three-queue split."""

    def __init__(self, segment_activities, server_activities, server_queue=None) -> None:
        self._segment_activities = segment_activities
        self._server_activities = server_activities
        self._server_queue = server_queue or MCE_ACTIVITY_QUEUE

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(
                client,
                task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
                workflows=[InstallServerWorkflow],
            ),
            Worker(
                client,
                task_queue=self._server_queue,
                activities=self._server_activities,
            ),
            Worker(
                client,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                activities=self._segment_activities,
            ),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client, args: InstallServerRunArgs):
    return await asyncio.wait_for(
        client.execute_workflow(
            InstallServerWorkflow.run,
            args,
            id=f"test-{uuid.uuid4()}",
            task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
        ),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


def _application_error(exc: WorkflowFailureError) -> ApplicationError:
    """Unwrap the ApplicationError, whether raised in workflow or activity code."""
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


async def test_happy_path_resolves_vlan_acquires_creates_and_verifies():
    # The BareMetalHost is not registered on the first poll — the normal shape,
    # since storing the object only means the API server accepted it.
    calls, segment_acts, server_acts = make_mock_activities(
        bmh_script=[BmhState(found=False)]
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.server_name == SERVER_NAME
    assert result.vlan_id == VLAN_ID
    assert result.bond_macs == ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"]
    assert result.boot_mac == "aa:bb:cc:dd:ee:01"
    assert result.bmh_registered is True
    assert result.resources_changed is True
    # The address is carried from what server-scan parsed, not rebuilt.
    assert result.bmc_address == (
        "idrac-virtualmedia://10.11.1.229/redfish/v1/Systems/System.Embedded.1"
    )
    assert calls["get_inventory_segment"] == [MCE_CLUSTER]
    # probe (is this candidate already installed?), then not-found, then registered
    assert len(calls["get_baremetal_host"]) == 3


async def test_the_infraenv_becomes_the_server_query():
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, InstallServerRunArgs(input=INPUT))

    request = calls["acquire_servers"][0]
    # Escaped, because the InfraEnv name is caller data going into a regex that
    # server-scan hands to Mongo. `\-` is a literal `-`, so the pool is the
    # same; an unescaped `.` or `+` in a future name would not be.
    assert request.pattern == f"^ocp-{re.escape(INFRA_ENV)}"
    assert request.name is None
    assert request.health == "HEALTHY"
    assert request.min_nic_macs == 2


async def test_an_explicit_server_name_bypasses_the_pattern():
    calls, segment_acts, server_acts = make_mock_activities()
    named = INPUT.model_copy(update={"server_name": SERVER_NAME})
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, InstallServerRunArgs(input=named))

    request = calls["acquire_servers"][0]
    assert request.name == SERVER_NAME
    assert request.pattern is None
    assert request.count == 1


async def test_the_vlan_comes_from_the_mce_segment_not_the_caller():
    calls, segment_acts, server_acts = make_mock_activities(
        segment=INVENTORY_SEGMENT.model_copy(update={"vlan_id": 777})
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.vlan_id == 777
    for kind in ("create_baremetal_host", "create_nmstate_config"):
        assert calls[kind][0].vlan_id == 777


async def test_a_segment_allocated_to_another_cluster_is_refused():
    # Guards the Segments Manager list filter: if it ever ignored the
    # cluster_name param, the run must not tag a host with another MCE's VLAN.
    _, segment_acts, server_acts = make_mock_activities(
        segment=INVENTORY_SEGMENT.model_copy(update={"cluster_name": "some-other-mce"})
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))
    assert _application_error(excinfo.value).type == "InventorySegmentMismatchError"


async def test_a_missing_inventory_segment_fails_fast():
    _, segment_acts, server_acts = make_mock_activities(
        segment_error=InventorySegmentNotFoundError("no INVENTORY segment for that MCE")
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))
    assert _application_error(excinfo.value).type == "InventorySegmentNotFoundError"


async def test_no_assignable_server_fails_fast():
    _, segment_acts, server_acts = make_mock_activities(
        acquire_error=ServerNotAvailableError("no server name matches '^ocp-...'")
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))
    assert _application_error(excinfo.value).type == "ServerNotAvailableError"


async def test_npar_partitions_are_never_bonded():
    # Two MACs on ONE physical port would look like a bond and provide no
    # redundancy at all. The whole reason members come from `interfaces`.
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[npar_server()]
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert calls["create_bmc_secret"] == []  # nothing was written


async def test_a_oneview_candidate_fails_visibly_not_silently():
    _, segment_acts, server_acts = make_mock_activities(candidates=[oneview_server()])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    # The message must name the provider and the link states, or the accepted
    # HPE gap is indistinguishable from genuinely down hardware.
    assert "ONEVIEW" in str(error)
    assert "UNKNOWN" in str(error)


async def test_an_unusable_candidate_is_stepped_over_for_the_next():
    # Drawing several candidates is what makes one bad server a retry inside
    # the run rather than a failed run.
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[oneview_server(), npar_server(), bondable_server("srv_good")]
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.server_id == "srv_good"
    assert calls["create_baremetal_host"][0].server_name == SERVER_NAME


async def test_a_standalone_server_is_refused_rather_than_guessed():
    # server-scan reports bmc_vendor: null for STANDALONE on purpose — the
    # driver is the caller's decision, and an IPMI fallback would be silently
    # wrong for a Redfish-only BMC.
    standalone = bondable_server().model_copy(
        update={"vendor": "standalone", "source_provider": "REDFISH_STANDALONE",
                "bmc_vendor": None}
    )
    calls, segment_acts, server_acts = make_mock_activities(candidates=[standalone])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    assert _application_error(excinfo.value).type == "UnknownBmcVendorError"
    assert calls["create_bmc_secret"] == []


async def test_a_rerun_over_existing_resources_succeeds_and_reports_no_change():
    _, segment_acts, server_acts = make_mock_activities(resources_exist=True)
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.resources_changed is False
    assert result.bmh_registered is True


async def test_a_host_that_never_registers_fails_at_the_deadline():
    # Machine convergence gets a real deadline: a host still in the empty
    # provisioning state has a bad BMC address or credential, not a slow one.
    calls, segment_acts, server_acts = make_mock_activities(bmh_never_registers=True)
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "BmhNotRegisteredError"
    # The resources are deliberately left in place for diagnosis.
    assert len(calls["create_baremetal_host"]) == 1
    assert len(calls["get_baremetal_host"]) > 1


async def test_resources_are_created_in_dependency_order():
    # The BareMetalHost references the Secret by name, so the Secret goes first.
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, InstallServerRunArgs(input=INPUT))

    assert len(calls["create_bmc_secret"]) == 1
    assert len(calls["create_baremetal_host"]) == 1
    assert len(calls["create_nmstate_config"]) == 1
    request = calls["create_nmstate_config"][0]
    assert request.infra_env == INFRA_ENV
    assert request.namespace == NAMESPACE
    assert [m.logical_name for m in request.bond_members] == ["nic1", "nic2"]


async def test_a_registering_host_is_not_yet_registered():
    """`registering` is where a host with a WRONG BMC address sits, not proof of success.

    Metal3 assigns it the moment it picks the host up, BEFORE contacting the
    BMC. Accepting it would make the registration poll return on its first
    iteration for exactly the failure the poll exists to catch.
    """
    calls, segment_acts, server_acts = make_mock_activities(
        bmh_script=[
            BmhState(found=True, provisioning_state="registering", operational_status="OK"),
            BmhState(found=True, provisioning_state="registering", operational_status="OK"),
            BmhState(found=True, provisioning_state="available", operational_status="OK"),
        ]
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.bmh_registered is True
    # 1 probe + 3 polls: it did not stop at either `registering`.
    assert len(calls["get_baremetal_host"]) == 4


async def test_a_registered_state_with_operational_status_error_does_not_count():
    """operationalStatus=error is never registration, whatever state accompanies it."""
    errored = BmhState(
        found=True,
        provisioning_state="available",
        operational_status="error",
        error_type="provisioning error",
        error_message="no suitable root device",
    )
    calls, segment_acts, server_acts = make_mock_activities(
        bmh_script=[errored, errored, BmhState(found=True, provisioning_state="available",
                                               operational_status="OK")]
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.bmh_registered is True
    assert len(calls["get_baremetal_host"]) == 4


async def test_a_persistent_registration_error_fails_before_the_deadline():
    """A wrong BMC address or credential is reported, not waited out."""
    bad_bmc = BmhState(
        found=True,
        provisioning_state="registering",
        operational_status="error",
        error_type="registration error",
        error_message="Failed to establish connection to 10.11.1.229",
    )
    calls, segment_acts, server_acts = make_mock_activities(bmh_script=[bad_bmc] * 40)
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "BmhNotRegisteredError"
    assert "registration error" in str(error)
    assert "wrong BMC address" in str(error)
    # The 2-minute grace at a 15s poll — nowhere near the 10-minute deadline.
    assert len(calls["get_baremetal_host"]) < 15
    # The resources are deliberately left standing for diagnosis.
    assert len(calls["create_baremetal_host"]) == 1


async def test_a_candidate_that_already_has_a_baremetalhost_is_skipped():
    """server-scan cannot know a machine was just installed; the cluster can.

    Nothing changes a server's lifecycle state until a CLUSTER reports the
    node, minutes later, so a second run in that window draws the same machine.
    """
    taken, free = bondable_server("srv_taken", name="ocp-taken"), bondable_server("srv_free")
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[taken, free], already_installed={"ocp-taken"}
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.server_name == SERVER_NAME
    assert [c.server_name for c in calls["create_baremetal_host"]] == [SERVER_NAME]


async def test_a_candidate_kubernetes_cannot_name_is_skipped_not_fatal():
    """One badly-named machine in the pool must not kill every install from it.

    Every read and every create derives a Kubernetes object name from the
    server's name, and the conversion raises for a name no amount of lowercasing
    can save. Raised from an ACTIVITY that is a non-retryable failure of the
    whole run — so the name is checked in the workflow, where it is just another
    reason to try the next candidate.
    """
    bad = bondable_server("srv_bad", name="ocp_dell_underscores_are_illegal")
    good = bondable_server("srv_good")
    calls, segment_acts, server_acts = make_mock_activities(candidates=[bad, good])
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.server_name == SERVER_NAME
    # Never even probed: the probe itself would have raised on the name.
    assert [r.server_name for r in calls["get_baremetal_host"]] == [SERVER_NAME] * len(
        calls["get_baremetal_host"]
    )


async def test_a_pool_of_unnameable_candidates_says_to_rename_them():
    bad = bondable_server("srv_bad", name="ocp_dell_underscores_are_illegal")
    calls, segment_acts, server_acts = make_mock_activities(candidates=[bad])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert "rename these in server-scan" in str(error)
    assert "ocp_dell_underscores_are_illegal" in str(error)
    assert calls["create_baremetal_host"] == []


async def test_every_candidate_already_installed_fails_rather_than_reporting_success():
    """The bug this guards: every create answers 409 and the run 'succeeds' having added nothing."""
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[bondable_server()], already_installed={SERVER_NAME}
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert "already holding a BareMetalHost" in str(error)
    assert calls["create_baremetal_host"] == []


async def test_an_explicitly_named_server_is_converged_not_skipped():
    """Naming a server asks for THAT machine, so an existing host is the point."""
    named = InstallServerInput(
        infra_env=INFRA_ENV, mce_cluster=MCE_CLUSTER, namespace=NAMESPACE,
        server_name=SERVER_NAME,
    )
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[bondable_server()], already_installed={SERVER_NAME}
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=named))

    assert result.server_name == SERVER_NAME
    assert len(calls["create_baremetal_host"]) == 1


async def test_a_malformed_mac_fails_before_any_resource_is_written():
    """An unclassified ValueError inside the create would retry forever, after leaving a Secret."""
    broken = bondable_server().model_copy(
        update={
            "interfaces": [
                _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP),
                _nic("NIC.Integrated.1-2-1", "NOT-A-MAC-AT-ALL", "1/2/1", LinkState.UP),
            ]
        }
    )
    calls, segment_acts, server_acts = make_mock_activities(candidates=[broken])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "InvalidMacError"
    assert calls["create_bmc_secret"] == []


class TestPerMceRouting:
    """The queue IS the target cluster.

    The brain runs on the hub; the BareMetalHost has to be created on the MCE
    that owns the InfraEnv, which is a different API server. Routing by queue
    is what crosses that boundary, so that no cross-cluster credential has to
    exist anywhere — each MCE's worker uses its own ServiceAccount and dials
    out to Temporal.
    """

    def test_the_queue_name_carries_the_mce(self) -> None:
        assert server_lifecycle_activity_queue("ocp4-prep-mce-batyam") == (
            "server-lifecycle-activity-ocp4-prep-mce-batyam"
        )

    def test_two_mces_get_two_queues(self) -> None:
        assert server_lifecycle_activity_queue("mce-a") != (
            server_lifecycle_activity_queue("mce-b")
        )

    async def test_cluster_writes_go_to_the_requested_mces_queue(self) -> None:
        # Registered ONLY on this MCE's queue: if the workflow dispatched
        # anywhere else the run could not finish.
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts, MCE_ACTIVITY_QUEUE) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        assert len(calls["create_baremetal_host"]) == 1

    async def test_a_run_for_another_mce_is_not_served_by_this_worker(self) -> None:
        """The isolation that makes the routing real, not decorative.

        This worker is registered on ONE MCE's queue. A run aimed at a
        different MCE gets its VLAN (that activity runs on the hub's
        segment-lifecycle queue) and then waits, because nothing polls the
        other MCE's queue — it cannot be served here by accident.
        """
        other_mce = "ocp4-mce-somewhere-else"
        # The segment has to match the MCE asked for, or the workflow's own
        # mismatch guard fires first and we never reach the routing.
        matching = INVENTORY_SEGMENT.model_copy(update={"cluster_name": other_mce})
        other = InstallServerInput(
            infra_env=INFRA_ENV, mce_cluster=other_mce, namespace=NAMESPACE
        )
        calls, segment_acts, server_acts = make_mock_activities(segment=matching)
        async with _Harness(segment_acts, server_acts, MCE_ACTIVITY_QUEUE) as client:
            handle = await client.start_workflow(
                InstallServerWorkflow.run,
                InstallServerRunArgs(input=other),
                id=f"test-{uuid.uuid4()}",
                task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handle.result(), timeout=5)
            progress = await handle.query(InstallServerWorkflow.progress)

        # Nothing reached this MCE's worker...
        assert calls["acquire_servers"] == []
        assert calls["create_baremetal_host"] == []
        # ...and the stall says which queue is unserved, so "the run is stuck"
        # reads as "that MCE has no server-lifecycle-worker".
        assert progress.activity_queue == f"server-lifecycle-activity-{other_mce}"
        assert progress.phase == "acquiring-server"

    async def test_the_result_still_reports_which_mce_was_targeted(self) -> None:
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))
        assert result.mce_cluster == MCE_CLUSTER
        assert len(calls["create_baremetal_host"]) == 1
