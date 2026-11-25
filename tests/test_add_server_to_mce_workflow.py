"""Workflow tests: real AddServerToMceWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_allocate_segment_workflow.py, with one extra
worker: add-server-to-mce spans TWO activity queues, because the inventory VLAN is
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
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from shared.consts import (
    ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
    REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.workflow_ids import remove_server_from_mce_workflow_id
from shared.exceptions import (
    BmhConflictError,
    InfraEnvNotFoundError,
    InvalidMacError,
    InventorySegmentNotFoundError,
    PxeMapAuthError,
    PxeSiteNotConfiguredError,
    ServerLooksInstalledError,
    ServerNotAvailableError,
    ServerReservedError,
    ServerScanAuthError,
)
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    AddServerToMceInput,
    AddServerToMceRunArgs,
    AgentRef,
    AgentState,
    AllocateServerRequest,
    BmcEndpoint,
    BmcSecretRef,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    HostDeleteRequest,
    HostDeletion,
    InfraEnvBootRequest,
    InfraEnvBootState,
    InfraEnvRef,
    InfraEnvState,
    InstallCandidateRecord,
    LinkState,
    NmStateConfigReplacement,
    NmStateConfigReplaceRequest,
    PxeBootRegistration,
    PxeBootRequest,
    PxeMapRef,
    PxeUnregistration,
    ReleaseServerRequest,
    RemoveServerFromMceResult,
    RemoveServerFromMceRunArgs,
    ReserveServerRequest,
    ServerInterface,
    ServerLockRecord,
    ServerMaintenance,
    ServerMaintenanceRequest,
    ServerMembership,
    ServerMembershipRequest,
    ServerReservation,
    mac_is_valid,
)
from workflow_domains.server_lifecycle import add_server_to_mce
from workflow_domains.server_lifecycle.add_server_to_mce import AddServerToMceWorkflow, InstallServerWorkflow

INFRA_ENV = "cisco-m6-bat-yam-64c-512gb"
MCE_CLUSTER = "ocp4-mce-bat-yam-01"
# The queue the worker INSIDE that MCE polls. The brain picks the target
# cluster purely by dispatching here, so the harness has to register the mock
# activities on the same MCE-scoped name the workflow computes.
MCE_ACTIVITY_QUEUE = server_lifecycle_activity_queue(MCE_CLUSTER)
NAMESPACE = "multicluster-engine"
VLAN_ID = 24
SERVER_NAME = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"

INPUT = AddServerToMceInput(
    infra_env=INFRA_ENV, mce_cluster=MCE_CLUSTER, namespace=NAMESPACE
)
# The InfraEnv's spec.agentLabels, as the naming convention writes them: a pool
# draw asks server-scan for `^ocp-c200-mtl-five-`.
AGENT_LABELS = {
    "redbull.io/flavor": "c200",
    "redbull.io/size": "mtl",
    "redbull.io/site": "five",
    "topology.kubernetes.io/zone": "az1",
}

# One inventory segment per MCE, found by cluster name — no BMC-protocol class.
INVENTORY_SEGMENT = SegmentEntry(
    segment="10.20.90.0/24",
    site="bat-yam",
    vlan_id=VLAN_ID,
    status="Allocated",
    type=SegmentType.INVENTORY.value,
    cluster_name=MCE_CLUSTER,
)


def _nic(name: str, mac: str, location: str | None, link: LinkState) -> ServerInterface:
    return ServerInterface(name=name, mac=mac, location=location, link_state=link)


def bondable_server(
    server_id: str = "srv_001",
    name: str = SERVER_NAME,
    mac_prefix: str = "aa:bb:cc:dd:ee",
) -> AcquiredServer:
    """A Dell server with two link-up NICs on two distinct physical ports.

    `mac_prefix` exists so two servers in one pool can have DIFFERENT MACs, as
    real ones do. It matters because an Agent is matched to its host by MAC:
    with every fixture server sharing one pair, a lookup for the second host
    resolves to the first and the two become indistinguishable.
    """
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
            _nic("NIC.Integrated.1-1-1", f"{mac_prefix}:01", "1/1/1", LinkState.UP),
            _nic("NIC.Integrated.1-2-1", f"{mac_prefix}:02", "1/2/1", LinkState.UP),
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


def ucs_server(
    server_id: str = "srv_ucs",
    name: str = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001",
    mac_prefix: str = "00:25:b5:00:00",
) -> AcquiredServer:
    """A UCS-managed Cisco blade: driven over IPMI, so it boots from the network."""
    return bondable_server(server_id, name=name, mac_prefix=mac_prefix).model_copy(
        update={
            "vendor": "cisco",
            "source_provider": "UCS_CENTRAL",
            "bmc_vendor": "CISCO",
            "bmc": BmcEndpoint(host="10.11.2.10", host_is_ip=True),
        }
    )


def standalone_server(server_id: str = "srv_standalone") -> AcquiredServer:
    """A STANDALONE Redfish machine: server-scan names no BMC driver for it (bmc_vendor null)."""
    return bondable_server(server_id, name="ocp-standalone-0001").model_copy(
        update={"vendor": "standalone", "source_provider": "REDFISH_STANDALONE", "bmc_vendor": None}
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
    agent_never_for: set[str] | None = None,
    agent_after_polls: int = 0,
    teardown_error: Exception | None = None,
    bmh_stays_deleting: set[str] | None = None,
    secret_outlives: bool = False,
    reserved_elsewhere: set[str] | None = None,
    gone_from_inventory: set[str] | None = None,
    lock_lapses_for: set[str] | None = None,
    reserve_error: Exception | None = None,
    pxe_sites: set[str] | None = None,
    pxe_error: Exception | None = None,
    infra_env_missing: bool = False,
    agent_labels: dict[str, str] | None = None,
    infra_env_predates_labels: bool = False,
    image_after_polls: int = 0,
    image_never_updates: bool = False,
    boot_superseded: bool = False,
    nmstate_error: Exception | None = None,
    record: InstallCandidateRecord | None = None,
    nmstate_differs: dict[str, list[str]] | None = None,
    in_use: dict[str, str] | None = None,
    nmstate_error_once: bool = False,
    scan_hides_locked: bool = False,
    scan_states: list[str | None] | None = None,
    scan_mce: str | None = None,
    scan_error: Exception | None = None,
    acquire_gate: asyncio.Event | None = None,
    reserve_gate: asyncio.Event | None = None,
    probe_gate: asyncio.Event | None = None,
    maintenance_error: Exception | None = None,
    maintenance_gone: bool = False,
):
    """Build the full mock activity set + a call recorder.

    `get_baremetal_host` serves two callers and this mock answers as the real
    cluster would: BEFORE the BareMetalHost is created it reports whether that
    server is in `already_installed` (the candidate probe), and afterwards it
    answers the ONE diagnostic read the workflow takes when an Agent never
    appeared — from `bmh_script` when given.

    `agent_never_for` names the servers that never register an Agent, which is
    what drives a rollback; `agent_after_polls` delays the Agent by that many
    polls for the servers that do. A rollback's delete is let go by metal3 at
    once, except for the servers in `bmh_stays_deleting`, which it never lets go
    (a BMC it cannot reach); `teardown_error` is raised by the delete, and
    `secret_outlives` makes the BMC Secret outlive its host. Both are per-SERVER rather than global so a
    pool can be set up to fail its first candidate and install its second, which
    is the case the retry loop exists for.

    The install lock behaves as server-scan's does: a claim by the same workflow
    extends, `reserved_elsewhere` names servers another run holds, and
    `lock_lapses_for` names servers whose lock another run takes between the
    first claim and the renewal before the Agent wait.

    `publish_infra_env_boot` answers as assisted-service + the PXE VM would:
    unpublished for the first `image_after_polls` polls (the image is still
    being regenerated), or forever with `image_never_updates`; `boot_superseded`
    makes the PXE VM report a newer image already published.

    `nmstate_error` is raised by `create_nmstate_config` — the incident in which
    the NMStateConfig already existed with other content (BmhConflictError) —
    every time, or only the first time with `nmstate_error_once`.

    `check_infra_env` reports `agent_labels` (default AGENT_LABELS) as the
    InfraEnv's spec.agentLabels; `infra_env_predates_labels` answers as a limb
    from before the field, whose payload has no `agent_labels` key at all.

    `scan_hides_locked` makes `acquire_servers` answer as server-scan's does
    for a NAMED machine under a live install lock: 404, whoever holds the lock
    — the run's own workflow id included (its `server_still_qualifies`).

    `record` is what `lookup_install_candidate` reads from server-scan for a
    named server `/servers/available` refused; None = server-scan does not
    know the name.

    `replace_conflicting_nmstate_config` answers per server: `nmstate_differs`
    names servers whose existing NMStateConfig differs (and is replaced, with
    those differences), `in_use` servers an OLDER limb refuses because a
    BareMetalHost or Agent shows them in use (the current limb never does).
    Anyone else has no NMStateConfig yet.

    `lookup_server_membership` is what server-scan reports after a success:
    `scan_states` in turn, one per read, the last one repeated (None = server-
    scan does not know the name; default: INSTALLED_TO_INVENTORY at once),
    `scan_mce` the MCE its agents job names, `scan_error` raised every read.

    `acquire_gate` / `reserve_gate` / `probe_gate` hold the allocation (and
    `acquire_servers`) / `reserve_server` / the pre-selection BareMetalHost
    probe open until the test sets them: a call in flight when the run is
    cancelled, which then lands anyway (a cancel stops waiting for it, not it).

    `allocate_server` answers as server-scan's POST /servers/available does: the
    first server of `candidates` (or the one named) not under a live lock —
    `reserved_elsewhere`, or one this mock already locked, the caller's own
    included — nor gone, nor shown installed by a membership read since, nor
    put in maintenance; it locks that one for the caller's workflow id. None:
    ServerNotAvailableError with each server's reason.

    `put_server_in_maintenance` puts the server in maintenance, which every
    later allocation skips; `maintenance_error` is raised instead, and
    `maintenance_gone` answers as for a server server-scan no longer knows.

    The BareMetalHost and NMStateConfig creates refuse a MAC they cannot use,
    as the limb's builders do (InvalidMacError), before writing.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_inventory_segment",
            "acquire_servers",
            "allocate_server",
            "create_bmc_secret",
            "create_baremetal_host",
            "create_nmstate_config",
            "get_baremetal_host",
            "find_agent_for_host",
            "delete_host_resources",
            "delete_bmc_secret",
            "reserve_server",
            "release_server",
            "register_pxe_boot",
            "unregister_pxe_boot",
            "publish_infra_env_boot",
            "check_infra_env",
            "lookup_install_candidate",
            "replace_conflicting_nmstate_config",
            "lookup_server_membership",
            "put_server_in_maintenance",
            # Activity names in call order, for the ones whose ORDER matters.
            "order",
        )
    }
    configured_pxe_sites = {"bat-yam"} if pxe_sites is None else pxe_sites
    nmstate_failed_once: list[bool] = []
    drawn = candidates if candidates is not None else [bondable_server()]
    script = list(bmh_script or [])
    installed = already_installed or set()
    no_agent = agent_never_for or set()
    torn_down: set[str] = set()
    stuck = bmh_stays_deleting or set()
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
        calls["order"].append("acquire_servers")
        if acquire_gate is not None:
            await acquire_gate.wait()
        if acquire_error is not None:
            raise acquire_error
        if scan_hides_locked and request.name is not None:
            locked = [s for s in drawn if s.name == request.name and s.id in locks]
            if locked:
                raise ServerNotAvailableError(
                    f"server-scan has no assignable server for this request: "
                    f"'{request.name}' is no longer available: health=HEALTHY, "
                    "openshift=AVAILABLE, maintenance=False, reachable=True"
                )
        return list(drawn)

    @activity.defn
    async def allocate_server(request: AllocateServerRequest) -> AcquiredServer:
        calls["allocate_server"].append(request)
        calls["order"].append("allocate_server")
        if acquire_gate is not None:
            await acquire_gate.wait()
        if acquire_error is not None:
            raise acquire_error
        # server-scan's POST /servers/available (its ADR-0063): one candidate at
        # a time, in its order, never one locked — whoever holds it, this run's
        # own workflow id included — nor one gone from the inventory; the one
        # returned is locked for the caller, as reserve_server's lock.
        pool = [s for s in drawn if s.name == request.name] if request.name else drawn
        reasons = []
        for server in pool:
            if server.name in gone:
                reasons.append(f"{server.name}: GONE_FROM_MANAGER")
                continue
            if server.name in claimed_in_scan:
                reasons.append(f"{server.name}: CLAIMED")
                continue
            if server.name in in_maintenance:
                reasons.append(f"{server.name}: MAINTENANCE")
                continue
            if server.name in held_elsewhere or server.id in locks:
                reasons.append(f"{server.name}: RESERVED")
                continue
            locks[server.id] = request.workflow_id
            return server
        asked = f"name={request.name!r}" if request.name else f"pattern={request.pattern!r}"
        raise ServerNotAvailableError(
            f"server-scan could not allocate a server ({asked}): "
            + ("; ".join(reasons) or "no server name matches")
        )

    @activity.defn
    async def lookup_install_candidate(
        request: ServerMembershipRequest,
    ) -> InstallCandidateRecord:
        calls["lookup_install_candidate"].append(request)
        calls["order"].append("lookup_install_candidate")
        return record if record is not None else InstallCandidateRecord()

    @activity.defn
    async def replace_conflicting_nmstate_config(
        request: NmStateConfigReplaceRequest,
    ) -> NmStateConfigReplacement:
        calls["replace_conflicting_nmstate_config"].append(request)
        calls["order"].append("replace_conflicting_nmstate_config")
        server = request.resource.server_name
        if server in (in_use or {}):
            raise ServerLooksInstalledError(
                f"NMStateConfig {server} differs from this install, and the "
                f"server looks installed or in progress: {(in_use or {})[server]}. Nothing "
                "was changed"
            )
        if server in (nmstate_differs or {}):
            return NmStateConfigReplacement(
                found=True, replaced=True, differences=(nmstate_differs or {})[server]
            )
        return NmStateConfigReplacement(found=False)

    def _created(kind: str, name: str) -> CreatedResource:
        return CreatedResource(kind=kind, name=name, changed=not resources_exist)

    @activity.defn
    async def check_infra_env(ref: InfraEnvRef) -> InfraEnvState:
        calls["check_infra_env"].append(ref)
        calls["order"].append("check_infra_env")
        if infra_env_missing:
            raise InfraEnvNotFoundError(
                f"InfraEnv {ref.name} does not exist in namespace {ref.namespace}"
            )
        if infra_env_predates_labels:
            # An older limb's payload: the model it sent had no such field.
            return {"name": ref.name, "namespace": ref.namespace, "cluster_ref": None}  # type: ignore[return-value]
        return InfraEnvState(
            name=ref.name,
            namespace=ref.namespace,
            agent_labels=AGENT_LABELS if agent_labels is None else agent_labels,
        )

    @activity.defn
    async def unregister_pxe_boot(ref: PxeMapRef) -> PxeUnregistration:
        calls["unregister_pxe_boot"].append(ref)
        calls["order"].append("unregister_pxe_boot")
        return PxeUnregistration(removed=list(ref.macs))

    @activity.defn
    async def register_pxe_boot(request: PxeBootRequest) -> PxeBootRegistration:
        calls["register_pxe_boot"].append(request)
        calls["order"].append("register_pxe_boot")
        if pxe_error is not None:
            raise pxe_error
        if request.site not in configured_pxe_sites:
            raise PxeSiteNotConfiguredError(
                f"no PXE map host is configured for site {request.site!r}"
            )
        return PxeBootRegistration(
            site=request.site,
            pxe_map_url=f"http://pxe.{request.site}.test:8080",
            macs=request.macs,
        )

    @activity.defn
    async def publish_infra_env_boot(request: InfraEnvBootRequest) -> InfraEnvBootState:
        calls["publish_infra_env_boot"].append(request)
        calls["order"].append("publish_infra_env_boot")
        polls = sum(
            1
            for c in calls["publish_infra_env_boot"]
            if c.server_name == request.server_name
        )
        if image_never_updates or polls <= image_after_polls:
            return InfraEnvBootState(
                published=False,
                detail="InfraEnv condition ImageCreated is not True: nmstate invalid",
            )
        return InfraEnvBootState(
            published=True,
            image_created_at="2026-10-19T10:00:05Z",
            superseded=boot_superseded,
        )

    @activity.defn
    async def create_bmc_secret(request: BmhResourceRequest) -> CreatedResource:
        calls["create_bmc_secret"].append(request)
        calls["order"].append("create_bmc_secret")
        return _created("Secret", f"dell-cred-{request.server_name}")

    def _limb_refuses_bad_macs(request: BmhResourceRequest) -> None:
        # bmh_resources._validate_macs: before the object is built, so nothing written.
        bad = [m.mac for m in request.bond_members if not mac_is_valid(m.mac)]
        if bad:
            raise InvalidMacError(f"{request.server_name}: invalid MAC(s) {bad}")

    @activity.defn
    async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
        calls["create_baremetal_host"].append(request)
        calls["order"].append("create_baremetal_host")
        _limb_refuses_bad_macs(request)
        return _created("BareMetalHost", request.server_name)

    @activity.defn
    async def create_nmstate_config(request: BmhResourceRequest) -> CreatedResource:
        calls["create_nmstate_config"].append(request)
        calls["order"].append("create_nmstate_config")
        _limb_refuses_bad_macs(request)
        if nmstate_error is not None and not nmstate_failed_once:
            if nmstate_error_once:
                nmstate_failed_once.append(True)
            raise nmstate_error
        return _created("NMStateConfig", request.server_name)

    @activity.defn
    async def get_baremetal_host(ref: BmhRef) -> BmhState:
        calls["get_baremetal_host"].append(ref)
        if ref.server_name in torn_down:
            # Rolled back: the machine is back in the inventory and the host is
            # gone, which is what lets the same pool be drawn again later.
            return BmhState(found=False)
        if ref.server_name in stuck and any(
            d.server_name == ref.server_name for d in calls["delete_host_resources"]
        ):
            # Deleted, but metal3 cannot power it off: its finalizer holds it.
            return BmhState(
                found=True, provisioning_state="powering off before delete",
                operational_status="error", error_type="power management error",
                error_message="Failed to power off: BMC unreachable",
            )
        created = [c.server_name for c in calls["create_baremetal_host"]]
        if ref.server_name not in created:
            # The pre-selection probe: does this candidate already have a host?
            if probe_gate is not None:
                await probe_gate.wait()
            return BmhState(
                found=ref.server_name in installed,
                provisioning_state="provisioned" if ref.server_name in installed else None,
            )
        if bmh_never_registers:
            return BmhState(found=True, provisioning_state="", operational_status="")
        if script:
            return script.pop(0)
        return registered

    def _owner_of(macs: list[str]) -> str | None:
        """Which server these bond MACs belong to.

        The real activity matches an Agent to a host the same way — on MAC —
        because BMAC names an Agent after the host's inventory UUID and the
        Agent carries no reference back to the BareMetalHost.
        """
        wanted = set(macs)
        for request in calls["create_baremetal_host"]:
            if {member.mac for member in request.bond_members} & wanted:
                return request.server_name
        return None

    @activity.defn
    async def find_agent_for_host(ref: AgentRef) -> AgentState:
        calls["find_agent_for_host"].append(ref)
        owner = _owner_of(ref.macs)
        if owner in no_agent:
            return AgentState(found=False)
        polls = sum(
            1 for c in calls["find_agent_for_host"] if set(c.macs) == set(ref.macs)
        )
        if polls <= agent_after_polls:
            return AgentState(found=False)
        return AgentState(found=True, name=f"agent-{owner}", approved=False)

    @activity.defn
    async def delete_host_resources(request: HostDeleteRequest) -> HostDeletion:
        calls["delete_host_resources"].append(request)
        calls["order"].append("delete_host_resources")
        if teardown_error is not None:
            raise teardown_error
        if request.server_name not in stuck:
            # metal3 deprovisions it and lets the BareMetalHost go.
            torn_down.add(request.server_name)
        return HostDeletion(
            removed=[
                f"NMStateConfig/{request.server_name}",
                f"BareMetalHost/{request.server_name}",
            ],
            bmc_secret=f"cisco-ucs-{request.server_name}",
        )

    @activity.defn
    async def delete_bmc_secret(ref: BmcSecretRef) -> bool:
        calls["delete_bmc_secret"].append(ref)
        return secret_outlives

    held_elsewhere = set(reserved_elsewhere or ())
    gone = gone_from_inventory or set()
    lapses = lock_lapses_for or set()
    locks: dict[str, str] = {}

    @activity.defn
    async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
        calls["reserve_server"].append(request)
        if reserve_gate is not None:
            await reserve_gate.wait()
        if reserve_error is not None:
            raise reserve_error
        if request.server_name in gone:
            raise ServerNotAvailableError(f"{request.server_name} is not in inventory")
        if request.server_name in lapses and request.server_id in locks:
            held_elsewhere.add(request.server_name)
            del locks[request.server_id]
        if request.server_name in held_elsewhere:
            raise ServerReservedError(
                f"{request.server_name} is reserved by 'add-server-to-mce' for MCE "
                "'ocp4-mce-other'"
            )
        # server-scan's rule (reservation_service.py there): a live lock is
        # refused to any OTHER workflow id; the same id re-claiming extends it.
        # No await between this check and the write below, so - like the
        # store's compare-and-set - two concurrent claims cannot both pass it.
        held_by = locks.get(request.server_id)
        if held_by is not None and held_by != request.workflow_id:
            raise ServerReservedError(
                f"{request.server_name} is reserved by 'add-server-to-mce' (run {held_by!r})"
            )
        locks[request.server_id] = request.workflow_id
        return ServerReservation(
            server_id=request.server_id,
            held=True,
            expires_at=f"in {request.ttl_seconds}s",
        )

    @activity.defn
    async def release_server(request: ReleaseServerRequest) -> ServerReservation:
        calls["release_server"].append(request)
        calls["order"].append("release_server")
        if locks.get(request.server_id) == request.workflow_id:
            del locks[request.server_id]
            return ServerReservation(server_id=request.server_id, held=False)
        return ServerReservation(
            server_id=request.server_id, held=False, detail="not ours"
        )

    in_maintenance: set[str] = set()

    @activity.defn
    async def put_server_in_maintenance(request: ServerMaintenanceRequest) -> ServerMaintenance:
        calls["put_server_in_maintenance"].append(request)
        calls["order"].append("put_server_in_maintenance")
        if maintenance_error is not None:
            raise maintenance_error
        if maintenance_gone:
            return ServerMaintenance(
                server_id=request.server_id,
                enabled=False,
                detail="the server is no longer in server-scan's inventory",
            )
        in_maintenance.add(request.server_name)
        return ServerMaintenance(server_id=request.server_id, enabled=True)

    states = list(scan_states) if scan_states is not None else ["INSTALLED_TO_INVENTORY"]
    claimed_in_scan: set[str] = set()

    @activity.defn
    async def lookup_server_membership(request: ServerMembershipRequest) -> ServerMembership:
        calls["lookup_server_membership"].append(request)
        calls["order"].append("lookup_server_membership")
        if scan_error is not None:
            raise scan_error
        state = states.pop(0) if len(states) > 1 else states[0]
        if state is None:
            return ServerMembership(found=False)
        if state in ("INSTALLED_TO_INVENTORY", "INSTALLED"):
            # server-scan knows the machine is taken now: it never allocates it again.
            claimed_in_scan.add(request.server_name)
        return ServerMembership(
            found=True, server_id="srv", server_name=request.server_name,
            lifecycle_state=state, mce_name=scan_mce if state == "INSTALLED_TO_INVENTORY" else None,
        )

    segment_activities = [get_inventory_segment]
    server_activities = [
        lookup_server_membership,
        acquire_servers,
        allocate_server,
        lookup_install_candidate,
        replace_conflicting_nmstate_config,
        create_bmc_secret,
        create_baremetal_host,
        create_nmstate_config,
        get_baremetal_host,
        find_agent_for_host,
        delete_host_resources,
        delete_bmc_secret,
        reserve_server,
        release_server,
        put_server_in_maintenance,
        register_pxe_boot,
        unregister_pxe_boot,
        publish_infra_env_boot,
        check_infra_env,
    ]
    return calls, segment_activities, server_activities


@workflow.defn(name="RemoveServerFromMceWorkflow", sandboxed=False)
class _FakeRemoveServerFromMce:
    """remove-server-from-mce as a cancelled install's rollback meets it.

    It takes the host out (found, powered off) and releases no lock, as since
    2026-11-17 — except a server whose name says `nowhere`, which it finds
    nothing of on the MCE; `oldchild`, which releases the lock as a child from
    before did; and `pxefail`, whose PXE cleanup fails.
    """

    @workflow.run
    async def run(self, run_args: RemoveServerFromMceRunArgs) -> RemoveServerFromMceResult:
        request = run_args.input
        found = "nowhere" not in request.server_name
        return RemoveServerFromMceResult(
            server_name=request.server_name, namespace=request.namespace,
            mce_cluster=request.mce_cluster, found=found, powered_off=found,
            lock_released=found and "oldchild" in request.server_name,
            pxe_failed=["bat-yam: PxeMapAuthError: check PXE_MAP_TOKEN"]
            if "pxefail" in request.server_name else [],
        )


class _Harness:
    """Brain + BOTH limbs, mirroring the real three-queue split (plus a fake
    remove-server-from-mce child, for a cancelled install's rollback)."""

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
                task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
                workflows=[AddServerToMceWorkflow],
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
            Worker(
                client,
                task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
                workflows=[_FakeRemoveServerFromMce],
            ),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client, args: AddServerToMceRunArgs):
    return await asyncio.wait_for(
        client.execute_workflow(
            AddServerToMceWorkflow.run,
            args,
            id=f"test-{uuid.uuid4()}",
            task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
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


async def test_happy_path_resolves_vlan_acquires_creates_and_awaits_the_agent():
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert result.server_name == SERVER_NAME
    assert result.vlan_id == VLAN_ID
    assert result.bond_macs == ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"]
    assert result.boot_mac == "aa:bb:cc:dd:ee:01"
    # Implied by the Agent: it cannot exist unless Ironic registered the host
    # and drove it through a boot.
    assert result.bmh_registered is True
    assert result.resources_changed is True
    assert result.attempts == 1
    assert result.agent_name == f"agent-{SERVER_NAME}"
    # The address is carried from what server-scan parsed, not rebuilt.
    assert result.bmc_address == (
        "idrac-virtualmedia://10.11.1.229/redfish/v1/Systems/System.Embedded.1"
    )
    assert calls["get_inventory_segment"] == [MCE_CLUSTER]
    # ONLY the candidate probe. The host is never polled for success — an Agent
    # is the proof — and the one diagnostic read happens only on a failure.
    assert len(calls["get_baremetal_host"]) == 1
    assert len(calls["find_agent_for_host"]) == 1


@pytest.mark.parametrize(
    ("labels", "prefix"),
    [
        (AGENT_LABELS, "c200-mtl-five"),
        ({**AGENT_LABELS, "redbull.io/flavor": "c200n"}, "c200n-mtl-five"),  # a variant
        ({**AGENT_LABELS, "redbull.io/size": "kv32"}, "c200-kv32-five"),  # a VM pool
    ],
)
def test_the_pool_is_the_infraenvs_flavor_size_and_site_labels(labels, prefix):
    assert add_server_to_mce.agent_label_problems(labels) == []
    assert add_server_to_mce.pool_prefix_from_labels(labels) == prefix
    pool = re.compile(f"^ocp-{re.escape(prefix)}-")
    assert pool.match(f"ocp-{prefix}-fch2145v1ab")
    assert not pool.match(f"ocp-{prefix}2-fch2145v1ab")


@pytest.mark.parametrize(
    ("labels", "problem"),
    [
        ({}, "redbull.io/flavor is missing"),
        ({k: v for k, v in AGENT_LABELS.items() if k != "redbull.io/site"}, "redbull.io/site is missing"),
        ({**AGENT_LABELS, "redbull.io/size": ""}, "redbull.io/size is missing"),
        # Each is ONE word of the server name: a `-` would span two, a `.`
        # is no name word at all, and server names are lowercase.
        ({**AGENT_LABELS, "redbull.io/site": "bat-yam"}, "redbull.io/site='bat-yam' is not one"),
        ({**AGENT_LABELS, "redbull.io/flavor": "c200.x"}, "redbull.io/flavor='c200.x' is not one"),
        ({**AGENT_LABELS, "redbull.io/flavor": "C200"}, "redbull.io/flavor='C200' is not one"),
    ],
)
def test_labels_that_cannot_name_a_server_are_each_named(labels, problem):
    problems = add_server_to_mce.agent_label_problems(labels)
    assert any(line.startswith(problem) for line in problems), problems


def test_the_zone_label_is_not_part_of_the_pool():
    # A server's zone is where it is cabled (server-scan's), never in its name.
    labels = {k: v for k, v in AGENT_LABELS.items() if k != "topology.kubernetes.io/zone"}
    assert add_server_to_mce.agent_label_problems(labels) == []
    assert add_server_to_mce.pool_prefix_from_labels(labels) == "c200-mtl-five"


@pytest.mark.parametrize(
    ("infra_env", "prefix"),
    [
        ("c200-mtl-haifa-az1", "c200-mtl-haifa"),
        ("cisco-m6-bat-yam-64c-512gb", "cisco-m6-bat"),
        ("dell-r660-tlv", "dell-r660-tlv"),
        ("dell-r660", "dell-r660"),  # fewer than three words: the whole name
    ],
)
def test_without_labels_the_pool_is_the_infraenvs_first_three_words(infra_env, prefix):
    # A limb predating agent_labels, and every run recorded before.
    assert add_server_to_mce.pool_prefix_from_name(infra_env) == prefix


async def test_the_infraenv_labels_become_the_server_query():
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute_as(client, AddServerToMceRunArgs(input=INPUT), "add-server-to-mce-the-query")

    # ONE server, asked for once — the candidate list (acquire_servers) is
    # only for runs recorded before the allocation.
    [request] = calls["allocate_server"]
    assert calls["acquire_servers"] == []
    # `ocp-<flavor>-<size>-<site>-` from the InfraEnv's agentLabels (the naming
    # convention, 2026-11-22), escaped: label values are caller data going
    # into a regex server-scan hands to Mongo. The `-` after the site is
    # load-bearing (the operator, 2026-11-17).
    assert request.pattern == f"^ocp-{re.escape('c200-mtl-five')}-"
    pool = re.compile(request.pattern)
    assert pool.match("ocp-c200-mtl-five-fch2145v1ab")
    assert pool.match("ocp-c200-mtl-five-7xk2m33")
    assert not pool.match("ocp-c200-mtl-five2-fch2145v1ab")  # another site
    assert not pool.match("ocp-c200n-mtl-five-fch2145v1ab")  # another variant
    assert not pool.match("ocp-c200-kv32-five-02a1b2c3d4e5")  # another size
    assert not pool.match(f"ocp-{INFRA_ENV}-FCH0001")  # the InfraEnv's name is not it
    assert request.name is None
    assert (request.max_health, request.min_nic_macs) == ("HEALTHY", 2)
    # The lock server-scan takes as it allocates IS this run's: its holder and
    # id, for the install's TTL — what the renewal and the release then name.
    assert (request.holder, request.workflow_id) == ("add-server-to-mce", "add-server-to-mce-the-query")
    assert (request.mce_cluster, request.infra_env, request.namespace) == (MCE_CLUSTER, INFRA_ENV, NAMESPACE)
    assert request.ttl_seconds == 2 * 3600


async def test_an_explicit_server_name_bypasses_the_pattern():
    calls, segment_acts, server_acts = make_mock_activities()
    named = INPUT.model_copy(update={"server_name": SERVER_NAME})
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, AddServerToMceRunArgs(input=named))

    [request] = calls["allocate_server"]
    assert request.name == SERVER_NAME
    assert request.pattern is None


class TestThePoolComesFromTheInfraEnvsAgentLabels:
    async def test_an_infraenv_without_them_fails_the_draw_before_anything_is_allocated(
        self,
    ) -> None:
        calls, segment_acts, server_acts = make_mock_activities(
            agent_labels={"topology.kubernetes.io/zone": "az1", "redbull.io/site": "Five"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "InfraEnvAgentLabelsError"
        # Every reason at once, so one fix of the InfraEnv is enough.
        for part in (
            f"InfraEnv {NAMESPACE}/{INFRA_ENV}",
            "redbull.io/flavor is missing",
            "redbull.io/size is missing",
            "redbull.io/site='Five' is not one",
        ):
            assert part in error.message
        assert calls["allocate_server"] == [] and calls["acquire_servers"] == []

    async def test_a_named_install_needs_none_of_them(self) -> None:
        calls, segment_acts, server_acts = make_mock_activities(agent_labels={})
        named = INPUT.model_copy(update={"server_name": SERVER_NAME})
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=named))

        assert result.server_name == SERVER_NAME
        [request] = calls["allocate_server"]
        assert (request.name, request.pattern) == (SERVER_NAME, None)

    async def test_a_limb_predating_them_draws_by_the_infraenvs_first_three_words(
        self,
    ) -> None:
        # The brain ships before the limb (CLAUDE.md §5): an older limb's answer
        # has no `agent_labels` key, so the run draws as it did before.
        calls, segment_acts, server_acts = make_mock_activities(infra_env_predates_labels=True)
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

        [request] = calls["allocate_server"]
        assert request.pattern == f"^ocp-{re.escape('cisco-m6-bat')}-"


async def test_the_vlan_comes_from_the_mce_segment_not_the_caller():
    calls, segment_acts, server_acts = make_mock_activities(
        segment=INVENTORY_SEGMENT.model_copy(update={"vlan_id": 777})
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

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
            await _execute(client, AddServerToMceRunArgs(input=INPUT))
    assert _application_error(excinfo.value).type == "InventorySegmentMismatchError"


class TestTheInventoryNetworkIsOneScopePerCluster:
    """An MCE owns ONE inventory network, found by its cluster name.

    It was briefly split by how a server's BMC is driven — INVENTORY_REDFISH
    for HP/Dell/Intersight, INVENTORY_IPMI for a UCS blade — which made the
    segment a property of the chosen machine and moved the lookup inside
    candidate selection. Reverted on both sides, so these tests pin the
    opposite: the cluster name is the whole lookup, and it happens once, before
    a candidate exists.
    """

    async def test_the_lookup_is_the_cluster_name_and_nothing_else(self) -> None:
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert calls["get_inventory_segment"] == [MCE_CLUSTER]

    async def test_a_ucs_blade_uses_the_same_segment_as_a_dell(self) -> None:
        """What the revert removed: the BMC protocol no longer picks a network.

        A UCS blade is driven over IPMI and a Dell over Redfish, and both now
        boot on the one inventory VLAN the MCE owns.
        """
        ucs = bondable_server().model_copy(update={"bmc_vendor": "CISCO"})
        calls, segment_acts, server_acts = make_mock_activities(candidates=[ucs])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.vlan_id == VLAN_ID
        assert calls["get_inventory_segment"] == [MCE_CLUSTER]

    async def test_the_segment_is_resolved_once_per_run_before_the_allocation(
        self,
    ) -> None:
        # The MCE's, so read once and first: no server could make a missing
        # allocation usable, so none is allocated (and locked) before it.
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert calls["get_inventory_segment"] == [MCE_CLUSTER]
        assert calls["order"][:2] == ["check_infra_env", "allocate_server"]

    async def test_an_mce_with_no_inventory_segment_fails_before_the_draw(
        self,
    ) -> None:
        """No candidate can make a missing allocation usable.

        With one scope per cluster this is the run's failure, not a candidate's,
        so it is raised by the LOOKUP and nothing is drawn or created. (While
        the type was split it had to be a per-candidate skip instead: an MCE
        missing one class still served the other.)
        """
        calls, segment_acts, server_acts = make_mock_activities(
            segment_error=InventorySegmentNotFoundError(
                f"MCE cluster {MCE_CLUSTER!r} has no INVENTORY segment"
            )
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert (
            _application_error(excinfo.value).type
            == "InventorySegmentNotFoundError"
        )
        assert calls["acquire_servers"] == []
        assert calls["create_bmc_secret"] == []


async def test_no_assignable_server_fails_fast():
    _, segment_acts, server_acts = make_mock_activities(
        acquire_error=ServerNotAvailableError("no server name matches '^ocp-...'")
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))
    assert _application_error(excinfo.value).type == "ServerNotAvailableError"


async def test_npar_partitions_are_never_bonded():
    # Two MACs on ONE physical port would look like a bond and provide no
    # redundancy at all. The whole reason members come from `interfaces`.
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[npar_server()]
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert calls["create_bmc_secret"] == []  # nothing was written


async def test_an_allocated_servers_nics_are_taken_as_server_scan_vouched_for_them():
    """server-scan allocates only a server whose network health is HEALTHY — two
    link-up ports actually read — so the run does not judge its NICs again (the
    operator, 2026-11-17). It still PICKS the two, for the NMStateConfig's bond
    and the BareMetalHost's boot MAC: in server-scan's order, one per port.

    A OneView server reports no link state (server-scan would never hand one
    out); allocated all the same, it is installed rather than refused.
    """
    hpe = oneview_server()
    calls, segment_acts, server_acts = make_mock_activities(candidates=[hpe])
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert result.server_name == hpe.name
    assert result.bond_macs == ["aa:bb:cc:dd:ee:11", "aa:bb:cc:dd:ee:12"]
    assert result.boot_mac == "aa:bb:cc:dd:ee:11"
    # Nothing was waived: nobody asked for allow_unhealthy.
    assert result.allowed_unhealthy == []
    assert [m.logical_name for m in calls["create_nmstate_config"][0].bond_members] == ["nic1", "nic2"]


async def test_an_unusable_allocated_server_is_given_back_and_fails_the_run():
    """ONE server per run (the operator, 2026-11-17): no next candidate.

    server-scan allocated it — locked for this run — so the lock goes back
    before the run fails, and nothing was written for it. Asking again is a
    new run, with a new allocation.
    """
    standalone = standalone_server()
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[standalone, bondable_server("srv_good")]
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "UnknownBmcVendorError"
    assert f"server-scan allocated {standalone.name}" in error.message
    assert "Its lock was given back" in error.message
    assert len(calls["allocate_server"]) == 1
    assert [r.server_id for r in calls["release_server"]] == [standalone.id]
    # Allocated already locked: no reserve, and nothing created.
    assert calls["reserve_server"] == calls["create_bmc_secret"] == []
    assert calls["order"][-1] == "release_server"


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
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert _application_error(excinfo.value).type == "UnknownBmcVendorError"
    assert calls["create_bmc_secret"] == []


async def test_a_rerun_over_existing_resources_succeeds_and_reports_no_change():
    _, segment_acts, server_acts = make_mock_activities(resources_exist=True)
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert result.resources_changed is False
    assert result.bmh_registered is True


async def test_a_host_that_never_produces_an_agent_is_rolled_back_released_and_fails():
    """An hour-deep failure ends the run — after its rollback.

    The server is created, waited on for the full deadline, torn down, put in
    maintenance in server-scan (the operator, 2026-11-17) and only then its
    lock given back; no other server is tried (one per run). Leaving the host
    would strand a machine with a BareMetalHost no other MCE can see:
    server-scan reports it unclaimed until a cluster reports the node.
    """
    doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
    good = bondable_server("srv_good")
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[doomed, good], agent_never_for={"ocp-doomed"}
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "AgentNeverAppearedError"
    assert "server-scan allocated ocp-doomed" in error.message
    assert [c.server_name for c in calls["create_baremetal_host"]] == ["ocp-doomed"]
    assert [t.server_name for t in calls["delete_host_resources"]] == ["ocp-doomed"]
    # Torn down, in maintenance, THEN given back: never a lock released over a
    # live host, nor before server-scan keeps the machine out of every draw.
    order = calls["order"]
    assert (
        order.index("delete_host_resources")
        < order.index("put_server_in_maintenance")
        < order.index("release_server")
    )
    assert [m.server_id for m in calls["put_server_in_maintenance"]] == ["srv_doomed"]
    assert [r.server_id for r in calls["release_server"]] == ["srv_doomed"]
    assert len(calls["allocate_server"]) == 1
    assert "server-scan had it in maintenance" in error.message


class TestTheRollbackNeverForcesMetal3:
    """2026-11-24 (the operator): deleted with no detach, metal3's finalizer never touched."""

    async def test_it_waits_for_metal3_to_let_the_host_go_then_takes_the_secret(self):
        calls, segment_acts, server_acts = make_mock_activities(
            agent_never_for={SERVER_NAME}, secret_outlives=True
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "AgentNeverAppearedError"
        assert calls["delete_host_resources"] == [
            HostDeleteRequest(server_name=SERVER_NAME, namespace=NAMESPACE)
        ]
        # Read until metal3 let it go: the last read is the one that says so.
        assert calls["get_baremetal_host"][-1] == BmhRef(server_name=SERVER_NAME, namespace=NAMESPACE)
        # Only then the Secret metal3 needed to power the host off.
        assert calls["delete_bmc_secret"] == [
            BmcSecretRef(namespace=NAMESPACE, name=f"cisco-ucs-{SERVER_NAME}")
        ]
        order = calls["order"]
        assert order.index("delete_host_resources") < order.index("put_server_in_maintenance")

    async def test_a_host_metal3_never_lets_go_fails_at_15_min_with_nothing_after(self):
        calls, segment_acts, server_acts = make_mock_activities(
            agent_never_for={SERVER_NAME}, bmh_stays_deleting={SERVER_NAME}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "BmhDeletionTimeoutError"
        # Why it was rolling back, and what metal3 is stuck on.
        assert "no Agent within 1:00:00" in error.message
        assert "'powering off before delete'" in error.message
        assert "BMC unreachable" in error.message
        # Nothing after the delete: a BareMetalHost still points at the machine,
        # so it is neither put in maintenance nor released — its lock runs out.
        assert calls["delete_bmc_secret"] == []
        assert calls["put_server_in_maintenance"] == [] and calls["release_server"] == []
        assert calls["unregister_pxe_boot"] == []


async def test_an_agent_that_takes_its_time_is_waited_for():
    """Bare metal POSTs for longer than a VM takes to boot; that is not a failure."""
    calls, segment_acts, server_acts = make_mock_activities(agent_after_polls=20)
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert result.attempts == 1
    assert result.bmh_registered is True
    assert len(calls["find_agent_for_host"]) == 21
    # Waited, never rolled back.
    assert calls["delete_host_resources"] == []


async def test_the_agent_is_looked_for_by_bond_mac_not_by_name():
    """MAC is the only link that holds: an Agent has no back-reference to the host.

    BMAC names an Agent after the host's own inventory UUID, and the live CRD's
    `agent.spec` carries approved/clusterDeploymentName/role and nothing else. A
    lookup keyed on the server's name would work only by accident.
    """
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, AddServerToMceRunArgs(input=INPUT))

    looked_up = calls["find_agent_for_host"][0]
    created = calls["create_baremetal_host"][0]
    assert looked_up.namespace == NAMESPACE
    # BOTH bond members: the host registers with whichever NIC brought the
    # discovery ISO up, and which one that was is not knowable in advance.
    assert set(looked_up.macs) == {m.mac for m in created.bond_members}
    assert len(looked_up.macs) == 2


async def test_a_registration_error_points_the_failure_at_the_bmc():
    """The host's own state cannot prove success, but it says WHERE a failure is.

    Ironic reporting a registration error means the BMC was never reached — an
    address or a credential. Without that, a missing Agent means the opposite:
    the BMC answered and the host was driven to boot, so the fault is the bond,
    the VLAN or DHCP. Two different investigations, so the message says which.
    """
    bad_bmc = BmhState(
        found=True,
        provisioning_state="registering",
        operational_status="error",
        error_type="registration error",
        error_message="Failed to establish connection to 10.11.1.229",
    )
    calls, segment_acts, server_acts = make_mock_activities(
        agent_never_for={SERVER_NAME}, bmh_script=[bad_bmc] * 4
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    message = str(_application_error(excinfo.value))
    assert "registration error" in message
    assert "BMC address or credential" in message
    assert len(calls["delete_host_resources"]) == 1


async def test_without_a_registration_error_the_failure_points_at_the_network():
    calls, segment_acts, server_acts = make_mock_activities(
        agent_never_for={SERVER_NAME},
        bmh_script=[
            BmhState(
                found=True, provisioning_state="available", operational_status="OK"
            )
        ]
        * 4,
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    message = str(_application_error(excinfo.value))
    assert "bond" in message and "VLAN" in message and "DHCP" in message
    assert "not at" in message  # ... not at the BMC


async def test_resources_are_created_in_dependency_order():
    # The BareMetalHost references the Secret by name, so the Secret goes first.
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert len(calls["create_bmc_secret"]) == 1
    assert len(calls["create_baremetal_host"]) == 1
    assert len(calls["create_nmstate_config"]) == 1
    request = calls["create_nmstate_config"][0]
    assert request.infra_env == INFRA_ENV
    assert request.namespace == NAMESPACE
    assert [m.logical_name for m in request.bond_members] == ["nic1", "nic2"]


async def test_an_allocated_server_that_already_has_a_baremetalhost_is_given_back():
    """server-scan cannot know a machine was just installed; the cluster can.

    Nothing changes a server's lifecycle state until a CLUSTER reports the
    node, minutes later. The install lock covers that window (held until
    server-scan sees the machine), so this is the rare case — and the run
    gives the lock back and fails rather than "succeed" over another's host.
    """
    taken, free = bondable_server("srv_taken", name="ocp-taken"), bondable_server("srv_free")
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[taken, free], already_installed={"ocp-taken"}
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert "already hold a BareMetalHost" in error.message
    assert calls["create_baremetal_host"] == []
    assert [r.server_id for r in calls["release_server"]] == ["srv_taken"]


async def test_an_allocated_server_kubernetes_cannot_name_is_given_back_unprobed():
    """Every read and every create derives a Kubernetes object name from the
    server's name, and the conversion raises for a name no amount of lowercasing
    can save — raised from an ACTIVITY that would be a non-retryable failure
    with nothing said about why. So the workflow checks it first, names it, and
    gives the lock back."""
    bad = bondable_server("srv_bad", name="ocp_dell_underscores_are_illegal")
    calls, segment_acts, server_acts = make_mock_activities(candidates=[bad])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    assert _application_error(excinfo.value).type == "InvalidServerNameError"
    # Never even probed: the probe itself would have raised on the name.
    assert calls["get_baremetal_host"] == []
    assert [r.server_id for r in calls["release_server"]] == ["srv_bad"]


async def test_a_pool_of_unnameable_candidates_says_to_rename_them():
    bad = bondable_server("srv_bad", name="ocp_dell_underscores_are_illegal")
    calls, segment_acts, server_acts = make_mock_activities(candidates=[bad])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    # Every candidate was rejected for the SAME reason, so the failure names it
    # rather than falling back to the general "nothing installable".
    assert error.type == "InvalidServerNameError"
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
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert "already hold a BareMetalHost" in str(error)
    assert calls["create_baremetal_host"] == []


async def test_an_explicitly_named_server_is_converged_not_skipped():
    """Naming a server asks for THAT machine, so an existing host is the point."""
    named = AddServerToMceInput(
        infra_env=INFRA_ENV, mce_cluster=MCE_CLUSTER, namespace=NAMESPACE,
        server_name=SERVER_NAME,
    )
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[bondable_server()], already_installed={SERVER_NAME}
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, AddServerToMceRunArgs(input=named))

    assert result.server_name == SERVER_NAME
    assert len(calls["create_baremetal_host"]) == 1


async def test_an_allocated_servers_macs_are_left_to_the_limb_which_refuses_a_bad_one():
    """server-scan normalizes MACs on ingest, so the run no longer checks them on an
    allocated server (the operator, 2026-11-17). Should one be malformed all the
    same, the limb refuses it — permanently, InvalidMacError, once — before the
    BareMetalHost or the NMStateConfig is written."""
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
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "InvalidMacError"
    assert len(calls["create_baremetal_host"]) == 1  # one attempt: permanent
    assert calls["create_nmstate_config"] == []


async def test_a_server_with_no_bmc_host_fails_in_seconds_not_in_ten_minutes():
    """Nothing downstream catches an empty BMC host, which is why this exists.

    `build_bmc_address` still produces a syntactically valid
    `redfish-virtualmedia:///redfish/v1/Systems/1`, and the API server stores
    the BareMetalHost without complaint — so the run would create all three
    resources, spend the whole registration deadline, and then blame the BMC
    credentials for an address the inventory never had.
    """
    hostless = bondable_server().model_copy(
        update={"bmc": BmcEndpoint(host="", host_is_ip=False, scheme=None)}
    )
    calls, segment_acts, server_acts = make_mock_activities(candidates=[hostless])
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "BmcEndpointMissingError"
    assert "no BMC host" in str(error)
    assert calls["create_bmc_secret"] == []
    assert calls["create_baremetal_host"] == []


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
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

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
        other = AddServerToMceInput(
            infra_env=INFRA_ENV, mce_cluster=other_mce, namespace=NAMESPACE
        )
        calls, segment_acts, server_acts = make_mock_activities(segment=matching)
        async with _Harness(segment_acts, server_acts, MCE_ACTIVITY_QUEUE) as client:
            handle = await client.start_workflow(
                AddServerToMceWorkflow.run,
                AddServerToMceRunArgs(input=other),
                id=f"test-{uuid.uuid4()}",
                task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handle.result(), timeout=5)
            progress = await handle.query(AddServerToMceWorkflow.progress)

        # Nothing reached this MCE's worker...
        assert calls["acquire_servers"] == []
        assert calls["create_baremetal_host"] == []
        # ...and the stall says which queue is unserved, so "the run is stuck"
        # reads as "that MCE has no server-lifecycle-worker".
        assert progress.activity_queue == f"server-lifecycle-activity-{other_mce}"
        # The first step on the MCE's queue is the InfraEnv check.
        assert progress.phase == "validating-infraenv"
        assert calls["check_infra_env"] == []

    async def test_the_result_still_reports_which_mce_was_targeted(self) -> None:
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))
        assert result.mce_cluster == MCE_CLUSTER
        assert len(calls["create_baremetal_host"]) == 1


class TestTheInstallLock:
    """server-scan's install lock (ADR-0035) — the guard ACROSS MCEs.

    This run's BareMetalHost probe only sees its own MCE, and server-scan
    reports a machine AVAILABLE until a cluster reports it. Without the lock, a
    server installed into one MCE is drawn again by a run for another.
    """

    async def test_the_allocated_machine_is_renewed_held_and_released_once_server_scan_sees_it(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        # Taken BY THE ALLOCATION — server-scan locks the server it hands over —
        # then renewed as the Agent wait begins and held for a day, until
        # server-scan shows it installed (the mock does at the first read).
        [allocation] = calls["allocate_server"]
        assert allocation.ttl_seconds == 7200
        claims = calls["reserve_server"]
        assert [c.ttl_seconds for c in claims] == [7200, 86_400]
        assert {c.server_id for c in claims} == {"srv_001"}
        # One holder and workflow id throughout: server-scan EXTENDS a lock its
        # own run re-takes, and treats anyone else's as a lost race.
        assert {(c.holder, c.workflow_id) for c in claims} == {
            (allocation.holder, allocation.workflow_id)
        }
        assert allocation.holder == "add-server-to-mce"
        assert allocation.workflow_id.startswith("test-")
        assert (claims[0].mce_cluster, claims[0].infra_env, claims[0].namespace) == (
            MCE_CLUSTER, INFRA_ENV, NAMESPACE,
        )
        (released,) = calls["release_server"]
        assert (released.server_id, released.holder, released.workflow_id) == (
            "srv_001", "add-server-to-mce", allocation.workflow_id,
        )
        assert result.lock_released is True and result.server_scan_state == "INSTALLED_TO_INVENTORY"
        assert result.reservation_expires_at is None

    async def test_a_machine_another_run_holds_is_never_allocated(self):
        # server-scan's own rule: a locked server is not handed out, so the run
        # is given the next one — nothing of the held one is touched.
        held = bondable_server("srv_held", name="ocp-held", mac_prefix="11:22:33:44:55")
        free = bondable_server("srv_free")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[held, free], reserved_elsewhere={"ocp-held"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        assert result.attempts == 1
        assert [c.server_name for c in calls["create_bmc_secret"]] == [SERVER_NAME]
        assert calls["delete_host_resources"] == []

    async def test_a_pool_all_held_elsewhere_fails_with_server_scans_reasons(self):
        calls, segment_acts, server_acts = make_mock_activities(
            reserved_elsewhere={SERVER_NAME}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "ServerNotAvailableError"
        assert "RESERVED" in error.message
        # Permanent: asked once, nothing written.
        assert len(calls["allocate_server"]) == 1
        assert calls["create_bmc_secret"] == calls["release_server"] == []

    async def test_a_server_gone_from_the_inventory_is_never_allocated(self):
        gone = bondable_server("srv_gone", name="ocp-gone", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[gone, bondable_server()], gone_from_inventory={"ocp-gone"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        assert [c.server_name for c in calls["create_bmc_secret"]] == [SERVER_NAME]

    async def test_a_rolled_back_machine_is_released_as_the_allocation_took_it(self):
        doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[doomed], agent_never_for={"ocp-doomed"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        # The lock server-scan took in the allocation, released as its holder
        # and run: server-scan compares both before it deletes anything.
        [allocation] = calls["allocate_server"]
        [released] = calls["release_server"]
        assert (released.server_id, released.holder, released.workflow_id) == (
            "srv_doomed", allocation.holder, allocation.workflow_id,
        )

    async def test_a_lock_lost_before_the_agent_wait_rolls_back_without_releasing(self):
        """Lapsed and taken by another run: undo OUR resources, leave THEIR lock — and fail."""
        lapsed = bondable_server("srv_lapsed", name="ocp-lapsed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[lapsed, bondable_server()], lock_lapses_for={"ocp-lapsed"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "ServerReservedError"
        assert "Its lock had lapsed and another run holds the machine now" in error.message
        assert [t.server_name for t in calls["delete_host_resources"]] == ["ocp-lapsed"]
        # Never the lapsed one's lock — another run holds it now.
        assert calls["release_server"] == []
        # Never watched for an Agent: the machine is someone else's now.
        assert calls["find_agent_for_host"] == []

    async def test_a_token_without_the_admin_role_fails_the_run(self):
        # The allocation LOCKS the server it returns, a write: ADMIN only.
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=ServerScanAuthError("needs the ADMIN role")
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "ServerScanAuthError"
        # Once: permanent, not retried every minute.
        assert len(calls["allocate_server"]) == 1
        assert calls["create_bmc_secret"] == []

    async def test_a_renewal_refused_for_its_token_fails_the_run(self):
        calls, segment_acts, server_acts = make_mock_activities(
            reserve_error=ServerScanAuthError("needs the ADMIN role")
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "ServerScanAuthError"
        assert len(calls["reserve_server"]) == 1


class TestThePxeBootOfAnIpmiServer:
    """An IPMI BMC has no virtual media, so a UCS blade boots from the network.

    The site's PXE VM serves the InfraEnv's iPXE script by MAC, and has to be
    told so BEFORE the BareMetalHost exists: Ironic may power the host on the
    moment it registers it.
    """

    async def test_a_ucs_blade_is_mapped_before_anything_is_created(self):
        calls, segment_acts, server_acts = make_mock_activities(candidates=[ucs_server()])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.pxe_registered is True
        watched = {"register_pxe_boot", "create_bmc_secret", "create_baremetal_host"}
        assert [name for name in calls["order"] if name in watched] == [
            "register_pxe_boot",
            "create_bmc_secret",
            "create_baremetal_host",
        ]
        request = calls["register_pxe_boot"][0]
        # The SITE comes from the MCE's inventory segment — the network the host
        # DHCPs on — and the InfraEnv is the one being filled.
        assert request.site == INVENTORY_SEGMENT.site
        assert request.infra_env == INFRA_ENV
        assert request.namespace == NAMESPACE
        # The BareMetalHost's boot MAC alone — the first link-up NIC.
        assert request.macs == ["00:25:b5:00:00:01"]
        assert request.macs == [result.boot_mac]
        # The Agent is still matched on BOTH bond MACs: it may report either.
        assert calls["find_agent_for_host"][0].macs == [
            "00:25:b5:00:00:01",
            "00:25:b5:00:00:02",
        ]

    async def test_a_redfish_server_never_touches_the_pxe_map(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.pxe_registered is False
        assert calls["register_pxe_boot"] == []

    async def test_the_mapping_follows_the_lock(self):
        # Mapping a machine another MCE's run holds would repoint ITS boot.
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], reserved_elsewhere={ucs_server().name}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert calls["register_pxe_boot"] == []

    async def test_a_site_without_a_pxe_vm_gives_the_lock_back_and_fails(self):
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server(), bondable_server()], pxe_sites=set()
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "PxeSiteNotConfiguredError"
        # Nothing was written for the blade, and its lock went straight back.
        assert calls["create_bmc_secret"] == calls["delete_host_resources"] == []
        assert [r.server_id for r in calls["release_server"]] == ["srv_ucs"]

    async def test_a_pool_of_blades_at_an_unmapped_site_fails_naming_the_fix(self):
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], pxe_sites=set()
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "PxeSiteNotConfiguredError"
        assert "PXE_MAP_URLS" in error.message
        assert "bat-yam" in error.message
        assert calls["create_bmc_secret"] == []

    async def test_a_rejected_pxe_token_fails_the_run(self):
        # No other blade would fare differently, so it is not a skip.
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], pxe_error=PxeMapAuthError("check PXE_MAP_TOKEN")
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "PxeMapAuthError"
        assert len(calls["register_pxe_boot"]) == 1
        assert calls["create_bmc_secret"] == []

    async def test_a_blade_with_no_agent_points_the_diagnosis_at_the_pxe_map_too(self):
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], agent_never_for={ucs_server().name}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert "PXE map" in _application_error(excinfo.value).message


class TestParallelRunsOnOnePool:
    """add-server-to-mce `count`'s premise: N runs allocating from one pool AT ONCE land on N machines.

    Both runs share one mock server-scan, whose allocation behaves as the real
    one (ADR-0063 there): it never hands out a server under a live lock, and
    locks the one it hands out for the caller — so the second run is given
    another machine rather than the first one's.
    """

    async def test_two_concurrent_runs_install_two_different_servers(self):
        pool = [
            bondable_server("srv_a", name="ocp-dell-a", mac_prefix="aa:aa:aa:aa:aa"),
            bondable_server("srv_b", name="ocp-dell-b", mac_prefix="bb:bb:bb:bb:bb"),
            bondable_server("srv_c", name="ocp-dell-c", mac_prefix="cc:cc:cc:cc:cc"),
        ]
        calls, segment_acts, server_acts = make_mock_activities(candidates=pool)
        args = AddServerToMceRunArgs(input=INPUT)
        async with _Harness(segment_acts, server_acts) as client:
            handles = [
                await client.start_workflow(
                    AddServerToMceWorkflow.run,
                    args,
                    id=f"add-server-to-mce-{INFRA_ENV}_{slot}",
                    task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
                )
                for slot in (1, 2)
            ]
            results = await asyncio.wait_for(
                asyncio.gather(*(h.result() for h in handles)), timeout=60
            )

        installed = [r.server_name for r in results]
        assert len(set(installed)) == 2, f"two runs installed the same server: {installed}"
        # And no machine ever had BareMetalHosts created by both runs.
        created = [c.server_name for c in calls["create_baremetal_host"]]
        assert sorted(created) == sorted(installed)


class TestAFinishedSlotIsFreeAgain:
    """add-server-to-mce `count` reuses a slot whose run has finished: Temporal must allow it."""

    async def test_a_completed_slot_id_starts_a_new_run(self):
        pool = [
            bondable_server("srv_a", name="ocp-dell-a", mac_prefix="aa:aa:aa:aa:aa"),
            bondable_server("srv_b", name="ocp-dell-b", mac_prefix="bb:bb:bb:bb:bb"),
        ]
        _, segment_acts, server_acts = make_mock_activities(candidates=pool)
        slot = f"add-server-to-mce-{INFRA_ENV}_1"
        args = AddServerToMceRunArgs(input=INPUT)
        async with _Harness(segment_acts, server_acts) as client:
            first = await asyncio.wait_for(
                client.execute_workflow(
                    AddServerToMceWorkflow.run, args, id=slot, task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE
                ),
                timeout=60,
            )
            # Same id again, the first run COMPLETED: a new run, not a refusal.
            second = await asyncio.wait_for(
                client.execute_workflow(
                    AddServerToMceWorkflow.run, args, id=slot, task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE
                ),
                timeout=60,
            )

        # And it installed the next machine: server-scan saw the first installed
        # (the first run released its lock only then) and allocates it no more.
        assert (first.server_name, second.server_name) == ("ocp-dell-a", "ocp-dell-b")


class TestTheInfraEnvBootEntry:
    """An IPMI blade boots its InfraEnv's boot entry, published only once the
    discovery image includes the blade's own NMStateConfig.

    Every NMStateConfig regenerates the InfraEnv's image, so with installs
    running in parallel an entry published before THIS host's NMStateConfig
    reached the image could hand it an image without its static network
    config. Hence: MACs -> entry, NMStateConfig, wait for the image, publish,
    and only then the BareMetalHost that lets Ironic power the host on.
    """

    _WATCHED = {
        "register_pxe_boot",
        "create_nmstate_config",
        "publish_infra_env_boot",
        "create_bmc_secret",
        "create_baremetal_host",
    }

    async def test_the_entry_is_published_after_the_nmstateconfig_and_before_the_bmh(self):
        calls, segment_acts, server_acts = make_mock_activities(candidates=[ucs_server()])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.pxe_registered is True
        assert [name for name in calls["order"] if name in self._WATCHED] == [
            "register_pxe_boot",
            "create_nmstate_config",
            "publish_infra_env_boot",
            "create_bmc_secret",
            "create_baremetal_host",
        ]
        # Created once, not again after the BareMetalHost.
        assert len(calls["create_nmstate_config"]) == 1
        # The MACs chain to THIS MCE's entry for the InfraEnv being filled.
        assert calls["register_pxe_boot"][0].mce_cluster == MCE_CLUSTER
        assert calls["publish_infra_env_boot"] == [
            InfraEnvBootRequest(
                server_name=ucs_server().name,
                namespace=NAMESPACE,
                infra_env=INFRA_ENV,
                mce_cluster=MCE_CLUSTER,
                site=INVENTORY_SEGMENT.site,
            )
        ]

    async def test_nothing_is_powered_on_until_the_image_includes_the_host(self):
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], image_after_polls=3
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.pxe_registered is True
        assert len(calls["publish_infra_env_boot"]) == 4
        order = calls["order"]
        last_publish = len(order) - 1 - order[::-1].index("publish_infra_env_boot")
        assert last_publish < order.index("create_baremetal_host")

    async def test_a_newer_image_already_published_is_success(self):
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[ucs_server()], boot_superseded=True
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.agent_name == f"agent-{ucs_server().name}"
        assert len(calls["publish_infra_env_boot"]) == 1

    async def test_an_image_that_never_updates_rolls_back_and_fails_the_run(self):
        # The InfraEnv's problem, not the machine's: the Dell behind the blade
        # would need the same InfraEnv, so it is not tried.
        blade = ucs_server()
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[blade, bondable_server()], image_never_updates=True
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "InfraEnvImageNotUpdatedError"
        assert "ImageCreated is not True: nmstate invalid" in error.message
        assert blade.name in error.message
        # Bounded: 10 min of 10 s polls, give or take the first and last.
        assert 55 <= len(calls["publish_infra_env_boot"]) <= 62
        # Never powered on; undone in the rollback order, lock last.
        assert calls["create_bmc_secret"] == []
        assert calls["create_baremetal_host"] == []
        watched = {"delete_host_resources", "unregister_pxe_boot", "release_server"}
        assert [name for name in calls["order"] if name in watched] == [
            "delete_host_resources",
            "unregister_pxe_boot",
            "release_server",
        ]
        assert [t.server_name for t in calls["delete_host_resources"]] == [blade.name]
        assert [r.server_name for r in calls["unregister_pxe_boot"]] == [blade.name]

    async def test_a_redfish_server_keeps_its_order_and_never_publishes(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert calls["publish_infra_env_boot"] == []
        assert [name for name in calls["order"] if name in self._WATCHED] == [
            "create_bmc_secret",
            "create_baremetal_host",
            "create_nmstate_config",
        ]


class TestTheInfraEnvIsCheckedFirst:
    """No server is drawn, locked or booted for an InfraEnv the MCE does not have."""

    async def test_the_infraenv_is_checked_on_the_mce_before_the_draw(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert calls["check_infra_env"] == [
            InfraEnvRef(name=INFRA_ENV, namespace=NAMESPACE)
        ]
        order = calls["order"]
        assert order.index("check_infra_env") < order.index("allocate_server")

    async def test_a_missing_infraenv_fails_the_run_before_anything_is_drawn(self):
        calls, segment_acts, server_acts = make_mock_activities(infra_env_missing=True)
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "InfraEnvNotFoundError"
        # Once: permanent, not retried every minute.
        assert len(calls["check_infra_env"]) == 1
        assert calls["allocate_server"] == calls["acquire_servers"] == []
        assert calls["reserve_server"] == []
        assert calls["create_bmc_secret"] == []


class TestThePxeMapIsCleanedOnRollback:
    """A rolled-back IPMI candidate must not keep booting this InfraEnv's discovery image."""

    async def test_a_blade_with_no_agent_is_unmapped_between_teardown_and_release(self):
        blade = ucs_server()
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[blade, bondable_server()], agent_never_for={blade.name}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        # Exactly what was mapped: the boot MAC.
        assert calls["unregister_pxe_boot"] == [
            PxeMapRef(
                server_name=blade.name,
                site=INVENTORY_SEGMENT.site,
                macs=["00:25:b5:00:00:01"],
            )
        ]
        assert calls["unregister_pxe_boot"][0].macs == calls["register_pxe_boot"][0].macs
        # Teardown first (Ironic lets go), then the PXE map, the maintenance,
        # THEN the lock: after the release another run may map the same MACs.
        watched = {
            "delete_host_resources",
            "unregister_pxe_boot",
            "put_server_in_maintenance",
            "release_server",
        }
        assert [name for name in calls["order"] if name in watched] == [
            "delete_host_resources",
            "unregister_pxe_boot",
            "put_server_in_maintenance",
            "release_server",
        ]
        assert [r.server_name for r in calls["release_server"]] == [blade.name]

    async def test_a_redfish_server_rolled_back_never_touches_the_pxe_map(self):
        doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[doomed], agent_never_for={"ocp-doomed"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert [t.server_name for t in calls["delete_host_resources"]] == ["ocp-doomed"]
        assert calls["unregister_pxe_boot"] == []

    async def test_a_lapsed_lock_leaves_the_pxe_map_to_the_run_that_took_it(self):
        """Another run holds the machine now and may have mapped the same MACs."""
        blade = ucs_server()
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[blade], lock_lapses_for={blade.name}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "ServerReservedError"
        assert [t.server_name for t in calls["delete_host_resources"]] == [blade.name]
        assert calls["unregister_pxe_boot"] == calls["release_server"] == []

    async def test_an_installed_blade_keeps_its_mapping(self):
        calls, segment_acts, server_acts = make_mock_activities(candidates=[ucs_server()])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.pxe_registered is True
        assert calls["unregister_pxe_boot"] == []


class TestAServerWithNoAgentGoesIntoMaintenance:
    """No Agent within the hour: the server goes into maintenance in server-scan,
    THEN its lock goes back (the operator, 2026-11-17) — so no run draws a
    machine that cannot boot into the InfraEnv again until a person has looked."""

    async def test_the_next_run_on_the_pool_is_given_another_server(self):
        doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[doomed, bondable_server()], agent_never_for={"ocp-doomed"}
        )
        args = AddServerToMceRunArgs(input=INPUT)
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute_as(client, args, "add-server-to-mce-pool_1")
            second = await _execute_as(client, args, "add-server-to-mce-pool_2")

        # Its lock was released, yet server-scan hands it out no more.
        assert [r.server_id for r in calls["release_server"]][0] == "srv_doomed"
        assert second.server_name == SERVER_NAME
        [maintenance] = calls["put_server_in_maintenance"]
        assert maintenance.server_name == "ocp-doomed"
        # The reason says which run, where, and where the host's state points.
        assert "add-server-to-mce run add-server-to-mce-pool_1" in maintenance.reason
        assert f"InfraEnv {NAMESPACE}/{INFRA_ENV} on {MCE_CLUSTER}" in maintenance.reason
        assert "no Agent in 60 min" in maintenance.reason
        assert maintenance.reason.endswith("End the maintenance once the cause is fixed")
        assert len(maintenance.reason) <= 500

    async def test_refused_by_server_scan_the_lock_is_kept_instead(self):
        """Not drawn again at once all the same: the lock expires at its TTL."""
        calls, segment_acts, server_acts = make_mock_activities(
            agent_never_for={SERVER_NAME},
            maintenance_error=ServerScanAuthError("needs the ADMIN role"),
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        # Still the run's own failure, not the maintenance's.
        assert error.type == "AgentNeverAppearedError"
        assert error.message.endswith(
            "Its lock is kept until it expires, because server-scan would not put it in "
            "maintenance (ServerScanAuthError)"
        )
        assert len(calls["put_server_in_maintenance"]) == 1  # permanent: once
        assert calls["release_server"] == []
        assert len(calls["delete_host_resources"]) == 1

    async def test_a_server_server_scan_no_longer_knows_is_released_as_before(self):
        calls, segment_acts, server_acts = make_mock_activities(
            agent_never_for={SERVER_NAME}, maintenance_gone=True
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _application_error(excinfo.value).message.endswith("Its lock was given back")
        assert len(calls["release_server"]) == 1


# --- allow_unhealthy (hidden; CLAUDE.md §4) ------------------------------------


def one_nic_up_server(
    server_id: str = "srv_001", name: str = SERVER_NAME, mac_prefix: str = "aa:bb:cc:dd:ee"
) -> AcquiredServer:
    """The incident's machine: two ports, only the SECOND one link-up.

    Listed DOWN first on purpose, so a test proves nic1 — the boot MAC — is
    the live NIC, not merely the first one server-scan lists.
    """
    return bondable_server(server_id, name=name, mac_prefix=mac_prefix).model_copy(
        update={
            "interfaces": [
                _nic("NIC.Integrated.1-1-1", f"{mac_prefix}:01", "1/1/1", LinkState.DOWN),
                _nic("NIC.Integrated.1-2-1", f"{mac_prefix}:02", "1/2/1", LinkState.UP),
            ],
            "health_overall": "MAJOR",
            "live_recheck_performed": False,
        }
    )


_SICK = {"overall": "MAJOR", "network": "MAJOR", "storage": "MAJOR", "cpu": "HEALTHY"}
_FINE = {"overall": "HEALTHY", "network": "HEALTHY", "storage": "HEALTHY"}


def scan_record(
    server: AcquiredServer,
    *,
    health: dict[str, str] | None = None,
    reachable: bool = True,
    lifecycle: str = "AVAILABLE",
    cluster: str | None = None,
    maintenance: bool = False,
    contested: str | None = None,
    lock: ServerLockRecord | None = None,
) -> InstallCandidateRecord:
    """server-scan's STORED record of a named server, as lookup_install_candidate reads it."""
    return InstallCandidateRecord(
        membership=ServerMembership(
            found=True,
            server_id=server.id,
            server_name=server.name,
            lifecycle_state=lifecycle,
            cluster_name=cluster,
            contested_with=contested,
            maintenance=maintenance,
            maintenance_reason="RMA" if maintenance else None,
        ),
        server=server,
        health=dict(_SICK if health is None else health),
        reachable=reachable,
        lock=lock or ServerLockRecord(),
    )


def _refused(name: str = SERVER_NAME) -> ServerNotAvailableError:
    """What /servers/available answers for a named machine it will not hand out."""
    return ServerNotAvailableError(
        f"server-scan has no assignable server for this request: '{name}' is no longer "
        "available: health=MAJOR, openshift=AVAILABLE, maintenance=False, reachable=True"
    )


def _unhealthy(name: str = SERVER_NAME) -> AddServerToMceInput:
    return INPUT.model_copy(update={"server_name": name, "allow_unhealthy": True})


async def _execute_as(client: Client, args: AddServerToMceRunArgs, workflow_id: str):
    return await asyncio.wait_for(
        client.execute_workflow(
            AddServerToMceWorkflow.run,
            args,
            id=workflow_id,
            task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
        ),
        timeout=60,
    )


class TestAllowUnhealthy:
    """The hidden option: a NAMED server installed although its own health fails.

    The incident it exists for: a server with one NIC up and no RAID had to be
    installed. Without it the run refuses, as it should; with it, the health
    refusals are waived and RECORDED, and every safety refusal still stands.
    """

    async def test_a_one_nic_server_is_installed_with_every_waiver_recorded(self):
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(one_nic_up_server())
        )
        async with _Harness(segment_acts, server_acts) as client:
            handle = await client.start_workflow(
                AddServerToMceWorkflow.run,
                AddServerToMceRunArgs(input=_unhealthy()),
                id=f"test-{uuid.uuid4()}",
                task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
            )
            result = await asyncio.wait_for(handle.result(), timeout=60)
            progress = await handle.query(AddServerToMceWorkflow.progress)

        assert result.server_name == SERVER_NAME
        # The usual choice: nic1, the boot MAC, is the live NIC; nic2 the one down.
        assert result.boot_mac == "aa:bb:cc:dd:ee:02"
        assert result.bond_macs == ["aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:01"]
        waived = {w.check: w.observed for w in result.allowed_unhealthy}
        assert list(waived) == ["server-scan-health", "network-health", "bond-link-up"]
        assert "overall MAJOR" in waived["server-scan-health"]
        assert "storage=MAJOR" in waived["server-scan-health"]
        assert "cpu" not in waived["server-scan-health"]
        assert waived["network-health"].startswith("network MAJOR")
        assert waived["bond-link-up"].startswith("1 of 2 bond members link-UP")
        assert "nic2 aa:bb:cc:dd:ee:01 [1/1] DOWN" in waived["bond-link-up"]
        # A failed run has no result, so the progress says it too.
        assert progress.allowed_unhealthy == result.allowed_unhealthy
        # Every create's input carries it, so history and the limb log say it.
        for kind in ("create_bmc_secret", "create_baremetal_host", "create_nmstate_config"):
            assert calls[kind][0].allowed_unhealthy == result.allowed_unhealthy
        assert [r.server_name for r in calls["lookup_install_candidate"]] == [SERVER_NAME]
        assert calls["reserve_server"], "the lock is taken as for any install"

    async def test_without_it_the_same_server_is_still_refused(self):
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(one_nic_up_server())
        )
        named = INPUT.model_copy(update={"server_name": SERVER_NAME})
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=named))

        assert _application_error(excinfo.value).type == "ServerNotAvailableError"
        assert calls["reserve_server"] == []
        assert calls["create_bmc_secret"] == []

    async def test_a_healthy_server_takes_the_normal_path_and_waives_nothing(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))

        assert result.allowed_unhealthy == []
        assert calls["lookup_install_candidate"] == []
        assert calls["create_baremetal_host"][0].allowed_unhealthy == []

    async def test_links_server_scan_cannot_report_are_waived_and_recorded(self):
        """An HPE server: OneView reports no link state, so no member reads UP."""
        hpe = oneview_server()
        _, segment_acts, server_acts = make_mock_activities(candidates=[hpe])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy(hpe.name)))

        assert result.boot_mac == "aa:bb:cc:dd:ee:11"
        [waiver] = result.allowed_unhealthy
        assert waiver.check == "bond-link-up"
        assert waiver.observed.startswith("0 of 2 bond members link-UP")
        assert "UNKNOWN" in waiver.observed

    @pytest.mark.parametrize(
        ("record", "error_type", "says"),
        [
            pytest.param(
                scan_record(one_nic_up_server(), lifecycle="INSTALLED", cluster="hc-a"),
                "ServerNotAvailableError",
                "a cluster claims it: server-scan reads it INSTALLED (cluster hc-a)",
                id="claimed-by-a-cluster",
            ),
            pytest.param(
                scan_record(one_nic_up_server(), lifecycle="INSTALLED_TO_INVENTORY"),
                "ServerNotAvailableError",
                "a cluster claims it",
                id="in-an-mce-inventory",
            ),
            pytest.param(
                scan_record(one_nic_up_server(), contested="ocp4-other-mce"),
                "ServerNotAvailableError",
                "two claims contest it",
                id="contested",
            ),
            pytest.param(
                scan_record(one_nic_up_server(), maintenance=True),
                "ServerNotAvailableError",
                "in maintenance in server-scan: RMA",
                id="maintenance",
            ),
            pytest.param(
                None,
                "ServerNotAvailableError",
                "server-scan has no server of that exact name",
                id="unknown-to-server-scan",
            ),
            pytest.param(
                scan_record(
                    one_nic_up_server(),
                    lock=ServerLockRecord(
                        live=True,
                        holder="add-server-to-mce",
                        workflow_id="add-server-to-mce-ocp4-other_x",
                        mce_cluster="ocp4-other",
                        expires_at="2026-10-31T12:00:00Z",
                    ),
                ),
                "ServerReservedError",
                "another run holds its install lock",
                id="another-runs-lock",
            ),
        ],
    )
    async def test_a_safety_refusal_is_never_waived(self, record, error_type, says):
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=record
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))

        error = _application_error(excinfo.value)
        assert error.type == error_type
        assert says in str(error)
        assert "allow_unhealthy never waives" in str(error)
        assert calls["reserve_server"] == []
        assert calls["create_bmc_secret"] == []

    async def test_an_expired_lock_of_another_run_is_no_refusal(self):
        stale = ServerLockRecord(live=False, holder="add-server-to-mce", workflow_id="old-run")
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(one_nic_up_server(), lock=stale)
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))
        assert result.server_name == SERVER_NAME

    async def test_its_own_leftover_lock_is_no_reason_to_refuse(self):
        """An earlier run on the SAME id failed and left its lock: re-taking it extends it."""
        workflow_id = f"add-server-to-mce-{MCE_CLUSTER}_{SERVER_NAME.lower()}"
        own = ServerLockRecord(live=True, holder="add-server-to-mce", workflow_id=workflow_id)
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(),
            record=scan_record(bondable_server(), health=_FINE, lock=own),
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute_as(
                client, AddServerToMceRunArgs(input=_unhealthy()), workflow_id
            )

        assert result.allowed_unhealthy == []  # nothing unhealthy was waived
        assert {c.workflow_id for c in calls["reserve_server"]} == {workflow_id}

    async def test_a_record_showing_nothing_wrong_waives_the_live_recheck(self):
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(bondable_server(), health=_FINE)
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))

        [waiver] = result.allowed_unhealthy
        assert waiver.check == "server-scan-recheck"
        assert "is no longer available" in waiver.observed

    async def test_an_unreachable_bmc_is_waived_and_recorded(self):
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(),
            record=scan_record(bondable_server(), health=_FINE, reachable=False),
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))
        assert [w.check for w in result.allowed_unhealthy] == ["bmc-reachable"]

    async def test_a_server_with_one_port_is_still_refused(self):
        """Link state is waived; the second port a bond needs is not."""
        single = bondable_server().model_copy(
            update={
                "interfaces": [
                    _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP)
                ]
            }
        )
        calls, segment_acts, server_acts = make_mock_activities(candidates=[single])
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))

        error = _application_error(excinfo.value)
        assert error.type == "NoBondableInterfacesError"
        assert "two distinct physical ports" in str(error)
        assert "NIC.Integrated.1-1-1[1/1]=UP" in str(error)
        assert calls["reserve_server"] == []

    async def test_a_pool_draw_asking_for_it_fails_before_anything_is_read(self):
        calls, segment_acts, server_acts = make_mock_activities()
        pool = INPUT.model_copy(update={"allow_unhealthy": True})
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=pool))

        assert _application_error(excinfo.value).type == "AllowUnhealthyNeedsServerNameError"
        assert calls["get_inventory_segment"] == []
        assert calls["acquire_servers"] == []

    async def test_an_ipmi_blade_with_one_nic_up_boots_from_its_live_nic(self):
        blade = ucs_server().model_copy(
            update={
                "interfaces": [
                    _nic("eth0", "00:25:b5:00:00:01", None, LinkState.DOWN),
                    _nic("eth1", "00:25:b5:00:00:02", None, LinkState.UP),
                ]
            }
        )
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(blade.name), record=scan_record(blade)
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=_unhealthy(blade.name)))

        # The PXE VM maps the BMH's boot MAC only — the live NIC.
        assert calls["register_pxe_boot"][0].macs == ["00:25:b5:00:00:02"]
        assert result.boot_mac == "00:25:b5:00:00:02"

    async def test_a_waived_install_that_never_boots_says_what_was_waived(self):
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(),
            record=scan_record(one_nic_up_server()),
            agent_never_for={SERVER_NAME},
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=_unhealthy()))

        error = _application_error(excinfo.value)
        assert error.type == "AgentNeverAppearedError"
        assert "health checks waived on request: server-scan-health" in str(error)
        # Rolled back and released like any other candidate.
        assert len(calls["delete_host_resources"]) == 1
        assert len(calls["release_server"]) == 1


# --- a conflicting NMStateConfig: replaced --------------------------------------

_STALE = ["VLAN id: want ['24'], found ['77']"]
_IN_USE = "BareMetalHost ocp-x exists (provisioning state 'provisioned')"


class TestAConflictingNmStateConfig:
    """The incident: a healthy server failed on an NMStateConfig already there.

    The run now checks it before anything else is written for the candidate —
    after the lock, ahead of the PXE map and every create, in BOTH orders — and
    the limb replaces it (tests/test_server_lifecycle_nmstate_replace.py).
    """

    _NAMED = INPUT.model_copy(update={"server_name": SERVER_NAME})

    async def test_the_incident_now_installs_instead_of_failing(self):
        calls, segment_acts, server_acts = make_mock_activities(
            nmstate_differs={SERVER_NAME: _STALE}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=self._NAMED))

        assert result.nmstate_replaced == _STALE
        assert result.resources_changed is True
        # Redfish order: replaced BEFORE any create, so create_nmstate_config
        # finds it matching.
        assert calls["order"].index("replace_conflicting_nmstate_config") < calls["order"].index(
            "create_bmc_secret"
        ) < calls["order"].index("create_baremetal_host") < calls["order"].index(
            "create_nmstate_config"
        )

    async def test_a_replacement_counts_as_a_change_even_over_existing_resources(self):
        _, segment_acts, server_acts = make_mock_activities(
            resources_exist=True, nmstate_differs={SERVER_NAME: _STALE}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=self._NAMED))
        assert result.resources_changed is True

    async def test_an_ipmi_blade_is_checked_before_its_pxe_map_and_its_nmstateconfig(self):
        blade = ucs_server()
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[blade], nmstate_differs={blade.name: _STALE}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.nmstate_replaced == _STALE
        order = calls["order"]
        assert order.index("replace_conflicting_nmstate_config") < order.index(
            "register_pxe_boot"
        ) < order.index("create_nmstate_config") < order.index("publish_infra_env_boot")

    async def test_the_check_compares_what_the_creates_will_write(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        [check] = calls["replace_conflicting_nmstate_config"]
        assert check.resource == calls["create_nmstate_config"][0]
        assert result.nmstate_replaced == []


class TestAnOlderLimbsInUseRefusal:
    """For a few hours on 2026-10-31 the limb refused a server that looked in use.

    It no longer looks (the operator: server-scan offering the machine is the
    proof it is free), but a limb deployed before that can still raise
    ServerLooksInstalledError while the brain rolls out first, so the run still
    answers it without writing anything for the candidate.
    """

    async def test_a_named_server_in_use_fails_naming_it_and_writes_nothing(self):
        blade = ucs_server()
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[blade], in_use={blade.name: _IN_USE}
        )
        named = INPUT.model_copy(update={"server_name": blade.name})
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=named))

        error = _application_error(excinfo.value)
        assert error.type == "ServerLooksInstalledError"
        assert _IN_USE in str(error) and "looks installed or in progress" in str(error)
        # Nothing written for it — not even the PXE map — and its lock given back,
        # so a re-run after the cleanup is not held off for two hours.
        for kind in ("register_pxe_boot", "create_nmstate_config", "create_bmc_secret",
                     "create_baremetal_host", "delete_host_resources"):
            assert calls[kind] == [], kind
        assert [r.server_name for r in calls["release_server"]] == [blade.name]

    async def test_a_pool_server_in_use_is_given_back_and_fails_the_run(self):
        busy = bondable_server("srv_busy", name="ocp-busy", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[busy, bondable_server()], in_use={"ocp-busy": _IN_USE}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert _IN_USE in str(_application_error(excinfo.value))
        # Straight back, nothing written; no second server is tried.
        assert [r.server_name for r in calls["release_server"]] == ["ocp-busy"]
        assert calls["create_bmc_secret"] == []

    async def test_a_pool_all_in_use_fails_with_each_sign(self):
        busy = bondable_server("srv_busy", name="ocp-busy", mac_prefix="11:22:33:44:55")
        _, segment_acts, server_acts = make_mock_activities(
            candidates=[busy], in_use={"ocp-busy": _IN_USE}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, AddServerToMceRunArgs(input=INPUT))

        message = str(_application_error(excinfo.value))
        assert "already hold a BareMetalHost" in message
        assert _IN_USE in message


# --- a re-run of a named install after a failure: its own leftover lock -------

_RERUN_ID = f"add-server-to-mce-{MCE_CLUSTER}_{SERVER_NAME.lower()}"
_OWN_LOCK = ServerLockRecord(
    live=True, holder="add-server-to-mce", workflow_id=_RERUN_ID, mce_cluster=MCE_CLUSTER,
    expires_at="2026-10-31T16:00:00Z",
)
_NAMED_RUN = INPUT.model_copy(update={"server_name": SERVER_NAME})


class TestARerunAfterAFailure:
    """The second half of the incident: the retry "told us it had failed".

    A run that fails after its lock — the NMStateConfig conflict did, at
    create_nmstate_config — leaves the lock to its 2 h TTL ON PURPOSE (its
    half-created resources may still name the machine). The re-run of the
    same named install, on the same workflow id, was meant to extend it. But
    server-scan's `/servers/available` hides a locked machine even from the
    run holding it, so the re-run failed at once with a bare "no assignable
    server" for two hours. A named re-run now looks again and recovers its OWN
    lock; anything else refused says why.
    """

    async def test_the_incident_end_to_end(self):
        calls, segment_acts, server_acts = make_mock_activities(
            nmstate_error=BmhConflictError(
                "NMStateConfig x already exists and does not match this "
                "request: VLAN id: want ['24'], found ['77']"
            ),
            nmstate_error_once=True,
            scan_hides_locked=True,
            record=scan_record(bondable_server(), health=_FINE, lock=_OWN_LOCK),
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)
            # The first run: failed mid-create, lock left to its TTL by design.
            assert _application_error(excinfo.value).type == "BmhConflictError"
            assert calls["release_server"] == []

            # The operator deletes the NMStateConfig and runs it again.
            result = await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)

        assert result.server_name == SERVER_NAME
        assert [r.server_name for r in calls["lookup_install_candidate"]] == [SERVER_NAME]
        # Its lock, extended — never released and re-taken — and given back
        # only at the end, once server-scan shows the server installed.
        assert {c.workflow_id for c in calls["reserve_server"]} == {_RERUN_ID}
        assert [(r.server_name, r.workflow_id) for r in calls["release_server"]] == [(SERVER_NAME, _RERUN_ID)]
        assert calls["order"][-1] == "release_server"
        assert result.allowed_unhealthy == []

    async def test_another_runs_lock_fails_saying_whose_and_until_when(self):
        theirs = ServerLockRecord(
            live=True, holder="add-server-to-mce", workflow_id="add-server-to-mce-ocp4-b_pool_1",
            mce_cluster="ocp4-b", expires_at="2026-10-31T16:00:00Z",
        )
        calls, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(bondable_server(), health=_FINE, lock=theirs)
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)

        error = _application_error(excinfo.value)
        assert error.type == "ServerReservedError"
        message = str(error)
        assert "add-server-to-mce-ocp4-b_pool_1" in message and "2026-10-31T16:00:00Z" in message
        assert "keeps its lock until it expires" in message
        assert "unhealthy" not in message  # the hidden option is never named
        assert calls["reserve_server"] == []

    async def test_an_unhealthy_server_fails_saying_which_check(self):
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(),
            record=scan_record(one_nic_up_server(), health={"overall": "HEALTHY", "network": "MAJOR"}),
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)

        error = _application_error(excinfo.value)
        assert error.type == "ServerNotAvailableError"
        assert "network-health: network MAJOR" in str(error)
        assert "allow_unhealthy" not in str(error)

    async def test_its_own_lock_does_not_excuse_ill_health(self):
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(), record=scan_record(one_nic_up_server(), lock=_OWN_LOCK)
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)
        assert _application_error(excinfo.value).type == "ServerNotAvailableError"

    async def test_a_claimed_server_fails_saying_so(self):
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=_refused(),
            record=scan_record(bondable_server(), health=_FINE, lifecycle="INSTALLED", cluster="hc-a"),
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)

        error = _application_error(excinfo.value)
        assert error.type == "ServerNotAvailableError"
        assert "a cluster claims it" in str(error)

    async def test_a_refusal_the_record_cannot_explain_stands_as_server_scan_gave_it(self):
        refused = _refused()
        _, segment_acts, server_acts = make_mock_activities(
            acquire_error=refused, record=scan_record(bondable_server(), health=_FINE)
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute_as(client, AddServerToMceRunArgs(input=_NAMED_RUN), _RERUN_ID)

        error = _application_error(excinfo.value)
        assert error.type == "ServerNotAvailableError"
        assert error.message == str(refused)  # untouched: the activity's own failure

    async def test_a_pool_draw_never_looks_again(self):
        calls, segment_acts, server_acts = make_mock_activities(acquire_error=_refused())
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError):
                await _execute(client, AddServerToMceRunArgs(input=INPUT))
        assert calls["lookup_install_candidate"] == []


# --- after success: the lock held only until server-scan sees the server --------


class TestTheLockIsReleasedOnceServerScanSeesTheServer:
    """The operator, 2026-11-09: after the Agent, read server-scan every 5 minutes;
    when the server is INSTALLED_TO_INVENTORY or INSTALLED, give the lock back.
    Not seen within the day the lock is held, keep it — it expires on its own —
    and COMPLETE the run all the same: the Agent proved the install."""

    async def test_it_waits_while_available_and_releases_once_installed_to_inventory(self):
        calls, segment_acts, server_acts = make_mock_activities(
            scan_states=["AVAILABLE", "AVAILABLE", "INSTALLED_TO_INVENTORY"], scan_mce="mce-bat-yam-01",
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert [r.server_name for r in calls["lookup_server_membership"]] == [SERVER_NAME] * 3
        # Held for the day first, read, then released — once, with this run's id.
        tail = [n for n in calls["order"] if n in ("lookup_server_membership", "release_server")]
        assert tail == ["lookup_server_membership"] * 3 + ["release_server"]
        assert calls["reserve_server"][-1].ttl_seconds == 86_400
        (released,) = calls["release_server"]
        assert released.workflow_id == calls["reserve_server"][-1].workflow_id
        # The MCE named without its ocp4- prefix is still this MCE.
        assert (result.lock_released, result.server_scan_state) == (True, "INSTALLED_TO_INVENTORY")
        assert result.reservation_expires_at is None and "released" in (result.lock_detail or "")

    async def test_installed_in_a_cluster_releases_too(self):
        calls, segment_acts, server_acts = make_mock_activities(scan_states=["INSTALLED"])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))
        assert len(calls["release_server"]) == 1
        assert (result.lock_released, result.server_scan_state) == (True, "INSTALLED")

    async def test_not_seen_within_the_day_completes_and_keeps_the_lock(self):
        calls, segment_acts, server_acts = make_mock_activities(scan_states=["AVAILABLE"])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))

        assert result.agent_name  # COMPLETED, not failed: the install worked
        assert len(calls["lookup_server_membership"]) == 24 * 60 // 5
        assert calls["release_server"] == []
        assert (result.lock_released, result.server_scan_state) == (False, "AVAILABLE")
        assert result.reservation_expires_at == "in 86400s"
        assert "after 24 h" in (result.lock_detail or "")

    async def test_another_mce_named_by_server_scan_keeps_the_lock(self):
        calls, segment_acts, server_acts = make_mock_activities(
            scan_states=["INSTALLED_TO_INVENTORY"], scan_mce="ocp4-mce-other",
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))
        assert calls["release_server"] == []
        assert result.lock_released is False and "ocp4-mce-other" in (result.lock_detail or "")

    async def test_server_scan_unreadable_completes_and_keeps_the_lock(self):
        calls, segment_acts, server_acts = make_mock_activities(
            scan_error=ServerScanAuthError("server-scan rejected our credentials (401)"),
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, AddServerToMceRunArgs(input=INPUT))
        assert len(calls["lookup_server_membership"]) == 1  # permanent: one read
        assert calls["release_server"] == []
        assert result.lock_released is False and "ServerScanAuthError" in (result.lock_detail or "")


# --- a cancel rolls the install back -------------------------------------------


async def _start(client: Client, args: AddServerToMceRunArgs) -> WorkflowHandle:
    return await client.start_workflow(
        AddServerToMceWorkflow.run, args, id=f"test-{uuid.uuid4()}", task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
    )


async def _until_phase(handle: WorkflowHandle, phase: str) -> None:
    """Poll the progress query until the run reports `phase` (real seconds, bounded)."""
    for _ in range(300):
        if (await handle.query(AddServerToMceWorkflow.progress)).phase == phase:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"the run never reached {phase}")


async def _cancelled(handle: WorkflowHandle):
    """Cancel, wait for the run to end, and return its final progress: it must end CANCELED."""
    with pytest.raises(WorkflowFailureError) as excinfo:
        await asyncio.wait_for(handle.result(), timeout=60)
    assert is_cancelled_exception(excinfo.value.cause), excinfo.value.cause
    return await handle.query(AddServerToMceWorkflow.progress)


async def _child_ran(client: Client, server_name: str) -> RemoveServerFromMceResult | None:
    try:
        return await client.get_workflow_handle(
            remove_server_from_mce_workflow_id(server_name), result_type=RemoveServerFromMceResult
        ).result()
    except RPCError:
        return None


class TestACancelRollsTheInstallBack:
    """The operator, 2026-11-09: Cancel undoes the candidate being installed — before the
    Agent. After the Agent the server stays installed. The run still ends CANCELED."""

    async def test_cancelled_while_waiting_for_the_agent_it_is_taken_out_by_remove_server_from_mce(self):
        calls, segment_acts, server_acts = make_mock_activities(agent_never_for={SERVER_NAME})
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)
            removal = await _child_ran(client, SERVER_NAME)
            history = await handle.fetch_history()

        assert removal is not None and removal.found and removal.mce_cluster == MCE_CLUSTER
        assert (progress.phase, progress.server_name) == ("cancelled", SERVER_NAME)
        assert "taken out of the MCE by remove-server-from-mce-" in (progress.cancel_rollback or "")
        assert (progress.cancel_rollback or "").endswith("; install lock released")
        # The child did the removal; the lock, which the child leaves to its holder, here.
        assert calls["delete_host_resources"] == []
        assert [(r.server_name, r.holder) for r in calls["release_server"]] == [
            (SERVER_NAME, "add-server-to-mce")
        ]
        # The cancel path replays as recorded.
        await Replayer(
            workflows=[AddServerToMceWorkflow, InstallServerWorkflow], data_converter=pydantic_data_converter,
        ).replay_workflow(history)

    async def test_cancelled_while_checking_the_allocated_server_its_lock_is_released(self):
        # Allocated = locked for this run, before anything is written for it.
        gate = asyncio.Event()
        calls, segment_acts, server_acts = make_mock_activities(probe_gate=gate)
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "selecting-server")
            await handle.cancel()
            # The run took the cancel and waits for calls in flight to land
            # before it gives the lock back.
            await _until_phase(handle, "cancel-settling")
            gate.set()
            await asyncio.sleep(0.2)
            progress = await _cancelled(handle)
            removal = await _child_ran(client, SERVER_NAME)

        assert removal is None  # nothing was written on the MCE
        assert calls["create_bmc_secret"] == [] and calls["replace_conflicting_nmstate_config"] == []
        assert [r.server_name for r in calls["release_server"]] == [SERVER_NAME]
        assert progress.phase == "cancelled" and "install lock released" in (progress.cancel_rollback or "")

    async def test_cancelled_while_server_scan_allocates_the_lock_is_left_to_expire(self):
        """A cancel stops waiting for the allocation, not the allocation: server-scan may
        still lock a server for this run, which the run never learns — its TTL frees it."""
        gate = asyncio.Event()
        calls, segment_acts, server_acts = make_mock_activities(acquire_gate=gate)
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "acquiring-server")
            await handle.cancel()
            await _until_phase(handle, "cancelled")
            gate.set()
            progress = await _cancelled(handle)

        assert calls["reserve_server"] == [] and calls["release_server"] == []
        assert progress.phase == "cancelled"
        assert "nothing known to undo" in (progress.cancel_rollback or "")
        assert "frees itself when its lock expires (120 min)" in (progress.cancel_rollback or "")

    async def test_cancelled_while_locking_a_named_server_the_lock_that_still_lands_is_released(self):
        """The run's own lock call — a named server's leftover lock, re-taken — cut off
        by the cancel: it lands after the cancel, and is released all the same."""
        gate = asyncio.Event()
        server = bondable_server()
        calls, segment_acts, server_acts = make_mock_activities(
            reserve_gate=gate,
            record=scan_record(
                server, health=_FINE,
                lock=ServerLockRecord(live=True, holder="add-server-to-mce", workflow_id="add-server-to-mce-own"),
            ),
            # server-scan's allocation refuses a locked server even to its holder.
            acquire_error=ServerNotAvailableError(
                f"server-scan could not allocate a server (name={SERVER_NAME!r}): RESERVED"
            ),
        )
        named = INPUT.model_copy(update={"server_name": SERVER_NAME})
        async with _Harness(segment_acts, server_acts) as client:
            handle = await client.start_workflow(
                AddServerToMceWorkflow.run, AddServerToMceRunArgs(input=named),
                id="add-server-to-mce-own", task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
            )
            await _until_phase(handle, "reserving-server")
            await handle.cancel()
            await _until_phase(handle, "cancel-settling")
            gate.set()
            await asyncio.sleep(0.2)
            progress = await _cancelled(handle)

        assert calls["create_bmc_secret"] == []
        assert [r.server_name for r in calls["release_server"]] == [SERVER_NAME]
        assert progress.phase == "cancelled" and "install lock released" in (progress.cancel_rollback or "")

    async def test_cancelled_after_the_agent_the_server_stays_installed(self):
        calls, segment_acts, server_acts = make_mock_activities(scan_states=["AVAILABLE"])
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "awaiting-server-scan")
            await handle.cancel()
            progress = await _cancelled(handle)
            removal = await _child_ran(client, SERVER_NAME)

        assert removal is None and calls["delete_host_resources"] == [] and calls["release_server"] == []
        assert progress.phase == "cancelled" and "is installed and stays" in (progress.cancel_rollback or "")

    async def test_a_failed_pxe_cleanup_in_the_child_keeps_the_lock(self):
        # Its MACs may still chain to this MCE's discovery image.
        server = ucs_server(name="ocp-cisco-m6-bat-yam-64c-512gb-pxefail")
        calls, segment_acts, server_acts = make_mock_activities(candidates=[server], agent_never_for={server.name})
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)

        assert calls["release_server"] == []
        assert "lock kept: the PXE cleanup failed" in (progress.cancel_rollback or "")

    async def test_a_child_from_before_that_released_the_lock_is_not_doubled(self):
        server = ucs_server(name="ocp-cisco-m6-bat-yam-64c-512gb-oldchild")
        calls, segment_acts, server_acts = make_mock_activities(candidates=[server], agent_never_for={server.name})
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)

        assert calls["release_server"] == []
        assert (progress.cancel_rollback or "").endswith(f"taken out of the MCE by remove-server-from-mce-{server.name.lower()}")

    async def test_a_blade_the_child_finds_nowhere_has_its_pxe_mapping_and_lock_undone_here(self):
        blade = ucs_server(name="ocp-cisco-m6-bat-yam-64c-512gb-nowhere")
        calls, segment_acts, server_acts = make_mock_activities(candidates=[blade], agent_never_for={blade.name})
        async with _Harness(segment_acts, server_acts) as client:
            handle = await _start(client, AddServerToMceRunArgs(input=INPUT))
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)
            removal = await _child_ran(client, blade.name)

        assert removal is not None and not removal.found
        assert [r.macs for r in calls["unregister_pxe_boot"]] == [r.macs for r in calls["register_pxe_boot"]]
        assert [r.server_name for r in calls["release_server"]] == [blade.name]
        assert "PXE mapping removed, install lock released" in (progress.cancel_rollback or "")
