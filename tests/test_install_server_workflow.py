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
from shared.exceptions import (
    InventorySegmentNotFoundError,
    ServerNotAvailableError,
    ServerReservedError,
    ServerScanAuthError,
)
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    AgentRef,
    AgentState,
    BmcEndpoint,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    InstallServerInput,
    InstallServerRunArgs,
    LinkState,
    ReleaseServerRequest,
    ReserveServerRequest,
    ServerInterface,
    ServerReservation,
    TeardownResult,
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
    finalizers_stuck: bool = False,
    reserved_elsewhere: set[str] | None = None,
    gone_from_inventory: set[str] | None = None,
    lock_lapses_for: set[str] | None = None,
    reserve_error: Exception | None = None,
):
    """Build the full mock activity set + a call recorder.

    `get_baremetal_host` serves two callers and this mock answers as the real
    cluster would: BEFORE the BareMetalHost is created it reports whether that
    server is in `already_installed` (the candidate probe), and afterwards it
    answers the ONE diagnostic read the workflow takes when an Agent never
    appeared — from `bmh_script` when given.

    `agent_never_for` names the servers that never register an Agent, which is
    what drives a rollback; `agent_after_polls` delays the Agent by that many
    polls for the servers that do. Both are per-SERVER rather than global so a
    pool can be set up to fail its first candidate and install its second, which
    is the case the retry loop exists for.

    The install lock behaves as server-scan's does: a claim by the same workflow
    extends, `reserved_elsewhere` names servers another run holds, and
    `lock_lapses_for` names servers whose lock another run takes between the
    first claim and the renewal before the Agent wait.
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
            "find_agent_for_host",
            "teardown_bmh_resources",
            "reserve_server",
            "release_server",
        )
    }
    drawn = candidates if candidates is not None else [bondable_server()]
    script = list(bmh_script or [])
    installed = already_installed or set()
    no_agent = agent_never_for or set()
    torn_down: set[str] = set()
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
        if ref.server_name in torn_down:
            # Rolled back: the machine is back in the inventory and the host is
            # gone, which is what lets the same pool be drawn again later.
            return BmhState(found=False)
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
    async def teardown_bmh_resources(ref: BmhRef) -> TeardownResult:
        calls["teardown_bmh_resources"].append(ref)
        if teardown_error is not None:
            raise teardown_error
        torn_down.add(ref.server_name)
        return TeardownResult(
            removed=[
                f"NMStateConfig/nmstate-config-{ref.server_name}",
                f"BareMetalHost/{ref.server_name}",
            ],
            finalizers_cleared=finalizers_stuck,
        )

    held_elsewhere = set(reserved_elsewhere or ())
    gone = gone_from_inventory or set()
    lapses = lock_lapses_for or set()
    locks: dict[str, str] = {}

    @activity.defn
    async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
        calls["reserve_server"].append(request)
        if reserve_error is not None:
            raise reserve_error
        if request.server_name in gone:
            raise ServerNotAvailableError(f"{request.server_name} is not in inventory")
        if request.server_name in lapses and request.server_id in locks:
            held_elsewhere.add(request.server_name)
            del locks[request.server_id]
        if request.server_name in held_elsewhere:
            raise ServerReservedError(
                f"{request.server_name} is reserved by 'install-server' for MCE "
                "'ocp4-mce-other'"
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
        if locks.get(request.server_id) == request.workflow_id:
            del locks[request.server_id]
            return ServerReservation(server_id=request.server_id, held=False)
        return ServerReservation(
            server_id=request.server_id, held=False, detail="not ours"
        )

    segment_activities = [get_inventory_segment]
    server_activities = [
        acquire_servers,
        create_bmc_secret,
        create_baremetal_host,
        create_nmstate_config,
        get_baremetal_host,
        find_agent_for_host,
        teardown_bmh_resources,
        reserve_server,
        release_server,
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


async def test_happy_path_resolves_vlan_acquires_creates_and_awaits_the_agent():
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

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
            await _execute(client, InstallServerRunArgs(input=INPUT))

        assert calls["get_inventory_segment"] == [MCE_CLUSTER]

    async def test_a_ucs_blade_uses_the_same_segment_as_a_dell(self) -> None:
        """What the revert removed: the BMC protocol no longer picks a network.

        A UCS blade is driven over IPMI and a Dell over Redfish, and both now
        boot on the one inventory VLAN the MCE owns.
        """
        ucs = bondable_server().model_copy(update={"bmc_vendor": "CISCO"})
        calls, segment_acts, server_acts = make_mock_activities(candidates=[ucs])
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.vlan_id == VLAN_ID
        assert calls["get_inventory_segment"] == [MCE_CLUSTER]

    async def test_the_segment_is_resolved_once_per_run_not_per_candidate(
        self,
    ) -> None:
        # A lookup per candidate would be a round trip to the Segments Manager
        # for every server in the draw, for an answer that cannot differ.
        pool = [
            bondable_server("srv_a", name="ocp-a"),
            bondable_server("srv_b", name="ocp-b"),
            bondable_server("srv_c", name="ocp-c"),
        ]
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=pool, already_installed={"ocp-a", "ocp-b"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.server_name == "ocp-c"
        assert calls["get_inventory_segment"] == [MCE_CLUSTER]

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
                await _execute(client, InstallServerRunArgs(input=INPUT))

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


async def test_a_host_that_never_produces_an_agent_is_rolled_back_and_retried():
    """The whole point of the loop: an hour-deep failure is a SKIP, not the run's.

    The first candidate is created, waited on for the full deadline, torn down —
    which returns the machine to the inventory — and the SECOND candidate is
    installed. Anything less would strand a machine with a BareMetalHost that no
    other MCE can see, because server-scan reports it unclaimed until a cluster
    reports the node.
    """
    doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
    good = bondable_server("srv_good")
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[doomed, good], agent_never_for={"ocp-doomed"}
    )
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.server_name == SERVER_NAME
    assert result.attempts == 2
    assert result.agent_name == f"agent-{SERVER_NAME}"
    # BOTH were created; only the doomed one was torn down.
    assert [c.server_name for c in calls["create_baremetal_host"]] == [
        "ocp-doomed",
        SERVER_NAME,
    ]
    assert [t.server_name for t in calls["teardown_bmh_resources"]] == ["ocp-doomed"]


async def test_a_whole_pool_without_agents_fails_and_tears_every_one_down():
    """Nothing left to try is the only thing that fails the run."""
    first = bondable_server("srv_1", name="ocp-one", mac_prefix="11:22:33:44:55")
    second = bondable_server("srv_2", name="ocp-two", mac_prefix="66:77:88:99:aa")
    calls, segment_acts, server_acts = make_mock_activities(
        candidates=[first, second], agent_never_for={"ocp-one", "ocp-two"}
    )
    async with _Harness(segment_acts, server_acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "AgentNeverAppearedError"
    # No machine is left holding a BareMetalHost.
    assert [t.server_name for t in calls["teardown_bmh_resources"]] == [
        "ocp-one",
        "ocp-two",
    ]


async def test_an_agent_that_takes_its_time_is_waited_for():
    """Bare metal POSTs for longer than a VM takes to boot; that is not a failure."""
    calls, segment_acts, server_acts = make_mock_activities(agent_after_polls=20)
    async with _Harness(segment_acts, server_acts) as client:
        result = await _execute(client, InstallServerRunArgs(input=INPUT))

    assert result.attempts == 1
    assert result.bmh_registered is True
    assert len(calls["find_agent_for_host"]) == 21
    # Waited, never rolled back.
    assert calls["teardown_bmh_resources"] == []


async def test_the_agent_is_looked_for_by_bond_mac_not_by_name():
    """MAC is the only link that holds: an Agent has no back-reference to the host.

    BMAC names an Agent after the host's own inventory UUID, and the live CRD's
    `agent.spec` carries approved/clusterDeploymentName/role and nothing else. A
    lookup keyed on the server's name would work only by accident.
    """
    calls, segment_acts, server_acts = make_mock_activities()
    async with _Harness(segment_acts, server_acts) as client:
        await _execute(client, InstallServerRunArgs(input=INPUT))

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
            await _execute(client, InstallServerRunArgs(input=INPUT))

    message = str(_application_error(excinfo.value))
    assert "registration error" in message
    assert "BMC address or credential" in message
    assert len(calls["teardown_bmh_resources"]) == 1


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
            await _execute(client, InstallServerRunArgs(input=INPUT))

    message = str(_application_error(excinfo.value))
    assert "bond" in message and "VLAN" in message and "DHCP" in message
    assert "not at" in message  # ... not at the BMC


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
            await _execute(client, InstallServerRunArgs(input=INPUT))

    error = _application_error(excinfo.value)
    assert error.type == "NoBondableInterfacesError"
    assert "already hold a BareMetalHost" in str(error)
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
            await _execute(client, InstallServerRunArgs(input=INPUT))

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


class TestTheInstallLock:
    """server-scan's install lock (ADR-0035) — the guard ACROSS MCEs.

    This run's BareMetalHost probe only sees its own MCE, and server-scan
    reports a machine AVAILABLE until a cluster reports it. Without the lock, a
    server installed into one MCE is drawn again by a run for another.
    """

    async def test_the_chosen_machine_is_locked_renewed_and_held_after_success(self):
        calls, segment_acts, server_acts = make_mock_activities()
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        claims = calls["reserve_server"]
        # Taken, renewed as the Agent wait begins, then held for a day.
        assert [c.ttl_seconds for c in claims] == [7200, 7200, 86_400]
        assert {c.server_id for c in claims} == {"srv_001"}
        assert {c.holder for c in claims} == {"install-server"}
        # One workflow id across all three: server-scan EXTENDS a lock its own
        # run re-takes, and treats anyone else's as a lost race.
        assert len({c.workflow_id for c in claims}) == 1
        assert claims[0].workflow_id.startswith("test-")
        assert claims[0].mce_cluster == MCE_CLUSTER
        assert claims[0].infra_env == INFRA_ENV
        assert claims[0].namespace == NAMESPACE
        assert calls["release_server"] == []
        assert result.reservation_expires_at == "in 86400s"

    async def test_a_machine_another_run_holds_is_skipped_before_anything_is_written(self):
        held = bondable_server("srv_held", name="ocp-held", mac_prefix="11:22:33:44:55")
        free = bondable_server("srv_free")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[held, free], reserved_elsewhere={"ocp-held"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        # A refused lock is not an attempt: nothing was installed or watched.
        assert result.attempts == 1
        assert [c.server_name for c in calls["create_bmc_secret"]] == [SERVER_NAME]
        assert calls["teardown_bmh_resources"] == []

    async def test_a_pool_held_elsewhere_fails_naming_the_holder(self):
        calls, segment_acts, server_acts = make_mock_activities(
            reserved_elsewhere={SERVER_NAME}
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, InstallServerRunArgs(input=INPUT))

        error = _application_error(excinfo.value)
        assert error.type == "ServerReservedError"
        assert "ocp4-mce-other" in error.message
        assert calls["create_bmc_secret"] == []

    async def test_a_server_gone_from_the_inventory_is_skipped(self):
        gone = bondable_server("srv_gone", name="ocp-gone", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[gone, bondable_server()], gone_from_inventory={"ocp-gone"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        assert [c.server_name for c in calls["create_bmc_secret"]] == [SERVER_NAME]

    async def test_an_already_installed_candidate_is_never_locked(self):
        # The probe runs first: locking a machine this MCE already holds would
        # only withhold it from the operator's view of the fleet.
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[bondable_server("srv_a", name="ocp-a"), bondable_server()],
            already_installed={"ocp-a"},
        )
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, InstallServerRunArgs(input=INPUT))

        assert {c.server_name for c in calls["reserve_server"]} == {SERVER_NAME}

    async def test_a_rolled_back_machine_is_released_after_its_teardown(self):
        doomed = bondable_server("srv_doomed", name="ocp-doomed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[doomed, bondable_server()], agent_never_for={"ocp-doomed"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            await _execute(client, InstallServerRunArgs(input=INPUT))

        released = calls["release_server"]
        assert [r.server_id for r in released] == ["srv_doomed"]
        assert released[0].holder == "install-server"
        assert released[0].workflow_id == calls["reserve_server"][0].workflow_id

    async def test_a_lock_lost_before_the_agent_wait_rolls_back_without_releasing(self):
        """Lapsed and taken by another run: undo OUR resources, leave THEIR lock."""
        lapsed = bondable_server("srv_lapsed", name="ocp-lapsed", mac_prefix="11:22:33:44:55")
        calls, segment_acts, server_acts = make_mock_activities(
            candidates=[lapsed, bondable_server()], lock_lapses_for={"ocp-lapsed"}
        )
        async with _Harness(segment_acts, server_acts) as client:
            result = await _execute(client, InstallServerRunArgs(input=INPUT))

        assert result.server_name == SERVER_NAME
        assert [t.server_name for t in calls["teardown_bmh_resources"]] == ["ocp-lapsed"]
        assert calls["release_server"] == []
        # Never watched for an Agent: the machine is someone else's now.
        assert all(
            "11:22:33:44:55:01" not in ref.macs for ref in calls["find_agent_for_host"]
        )

    async def test_a_token_without_the_admin_role_fails_the_run(self):
        calls, segment_acts, server_acts = make_mock_activities(
            reserve_error=ServerScanAuthError("needs the ADMIN role")
        )
        async with _Harness(segment_acts, server_acts) as client:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await _execute(client, InstallServerRunArgs(input=INPUT))

        assert _application_error(excinfo.value).type == "ServerScanAuthError"
        # Once: permanent, not retried every minute, and not a per-candidate skip.
        assert len(calls["reserve_server"]) == 1
        assert calls["create_bmc_secret"] == []
