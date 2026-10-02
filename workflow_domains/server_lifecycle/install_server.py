"""install-server — takes one healthy, unclaimed server from the server-scan
inventory platform and makes it installable in an MCE InfraEnv, by creating the
BareMetalHost + BMC Secret + NMStateConfig that the Assisted Installer needs.

The FIRST workflow of the `server-lifecycle` domain. It replaces the
`bmh-generator-operator` (BareMetalHostUCS), a Kopf operator that queried HP
OneView / UCS Central / Dell OME / Intersight live on every reconcile to learn a
server's MACs and BMC address. server-scan already collects that from all five
collectors on a 6-hour cron and knows which servers are unclaimed, so the whole
vendor-strategy layer is gone: one HTTP call replaces four vendor SDKs.

Shape of the run:

  1. resolving-vlan      — the VLAN belongs to the target MCE, not to the
                           server or the caller: each MCE owns one inventory
                           segment, looked up in the Segments Manager by
                           cluster name. Runs on the SEGMENT-LIFECYCLE queue,
                           where that credential already lives.
  2. acquiring-server    — GET /servers/available. The InfraEnv name states
                           which hardware it is for
                           (cisco-m6-bat-yam-64c-512gb), and server names carry
                           the same tokens behind an `ocp-` prefix, so the
                           InfraEnv IS the query. Several candidates are drawn
                           so one unusable server is a retry, not a failure.
  3. selecting-server    — the first candidate that can actually be installed.
                           EVERY reason a candidate is unusable is a skip here,
                           so the draw keeps its promise; the reasons are
                           reported together if none survives.
     reserving-server    — server-scan's install lock on the chosen machine
                           (ADR-0035). A lock another run holds is one more
                           skip reason.
  4. creating-secret
     creating-baremetalhost
     creating-nmstateconfig  — all idempotent; an existing resource is success.
  5. awaiting-agent      — a BOUNDED wait for an AGENT to register for the
                           host, which is the only observation that proves the
                           install worked: an Agent exists because the machine
                           booted the discovery ISO and reached
                           assisted-service, so the BMC accepted virtual media,
                           the bond formed, the VLAN was right and DHCP
                           answered. An hour, because bare metal POSTs for
                           longer than a VM takes to boot. The lock is renewed
                           on entry so it covers the whole wait.
     holding-server      — on success the lock is extended to a day, so the
                           machine stays out of every other MCE's draw until
                           server-scan's membership jobs see it in a cluster.
  6. rolling-back        — no Agent by the deadline is that CANDIDATE's
     releasing-server      failure, not the run's: the NMStateConfig and
                           BareMetalHost are removed and the lock released,
                           which returns the machine to the inventory, and the
                           next candidate is tried from step 3.

Steps 3-6 are therefore a LOOP, not a pipeline. A candidate cannot be shown to
be installable without creating its resources and watching what happens, so the
draw is a list of things to TRY rather than to validate, and an hour-deep
failure is a skip like any other. The run fails only when nothing is left.

THE VLAN IS THE MCE'S, AND IS RESOLVED ONCE. An MCE owns one inventory network,
so its VLAN is a property of the cluster this run targets and nothing else — not
of the machine chosen, and not of the caller, who could otherwise supply a VLAN
contradicting the segment the cluster actually owns. It is read before any
candidate is drawn, because no candidate could make a missing allocation usable.
The lookup runs on the SEGMENT-LIFECYCLE queue, where that credential already
lives, rather than putting a second copy of the Segments Manager token on this
domain's worker.

(It was briefly resolved per candidate instead, while INVENTORY was split into
INVENTORY_REDFISH and INVENTORY_IPMI by how a server's BMC is driven — an MCE
then held up to two inventory segments and which applied was not known until a
machine was chosen. Reverted on both sides: one inventory scope per cluster,
found by cluster name.)

This module is the SHAPE of the run only. The rule deciding which two NICs carry
the bond is policy with no I/O in it, so it lives in bond_selection.py and is
tested without a Temporal environment; resource naming and the BMC address live
in shared/ because the limb needs them too.

CROSSING CLUSTERS. This workflow runs on the hub, but the resources belong on
the MCE that owns the InfraEnv — a different API server at its own
`api.<mce>.<domain>`. `mce_cluster` is what bridges that: it names both the
inventory segment to read the VLAN from AND the activity queue the cluster
writes are dispatched to (`server-lifecycle-activity-<mce_cluster>`).

One server-lifecycle-worker runs INSIDE each MCE, polls only its own queue,
and talks to its own API server as its own ServiceAccount. So no cross-cluster
kubeconfig exists anywhere, the hub never needs inbound access to an MCE, and
the VLAN's cluster cannot disagree with the cluster written to — they are the
same string. If no worker polls that queue the run waits rather than acting,
and the `progress` query names the queue so it reads as "that MCE has no
worker" instead of "the run is stuck".

TEARDOWN IS PART OF THE LOOP, NOT A CANCELLATION HANDLER. A candidate that
never produced an Agent is rolled back because the run intends to try another
machine, and leaving a BareMetalHost behind would make that machine undrawable
here and invisible to every other MCE — server-scan reports it unclaimed until
a cluster reports the node, so nothing else would notice. On CANCELLATION there
is still deliberately no compensating cleanup: the three resources are
idempotent, so a re-run converges on them, and a half-created set is what an
operator needs in order to see how far the run got. Removing a SUCCESSFULLY
installed server remains a separate uninstall-server workflow.

THE INSTALL LOCK IS WHAT MAKES "UNCLAIMED" TRUE ACROSS MCEs. server-scan
reports a machine AVAILABLE until a cluster reports it, and this run's
BareMetalHost probe only sees its OWN MCE — so without the lock a server
installed into MCE-A is drawn again by a run for MCE-B, and both clusters drive
one BMC. The lock is taken only once a candidate is chosen (server-scan's draw
cannot know which one will be), keyed on this run's workflow id so a retry or a
renewal is an extension rather than a lost race, and it expires on its own, so
a run that dies costs one TTL rather than the machine. It is released only
after a teardown has proved nothing on the cluster still points at the machine
— a failure anywhere else leaves it held until it expires, deliberately, since
half-created resources may still name it. Runs started before the lock existed
replay without it (`workflow.patched`).

The teardown order is load-bearing and was established against a live cluster:
metal3 must be told to detach the host BEFORE the delete, or its
`baremetalhost.metal3.io` finalizer blocks forever trying to deprovision
through the BMC that just failed to answer. See teardown_bmh_resources.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.bmc_address import (
        BMC_VENDORS,
        build_bmc_address,
        is_k8s_resource_name,
    )
    from shared.consts import (
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        server_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        AmbiguousInventorySegmentError,
        AmbiguousServerNameError,
        BmcCredentialsMissingError,
        BmcEndpointMissingError,
        BmhConflictError,
        AgentNeverAppearedError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        InvalidMacError,
        InvalidServerNameError,
        InventorySegmentMismatchError,
        InventorySegmentNotFoundError,
        NoBondableInterfacesError,
        SegmentsManagerAuthError,
        ServerNotAvailableError,
        ServerReservedError,
        ServerScanAuthError,
        ServerScanRequestInvalidError,
        UnknownBmcVendorError,
    )
    from shared.interfaces.segment_lifecycle import get_inventory_segment
    from shared.models.segment_lifecycle import SegmentEntry
    from shared.interfaces.server_lifecycle import (
        acquire_servers,
        create_baremetal_host,
        create_bmc_secret,
        create_nmstate_config,
        find_agent_for_host,
        get_baremetal_host,
        release_server,
        reserve_server,
        teardown_bmh_resources,
    )
    from shared.models.server_lifecycle import (
        AcquiredServer,
        AcquireServerRequest,
        AgentRef,
        AgentState,
        BmhRef,
        BmhResourceRequest,
        BmhState,
        BondMember,
        CreatedResource,
        InstallServerProgress,
        InstallServerInput,
        InstallServerResult,
        InstallServerRunArgs,
        ReleaseServerRequest,
        ReserveServerRequest,
        ServerReservation,
        TeardownResult,
        mac_is_valid,
    )
    from workflow_domains.server_lifecycle.bond_selection import (
        BOND_MEMBER_COUNT,
        describe_candidate,
        select_bond_members,
    )

# Same budget rules as the segment-lifecycle workflows: a bounded per-attempt
# timeout (with the HTTP client timing out below it) and UNBOUNDED retries, so a
# transient outage is out-waited.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)

# The ACTIVITY-raised failures that no retry can fix. Listed as CLASSES and
# reduced to names below, not written out as string literals: Temporal matches
# this list against the error type name, so a literal with a typo in it reads as
# "retry forever" and there is nothing to notice — the run sits RUNNING rather
# than FAILED, with the failure buried on the activity. A wrong class name is an
# ImportError at worker startup instead.
#
# Workflow-raised failures are deliberately absent: `non_retryable_error_types`
# is inert for them (Temporal never retries a workflow failure), so listing
# UnknownBmcVendorError or NoBondableInterfacesError here would only suggest it
# was doing something. See shared/exceptions.py for which kind each error is.
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    ServerScanAuthError,
    ServerNotAvailableError,
    ServerReservedError,
    ServerScanRequestInvalidError,
    AmbiguousServerNameError,
    BmcCredentialsMissingError,
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    InvalidMacError,
    InvalidServerNameError,
    AmbiguousInventorySegmentError,
    InventorySegmentNotFoundError,
    SegmentsManagerAuthError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# How long a cluster write may sit in an MCE's queue before Temporal reports
# that nobody picked it up. Temporal advises against schedule-to-start timeouts
# in general; the exception it names is exactly this case — a queue served by
# specific hosts, where the timeout is what distinguishes "no worker is running
# there" from "the work is slow". Retryable, because a worker rolling out is a
# normal few-second gap, so the signal is repeated timeouts in history rather
# than a failed run.
_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)

# THE SUCCESS SIGNAL, on a bare-metal-shaped deadline. An Agent appears only
# once the host has POSTed, booted the discovery ISO and reached
# assisted-service; on real hardware POST alone outlasts a VM's entire boot, so
# the wait is an hour. Decided with the operator, 2026-09-27.
#
# This replaced a 10-minute Ironic-registration deadline, which was the wrong
# signal in both directions. It PASSED hosts that would never boot — a host
# whose bond or VLAN is wrong registers perfectly and is then never heard from
# again — and it could not recognise a dead one: on an unreachable BMC metal3
# reports provisioning state `registering` with operationalStatus OK and no
# errorType at all, observed unchanged for 14 hours on a live cluster. So
# nothing but the deadline itself ever distinguished a slow host from a dead
# one, while the Agent distinguishes them by existing.
#
# MACHINE convergence still, so still a real deadline — but reaching it is a
# CANDIDATE's failure, not the run's: the resources are torn down, the machine
# goes back to the inventory and the next candidate is tried.
#
# Changing either constant is a non-deterministic change for in-flight runs.
_AGENT_DEADLINE = timedelta(hours=1)
_AGENT_POLL_INTERVAL = timedelta(seconds=30)

# server-scan's install lock (ADR-0035). The TTL while installing covers the
# Agent wait plus an hour, and is renewed as the wait begins, so creates that
# stalled on a missing MCE worker cannot let it lapse mid-install. Once an Agent
# exists the machine IS taken, so holding it longer costs nothing: a day — the
# ceiling server-scan accepts — outlasts any membership job that will then
# report it in a cluster.
_RESERVATION_HOLDER = "install-server"
_RESERVATION_TTL = _AGENT_DEADLINE + timedelta(hours=1)
_INSTALLED_HOLD = timedelta(hours=24)
# Gates every lock call, so a run started before the lock replays unchanged.
_RESERVATION_PATCH = "server-scan-install-lock"

# The server-scan health verdict this workflow accepts. Deliberately only the
# top tier: the endpoint would otherwise fill HEALTHY, then WARNING, then MAJOR.
_REQUIRED_HEALTH = "HEALTHY"

# One idempotent create, as the workflow addresses it: every one of the three
# takes the same request and answers the same way, which is what lets them share
# one dispatch helper instead of three near-identical execute_activity calls.
_ResourceCreate = Callable[[BmhResourceRequest], Awaitable[CreatedResource]]


def _acquire_request(
    server_name: str | None, infra_env: str, candidate_count: int
) -> AcquireServerRequest:
    """What to ask server-scan for: one named machine, or a pool draw."""
    if server_name:
        return AcquireServerRequest(
            name=server_name,
            count=1,
            health=_REQUIRED_HEALTH,
            min_nic_macs=BOND_MEMBER_COUNT,
        )
    return AcquireServerRequest(
        # Escaped: the InfraEnv name is caller data going into a regex.
        # Today's names are [a-z0-9-] so it changes nothing, but a `.`
        # would silently widen the pool without it.
        pattern=f"^ocp-{re.escape(infra_env)}",
        count=candidate_count,
        health=_REQUIRED_HEALTH,
        min_nic_macs=BOND_MEMBER_COUNT,
    )


# Why one candidate could not be installed, and the failure a run gets when
# EVERY candidate was rejected for that ONE reason. A mixed pool falls back to
# the general "nothing installable", because no single type is then true.
#
# Every one of these is a SKIP, not a failure: the draw exists so that one
# unusable server is a retry rather than a failed run, and a reason that failed
# the run outright would quietly break that promise for part of the fleet.
_REJECTION_TYPE = {
    "unnameable": InvalidServerNameError,
    "no-bond": NoBondableInterfacesError,
    "already-installed": NoBondableInterfacesError,
    "unknown-bmc-vendor": UnknownBmcVendorError,
    "no-bmc-host": BmcEndpointMissingError,
    "bad-mac": InvalidMacError,
    "no-agent": AgentNeverAppearedError,
    "reserved": ServerReservedError,
    "gone": ServerNotAvailableError,
}

_REJECTION_SUMMARY = {
    "unnameable": (
        "cannot be a Kubernetes resource name even lowercased — rename these "
        "in server-scan"
    ),
    "no-bond": (
        "offer fewer than two link-up NICs on two distinct physical ports. "
        "HPE OneView reports no link state at all and Intersight vNICs usually "
        "report none, so servers from those collectors cannot satisfy a strict "
        "link-up requirement"
    ),
    "already-installed": "already hold a BareMetalHost in this namespace",
    "unknown-bmc-vendor": (
        "have no BMC driver vocabulary server-scan can name. A STANDALONE "
        "machine's driver is the caller's decision, and guessing IPMI would be "
        "silently wrong for a Redfish-only BMC"
    ),
    "no-bmc-host": (
        "have no BMC host in the inventory, so Ironic would have nothing to "
        "connect to. A collector run fills this in"
    ),
    "bad-mac": (
        "carry a malformed MAC. server-scan normalizes MACs on ingest, so this "
        "is a bad payload rather than a condition to wait out"
    ),
    "no-agent": (
        "were installed and never registered an Agent before the deadline, so "
        "each was rolled back and its machine returned to the inventory. An "
        "Agent appears once the host boots the discovery ISO and reaches "
        "assisted-service, so what failed is DOWNSTREAM of the BareMetalHost — "
        "the BMC, the bond, the VLAN, or DHCP on that inventory segment. Each "
        "entry below says which of those the host's own state points at"
    ),
    "reserved": (
        "are held by another install's lock in server-scan — most likely being "
        "installed into another MCE right now. Each entry names the holder and "
        "when its lock expires"
    ),
    "gone": (
        "left server-scan's inventory between the draw and the reservation, so "
        "there was nothing to lock"
    ),
}


def _no_installable_candidate(
    infra_env: str,
    namespace: str,
    candidates: list[AcquiredServer],
    rejections: list[tuple[str, str]],
) -> ApplicationError:
    """The failure for a draw in which no candidate could be installed.

    Every reason is reported as its own group, because they are different
    people's problem: a pool in use, an inventory that needs renaming, a
    collector that cannot report link state, an MCE missing a segment. Merging
    them into one sentence sends whoever reads it to the wrong place.

    The failure TYPE is the specific one when every candidate was rejected for
    the same reason — which is the common case, and the whole case when a
    server was named explicitly — and the general one otherwise.
    """
    by_reason: dict[str, list[str]] = {}
    for reason, description in rejections:
        by_reason.setdefault(reason, []).append(description)

    groups = [
        f"{_REJECTION_SUMMARY[reason]}: " + "; ".join(described)
        for reason, described in by_reason.items()
    ]
    reasons = set(by_reason)
    error = (
        _REJECTION_TYPE[reasons.pop()]
        if len(reasons) == 1
        else NoBondableInterfacesError
    )
    return ApplicationError(
        f"No candidate for InfraEnv {infra_env} could be installed into "
        f"{namespace}. Checked {len(candidates)} candidate(s) — "
        + ". ".join(groups),
        type=error.__name__,
    )


def _describe_bmh_state(state: BmhState) -> str:
    """What the last poll actually observed, for the deadline failure."""
    if not state.found:
        return "the BareMetalHost was never observed"
    return (
        f"provisioning state {state.provisioning_state!r}, "
        f"operationalStatus {state.operational_status!r}"
        f"{f', errorType {state.error_type!r}' if state.error_type else ''}"
        f"{f', error: {state.error_message}' if state.error_message else ''}"
    )


def _describe_no_agent(server_name: str, state: BmhState | None) -> str:
    """Why one candidate is believed to have failed, for the rejection group.

    The BareMetalHost's own state cannot say an install SUCCEEDED — that is the
    whole reason this workflow waits on an Agent instead — but it is decisive
    about where a failure lies. Ironic reporting a registration error means the
    BMC was never reached, which is an address or a credential. Ironic content
    with the host while no Agent ever appeared means the opposite: the BMC
    worked and the host was told to boot, so the fault is on the wire — the
    bond, the VLAN, or DHCP on that inventory segment.
    """
    minutes = int(_AGENT_DEADLINE.total_seconds() // 60)
    if state is not None and state.is_registration_error():
        return (
            f"{server_name} (no Agent in {minutes} min, and Ironic never "
            f"registered the host: errorType {state.error_type!r}"
            f"{f' — {state.error_message}' if state.error_message else ''}. "
            f"The BMC address or credential is the place to look)"
        )
    observed = _describe_bmh_state(state) if state is not None else "not observed"
    return (
        f"{server_name} (no Agent in {minutes} min; {observed}. Ironic raised no "
        f"registration error, so the BMC answered and the host was driven to "
        f"boot — look at the bond, the VLAN and DHCP on this segment, not at "
        f"the BMC)"
    )


@workflow.defn
class InstallServerWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._server_name: str | None = None
        self._activity_queue: str | None = None
        self._reserving = False

    @workflow.query
    def progress(self) -> InstallServerProgress:
        """Cheap progress surface for the async caller (GET status endpoint).

        `activity_queue` is what makes a stalled run diagnosable: a phase that
        does not advance means no `server-lifecycle-worker` is polling that
        MCE's queue, and the queue name says which MCE to go and look at.
        """
        return InstallServerProgress(
            phase=self._phase,
            server_name=self._server_name,
            activity_queue=self._activity_queue,
        )

    @workflow.run
    async def run(self, run_args: InstallServerRunArgs) -> InstallServerResult:
        install_input = run_args.input
        infra_env = install_input.infra_env
        namespace = install_input.namespace
        # The MCE this run writes to. Everything that touches the target
        # cluster goes to this queue, and only the worker inside that MCE
        # polls it.
        activity_queue = server_lifecycle_activity_queue(install_input.mce_cluster)
        self._activity_queue = activity_queue
        self._reserving = workflow.patched(_RESERVATION_PATCH)
        workflow.logger.info(
            "Installing a server into InfraEnv=%s for MCE=%s",
            infra_env,
            install_input.mce_cluster,
        )

        # Step 1 — the VLAN is the MCE's, so it is read from the Segments
        # Manager rather than taken from the caller: a supplied VLAN could
        # contradict the segment the cluster actually owns. Before the draw,
        # because no candidate could make a missing allocation usable. Runs on
        # the segment-lifecycle queue, which already holds that credential.
        self._phase = "resolving-vlan"
        segment = await self._inventory_segment(install_input.mce_cluster)

        # Step 2 — the InfraEnv name IS the hardware query. It encodes vendor,
        # model, site and spec, and server names carry the same tokens behind
        # an `ocp-` prefix, so no part of either is parsed apart.
        self._phase = "acquiring-server"
        candidates = await workflow.execute_activity(
            acquire_servers,
            _acquire_request(
                install_input.server_name, infra_env, install_input.candidate_count
            ),
            task_queue=activity_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Step 3 — INSTALL ONE CANDIDATE AT A TIME, all the way to an Agent,
        # and roll it back if it never produces one.
        #
        # The draw is not a list of things to validate, it is a list of things
        # to TRY: the only proof a server is installable is a host that booted
        # and reached assisted-service, and that cannot be established without
        # creating its resources first. So each candidate gets the full
        # sequence, and a candidate that fails at the end is torn down — which
        # returns the machine to the inventory — before the next is tried.
        #
        # That is why every rejection below, including one an hour deep, is a
        # SKIP: the run fails only once nothing is left to try.
        rejections: list[tuple[str, str]] = []
        attempts = 0

        for candidate in candidates:
            self._phase = "selecting-server"
            bond_members = await self._evaluate_candidate(
                candidate,
                install_input.server_name,
                namespace,
                activity_queue,
                rejections,
            )
            if bond_members is None:
                continue

            # Locked only now, once THIS machine is the one being installed:
            # server-scan's draw cannot know which candidate a run will choose.
            if self._reserving:
                self._phase = "reserving-server"
                lock = await self._reserve(
                    candidate, install_input, _RESERVATION_TTL, activity_queue, rejections
                )
                if lock is None:
                    continue

            attempts += 1
            self._server_name = candidate.name
            resource_request = BmhResourceRequest(
                server_name=candidate.name,
                namespace=namespace,
                infra_env=infra_env,
                # Narrowed by the evaluation: an unusable vendor is a rejection
                # reason, so a candidate that gets here always has one.
                bmc_vendor=str(candidate.bmc_vendor),
                bmc=candidate.bmc,
                bond_members=bond_members,
                vlan_id=segment.vlan_id,
                labels=install_input.labels,
            )

            # Step 4 — the three resources, in dependency order: the
            # BareMetalHost references the Secret by name, so the Secret goes
            # first. Each create treats "already exists" as success, which is
            # what makes the whole run re-runnable.
            self._phase = "creating-secret"
            secret = await self._create(
                create_bmc_secret, resource_request, activity_queue
            )

            self._phase = "creating-baremetalhost"
            bmh = await self._create(
                create_baremetal_host, resource_request, activity_queue
            )

            self._phase = "creating-nmstateconfig"
            nmstate = await self._create(
                create_nmstate_config, resource_request, activity_queue
            )

            # Step 5 — wait for the host to actually come up. An Agent existing
            # is the only observation that proves the BMC accepted virtual
            # media, the bond formed, the VLAN was right and DHCP answered.
            self._phase = "awaiting-agent"
            bmh_ref = BmhRef(server_name=candidate.name, namespace=namespace)
            # Renewed so the lock covers the whole wait however long the creates
            # took. Refused means it lapsed and another run took the machine:
            # undo ours, and leave theirs alone.
            if self._reserving and (
                await self._reserve(
                    candidate, install_input, _RESERVATION_TTL, activity_queue, rejections
                )
                is None
            ):
                self._phase = "rolling-back"
                await self._teardown(bmh_ref, activity_queue)
                continue
            agent, last_state = await self._await_agent(
                bmh_ref, [member.mac for member in bond_members], activity_queue
            )

            if agent.found:
                reservation_expires_at = None
                if self._reserving:
                    self._phase = "holding-server"
                    held = await self._reserve(
                        candidate, install_input, _INSTALLED_HOLD, activity_queue, None
                    )
                    reservation_expires_at = held.expires_at if held else None
                self._phase = "completed"
                workflow.logger.info(
                    "Server %s installed into InfraEnv %s as Agent %s "
                    "(vlan=%d, bond=%s, attempt %d)",
                    candidate.name,
                    infra_env,
                    agent.name,
                    segment.vlan_id,
                    [member.mac for member in bond_members],
                    attempts,
                )
                return InstallServerResult(
                    server_id=candidate.id,
                    server_name=candidate.name,
                    infra_env=infra_env,
                    namespace=namespace,
                    mce_cluster=install_input.mce_cluster,
                    vlan_id=segment.vlan_id,
                    bmc_vendor=resource_request.bmc_vendor,
                    bmc_address=build_bmc_address(
                        resource_request.bmc_vendor, candidate.bmc
                    ),
                    bond_macs=[member.mac for member in bond_members],
                    boot_mac=bond_members[0].mac,
                    resources_changed=any(
                        resource.changed for resource in (secret, bmh, nmstate)
                    ),
                    # An Agent cannot exist unless Ironic registered the host
                    # and drove it through a boot, so this is implied rather
                    # than separately observed.
                    bmh_registered=True,
                    agent_name=agent.name,
                    attempts=attempts,
                    reservation_expires_at=reservation_expires_at,
                )

            # No Agent. Take the machine back out of this MCE before trying
            # another — a BareMetalHost left pointing at it would make the same
            # server undrawable, and on another MCE, invisible.
            self._phase = "rolling-back"
            teardown = await self._teardown(bmh_ref, activity_queue)
            workflow.logger.warning(
                "Rolled back %s after no Agent in %s: removed %s",
                candidate.name,
                _AGENT_DEADLINE,
                teardown.removed or "nothing (already absent)",
            )
            if self._reserving:
                self._phase = "releasing-server"
                await self._release(candidate, activity_queue)
            rejections.append(
                ("no-agent", _describe_no_agent(candidate.name, last_state))
            )

        raise _no_installable_candidate(infra_env, namespace, candidates, rejections)

    async def _evaluate_candidate(
        self,
        candidate: AcquiredServer,
        requested_name: str | None,
        namespace: str,
        task_queue: str,
        rejections: list[tuple[str, str]],
    ) -> list[BondMember] | None:
        """One candidate's bond members, or None with a reason recorded.

        EVERY reason a candidate cannot be installed is a skip, not a failure.
        The draw exists so one unusable server is a retry rather than a failed
        run, and a reason handled outside this function would silently break
        that promise: a pool of three would die on a STANDALONE machine at the
        front while two installable servers sat behind it.

        Why an already-installed candidate has to be skipped at all: server-scan
        reports a machine as unclaimed until a CLUSTER reports the node, minutes
        after this workflow finishes. A second run started in that window draws
        the same machine, every create answers "already exists", and the run
        would report success having added nothing.
        """
        described = describe_candidate(candidate)

        # Checked before the name is used anywhere: every read and every
        # create derives a Kubernetes object name from it.
        if not is_k8s_resource_name(candidate.name):
            rejections.append(("unnameable", candidate.name))
            return None

        vendor = candidate.bmc_vendor
        if vendor is None or vendor.upper() not in BMC_VENDORS:
            rejections.append(
                ("unknown-bmc-vendor", f"{candidate.name} (bmc_vendor={vendor!r})")
            )
            return None

        if not candidate.bmc.host.strip("[] "):
            rejections.append(("no-bmc-host", candidate.name))
            return None

        members = select_bond_members(candidate)
        if members is None:
            rejections.append(("no-bond", described))
            return None

        bad = [m.mac for m in members if not mac_is_valid(m.mac)]
        if bad:
            rejections.append(("bad-mac", f"{candidate.name} {bad}"))
            return None

        # An explicitly named server is a request to converge THAT machine, so
        # it is never skipped — only an unnamed draw from a pool is.
        if requested_name is None:
            existing = await self._read_bmh(
                BmhRef(server_name=candidate.name, namespace=namespace), task_queue
            )
            if existing.found:
                rejections.append(("already-installed", candidate.name))
                return None

        return members

    async def _reserve(
        self,
        candidate: AcquiredServer,
        install_input: InstallServerInput,
        ttl: timedelta,
        task_queue: str,
        rejections: list[tuple[str, str]] | None,
    ) -> ServerReservation | None:
        """Take or extend server-scan's install lock, or None with a reason recorded.

        Two refusals are a candidate's, not the run's: a live lock held by
        another run, and a server that left the inventory. Both are recorded as
        skips in `rejections`; with None (the hold after a success, where the
        machine is installed whatever the answer) they are only logged.
        Everything else — a token without the ADMIN role above all — fails the
        run, because no other candidate would fare differently.
        """
        try:
            return await workflow.execute_activity(
                reserve_server,
                ReserveServerRequest(
                    server_id=candidate.id,
                    server_name=candidate.name,
                    holder=_RESERVATION_HOLDER,
                    workflow_id=workflow.info().workflow_id,
                    mce_cluster=install_input.mce_cluster,
                    infra_env=install_input.infra_env,
                    namespace=install_input.namespace,
                    ttl_seconds=int(ttl.total_seconds()),
                ),
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        except ActivityError as err:
            cause = err.cause
            if not isinstance(cause, ApplicationError):
                raise
            if cause.type not in (
                ServerReservedError.__name__,
                ServerNotAvailableError.__name__,
            ):
                raise
            workflow.logger.warning(
                "server-scan refused the install lock on %s: %s",
                candidate.name,
                cause.message,
            )
            if rejections is None:
                return None
            if cause.type == ServerReservedError.__name__:
                rejections.append(("reserved", f"{candidate.name} ({cause.message})"))
            else:
                rejections.append(("gone", candidate.name))
            return None

    async def _release(self, candidate: AcquiredServer, task_queue: str) -> None:
        """Give the lock back once a teardown proved nothing still points at the machine."""
        released = await workflow.execute_activity(
            release_server,
            ReleaseServerRequest(
                server_id=candidate.id,
                server_name=candidate.name,
                holder=_RESERVATION_HOLDER,
                workflow_id=workflow.info().workflow_id,
            ),
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if released.detail:
            workflow.logger.info(
                "Released %s in server-scan: %s", candidate.name, released.detail
            )

    async def _inventory_segment(self, mce_cluster: str) -> SegmentEntry:
        """The MCE's inventory segment, the one thing the VLAN comes from.

        Runs on the SEGMENT-LIFECYCLE queue: it reads the Segments Manager,
        whose credential already lives on that limb, so routing one activity
        there beats a second copy of the token on this domain's worker.

        The activity raises when the MCE has no INVENTORY allocation, and that
        is right — there is one inventory scope per cluster, so no candidate the
        run might draw could make a missing one usable.
        """
        segment = await workflow.execute_activity(
            get_inventory_segment,
            mce_cluster,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # Re-checked even though the activity already matched the cluster: that
        # match is client-side, because the list endpoint has no cluster filter,
        # and this is the one value that decides which VLAN a host is tagged
        # onto. A host on another cluster's inventory network comes up
        # somewhere this MCE cannot reach.
        if segment.cluster_name != mce_cluster:
            raise ApplicationError(
                f"Segments Manager returned segment {segment.segment} allocated "
                f"to {segment.cluster_name!r}, not to the requested MCE "
                f"{mce_cluster!r}",
                type=InventorySegmentMismatchError.__name__,
            )
        return segment

    async def _create(
        self,
        activity_fn: _ResourceCreate,
        request: BmhResourceRequest,
        task_queue: str,
    ) -> CreatedResource:
        """Run one idempotent resource create on the target MCE's queue."""
        return await workflow.execute_activity(
            activity_fn,
            request,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _read_bmh(self, ref: BmhRef, task_queue: str) -> BmhState:
        """Read one BareMetalHost back from the target MCE's queue.

        Shared by the candidate probe and the registration poll: the same
        activity on the same queue under the same budget, in one place.
        """
        return await workflow.execute_activity(
            get_baremetal_host,
            ref,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _await_agent(
        self, ref: BmhRef, macs: list[str], task_queue: str
    ) -> tuple[AgentState, BmhState | None]:
        """Wait for an Agent to register for this host, bounded by the deadline.

        Returns the Agent as soon as one appears. On the deadline it returns the
        empty result plus ONE read of the BareMetalHost — the host's own view of
        itself is worthless as a success signal but is exactly what separates a
        BMC that was never reached from a bond, VLAN or DHCP scope that did not
        work, which is the difference between two very different investigations.
        """
        started_at = workflow.now()
        agent_ref = AgentRef(namespace=ref.namespace, macs=macs)
        while True:
            agent = await workflow.execute_activity(
                find_agent_for_host,
                agent_ref,
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if agent.found:
                return agent, None
            if workflow.now() - started_at >= _AGENT_DEADLINE:
                return agent, await self._read_bmh(ref, task_queue)
            await workflow.sleep(_AGENT_POLL_INTERVAL)  # durable, replay-safe

    async def _teardown(self, ref: BmhRef, task_queue: str) -> TeardownResult:
        """Roll one candidate back so its machine returns to the inventory.

        Retried like any other activity, and that matters more here than
        elsewhere: the usual reason teardown does not finish is metal3 holding
        its finalizer while it tries to deprovision through the BMC that just
        failed to answer, which is a state the activity is built to break out of
        but should still be allowed to retry into.
        """
        return await workflow.execute_activity(
            teardown_bmh_resources,
            ref,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
