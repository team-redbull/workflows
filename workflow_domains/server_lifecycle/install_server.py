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

  1. acquiring-server    — GET /servers/available. The InfraEnv name states
                           which hardware it is for
                           (cisco-m6-bat-yam-64c-512gb), and server names carry
                           the same tokens behind an `ocp-` prefix, so the
                           InfraEnv IS the query. Several candidates are drawn
                           so one unusable server is a retry, not a failure.
  2. selecting-server    — the first candidate that can actually be installed,
                           and the inventory VLAN it needs. EVERY reason a
                           candidate is unusable is a skip here, so the draw
                           keeps its promise; the reasons are reported together
                           if none survives.
  3. creating-secret
     creating-baremetalhost
     creating-nmstateconfig  — all idempotent; an existing resource is success.
  4. verifying-registration — a BOUNDED poll. Storing the BareMetalHost only
                           means the API server accepted it; Ironic still has
                           to reach the BMC, and a wrong address or credential
                           surfaces nowhere else.

THE VLAN IS RESOLVED INSIDE SELECTION, not before it. An MCE's inventory
network is really up to TWO networks, split by how a server's BMC is driven:
Ironic reaches a Redfish BMC (HP via OneView, Dell via iDRAC, Cisco via
Intersight) on one and a UCS-managed blade over IPMI on another, allocated as
INVENTORY_REDFISH and INVENTORY_IPMI. So which segment applies is not known
until a machine is chosen — and an MCE holds only the classes it serves, which
is why a UCS blade drawn against a Redfish-only MCE is passed over rather than
failing a run that could still install the Dell behind it. The lookup runs on
the SEGMENT-LIFECYCLE queue, where that credential already lives, once per
class rather than once per candidate.

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

On cancellation there is deliberately no compensating cleanup: the three
resources are idempotent, so a re-run converges on them, and a half-created set
is what an operator needs in order to see how far the run got. Removing a
server is a separate uninstall-server workflow, not a rollback of this one.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.bmc_address import (
        BMC_VENDORS,
        BmcDriverClass,
        bmc_driver_class,
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
        BmhNotRegisteredError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        InvalidMacError,
        InvalidServerNameError,
        InventorySegmentMismatchError,
        InventorySegmentNotFoundError,
        NoBondableInterfacesError,
        SegmentsManagerAuthError,
        ServerNotAvailableError,
        ServerScanAuthError,
        UnknownBmcVendorError,
    )
    from shared.interfaces.segment_lifecycle import get_inventory_segment
    from shared.models.segment_lifecycle import (
        InventorySegmentLookup,
        InventorySegmentRequest,
        SegmentEntry,
        SegmentType,
    )
    from shared.interfaces.server_lifecycle import (
        acquire_servers,
        create_baremetal_host,
        create_bmc_secret,
        create_nmstate_config,
        get_baremetal_host,
    )
    from shared.models.server_lifecycle import (
        AcquiredServer,
        AcquireServerRequest,
        BmhRef,
        BmhResourceRequest,
        BmhState,
        BondMember,
        CreatedResource,
        InstallServerProgress,
        InstallServerResult,
        InstallServerRunArgs,
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
    AmbiguousServerNameError,
    BmcCredentialsMissingError,
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    InvalidMacError,
    InvalidServerNameError,
    AmbiguousInventorySegmentError,
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

# Registration is MACHINE convergence — Ironic reaching the BMC — so the wait
# gets a real deadline and fails loudly, exactly as allocate-segment's DHCP
# scope poll does. A host that has not registered in 10 minutes has a bad BMC
# address or credential, not a slow one. Changing either constant is a
# non-deterministic change for in-flight runs.
_BMH_POLL_INTERVAL = timedelta(seconds=15)
_BMH_REGISTRATION_DEADLINE = timedelta(minutes=10)

# How long a registration error is tolerated before the run gives up on it.
# Metal3 retries registration itself, so a single observation can be a BMC
# that was briefly busy; an error still standing after this has a cause no
# amount of further waiting fixes.
_BMH_REGISTRATION_ERROR_GRACE = timedelta(minutes=2)

# States that mean Ironic has FINISHED registering the host — it reached the
# BMC, authenticated, and moved on.
#
# `registering` is deliberately NOT here. It is the state Metal3 assigns the
# moment it picks the host up, before contacting the BMC at all, and a host
# whose BMC address or credential is wrong STAYS in it — so accepting it would
# make this poll return on its first iteration for exactly the failure the
# poll exists to catch. `inspecting` is included because inspection is
# disabled on these hosts, so it is passed through rather than settled in, and
# reaching it already proves the BMC was contacted.
_REGISTERED_STATES = frozenset(
    {
        "inspecting",
        "preparing",
        "available",
        "ready",
        "provisioning",
        "provisioned",
        "externally provisioned",
    }
)

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
# Which of an MCE's inventory networks a server's BMC is reachable on. One
# entry per driver class, so a class added in shared/bmc_address.py fails here
# loudly rather than defaulting a host onto the wrong network.
_INVENTORY_TYPE_BY_DRIVER_CLASS = {
    BmcDriverClass.REDFISH: SegmentType.INVENTORY_REDFISH,
    BmcDriverClass.IPMI: SegmentType.INVENTORY_IPMI,
}

_REJECTION_TYPE = {
    "unnameable": InvalidServerNameError,
    "no-bond": NoBondableInterfacesError,
    "already-installed": NoBondableInterfacesError,
    "unknown-bmc-vendor": UnknownBmcVendorError,
    "no-bmc-host": BmcEndpointMissingError,
    "bad-mac": InvalidMacError,
    "no-inventory-segment": InventorySegmentNotFoundError,
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
    "no-inventory-segment": (
        "need an inventory network this MCE does not have allocated. An MCE "
        "holds one segment per BMC protocol class, and only the classes it "
        "serves — allocate the missing one in the Segments Manager"
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


@workflow.defn
class InstallServerWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._server_name: str | None = None
        self._activity_queue: str | None = None

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
        # The MCE this run writes to. Everything that touches the target
        # cluster goes to this queue, and only the worker inside that MCE
        # polls it.
        activity_queue = server_lifecycle_activity_queue(install_input.mce_cluster)
        self._activity_queue = activity_queue
        workflow.logger.info(
            "Installing a server into InfraEnv=%s for MCE=%s",
            infra_env,
            install_input.mce_cluster,
        )

        # Step 1 — the InfraEnv name IS the hardware query. It encodes vendor,
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

        # Step 2 — the first candidate that can actually be installed, and the
        # inventory VLAN it needs. The VLAN is resolved HERE, not up front,
        # because it depends on the chosen server: an MCE's inventory network
        # is split by BMC protocol class, so which segment applies is not known
        # until a machine is picked.
        self._phase = "selecting-server"
        server, bond_members, segment = await self._select_candidate(
            install_input.server_name,
            infra_env,
            install_input.namespace,
            install_input.mce_cluster,
            candidates,
            activity_queue,
        )
        self._server_name = server.name

        resource_request = BmhResourceRequest(
            server_name=server.name,
            namespace=install_input.namespace,
            infra_env=infra_env,
            # Narrowed by the selection: an unusable vendor is a rejection
            # reason, so a chosen candidate always has one.
            bmc_vendor=str(server.bmc_vendor),
            bmc=server.bmc,
            bond_members=bond_members,
            vlan_id=segment.vlan_id,
            labels=install_input.labels,
        )

        # Step 3 — the three resources, in dependency order: the BareMetalHost
        # references the Secret by name, so the Secret goes first. Each create
        # treats "already exists" as success, which is what makes the whole run
        # re-runnable.
        self._phase = "creating-secret"
        secret = await self._create(create_bmc_secret, resource_request, activity_queue)

        self._phase = "creating-baremetalhost"
        bmh = await self._create(create_baremetal_host, resource_request, activity_queue)

        self._phase = "creating-nmstateconfig"
        nmstate = await self._create(
            create_nmstate_config, resource_request, activity_queue
        )

        # Step 4 — a stored object is not a registered host. Ironic still has
        # to reach the BMC, and a wrong address or credential shows up only
        # here, so the run does not claim success until it has.
        self._phase = "verifying-registration"
        await self._await_registration(
            BmhRef(server_name=server.name, namespace=install_input.namespace),
            activity_queue,
        )

        self._phase = "completed"
        workflow.logger.info(
            "Server %s installed into InfraEnv %s (vlan=%d, bond=%s)",
            server.name,
            infra_env,
            segment.vlan_id,
            [member.mac for member in bond_members],
        )
        return InstallServerResult(
            server_id=server.id,
            server_name=server.name,
            infra_env=infra_env,
            namespace=resource_request.namespace,
            mce_cluster=install_input.mce_cluster,
            vlan_id=segment.vlan_id,
            bmc_vendor=resource_request.bmc_vendor,
            bmc_address=build_bmc_address(resource_request.bmc_vendor, server.bmc),
            bond_macs=[member.mac for member in bond_members],
            boot_mac=bond_members[0].mac,
            resources_changed=any(
                resource.changed for resource in (secret, bmh, nmstate)
            ),
            bmh_registered=True,
        )

    async def _select_candidate(
        self,
        requested_name: str | None,
        infra_env: str,
        namespace: str,
        mce_cluster: str,
        candidates: list[AcquiredServer],
        task_queue: str,
    ) -> tuple[AcquiredServer, list[BondMember], SegmentEntry]:
        """The first candidate that can be installed, with its inventory VLAN.

        EVERY reason a candidate cannot be installed is a skip here, not a
        failure. The draw exists so one unusable server is a retry rather than a
        failed run, and a reason handled outside this loop would silently break
        that promise: a pool of three would die on a STANDALONE machine at the
        front while two installable servers sat behind it. When nothing
        survives, the reasons are reported together and the failure takes the
        specific type if they all agree — which they do whenever a server was
        named explicitly, since that pool holds one.

        Why an already-installed candidate has to be skipped at all: server-scan
        reports a machine as unclaimed until a CLUSTER reports the node, minutes
        after this workflow finishes. A second run started in that window draws
        the same machine, every create answers "already exists", and the run
        reports success having added nothing.
        """
        rejections: list[tuple[str, str]] = []
        # One lookup per BMC protocol class, at most two per run, and none at
        # all for a draw whose candidates are all rejected before this point.
        segments: dict[SegmentType, InventorySegmentLookup] = {}

        for candidate in candidates:
            described = describe_candidate(candidate)

            # Checked before the name is used anywhere: every read and every
            # create derives a Kubernetes object name from it.
            if not is_k8s_resource_name(candidate.name):
                rejections.append(("unnameable", candidate.name))
                continue

            vendor = candidate.bmc_vendor
            if vendor is None or vendor.upper() not in BMC_VENDORS:
                rejections.append(
                    ("unknown-bmc-vendor", f"{candidate.name} (bmc_vendor={vendor!r})")
                )
                continue

            if not candidate.bmc.host.strip("[] "):
                rejections.append(("no-bmc-host", candidate.name))
                continue

            members = select_bond_members(candidate)
            if members is None:
                rejections.append(("no-bond", described))
                continue

            bad = [m.mac for m in members if not mac_is_valid(m.mac)]
            if bad:
                rejections.append(("bad-mac", f"{candidate.name} {bad}"))
                continue

            # The inventory network this machine's BMC is reachable on. An MCE
            # holds one segment per protocol class and only the classes it
            # serves, so a UCS blade on a Redfish-only MCE is passed over here.
            segment_type = _INVENTORY_TYPE_BY_DRIVER_CLASS[bmc_driver_class(vendor)]
            if segment_type not in segments:
                segments[segment_type] = await self._inventory_segment(
                    mce_cluster, segment_type
                )
            lookup = segments[segment_type]
            if not lookup.found or lookup.entry is None:
                rejections.append(
                    ("no-inventory-segment", f"{candidate.name} needs {segment_type.value}")
                )
                continue

            # An explicitly named server is a request to converge THAT machine,
            # so it is never skipped — only an unnamed draw from a pool is.
            if requested_name is None:
                existing = await self._read_bmh(
                    BmhRef(server_name=candidate.name, namespace=namespace),
                    task_queue,
                )
                if existing.found:
                    rejections.append(("already-installed", candidate.name))
                    continue

            return candidate, members, lookup.entry

        raise _no_installable_candidate(infra_env, namespace, candidates, rejections)

    async def _inventory_segment(
        self, mce_cluster: str, segment_type: SegmentType
    ) -> InventorySegmentLookup:
        """One MCE's inventory segment of one BMC protocol class.

        Runs on the SEGMENT-LIFECYCLE queue: it reads the Segments Manager,
        whose credential already lives on that limb, so routing one activity
        there beats a second copy of the token on this domain's worker.
        """
        lookup = await workflow.execute_activity(
            get_inventory_segment,
            InventorySegmentRequest(
                mce_cluster=mce_cluster, segment_type=segment_type
            ),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # Re-checked even though the activity already matched the cluster: that
        # match is client-side, because the list endpoint has no cluster filter,
        # and this is the one value that decides which VLAN a host is tagged
        # onto. A host on another cluster's inventory network comes up
        # somewhere this MCE cannot reach.
        entry = lookup.entry
        if entry is not None and entry.cluster_name != mce_cluster:
            raise ApplicationError(
                f"Segments Manager returned segment {entry.segment} allocated "
                f"to {entry.cluster_name!r}, not to the requested MCE "
                f"{mce_cluster!r}",
                type=InventorySegmentMismatchError.__name__,
            )
        return lookup

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

    async def _await_registration(self, ref: BmhRef, task_queue: str) -> None:
        """Poll until Ironic has registered the host, or fail at the deadline."""
        started_at = workflow.now()
        errored_since: datetime | None = None
        while True:
            state = await self._read_bmh(ref, task_queue)
            # An errored host is never registered, whatever state it sits in:
            # Metal3 reports a failed registration as `registering` plus
            # operationalStatus=error, so the state alone cannot tell the two
            # apart.
            if (
                state.found
                and not state.is_errored()
                and (state.provisioning_state or "").lower() in _REGISTERED_STATES
            ):
                return

            # Fail on a registration error that has stopped being transient,
            # rather than spending the rest of the deadline re-reading it.
            if state.is_registration_error():
                errored_since = errored_since or workflow.now()
                if workflow.now() - errored_since >= _BMH_REGISTRATION_ERROR_GRACE:
                    raise ApplicationError(
                        f"Ironic cannot register BareMetalHost "
                        f"{ref.server_name}: errorType {state.error_type!r}"
                        f"{f' — {state.error_message}' if state.error_message else ''}. "
                        "That is a wrong BMC address, a wrong credential, or a "
                        "BMC this cluster cannot reach — none of which clear on "
                        "their own. The Secret, BareMetalHost and NMStateConfig "
                        "stand for inspection",
                        type=BmhNotRegisteredError.__name__,
                    )
            else:
                errored_since = None

            if workflow.now() - started_at >= _BMH_REGISTRATION_DEADLINE:
                raise ApplicationError(
                    f"BareMetalHost {ref.server_name} did not register within "
                    f"{int(_BMH_REGISTRATION_DEADLINE.total_seconds() // 60)} "
                    f"minutes: {_describe_bmh_state(state)}. The Secret, "
                    "BareMetalHost and NMStateConfig stand — check the BMC "
                    "address and credentials, and Ironic's reachability of that "
                    "BMC",
                    type=BmhNotRegisteredError.__name__,
                )
            await workflow.sleep(_BMH_POLL_INTERVAL)  # durable, replay-safe timer
