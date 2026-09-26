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
                           segment, looked up in the Segments Manager. Runs on
                           the SEGMENT-LIFECYCLE queue, where that credential
                           already lives.
  2. acquiring-server    — GET /servers/available. The InfraEnv name states
                           which hardware it is for
                           (cisco-m6-bat-yam-64c-512gb), and server names carry
                           the same tokens behind an `ocp-` prefix, so the
                           InfraEnv IS the query. Several candidates are drawn
                           so one unusable server is a retry, not a failure.
  3. selecting-bond      — the pure rule in bond_selection.py, plus one read per
                           surviving candidate to skip machines already
                           installed.
  4. creating-secret
     creating-baremetalhost
     creating-nmstateconfig  — all idempotent; an existing resource is success.
  5. verifying-registration — a BOUNDED poll. Storing the BareMetalHost only
                           means the API server accepted it; Ironic still has
                           to reach the BMC, and a wrong address or credential
                           surfaces nowhere else.

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
    from shared.bmc_address import BMC_VENDORS, build_bmc_address, is_k8s_resource_name
    from shared.consts import (
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        server_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        AmbiguousInventorySegmentError,
        AmbiguousServerNameError,
        BmcCredentialsMissingError,
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
    InventorySegmentNotFoundError,
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


def _require_bmc_vendor(server: AcquiredServer) -> str:
    """The server's BMC vendor, or fail rather than guess a driver.

    The vocabulary comes from shared/bmc_address.py — the module that owns
    the driver mapping — so a vendor added there is drivable here without a
    second edit.
    """
    vendor = server.bmc_vendor
    if vendor is None or vendor.upper() not in BMC_VENDORS:
        raise ApplicationError(
            f"server-scan reports no usable BMC driver vocabulary for "
            f"{server.name} (bmc_vendor={vendor!r}, vendor={server.vendor}, "
            f"provider={server.source_provider}). A STANDALONE machine's "
            "driver is the caller's decision, and guessing IPMI would be "
            "silently wrong for a Redfish-only BMC",
            type=UnknownBmcVendorError.__name__,
        )
    return vendor


def _require_valid_macs(server_name: str, bond_members: list[BondMember]) -> None:
    """Reject a malformed MAC BEFORE the first resource is written.

    The same check runs again in the resource builders, as a guard on their
    own inputs. It matters here because a MAC discovered to be bad inside
    create_baremetal_host would already have left a Secret behind, and the
    builders' InvalidMacError arrives on the SECOND of three creates.
    """
    bad = [member.mac for member in bond_members if not mac_is_valid(member.mac)]
    if bad:
        raise ApplicationError(
            f"server-scan reported malformed MAC(s) {bad} for {server_name}; "
            "MACs are normalized on ingest, so this is a bad payload rather "
            "than a condition to wait out",
            type=InvalidMacError.__name__,
        )


def _no_installable_candidate(
    infra_env: str,
    namespace: str,
    candidates: list[AcquiredServer],
    already_installed: list[str],
    unnameable: list[str],
) -> ApplicationError:
    """The failure for a draw in which no candidate could be installed.

    Each reason a candidate was passed over is reported separately, because they
    are three different people's problem: an already-installed machine means the
    pool is in use, an unnameable one means the inventory needs renaming, and
    anything left means the collector cannot report link state.
    """
    parts = [
        f"checked {len(candidates)} candidate(s): "
        + "; ".join(describe_candidate(c) for c in candidates)
    ]
    if already_installed:
        parts.append(
            f"skipped as already holding a BareMetalHost in {namespace}: "
            + ", ".join(already_installed)
        )
    if unnameable:
        parts.append(
            "skipped as unusable Kubernetes resource names even lowercased "
            "(rename these in server-scan): " + ", ".join(unnameable)
        )
    parts.append(
        "anything left offers fewer than two link-up NICs on two distinct "
        "physical ports. Note that HPE OneView reports no link state at all "
        "and Intersight vNICs usually report none, so servers from those "
        "collectors cannot satisfy a strict link-up requirement"
    )
    return ApplicationError(
        f"No candidate for InfraEnv {infra_env} is installable. " + ". ".join(parts),
        type=NoBondableInterfacesError.__name__,
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

        # Step 1 — the VLAN is the MCE's, so it is read from the Segments
        # Manager rather than taken from the caller: a supplied VLAN could
        # contradict the segment the cluster actually owns. Runs on the
        # segment-lifecycle queue, which already holds that credential.
        self._phase = "resolving-vlan"
        segment = await workflow.execute_activity(
            get_inventory_segment,
            install_input.mce_cluster,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # Re-checked here even though the activity already matched the cluster:
        # that match is client-side (the list endpoint has no cluster filter),
        # and this is the one value that decides which VLAN a host is tagged
        # onto.
        if segment.cluster_name != install_input.mce_cluster:
            raise ApplicationError(
                f"Segments Manager returned segment {segment.segment} allocated "
                f"to {segment.cluster_name!r}, not to the requested MCE "
                f"{install_input.mce_cluster!r}",
                type=InventorySegmentMismatchError.__name__,
            )

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

        # Step 3 — the first candidate that can actually be installed.
        self._phase = "selecting-bond"
        server, bond_members = await self._select_candidate(
            install_input.server_name,
            infra_env,
            install_input.namespace,
            candidates,
            activity_queue,
        )
        self._server_name = server.name

        bmc_vendor = _require_bmc_vendor(server)
        _require_valid_macs(server.name, bond_members)

        resource_request = BmhResourceRequest(
            server_name=server.name,
            namespace=install_input.namespace,
            infra_env=infra_env,
            bmc_vendor=bmc_vendor,
            bmc=server.bmc,
            bond_members=bond_members,
            vlan_id=segment.vlan_id,
            labels=install_input.labels,
        )

        # Step 4 — the three resources, in dependency order: the BareMetalHost
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

        # Step 5 — a stored object is not a registered host. Ironic still has
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
            bmc_vendor=bmc_vendor,
            bmc_address=build_bmc_address(bmc_vendor, server.bmc),
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
        candidates: list[AcquiredServer],
        task_queue: str,
    ) -> tuple[AcquiredServer, list[BondMember]]:
        """The first candidate that can carry a bond and is not already installed.

        The bond rule is pure logic, so it costs no round trip; the installed
        check is one read per surviving candidate.

        Why the installed check is needed at all: server-scan reports a machine
        as unclaimed until a CLUSTER reports the node, minutes after this
        workflow finishes. A second run started in that window draws the same
        machine, every create answers "already exists", and the run reports
        success having added nothing. Skipping candidates that already have a
        BareMetalHost is what makes a second run either take a DIFFERENT machine
        or fail honestly.
        """
        already_installed: list[str] = []
        unnameable: list[str] = []
        for candidate in candidates:
            # Checked before the name is used anywhere: the read below and every
            # create derive a Kubernetes object name from it, and the raising
            # form of that conversion in an activity would fail the whole run
            # over one badly-named machine in the pool.
            if not is_k8s_resource_name(candidate.name):
                unnameable.append(candidate.name)
                continue
            members = select_bond_members(candidate)
            if members is None:
                continue
            # An explicitly named server is a request to converge THAT machine,
            # so it is never skipped — only an unnamed draw from a pool is.
            if requested_name is None:
                existing = await self._read_bmh(
                    BmhRef(server_name=candidate.name, namespace=namespace),
                    task_queue,
                )
                if existing.found:
                    already_installed.append(candidate.name)
                    continue
            return candidate, members
        raise _no_installable_candidate(
            infra_env, namespace, candidates, already_installed, unnameable
        )

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
