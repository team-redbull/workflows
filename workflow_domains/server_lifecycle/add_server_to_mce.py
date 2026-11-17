"""add-server-to-mce — takes one healthy, unclaimed server from the server-scan
inventory platform and makes it installable in an MCE InfraEnv, by creating the
BareMetalHost + BMC Secret + NMStateConfig that the Assisted Installer needs.

The FIRST workflow of the `server-lifecycle` domain. It replaces the
`bmh-generator-operator` (BareMetalHostUCS), a Kopf operator that queried HP
OneView / UCS Central / Dell OME / Intersight live on every reconcile to learn a
server's MACs and BMC address. server-scan already collects that from all five
collectors on a 6-hour cron and knows which servers are unclaimed, so the whole
vendor-strategy layer is gone: one HTTP call replaces four vendor SDKs.

Named install-server until 2026-11-01 — route, ids, queue, type and lock
holder. A run started before the rename is an `InstallServerWorkflow` (the
subclass at the bottom of this file) and replays on its old queue, holding its
lock as `install-server` to the end.

Shape of the run:

  1. resolving-vlan      — the VLAN belongs to the target MCE, not to the
                           server or the caller: each MCE owns one inventory
                           segment, looked up in the Segments Manager by
                           cluster name. Runs on the SEGMENT-LIFECYCLE queue,
                           where that credential already lives.
     validating-infraenv — the InfraEnv must exist on the MCE, in the run's
                           namespace, before any server is drawn; missing
                           fails the run (no candidate could fix it).
  2. acquiring-server    — POST /servers/available: server-scan ALLOCATES one
                           server (its ADR-0063). The InfraEnv name states
                           which hardware it is for
                           (cisco-m6-bat-yam-64c-512gb), and server names carry
                           the same tokens behind an `ocp-` prefix, so the
                           InfraEnv IS the query. server-scan locks a
                           candidate for this run, live-checks it WHILE locked
                           and returns it, or releases it and tries its next —
                           so the server comes back already this run's, and no
                           other run can take it in between.
  3. selecting-server    — can THIS server be installed: a usable name, a known
                           BMC, no BareMetalHost already; and its two bond NICs,
                           which the NMStateConfig and the BareMetalHost's boot
                           MAC are written from — taken as server-scan vouched
                           for them (two link-up ports, normalized MACs), not
                           judged again. If not, its lock goes back and the run
                           fails, saying why: one server per run (the operator,
                           2026-11-17) — asking again is a new run.
     reserving-server    — only for a named server the allocation refused that
                           the run takes anyway: its own leftover lock, or
                           `allow_unhealthy` (ADR-0035's lock, taken here).
     checking-nmstateconfig — before anything else is written for it, in both
                           create orders: an NMStateConfig already under the
                           server's name that differs from this install is
                           replaced. server-scan offering the machine is the
                           proof it is free, so nothing else is checked first.
     registering-pxe-boot — IPMI servers only (a UCS-managed blade). IPMI has
                           no virtual media, so the host PXE-boots and the PXE
                           VM of the inventory segment's SITE must map both
                           bond MACs BEFORE the BareMetalHost exists — Ironic
                           may power the host on the moment it registers it.
                           The MACs chain to the InfraEnv's BOOT ENTRY on that
                           VM (`<mce>/<infraenv>.ipxe`), one per InfraEnv. A
                           site with no PXE VM configured is a skip.
     creating-nmstateconfig — IPMI only, ahead of the BareMetalHost, then
     publishing-pxe-boot    a BOUNDED wait for the InfraEnv to show a
                           discovery image generated after it, at which point
                           its iPXE script is published as the boot entry —
                           so the host can never boot an image without its own
                           static network config, however many installs of the
                           InfraEnv run at once. No new image in 10 min fails
                           the run (rolled back first): assisted-service is not
                           regenerating, which no other candidate changes.
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
     awaiting-server-scan — then server-scan is read every 5 min, for up to
     releasing-lock        that day, and the lock released as soon as it shows
                           the machine INSTALLED_TO_INVENTORY or INSTALLED:
                           server-scan hides a claimed machine from every draw
                           itself. Not seen within the day, the lock is left to
                           expire and the run still COMPLETES (the operator,
                           2026-11-09).
  6. rolling-back        — no Agent by the deadline: the NMStateConfig and
     unregistering-pxe-boot BareMetalHost are removed, an IPMI server's MACs
     setting-maintenance   are deleted from its site's PXE VM (while the lock
     releasing-server      is still ours), the server is put in MAINTENANCE in
                           server-scan so no run draws it again until a person
                           ends it (the operator, 2026-11-17), and only then
                           the lock released; then the run fails
                           (AgentNeverAppearedError), saying where the host's
                           own state points. A run recorded before releases
                           with no maintenance, the machine back in the pool.

ONE SERVER PER RUN since `add-server-allocates-one-server` (the operator,
2026-11-17, on server-scan's ADR-0063). It used to be a candidate LOOP: the
run drew several servers with GET /servers/available, locked the one it chose,
and every reason one was unusable — an hour-deep missing Agent included — was a
skip to the next. server-scan now picks, locks and live-checks one server
itself, so a run installs exactly that one or fails, its lock given back; a
`count` request is still N runs, one server each. A run recorded before keeps
the loop to its end (the same code, over a list) — `candidate_count` sized it.

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

A FAILED INSTALL IS TORN DOWN. A server that never produced an Agent is rolled
back before the run fails, because leaving a BareMetalHost behind would make
that machine undrawable here and invisible to every other MCE — server-scan
reports it unclaimed until a cluster reports the node, so nothing else would
notice. Since the allocation it is also put in maintenance in server-scan before
its lock goes back: a machine that cannot boot into the InfraEnv would only fail
the next run that drew it.

A CANCEL ROLLS BACK TOO (the operator, 2026-11-09; until then it deliberately
left everything in place). Before the Agent, the candidate being installed is
undone — through remove-server-from-mce as a child when anything may be on the
MCE, else its PXE mapping and its lock directly — and the run still ends
CANCELED (_roll_back_after_cancel). After the Agent the server IS installed and
stays; the cancel only stops the wait for server-scan. Terminate runs no code,
so it never rolls back. Removing a SUCCESSFULLY installed server remains
uninstall-server-from-hc / remove-server-from-mce.

THE INSTALL LOCK IS WHAT MAKES "UNCLAIMED" TRUE ACROSS MCEs. server-scan
reports a machine AVAILABLE until a cluster reports it, and this run's
BareMetalHost probe only sees its OWN MCE — so without the lock a server
installed into MCE-A is drawn again by a run for MCE-B, and both clusters drive
one BMC. server-scan takes it as it allocates the server, keyed on this run's
holder and workflow id, so the run's own renewal is an extension rather than a
lost race, and it expires on its own, so a run that dies costs one TTL rather
than the machine. (A run recorded before the allocation took it itself, once it
had chosen a candidate.) The allocation is NOT idempotent: a retry after a lost
answer takes another server and the first frees itself at its TTL, and a
cancel during it leaves the server it took locked to that TTL as well. It is released only
after a teardown has proved nothing on the cluster still points at the machine
— a failure anywhere else leaves it held until it expires, deliberately, since
half-created resources may still name it. Runs started before the lock existed
replay without it (`workflow.patched`). A RE-RUN of a named install after such a
failure meets its own lock: server-scan hides a locked machine from
`/servers/available` whoever asks, so the machine's record is read and the run
goes on when that lock — its own workflow id's — is the only reason
(_named_server_refused); re-taking it is an extension.

A NAMED SERVER MAY BE INSTALLED UNHEALTHY, ON REQUEST — the hidden
`allow_unhealthy` (CLAUDE.md §4). `/servers/available` is still asked first;
a named machine it refuses is read once more from server-scan's stored record
(`lookup_install_candidate`) and health_waiver.py splits the reasons: the
server's own health is waived and recorded (`allowed_unhealthy` on the progress,
the result and every create), a safety reason fails the run as before. The bond
then accepts a member whose link is not UP — never one port alone.

The teardown order is load-bearing and was established against a live cluster:
metal3 must be told to detach the host BEFORE the delete, or its
`baremetalhost.metal3.io` finalizer blocks forever trying to deprovision
through the BMC that just failed to answer. See teardown_bmh_resources.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import cast

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    WorkflowAlreadyStartedError,
    is_cancelled_exception,
)
from temporalio.workflow import ActivityCancellationType, ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import canonical_mce_cluster
    from shared.bmc_address import (
        BMC_VENDORS,
        boots_from_network,
        build_bmc_address,
        is_k8s_resource_name,
    )
    from shared.consts import (
        REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        server_lifecycle_activity_queue,
    )
    from shared.workflow_ids import remove_server_from_mce_workflow_id
    from shared.exceptions import (
        AllowUnhealthyNeedsServerNameError,
        AmbiguousInventorySegmentError,
        AmbiguousServerNameError,
        BmcCredentialsMissingError,
        BmcEndpointMissingError,
        BmhConflictError,
        AgentNeverAppearedError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        InfraEnvImageNotUpdatedError,
        InfraEnvNotFoundError,
        InvalidMacError,
        InvalidServerNameError,
        InventorySegmentMismatchError,
        InventorySegmentNotFoundError,
        NoBondableInterfacesError,
        PxeMapAuthError,
        PxeMapRequestRejectedError,
        PxeSiteNotConfiguredError,
        SegmentsManagerAuthError,
        ServerNotAvailableError,
        ServerLooksInstalledError,
        ServerReservedError,
        ServerScanAuthError,
        ServerScanRequestInvalidError,
        UnknownBmcVendorError,
    )
    from shared.interfaces.segment_lifecycle import get_inventory_segment
    from shared.models.segment_lifecycle import SegmentEntry
    from shared.interfaces.server_lifecycle import (
        acquire_servers,
        allocate_server,
        check_infra_env,
        create_baremetal_host,
        create_bmc_secret,
        create_nmstate_config,
        find_agent_for_host,
        get_baremetal_host,
        lookup_install_candidate,
        lookup_server_membership,
        publish_infra_env_boot,
        put_server_in_maintenance,
        register_pxe_boot,
        release_server,
        replace_conflicting_nmstate_config,
        reserve_server,
        teardown_bmh_resources,
        unregister_pxe_boot,
    )
    from shared.models.server_lifecycle import (
        AcquiredServer,
        AcquireServerRequest,
        AddServerToMceInput,
        AddServerToMceProgress,
        AddServerToMceResult,
        AddServerToMceRunArgs,
        AgentRef,
        AgentState,
        AllocateServerRequest,
        BmhRef,
        BmhResourceRequest,
        BmhState,
        BondMember,
        CreatedResource,
        InfraEnvBootRequest,
        InfraEnvBootState,
        InfraEnvRef,
        NmStateConfigReplacement,
        NmStateConfigReplaceRequest,
        PxeBootRequest,
        PxeMapRef,
        ReleaseServerRequest,
        RemoveServerFromMceInput,
        RemoveServerFromMceResult,
        RemoveServerFromMceRunArgs,
        ReserveServerRequest,
        ServerMaintenanceRequest,
        ServerMembershipRequest,
        ServerReservation,
        TeardownResult,
        WaivedHealthCheck,
        mac_is_valid,
    )
    from workflow_domains.server_lifecycle.bond_selection import (
        describe_candidate,
        select_bond_members,
    )
    from workflow_domains.server_lifecycle.health_waiver import (
        bond_link_waiver,
        health_findings,
        holds_own_lock,
        safety_refusal,
    )

# A cancelled install is taken back out of the MCE through remove-server-from-mce,
# as uninstall-server-from-hc's inventory half is — never reimplemented here.
from workflow_domains.server_lifecycle.remove_server_from_mce import RemoveServerFromMceWorkflow

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
    PxeSiteNotConfiguredError,
    InfraEnvNotFoundError,
    PxeMapAuthError,
    PxeMapRequestRejectedError,
    ServerLooksInstalledError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)
# What a cancel of the run does to the activity it is waiting on: ABANDON —
# stop waiting, send Temporal no cancel request. Only allocate_server
# heartbeats, and it is the one call whose cancel must not reach server-scan
# half-way either, so a request could do no good (each runs to its end either
# way), and sending it races the activity's own completion: an activity that
# finishes as the cancel lands makes the request name an activity Temporal no
# longer has (ACTIVITY_UNKNOWN), failing the workflow task. The rollback on
# cancel already assumes a call may still land (_CANCEL_SETTLE).
_ON_CANCEL = ActivityCancellationType.ABANDON

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
# MACHINE convergence still, so still a real deadline: reaching it, the
# resources are torn down and the machine goes back to the inventory — then the
# run fails (one server per run); a run recorded before the allocation tries its
# next candidate instead.
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
_RESERVATION_HOLDER = "add-server-to-mce"
# The holder a run started as install-server took its lock under. server-scan
# extends or releases a lock only for the SAME holder and workflow id, so such a
# run keeps that holder to the end (_reservation_holder).
_LEGACY_RESERVATION_HOLDER = "install-server"
# The workflow type of every run started before the rename.
_LEGACY_WORKFLOW_TYPE = "InstallServerWorkflow"
_RESERVATION_TTL = _AGENT_DEADLINE + timedelta(hours=1)
_INSTALLED_HOLD = timedelta(hours=24)
# After the hold, the run reads server-scan every 5 minutes until its
# membership jobs report the machine — INSTALLED_TO_INVENTORY (the MCE's agents
# job, every 5 min) or INSTALLED (a cluster's nodes job) — and then gives the
# lock back: server-scan hides a claimed machine from /servers/available by
# itself, so the day-long hold is needed only until it knows (the operator,
# 2026-11-09). Bounded by the hold: not seen within it, the lock is left to
# expire and the run still COMPLETES — the Agent proved the install. Changing
# any of these is a non-deterministic change for runs in flight.
_SERVER_SCAN_POLL_INTERVAL = timedelta(minutes=5)
_SERVER_SCAN_WAIT = _INSTALLED_HOLD
_INSTALLED_STATES = frozenset({"INSTALLED_TO_INVENTORY", "INSTALLED"})
# Gates every lock call, so a run started before the lock replays unchanged.
_RESERVATION_PATCH = "server-scan-install-lock"
# Gates the PXE map registration for IPMI servers, for the same reason: an
# in-flight IPMI run past selection would otherwise meet a new activity in
# front of its recorded creates.
_PXE_BOOT_PATCH = "pxe-map-ipmi-boot"
# Gate the InfraEnv pre-check and the rollback's PXE map cleanup, for the same
# reason: each puts a new activity where an in-flight run recorded another.
_INFRAENV_CHECK_PATCH = "infraenv-precheck"
_PXE_CLEANUP_PATCH = "pxe-map-rollback-cleanup"
# Gates the InfraEnv BOOT ENTRY for IPMI servers: MACs mapped to
# `<mce>/<infraenv>.ipxe` on the PXE VM, the NMStateConfig created before the
# BareMetalHost, and the entry published only once the discovery image
# includes it. A run recorded before it maps the URL straight onto the MACs.
_PXE_INFRAENV_BOOT_PATCH = "pxe-map-infraenv-boot"
# Gates mapping ONLY the BareMetalHost's boot MAC on the PXE VM (the first
# link-up NIC server-scan lists — bond member nic1). A run recorded before it
# mapped both bond MACs, so its rollback must unmap both.
_PXE_BOOT_MAC_ONLY_PATCH = "pxe-map-boot-mac-only"
# Gates the NMStateConfig check before a candidate's first write: one that
# differs from the install is REPLACED. A run recorded before it met the
# conflict at create_nmstate_config instead (BmhConflictError). The id is from
# when the limb also refused a server that looked in use; it stays, being in
# recorded histories.
_NMSTATE_REPLACE_PATCH = "nmstateconfig-replace-unless-in-use"
# Gates the second look at a NAMED server /servers/available refuses: a re-run
# recovers its own workflow id's leftover lock, and any other refusal says why.
# A run recorded before it failed on the refusal as server-scan gave it.
_OWN_LOCK_RERUN_PATCH = "named-install-own-lock-rerun"
# Gates the wait for server-scan after success and the early release of the
# lock. A run recorded before it completes at the hold, its lock left to expire.
_RELEASE_ONCE_INSTALLED_PATCH = "add-server-release-once-installed"
# Gates the rollback on CANCEL (the operator, 2026-11-09). A run recorded before
# it stops where it is when cancelled, everything left in place.
_ROLLBACK_ON_CANCEL_PATCH = "add-server-rollback-on-cancel"
# Gates the ALLOCATION (the operator, 2026-11-17, on server-scan's ADR-0063):
# the run asks server-scan for ONE server, which server-scan locks for it and
# live-checks while locked, and installs that one or fails. A run recorded
# before it draws a candidate list with GET /servers/available, locks the one
# it chooses itself and tries the next on a failure, as it did. Two more
# changes ride on the same gate, decided with the operator the same day: an
# allocated server's NICs and MACs are not judged again (server-scan vouched
# for them, _evaluate_candidate), and a server with no Agent in the hour is put
# in maintenance in server-scan before its lock goes back (_put_in_maintenance).
_ALLOCATE_PATCH = "add-server-allocates-one-server"
# The allocation live-checks each candidate it locks, one at a time, against
# its vendor manager — seconds as a rule, ~22 minutes at server-scan's worst
# (five candidates, UCS Central's probe deadline each). Its HTTP client gives up
# at 25 minutes; the activity heartbeats every 20 s, so a dead worker shows in
# two minutes, not thirty.
_ALLOCATE_TIMEOUT = timedelta(minutes=30)
_ALLOCATE_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
# A bond needs two MACs: server-scan skips a candidate with fewer read.
_BOND_NIC_MACS = 2
# The most of a maintenance reason the run sends: what a person reads in
# server-scan, the host's own state included, never a page of it.
_MAINTENANCE_REASON_CHARS = 500
# How long a cancelled run waits before it cleans up: a cancel stops waiting for
# an activity but not the activity — none here heartbeats — so a create, a lock
# or a PXE mapping already sent may still land. One attempt's whole budget, and
# a margin, lets it land first, so nothing reappears after the cleanup.
_CANCEL_SETTLE = _ACTIVITY_TIMEOUT + timedelta(seconds=30)

# assisted-service regenerates the discovery image within seconds of an
# NMStateConfig change; ten minutes of no new image means it is not going to
# (an NMStateConfig it rejected, or a fault of its own), which no other
# candidate would change — so the deadline fails the run. MACHINE convergence,
# so a real deadline, on a durable timer. Changing either constant is a
# non-deterministic change for in-flight runs.
_INFRAENV_IMAGE_DEADLINE = timedelta(minutes=10)
_INFRAENV_IMAGE_POLL_INTERVAL = timedelta(seconds=10)

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
        )
    return AcquireServerRequest(
        # Escaped: the InfraEnv name is caller data going into a regex.
        # Today's names are [a-z0-9-] so it changes nothing, but a `.`
        # would silently widen the pool without it. (Runs recorded before the
        # allocation only; new runs end the InfraEnv with `-`, _allocation_request.)
        pattern=f"^ocp-{re.escape(infra_env)}",
        count=candidate_count,
        health=_REQUIRED_HEALTH,
    )


def _allocation_request(install_input: AddServerToMceInput) -> AllocateServerRequest:
    """What to ask server-scan to allocate: the named machine, or one from the InfraEnv's pool.

    Locked for this run's holder and workflow id for the install's TTL, which
    the run renews as the Agent wait begins, exactly as a lock it took itself.
    """
    server_name = install_input.server_name
    return AllocateServerRequest(
        name=server_name or None,
        # Escaped: the InfraEnv name is caller data going into a regex (see
        # _acquire_request). The trailing `-` is load-bearing (the operator,
        # 2026-11-17): the InfraEnv name must END where the server name's own
        # part begins, or InfraEnv `…-512gb` would also draw servers of
        # `…-512gbx` / `…-512gb2`.
        pattern=None if server_name else f"^ocp-{re.escape(install_input.infra_env)}-",
        max_health=_REQUIRED_HEALTH,
        min_nic_macs=_BOND_NIC_MACS,
        holder=_reservation_holder(),
        workflow_id=workflow.info().workflow_id,
        mce_cluster=install_input.mce_cluster,
        infra_env=install_input.infra_env,
        namespace=install_input.namespace,
        ttl_seconds=int(_RESERVATION_TTL.total_seconds()),
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
    "no-pxe-site": PxeSiteNotConfiguredError,
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
    "no-pxe-site": (
        "are driven over IPMI, so they boot from the network, but this MCE's "
        "server-lifecycle-worker has no PXE map host for their site — add the "
        "site to PXE_MAP_URLS. Each was released untouched"
    ),
}


def _no_installable_candidate(
    infra_env: str,
    namespace: str,
    candidates: list[AcquiredServer],
    rejections: list[tuple[str, str]],
    *,
    allocated: bool = False,
    lock_note: str | None = None,
) -> ApplicationError:
    """The failure for a draw in which no candidate could be installed.

    Every reason is reported as its own group, because they are different
    people's problem: a pool in use, an inventory that needs renaming, a
    collector that cannot report link state, an MCE missing a segment. Merging
    them into one sentence sends whoever reads it to the wrong place.

    The failure TYPE is the specific one when every candidate was rejected for
    the same reason — which is the common case, and the whole case when a
    server was named explicitly — and the general one otherwise.

    `allocated`: the run's ONE server, which server-scan allocated (and whose
    lock the run has given back) — so one reason, one type, and no "next".
    `lock_note` replaces the closing words about its lock (the maintenance
    after no Agent); it starts "Its lock" and holds no full stop, which is how
    the server-lifecycle page finds the end of the reason.
    """
    if allocated and candidates:
        reason, description = rejections[0] if rejections else ("gone", candidates[0].name)
        lock = lock_note or (
            "Its lock had lapsed and another run holds the machine now"
            if reason == "reserved"
            else "Its lock was given back"
        )
        return ApplicationError(
            f"server-scan allocated {candidates[0].name} for InfraEnv {infra_env}, and it "
            f"could not be installed into {namespace} — {_REJECTION_SUMMARY[reason]}: "
            f"{description}. {lock}",
            type=_REJECTION_TYPE[reason].__name__,
        )
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


def _describe_no_agent(
    server_name: str, state: BmhState | None, network_boot: bool = False
) -> str:
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
    # A network-booted host has one more link in the chain before the ISO:
    # the site's PXE VM handing its MAC the InfraEnv's iPXE script.
    pxe = ", the site's PXE map entry for its MACs" if network_boot else ""
    return (
        f"{server_name} (no Agent in {minutes} min; {observed}. Ironic raised no "
        f"registration error, so the BMC answered and the host was driven to "
        f"boot — look at the bond, the VLAN{pxe} and DHCP on this segment, not "
        f"at the BMC)"
    )


_MAINTENANCE_REASON_END = ". End the maintenance once the cause is fixed"


def _maintenance_reason(why: str) -> str:
    """`why` and what to do about it, in at most _MAINTENANCE_REASON_CHARS: `why` is cut, never the end."""
    room = _MAINTENANCE_REASON_CHARS - len(_MAINTENANCE_REASON_END)
    if len(why) > room:
        why = why[: room - 1] + "…"
    return why + _MAINTENANCE_REASON_END


def _describe_waived(waived: list[WaivedHealthCheck]) -> str:
    """The health checks a failed candidate was installed past, for its rejection."""
    if not waived:
        return ""
    return " [installed with health checks waived on request: " + "; ".join(
        f"{w.check}: {w.observed}" for w in waived
    ) + "]"


def _reservation_holder() -> str:
    """Who this run holds server-scan's install lock as.

    Decided by the run's workflow TYPE, which never changes for a run: one
    started as install-server keeps `install-server` for every reserve,
    renewal and release, or server-scan would refuse its own lock back to it.
    """
    if workflow.info().workflow_type == _LEGACY_WORKFLOW_TYPE:
        return _LEGACY_RESERVATION_HOLDER
    return _RESERVATION_HOLDER


@workflow.defn
class AddServerToMceWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._server_name: str | None = None
        self._activity_queue: str | None = None
        self._reserving = False
        self._pxe_booting = False
        self._checking_infra_env = False
        self._pxe_cleanup = False
        self._infra_env_boot = False
        self._pxe_boot_mac_only = False
        self._nmstate_replace = False
        self._own_lock_rerun = False
        self._release_once_installed = False
        self._rollback_on_cancel = False
        self._allocating = False
        # What a cancel would have to undo, set BEFORE each call that may lock,
        # write or map (a call a cancel cuts off may still land): the candidate
        # being installed, whether anything may be on the MCE for it, and its PXE
        # mapping. Reset as each candidate is done with; `_installed` once the
        # Agent proved the install, after which a cancel undoes nothing.
        self._held: AcquiredServer | None = None
        self._mce_written = False
        self._pxe_mapped: tuple[list[BondMember], str] | None = None
        self._installed = False
        self._cancel_rollback: str | None = None
        # The health refusals waived for the server being installed
        # (`allow_unhealthy`), each with what was observed.
        self._allowed_unhealthy: list[WaivedHealthCheck] = []

    @workflow.query
    def progress(self) -> AddServerToMceProgress:
        """Cheap progress surface for the async caller (GET status endpoint).

        `activity_queue` is what makes a stalled run diagnosable: a phase that
        does not advance means no `server-lifecycle-worker` is polling that
        MCE's queue, and the queue name says which MCE to go and look at.
        """
        return AddServerToMceProgress(
            phase=self._phase,
            server_name=self._server_name,
            activity_queue=self._activity_queue,
            allowed_unhealthy=self._allowed_unhealthy,
            cancel_rollback=self._cancel_rollback,
        )

    @workflow.run
    async def run(self, run_args: AddServerToMceRunArgs) -> AddServerToMceResult:
        """The install; on a CANCEL, its rollback first (_roll_back_after_cancel), then CANCELED.

        A cancel arrives as asyncio.CancelledError, or wrapped in the ActivityError
        or ChildWorkflowError of the call it cut off. It is re-raised after the
        rollback, so the run still ends CANCELED, never FAILED. Terminate runs no
        code at all: it never rolls back.
        """
        try:
            return await self._install(run_args)
        except (asyncio.CancelledError, ActivityError, ChildWorkflowError) as err:
            if not (self._rollback_on_cancel and is_cancelled_exception(err)):
                raise
            await self._roll_back_after_cancel(run_args.input)
            raise

    async def _install(self, run_args: AddServerToMceRunArgs) -> AddServerToMceResult:
        install_input = run_args.input
        infra_env = install_input.infra_env
        namespace = install_input.namespace
        # The MCE this run writes to. Everything that touches the target
        # cluster goes to this queue, and only the worker inside that MCE
        # polls it.
        activity_queue = server_lifecycle_activity_queue(install_input.mce_cluster)
        self._activity_queue = activity_queue
        self._reserving = workflow.patched(_RESERVATION_PATCH)
        self._pxe_booting = workflow.patched(_PXE_BOOT_PATCH)
        self._checking_infra_env = workflow.patched(_INFRAENV_CHECK_PATCH)
        self._pxe_cleanup = workflow.patched(_PXE_CLEANUP_PATCH)
        self._infra_env_boot = workflow.patched(_PXE_INFRAENV_BOOT_PATCH)
        self._pxe_boot_mac_only = workflow.patched(_PXE_BOOT_MAC_ONLY_PATCH)
        self._nmstate_replace = workflow.patched(_NMSTATE_REPLACE_PATCH)
        self._own_lock_rerun = workflow.patched(_OWN_LOCK_RERUN_PATCH)
        self._release_once_installed = workflow.patched(_RELEASE_ONCE_INSTALLED_PATCH)
        self._rollback_on_cancel = workflow.patched(_ROLLBACK_ON_CANCEL_PATCH)
        self._allocating = workflow.patched(_ALLOCATE_PATCH)
        workflow.logger.info(
            "Installing a server into InfraEnv=%s for MCE=%s",
            infra_env,
            install_input.mce_cluster,
        )
        # allow_unhealthy is a person's decision about ONE named machine. The
        # API refuses it on a pool draw; so does the run, for one started some
        # other way — before anything is read or written. (No patch gate: no
        # recorded run carries the field, so none can take this branch.)
        if install_input.allow_unhealthy and not install_input.server_name:
            raise ApplicationError(
                "allow_unhealthy waives health refusals only for a server named in "
                "the request; it is never applied to a draw from the pool",
                type=AllowUnhealthyNeedsServerNameError.__name__,
            )

        # Step 1 — the VLAN is the MCE's, so it is read from the Segments
        # Manager rather than taken from the caller: a supplied VLAN could
        # contradict the segment the cluster actually owns. Before the draw,
        # because no candidate could make a missing allocation usable. Runs on
        # the segment-lifecycle queue, which already holds that credential.
        self._phase = "resolving-vlan"
        segment = await self._inventory_segment(install_input.mce_cluster)

        # The InfraEnv must exist on the MCE before any server is drawn: every
        # resource carries its label and every host registers into it, so a
        # missing one would lock, boot and wait an hour on each candidate only
        # to fail them all. Raised by the activity (InfraEnvNotFoundError) —
        # no candidate could fix it, so it fails the run.
        if self._checking_infra_env:
            self._phase = "validating-infraenv"
            state = await workflow.execute_activity(
                check_infra_env,
                InfraEnvRef(name=infra_env, namespace=namespace),
                task_queue=activity_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
            if state.cluster_ref:
                workflow.logger.warning(
                    "InfraEnv %s/%s is early-bound to cluster %s: hosts added to "
                    "it are bound to that cluster at once, not held in inventory",
                    namespace,
                    infra_env,
                    state.cluster_ref,
                )

        # Step 2 — the InfraEnv name IS the hardware query. It encodes vendor,
        # model, site and spec, and server names carry the same tokens behind
        # an `ocp-` prefix, so no part of either is parsed apart. Since the
        # allocation, ONE server, already locked for this run by server-scan
        # (`allocated`); a run recorded before draws a list of candidates.
        self._phase = "acquiring-server"
        candidates, record_waivers, allocated = await self._acquire(
            install_input, activity_queue
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
        # The failure's closing words about the lock, when the maintenance
        # after a missing Agent decided them (one server per run since then).
        lock_note: str | None = None

        for candidate in candidates:
            # Whatever the last candidate held was given back, or is another
            # run's now, before the loop moved on: nothing of it for a cancel.
            # An ALLOCATED server is held from the start: server-scan locked it.
            self._held = candidate if allocated else None
            self._mce_written, self._pxe_mapped = False, None
            self._phase = "selecting-server"
            bond_members = await self._evaluate_candidate(
                candidate,
                install_input.server_name,
                namespace,
                activity_queue,
                rejections,
                allow_link_down=install_input.allow_unhealthy,
                vouched=allocated,
            )
            if bond_members is None:
                if allocated:
                    # Locked for this run and nothing written: straight back to
                    # the inventory. There is no next candidate — the run fails
                    # with this reason after the loop.
                    self._phase = "releasing-server"
                    await self._release(candidate, activity_queue)
                    self._held = None
                continue
            # Only ever non-empty for a named install that asked for it: the
            # record's waivers come from the one machine named, and a bond
            # member is down by request only when the link-up rule was waived.
            # (An allocated server's members are taken as server-scan vouched
            # for them — two link-up ports — and are not judged again.)
            bond_waiver = (
                bond_link_waiver(candidate, bond_members)
                if install_input.allow_unhealthy
                else None
            )
            self._allowed_unhealthy = [*record_waivers, *([bond_waiver] if bond_waiver else [])]
            if self._allowed_unhealthy:
                workflow.logger.warning(
                    "allow_unhealthy: installing %s with health checks waived: %s",
                    candidate.name,
                    "; ".join(f"{w.check}: {w.observed}" for w in self._allowed_unhealthy),
                )

            # Locked only now, once THIS machine is the one being installed:
            # server-scan's draw cannot know which candidate a run will choose.
            # An allocated server is locked already, by the allocation itself.
            if self._reserving and not allocated:
                self._phase = "reserving-server"
                self._held = candidate  # before the call: a lock it sends may land
                lock = await self._reserve(
                    candidate, install_input, _RESERVATION_TTL, activity_queue, rejections
                )
                if lock is None:
                    continue

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
                allowed_unhealthy=self._allowed_unhealthy,
            )

            # An NMStateConfig already under the server's name that differs from
            # this install is REPLACED. Checked here, after the lock and BEFORE
            # anything else is written for the candidate (the PXE map, a
            # create) in BOTH create orders: create_nmstate_config would fail
            # on it, and nothing written yet means nothing to roll back.
            nmstate_replaced: list[str] = []
            # From here on something may be written on the MCE for it.
            self._mce_written = True
            if self._nmstate_replace:
                self._phase = "checking-nmstateconfig"
                replacement = await self._replace_conflicting_nmstate(
                    candidate, resource_request, install_input, activity_queue, rejections
                )
                if replacement is None:
                    continue
                nmstate_replaced = replacement.differences

            # An IPMI BMC has no virtual media: the host boots from the network,
            # so its site's PXE VM must know what to hand its MACs BEFORE the
            # BareMetalHost exists — Ironic may power it on as soon as it
            # registers it. After the lock, so another MCE's run can never
            # overwrite the mapping of a machine this run holds. Narrowed by
            # the evaluation: the vendor is known to have a driver here.
            network_boot = boots_from_network(str(candidate.bmc_vendor))
            pxe_registered = False
            if self._pxe_booting and network_boot:
                self._phase = "registering-pxe-boot"
                self._pxe_mapped = (bond_members, segment.site)  # before the call, as the lock
                pxe_registered = await self._register_pxe_boot(
                    candidate,
                    bond_members,
                    install_input,
                    segment.site,
                    activity_queue,
                    rejections,
                )
                if not pxe_registered:
                    # Nothing was written to the cluster, so the machine can go
                    # straight back to the inventory.
                    if self._reserving:
                        self._phase = "releasing-server"
                        await self._release(candidate, activity_queue)
                    continue

            attempts += 1
            self._server_name = candidate.name
            bmh_ref = BmhRef(server_name=candidate.name, namespace=namespace)

            # Step 3b — an IPMI host boots whatever its site's PXE VM hands it,
            # the moment Ironic powers it on, and the discovery image carries
            # only the NMStateConfigs it was generated from. So for one, the
            # NMStateConfig comes FIRST, ahead of the BareMetalHost, and the
            # InfraEnv's boot entry on the PXE VM — which its MACs already chain
            # to — is published only once assisted-service shows an image
            # generated after it. Every other install of the InfraEnv publishes
            # through the same entry, newest image winning on the PXE VM, so
            # parallel installs cannot leave a host on an image that predates
            # its own NMStateConfig.
            nmstate: CreatedResource | None = None
            if pxe_registered and self._infra_env_boot:
                self._phase = "creating-nmstateconfig"
                nmstate = await self._create(
                    create_nmstate_config, resource_request, activity_queue
                )
                self._phase = "publishing-pxe-boot"
                boot = await self._await_infra_env_boot(
                    candidate, install_input, segment.site, activity_queue
                )
                if not boot.published:
                    # Not this machine's fault, so not a skip — but undo the
                    # candidate (NMStateConfig, MAC entries, lock) before failing.
                    self._phase = "rolling-back"
                    await self._teardown(bmh_ref, activity_queue)
                    if self._pxe_cleanup:
                        self._phase = "unregistering-pxe-boot"
                        await self._unregister_pxe_boot(
                            candidate, bond_members, segment.site, activity_queue
                        )
                    if self._reserving:
                        self._phase = "releasing-server"
                        await self._release(candidate, activity_queue)
                    minutes = int(_INFRAENV_IMAGE_DEADLINE.total_seconds() // 60)
                    raise ApplicationError(
                        f"InfraEnv {namespace}/{infra_env} published no discovery "
                        f"image generated after {candidate.name}'s NMStateConfig "
                        f"within {minutes} min: {boot.detail}. assisted-service "
                        "regenerates within seconds of an NMStateConfig change, so "
                        "it is not regenerating — check the InfraEnv's ImageCreated "
                        f"condition and the assisted-service log. {candidate.name} "
                        "was rolled back",
                        type=InfraEnvImageNotUpdatedError.__name__,
                    )
                if boot.superseded:
                    workflow.logger.info(
                        "Boot entry for %s already held a newer image than %s",
                        infra_env,
                        boot.image_created_at,
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

            if nmstate is None:
                self._phase = "creating-nmstateconfig"
                nmstate = await self._create(
                    create_nmstate_config, resource_request, activity_queue
                )

            # Step 5 — wait for the host to actually come up. An Agent existing
            # is the only observation that proves the BMC accepted virtual
            # media, the bond formed, the VLAN was right and DHCP answered.
            self._phase = "awaiting-agent"
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
                # Installed: a cancel from here on undoes nothing (the operator,
                # 2026-11-09) — it only stops the wait for server-scan.
                self._installed = True
                reservation_expires_at = None
                held: ServerReservation | None = None
                if self._reserving:
                    self._phase = "holding-server"
                    held = await self._reserve(
                        candidate, install_input, _INSTALLED_HOLD, activity_queue, None
                    )
                    reservation_expires_at = held.expires_at if held else None
                # Then hold it only until server-scan knows the machine is taken.
                # A refused hold means the lock is no longer this run's: nothing
                # to give back, so nothing to wait for.
                lock_released: bool | None = None
                server_scan_state: str | None = None
                lock_detail: str | None = None
                if self._release_once_installed and held is not None:
                    self._phase = "awaiting-server-scan"
                    lock_released, server_scan_state, lock_detail = await self._release_once_seen_installed(
                        candidate, install_input, activity_queue
                    )
                    if lock_released:
                        reservation_expires_at = None
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
                return AddServerToMceResult(
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
                    resources_changed=bool(nmstate_replaced)
                    or any(resource.changed for resource in (secret, bmh, nmstate)),
                    # An Agent cannot exist unless Ironic registered the host
                    # and drove it through a boot, so this is implied rather
                    # than separately observed.
                    bmh_registered=True,
                    agent_name=agent.name,
                    attempts=attempts,
                    reservation_expires_at=reservation_expires_at,
                    pxe_registered=pxe_registered,
                    allowed_unhealthy=self._allowed_unhealthy,
                    nmstate_replaced=nmstate_replaced,
                    lock_released=lock_released,
                    server_scan_state=server_scan_state,
                    lock_detail=lock_detail,
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
            # An IPMI candidate's MACs still point at this InfraEnv's discovery
            # image on its site's PXE VM; left there, the machine would boot
            # into it again whoever draws it next. Removed while the lock is
            # still ours — after the release another run may map the same MACs
            # to its own InfraEnv, which this must never delete. (The lock-lapse
            # rollback above skips this for exactly that reason.)
            if self._pxe_cleanup and pxe_registered:
                self._phase = "unregistering-pxe-boot"
                await self._unregister_pxe_boot(
                    candidate, bond_members, segment.site, activity_queue
                )
            no_agent = _describe_no_agent(
                candidate.name, last_state, pxe_registered
            ) + _describe_waived(self._allowed_unhealthy)
            # Since the allocation (the operator, 2026-11-17): into maintenance
            # in server-scan BEFORE the lock goes back, so no run draws it again
            # until a person has looked at it. Kept instead when server-scan
            # would not take it out of the draws.
            release = self._reserving
            if self._allocating and self._reserving:
                self._phase = "setting-maintenance"
                release, lock_note = await self._put_in_maintenance(
                    candidate, install_input, no_agent, activity_queue
                )
            if release:
                self._phase = "releasing-server"
                await self._release(candidate, activity_queue)
            rejections.append(("no-agent", no_agent))

        raise _no_installable_candidate(
            infra_env,
            namespace,
            candidates,
            rejections,
            allocated=allocated,
            lock_note=lock_note,
        )

    async def _evaluate_candidate(
        self,
        candidate: AcquiredServer,
        requested_name: str | None,
        namespace: str,
        task_queue: str,
        rejections: list[tuple[str, str]],
        *,
        allow_link_down: bool = False,
        vouched: bool = False,
    ) -> list[BondMember] | None:
        """One candidate's bond members, or None with a reason recorded.

        `allow_link_down` (a named install's `allow_unhealthy`) relaxes the
        bond's link-up rule and nothing else: every other reason below is still
        a refusal.

        `vouched`: server-scan ALLOCATED the server, live-checked while locked —
        network health HEALTHY, i.e. two link-up ports actually read, and its
        MACs normalized on ingest. Its NICs and MACs are then not judged again
        (the operator, 2026-11-17): the two bond members are taken as it lists
        them, link-UP first, which with its guarantee is exactly the strict
        choice. A MAC the limb cannot use is still refused there, as
        InvalidMacError, before the BareMetalHost or the NMStateConfig is
        written. Only fewer than two ports with a MAC — nothing to bond at all —
        stops it here.

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

        # With the link-up rule waived, None means fewer than two physical
        # ports with a MAC at all — still "no-bond", whose summary holds for it.
        # (No reason of its own: the server-lifecycle page copies these tables,
        # and the hidden option's name must not reach a UI.)
        members = select_bond_members(
            candidate, allow_link_down=allow_link_down or vouched
        )
        if members is None:
            rejections.append(("no-bond", described))
            return None

        bad = [] if vouched else [m.mac for m in members if not mac_is_valid(m.mac)]
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

    async def _acquire(
        self, install_input: AddServerToMceInput, task_queue: str
    ) -> tuple[list[AcquiredServer], list[WaivedHealthCheck], bool]:
        """The server(s) to try, the health refusals waived to get them, and whether server-scan locked them.

        Since the allocation, ONE server: `POST /servers/available` draws it,
        locks it for this run and live-checks it while locked (server-scan
        ADR-0063), so it comes back held — `True`. A run recorded before asks
        `GET /servers/available` for a list of candidates and locks the one it
        chooses itself.

        Either way, a NAMED machine refused is looked up once more
        (_named_server_refused) — to recover this workflow id's own leftover
        lock, which server-scan refuses even to its holder, and, when the
        request asked for `allow_unhealthy`, to waive its health. That machine
        is not locked yet (`False`): the loop takes the lock, as before. A pool
        draw's refusal fails the run.
        """
        try:
            if self._allocating:
                server = await workflow.execute_activity(
                    allocate_server,
                    _allocation_request(install_input),
                    task_queue=task_queue,
                    start_to_close_timeout=_ALLOCATE_TIMEOUT,
                    heartbeat_timeout=_ALLOCATE_HEARTBEAT_TIMEOUT,
                    schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                    retry_policy=_RETRY_POLICY,
                    cancellation_type=_ON_CANCEL,
                )
                return [server], [], True
            candidates = await workflow.execute_activity(
                acquire_servers,
                _acquire_request(
                    install_input.server_name,
                    install_input.infra_env,
                    install_input.candidate_count,
                ),
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
        except ActivityError as err:
            cause = err.cause
            if not (
                install_input.server_name
                and (install_input.allow_unhealthy or self._own_lock_rerun)
                and isinstance(cause, ApplicationError)
                and cause.type == ServerNotAvailableError.__name__
            ):
                raise
            found = await self._named_server_refused(
                install_input.server_name,
                cause,
                task_queue,
                waive_health=install_input.allow_unhealthy,
            )
            if found is None:
                raise
            servers, waived = found
            return servers, waived, False
        return candidates, [], False

    async def _named_server_refused(
        self,
        server_name: str,
        refused: ApplicationError,
        task_queue: str,
        *,
        waive_health: bool,
    ) -> tuple[list[AcquiredServer], list[WaivedHealthCheck]] | None:
        """A named machine `/servers/available` refused: why, and whether the run may go on.

        Its refusal — the GET's 404, the allocation's 503 — cannot say whose
        lock it met or which health category failed, so server-scan's stored
        record is read (lookup_install_candidate) and sorted by health_waiver:

          * a SAFETY reason (unknown, claimed, contested, maintenance, ANOTHER
            run's lock) fails the run, whatever was asked, saying which;
          * the server's own HEALTH fails it too, saying which — unless the
            request asked for `allow_unhealthy`: then it is waived and recorded,
            as is a record showing nothing wrong (the live recheck refused it);
          * THIS workflow id's own live lock is no reason at all. A run that
            failed after its lock — mid-create — leaves it to its TTL, on
            purpose (CLAUDE.md §4), and server-scan hides a locked machine from
            `/servers/available` even from the run that holds it, so without
            this a re-run of the same named install — the documented recovery —
            failed for two hours on a bare "no assignable server". Re-taking the
            lock is an extension (server-scan's own rule).

        None: the record shows no reason at all and nothing was asked to be
        waived, so the refusal stands as server-scan gave it.
        """
        record = await workflow.execute_activity(
            lookup_install_candidate,
            ServerMembershipRequest(server_name=server_name),
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
            cancellation_type=_ON_CANCEL,
        )
        workflow_id = workflow.info().workflow_id
        refusal = safety_refusal(record, workflow_id)
        if refusal is not None:
            message = (
                f"server-scan will not hand out {server_name} ({refused.message}), and "
                f"its record says why: {refusal.reason}"
            )
            if waive_health:
                message += ". That is a safety refusal, which allow_unhealthy never waives"
            elif refusal.another_runs_lock:
                message += (
                    ". A run that failed keeps its lock until it expires; wait for that, "
                    "or release the lock in server-scan once nothing on the MCE still "
                    "points at the machine"
                )
            if refusal.another_runs_lock:
                raise ApplicationError(message, type=ServerReservedError.__name__)
            raise ApplicationError(message, type=ServerNotAvailableError.__name__)

        findings = health_findings(record)
        own_lock = holds_own_lock(record, workflow_id)
        # Never None past safety_refusal, which refuses a record with no server.
        server = cast(AcquiredServer, record.server)
        if waive_health:
            waived = findings
            if not waived and not own_lock:
                waived = [WaivedHealthCheck(check="server-scan-recheck", observed=refused.message)]
            workflow.logger.warning(
                "allow_unhealthy: server-scan refused %s (%s); its record shows no safety "
                "reason, so the install goes ahead",
                server_name,
                refused.message,
            )
            return [server], waived
        if findings:
            raise ApplicationError(
                f"server-scan will not hand out {server_name} ({refused.message}): it is "
                "not healthy — "
                + "; ".join(f"{f.check}: {f.observed}" for f in findings),
                type=ServerNotAvailableError.__name__,
            )
        if own_lock:
            workflow.logger.warning(
                "%s is held by this workflow id's own install lock (%s, until %s), left by "
                "an earlier run of it; re-taking it extends it, and nothing else in "
                "server-scan's record refuses the machine",
                server_name,
                record.lock.holder,
                record.lock.expires_at,
            )
            return [server], []
        return None

    async def _reserve(
        self,
        candidate: AcquiredServer,
        install_input: AddServerToMceInput,
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
                    holder=_reservation_holder(),
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
                cancellation_type=_ON_CANCEL,
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

    async def _replace_conflicting_nmstate(
        self,
        candidate: AcquiredServer,
        resource_request: BmhResourceRequest,
        install_input: AddServerToMceInput,
        task_queue: str,
        rejections: list[tuple[str, str]],
    ) -> NmStateConfigReplacement | None:
        """Replace a differing NMStateConfig, or None with a skip recorded.

        The current limb replaces without looking further (the operator,
        2026-10-31: server-scan offering the machine is the proof it is free).
        A limb from before that refuses with ServerLooksInstalledError when a
        BareMetalHost or Agent shows the machine in use, so that answer is
        still handled: nothing has been written for the candidate yet, so its
        lock goes straight back; a NAMED server fails the run with that error,
        and a pool candidate is a SKIP under `already-installed`.
        """
        try:
            replacement = await workflow.execute_activity(
                replace_conflicting_nmstate_config,
                NmStateConfigReplaceRequest(resource=resource_request),
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
        except ActivityError as err:
            cause = err.cause
            if not (
                isinstance(cause, ApplicationError)
                and cause.type == ServerLooksInstalledError.__name__
            ):
                raise
            workflow.logger.warning("%s looks installed or in progress: %s", candidate.name, cause.message)
            if self._reserving:
                self._phase = "releasing-server"
                await self._release(candidate, task_queue)
            if install_input.server_name:
                raise
            rejections.append(("already-installed", f"{candidate.name} ({cause.message})"))
            return None
        if replacement.replaced:
            workflow.logger.warning(
                "nmstateconfig-replaced for %s: %s",
                candidate.name,
                "; ".join(replacement.differences),
            )
        return replacement

    def _pxe_macs(self, bond_members: list[BondMember]) -> list[str]:
        """The MACs an IPMI host gets on its site's PXE VM.

        The BareMetalHost's `bootMACAddress` alone — bond member nic1, which
        select_bond_members takes from the FIRST link-up NIC server-scan lists
        — so the PXE VM answers exactly the NIC Ironic is told the host boots
        from. Runs recorded before `pxe-map-boot-mac-only` mapped both bond
        members (in case the firmware PXE-boots from the other one) and keep
        doing so on replay.
        """
        if self._pxe_boot_mac_only:
            return [bond_members[0].mac]
        return [member.mac for member in bond_members]

    async def _register_pxe_boot(
        self,
        candidate: AcquiredServer,
        bond_members: list[BondMember],
        install_input: AddServerToMceInput,
        site: str,
        task_queue: str,
        rejections: list[tuple[str, str]],
    ) -> bool:
        """Map the host's MAC(s) to the InfraEnv's iPXE script, or False with a skip recorded.

        Which MACs is _pxe_macs: the BareMetalHost's boot MAC alone.

        Only a site with no PXE VM configured is the CANDIDATE's problem — a
        Redfish machine later in the draw needs no PXE map at all. Everything
        else (no InfraEnv, a rejected token) fails the run, because no other
        IPMI candidate would fare differently.
        """
        try:
            await workflow.execute_activity(
                register_pxe_boot,
                PxeBootRequest(
                    server_name=candidate.name,
                    namespace=install_input.namespace,
                    infra_env=install_input.infra_env,
                    site=site,
                    macs=self._pxe_macs(bond_members),
                    # Set: map the MACs to the InfraEnv's boot entry, which
                    # _await_infra_env_boot publishes. Unset (a recorded run):
                    # map them straight to the InfraEnv's iPXE URL.
                    mce_cluster=(
                        install_input.mce_cluster if self._infra_env_boot else None
                    ),
                ),
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
        except ActivityError as err:
            cause = err.cause
            if not (
                isinstance(cause, ApplicationError)
                and cause.type == PxeSiteNotConfiguredError.__name__
            ):
                raise
            workflow.logger.warning(
                "No PXE map host for %s at site %s: %s",
                candidate.name,
                site,
                cause.message,
            )
            rejections.append(("no-pxe-site", f"{candidate.name} (site {site!r})"))
            return False
        return True

    async def _await_infra_env_boot(
        self,
        candidate: AcquiredServer,
        install_input: AddServerToMceInput,
        site: str,
        task_queue: str,
    ) -> InfraEnvBootState:
        """Publish the InfraEnv's boot entry once its image includes this host, bounded.

        Each poll is one publish ATTEMPT: the activity writes the entry the
        first time it sees an image generated after the NMStateConfig, and
        otherwise reports what it is still waiting for. On the deadline the
        last unpublished state is returned, whose `detail` names that.
        """
        request = InfraEnvBootRequest(
            server_name=candidate.name,
            namespace=install_input.namespace,
            infra_env=install_input.infra_env,
            mce_cluster=install_input.mce_cluster,
            site=site,
        )
        started_at = workflow.now()
        while True:
            state = await workflow.execute_activity(
                publish_infra_env_boot,
                request,
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
            if state.published:
                return state
            if workflow.now() - started_at >= _INFRAENV_IMAGE_DEADLINE:
                return state
            await workflow.sleep(_INFRAENV_IMAGE_POLL_INTERVAL)  # durable, replay-safe

    async def _unregister_pxe_boot(
        self,
        candidate: AcquiredServer,
        bond_members: list[BondMember],
        site: str,
        task_queue: str,
    ) -> None:
        """Take a rolled-back IPMI candidate's MACs off its site's PXE VM.

        The same _pxe_macs the registration mapped — so a run recorded before
        the boot-MAC-only patch unmaps both bond MACs it mapped.
        """
        result = await workflow.execute_activity(
            unregister_pxe_boot,
            PxeMapRef(
                server_name=candidate.name,
                site=site,
                macs=self._pxe_macs(bond_members),
            ),
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
            cancellation_type=_ON_CANCEL,
        )
        if result.detail:
            workflow.logger.warning(
                "PXE map cleanup of %s skipped: %s", candidate.name, result.detail
            )

    async def _roll_back_after_cancel(self, install_input: AddServerToMceInput) -> None:
        """Undo the candidate this cancelled run was installing (the operator, 2026-11-09).

        Before it a cancel left everything in place. Nothing to undo before a
        lock, and nothing once the Agent proved the install: the server stays,
        the cancel only stops the wait for server-scan, and the lock expires.
        Otherwise, after _CANCEL_SETTLE (a call the cancel cut off may still land):

          * anything possibly written on the MCE is taken out by
            remove-server-from-mce as an awaited child, as uninstall-server-from-hc
            does — host powered off first, BareMetalHost + NMStateConfig, any Agent
            it registered, its MACs on its site's PXE VM, a verification, and
            add-server-to-mce's lock for this MCE released;
          * when that child found nothing of it on the MCE — or nothing was
            written — an IPMI mapping this run sent is removed here (the child
            touches the PXE map only for a host it finds), and the lock released
            with this run's holder and id (none of ours is success).

        Never raises: a step that fails is logged and named in `progress`
        (`cancel_rollback`), and the run still ends CANCELED — what is left then
        is what a cancel left before.
        """
        candidate = self._held
        if candidate is None or self._installed:
            if candidate is not None:
                why = f"{candidate.name} is installed and stays: the lock expires on its own"
            elif self._allocating and self._phase == "acquiring-server":
                # The allocation may still land a lock for this run, on a server
                # the run never learns: nothing to release, so its TTL frees it.
                minutes = int(_RESERVATION_TTL.total_seconds() // 60)
                why = (
                    "nothing known to undo: the cancel came while server-scan was "
                    "allocating a server, and one it took for this run frees itself "
                    f"when its lock expires ({minutes} min)"
                )
            else:
                why = "nothing to undo: no server was locked yet"
            self._phase = "cancelled"
            self._cancel_rollback = why
            return
        task_queue = server_lifecycle_activity_queue(install_input.mce_cluster)
        self._phase = "cancel-settling"
        await workflow.sleep(_CANCEL_SETTLE)
        removal: RemoveServerFromMceResult | None = None
        if self._mce_written:
            self._phase = "cancel-removing-server"
            child_id = remove_server_from_mce_workflow_id(candidate.name)
            try:
                removal = await workflow.execute_child_workflow(
                    RemoveServerFromMceWorkflow.run,
                    RemoveServerFromMceRunArgs(
                        input=RemoveServerFromMceInput(
                            server_name=candidate.name,
                            mce_cluster=install_input.mce_cluster,
                            namespace=install_input.namespace,
                        )
                    ),
                    id=child_id,
                    task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
                    # It powers off and deletes in a load-bearing order: closing
                    # this run must never cut it off between two of those steps.
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
            except (WorkflowAlreadyStartedError, ChildWorkflowError) as err:
                cause = getattr(err, "cause", None)
                why = (
                    f"{cause.type}: {cause.message}" if isinstance(cause, ApplicationError)
                    else f"{type(err).__name__}: {err}"
                )
                workflow.logger.warning("Rollback of cancelled %s failed at %s: %s", candidate.name, child_id, why)
                self._phase = "cancelled"
                self._cancel_rollback = (
                    f"{child_id} could not take {candidate.name} out of the MCE ({why}): what it "
                    "left is still there, and the install lock expires on its own"
                )
                return
        done = (
            f"{candidate.name} taken out of the MCE by {remove_server_from_mce_workflow_id(candidate.name)}"
            + ("" if removal.lock_released else f"; lock: {removal.lock_detail or 'kept'}")
            if removal is not None and removal.found
            else None
        )
        if done is None:
            try:
                if self._pxe_mapped is not None:
                    self._phase = "cancel-releasing-lock"
                    bond_members, site = self._pxe_mapped
                    await self._unregister_pxe_boot(candidate, bond_members, site, task_queue)
                self._phase = "cancel-releasing-lock"
                await self._release(candidate, task_queue)
                done = f"{candidate.name}: nothing on the MCE; " + (
                    "PXE mapping removed, " if self._pxe_mapped is not None else ""
                ) + "install lock released"
            except ActivityError as err:
                cause = err.cause
                why = f"{cause.type}: {cause.message}" if isinstance(cause, ApplicationError) else str(cause or err)
                workflow.logger.warning("Rollback of cancelled %s: %s", candidate.name, why)
                done = f"{candidate.name}: cleanup incomplete ({why}); the install lock expires on its own"
        workflow.logger.info("Cancelled run rolled back: %s", done)
        self._phase = "cancelled"
        self._cancel_rollback = done

    async def _release_once_seen_installed(
        self, candidate: AcquiredServer, install_input: AddServerToMceInput, task_queue: str
    ) -> tuple[bool, str | None, str]:
        """Give the lock back once server-scan shows the machine installed: (released, state, why).

        server-scan is read every _SERVER_SCAN_POLL_INTERVAL for up to
        _SERVER_SCAN_WAIT (lookup_server_membership: the search and the
        exact-name detail, never /servers/available). On INSTALLED_TO_INVENTORY
        or INSTALLED the lock is released with this run's holder and workflow
        id, which server-scan compares, so a lock someone took over meanwhile
        is left alone. Every other ending keeps the lock — it then expires on
        its own — and NONE fails the run, which the Agent already proved: not
        seen within the wait, server-scan unreadable for the rest of it, or it
        naming ANOTHER MCE (a contradiction for a person to look at).
        """
        started_at = workflow.now()
        state: str | None = None
        while True:
            remaining = _SERVER_SCAN_WAIT - (workflow.now() - started_at)
            if remaining <= timedelta(0):
                hours = int(_SERVER_SCAN_WAIT.total_seconds() // 3600)
                return False, state, (
                    f"server-scan still shows {candidate.name} {state or 'not installed'} after "
                    f"{hours} h: the install lock is left to expire on its own"
                )
            try:
                membership = await workflow.execute_activity(
                    lookup_server_membership,
                    ServerMembershipRequest(server_name=candidate.name),
                    task_queue=task_queue,
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    # Best effort, bounded by what is left of the wait: server-scan
                    # or this MCE's worker away for the rest of it ends the wait,
                    # never the run (caught below).
                    schedule_to_close_timeout=remaining,
                    retry_policy=_RETRY_POLICY,
                    cancellation_type=_ON_CANCEL,
                )
            except ActivityError as err:
                if is_cancelled_exception(err):
                    raise  # a cancel of the run, not server-scan unreadable
                cause = err.cause
                why = f"{cause.type}: {cause.message}" if isinstance(cause, ApplicationError) else str(cause or err)
                workflow.logger.warning("server-scan could not be read for %s: %s", candidate.name, why)
                return False, state, (
                    f"server-scan could not be read for {candidate.name} ({why}): the install "
                    "lock is left to expire on its own"
                )
            state = membership.lifecycle_state if membership.found else None
            if state in _INSTALLED_STATES:
                if membership.mce_name and canonical_mce_cluster(membership.mce_name) != canonical_mce_cluster(
                    install_input.mce_cluster
                ):
                    workflow.logger.warning(
                        "server-scan shows %s %s in MCE %s, not %s: lock kept",
                        candidate.name, state, membership.mce_name, install_input.mce_cluster,
                    )
                    return False, state, (
                        f"server-scan shows {candidate.name} {state} in MCE {membership.mce_name}, not "
                        f"{install_input.mce_cluster}: the install lock is left to expire on its own"
                    )
                self._phase = "releasing-lock"
                released = await self._release(candidate, task_queue)
                if released.detail:
                    return False, state, (
                        f"server-scan shows {candidate.name} {state}, but the install lock was not "
                        f"released: {released.detail}"
                    )
                workflow.logger.info("server-scan shows %s %s: install lock released", candidate.name, state)
                return True, state, f"server-scan shows {candidate.name} {state}: the install lock was released"
            await workflow.sleep(_SERVER_SCAN_POLL_INTERVAL)  # durable, replay-safe

    async def _put_in_maintenance(
        self,
        candidate: AcquiredServer,
        install_input: AddServerToMceInput,
        no_agent: str,
        task_queue: str,
    ) -> tuple[bool, str | None]:
        """Take a server that produced no Agent out of every draw: (release its lock now, closing words).

        Into maintenance in server-scan BEFORE the lock goes back — the other
        order leaves a moment in which the next run can draw it — and read back
        by the activity. The reason says which run, where, and what the host's
        own state points at, for the person who ends the maintenance.

        server-scan no longer knowing the server leaves nothing to keep out of
        a draw: the lock is released as before. A refusal (a token without the
        ADMIN role, a rejected request) KEEPS the lock, to expire at its TTL, so
        the server is not drawn again at once — and the run still fails for its
        missing Agent, not for this. A cancel is never swallowed here.
        """
        minutes = int(_AGENT_DEADLINE.total_seconds() // 60)
        # `no_agent` names the server, the deadline and where its state points.
        reason = _maintenance_reason(
            f"add-server-to-mce run {workflow.info().workflow_id} installed it into "
            f"InfraEnv {install_input.namespace}/{install_input.infra_env} on "
            f"{install_input.mce_cluster}: {no_agent}"
        )
        try:
            maintenance = await workflow.execute_activity(
                put_server_in_maintenance,
                ServerMaintenanceRequest(
                    server_id=candidate.id, server_name=candidate.name, reason=reason
                ),
                task_queue=task_queue,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=_ON_CANCEL,
            )
        except ActivityError as err:
            if is_cancelled_exception(err):
                raise
            cause = err.cause
            kind = (
                cause.type
                if isinstance(cause, ApplicationError) and cause.type
                else type(cause).__name__
            )
            workflow.logger.error(
                "server-scan would not put %s in maintenance (%s): its install lock is "
                "kept until it expires instead",
                candidate.name,
                cause,
            )
            return False, (
                "Its lock is kept until it expires, because server-scan would not put "
                f"it in maintenance ({kind})"
            )
        if not maintenance.enabled:
            workflow.logger.warning(
                "%s not put in maintenance: %s", candidate.name, maintenance.detail
            )
            return True, None
        workflow.logger.warning(
            "%s put in maintenance in server-scan: no Agent in %d min", candidate.name, minutes
        )
        return True, (
            "Its lock was given back once server-scan had it in maintenance, so no run "
            "draws it again: end the maintenance there once the cause is fixed"
        )

    async def _release(self, candidate: AcquiredServer, task_queue: str) -> ServerReservation:
        """Give the lock back: after a teardown proved nothing points at the machine, or once server-scan knows it."""
        released = await workflow.execute_activity(
            release_server,
            ReleaseServerRequest(
                server_id=candidate.id,
                server_name=candidate.name,
                holder=_reservation_holder(),
                workflow_id=workflow.info().workflow_id,
            ),
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
            cancellation_type=_ON_CANCEL,
        )
        if released.detail:
            workflow.logger.info(
                "Released %s in server-scan: %s", candidate.name, released.detail
            )
        return released

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
            cancellation_type=_ON_CANCEL,
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
            cancellation_type=_ON_CANCEL,
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
            cancellation_type=_ON_CANCEL,
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
                cancellation_type=_ON_CANCEL,
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
            cancellation_type=_ON_CANCEL,
        )


@workflow.defn(name=_LEGACY_WORKFLOW_TYPE)
class InstallServerWorkflow(AddServerToMceWorkflow):
    """add-server-to-mce under its old type, for runs started before the rename.

    Their history names `InstallServerWorkflow` on the old queue
    (LEGACY_INSTALL_SERVER_WORKFLOW_QUEUE), and only a worker registering that
    type there can replay them — tests/histories/install_server/ is the proof.
    Same run, same queries; _reservation_holder keeps their lock under
    `install-server`. Nothing starts this type any more: delete it, and its
    registration in main_worker_init.py, once
    `WorkflowType='InstallServerWorkflow' AND ExecutionStatus='Running'` finds
    nothing.
    """

    # The SDK takes @workflow.run only from the class it registers, not a base.
    @workflow.run
    async def run(self, run_args: AddServerToMceRunArgs) -> AddServerToMceResult:
        return await super().run(run_args)
