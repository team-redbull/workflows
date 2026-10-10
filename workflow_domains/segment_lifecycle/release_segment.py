"""release-segment — gives a decommissioned hosted cluster's VLAN segment back
to the pool, removing its DHCP scope first if one is still there, from one
durable run.

The THIRD workflow of the `segment-lifecycle` domain, and the mirror of
allocate-segment: it reuses the running `segment-lifecycle-worker` limb, its
activity queue and its ConfigMap — only this module, its own workflow queue
and its route are new.

Where it sits. A hosted cluster is decommissioned by deleting its
`<cluster>.yaml` from the day1 values repo's main. Argo CD then deletes the
Application named after the cluster; the Application's resources finalizer
deletes the Crossplane Request CR, and provider-http sends the DHCP API its
DELETE for the scope. Nothing in that cascade gives the segment back — the
Segments Manager keeps it Allocated to a cluster that no longer exists. This
run does, and it is started only AFTER the Application and everything it
managed are gone: today by the hostedcluster-setup chart's PostDelete hook
(Argo CD runs it once every resource of the Application is deleted), later as
a child of a deprovision-cluster workflow that owns that wait itself.

Shape of the run:

  0. up-front gate        — type must be HC: allocate-segment only ever
                            allocates HC, so any other allocation was not made
                            by this system and its teardown is not ours.
  1. locating-allocation  — the Segments Manager's Allocated segment for
                            (cluster, type). None is a COMPLETED no-op
                            (released=False): already released, or never
                            allocated — a late or repeated trigger stays green.
  2. removing-dhcp-scope  — GET the scope for the segment's network. Normally
                            404 (the cascade deleted it). If it survived — an
                            orphaned Request, a cascade that did not run — this
                            is the safety net: DELETE it, then read back its
                            absence. The scope goes BEFORE the segment: a
                            segment re-allocated while its old scope still
                            serves leases would collide with them.
  3. releasing-segment    — re-read the segment's owner (the Segments
                            Manager's release checks nobody's ownership, and
                            the whole DHCP phase sits between the lookup and
                            here), then POST /api/segments/release. Already
                            Available is success — someone released it first.
  4. verifying-release    — read it back: Available, no cluster, no type.

There is deliberately NO git step: deleting the cluster's file IS the
decommission, and it is already gone when the run starts.

This run is the ONLY DELETE caller of the DHCP API in the orchestrator, and
it never POSTs or PUTs a scope — Crossplane stays the only creator/updater
(CLAUDE.md §4). Its DELETE is safe only once the Request CR is gone, or
provider-http would re-create the scope on its next poll; the PostDelete hook
guarantees that, a manual caller must wait for the Application to disappear.

A DHCP API that is down (or 503s with no DHCP backend) keeps the run RUNNING
in removing-dhcp-scope, retrying, and the segment is NOT released until it
answers — by design, the scope goes before the pool.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
    from shared.exceptions import (
        AllocationOwnerMismatchError,
        AmbiguousAllocationError,
        DhcpApiAuthError,
        DhcpScopeInvalidError,
        DhcpScopeStillPresentError,
        ReleaseNotConfirmedError,
        SegmentNotFoundError,
        SegmentsManagerAuthError,
        SegmentValidationError,
        UnsupportedSegmentType,
    )
    from shared.interfaces.segment_lifecycle import (
        delete_dhcp_scope,
        find_cluster_allocation,
        get_dhcp_scope,
        get_segment,
        release_segment,
    )
    from shared.models.segment_lifecycle import (
        ClusterAllocationLookupRequest,
        ReleaseSegmentProgress,
        ReleaseSegmentResult,
        ReleaseSegmentRunArgs,
        SegmentType,
    )

# Same budget rules as allocate-segment: bounded attempts (the HTTP client
# times out below them), UNBOUNDED retries so transient outages are
# out-waited, and every known-permanent error classified. The DHCP API runs
# PowerShell against a remote Windows server, each command capped at 60s on
# its side and a DELETE chaining several, so its two activities get a larger
# per-attempt budget (the activity's own HTTP timeout is 150s).
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
_DHCP_ACTIVITY_TIMEOUT = timedelta(seconds=180)

# Every deterministic error the activities below can raise. Built from the
# CLASSES, never written as strings: Temporal matches the list against the
# error's type name, so a misspelt string would silently mean "retry forever",
# while a wrong name here is an ImportError at worker startup.
_PERMANENT_ACTIVITY_ERRORS = (
    SegmentsManagerAuthError,
    SegmentNotFoundError,
    SegmentValidationError,
    # Two Allocated segments of one type for one cluster: a Segments Manager
    # data error a human resolves — never guess which to release.
    AmbiguousAllocationError,
    # The worker's copy of the DHCP API token is wrong or rotated.
    DhcpApiAuthError,
    # 400 INVALID_SCOPE: the network we derived is not one the API takes.
    DhcpScopeInvalidError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


@workflow.defn
class ReleaseSegmentWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> ReleaseSegmentProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return ReleaseSegmentProgress(phase=self._phase)

    @workflow.run
    async def run(self, run_args: ReleaseSegmentRunArgs) -> ReleaseSegmentResult:
        release_input = run_args.input
        cluster = release_input.cluster
        segment_type = release_input.type
        workflow.logger.info(
            "Releasing the %s segment of cluster=%s", segment_type.value, cluster
        )
        # Strict first check (§7), symmetric with allocate-segment. Raised
        # from workflow code, so a plain ApplicationError — non_retryable would
        # be inert here, workflow failures are never retried.
        if segment_type != SegmentType.HC:
            raise ApplicationError(
                f"release-segment supports type=HC only (got {segment_type.value}) "
                "— allocate-segment allocates nothing else, so no other "
                "allocation was made by this system",
                type=UnsupportedSegmentType.__name__,
            )

        # Step 1 — which segment does the cluster hold?
        self._phase = "locating-allocation"
        lookup = await workflow.execute_activity(
            find_cluster_allocation,
            ClusterAllocationLookupRequest(cluster=cluster, type=segment_type),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if not lookup.found or lookup.entry is None:
            # Already released, or never allocated: nothing to give back. A
            # COMPLETED no-op, so a repeated trigger (a retried hook, a second
            # deletion of the same Application) never turns red.
            self._phase = "completed"
            workflow.logger.info(
                "Cluster %s holds no %s segment — nothing to release",
                cluster,
                segment_type.value,
            )
            return ReleaseSegmentResult(
                cluster=cluster,
                type=segment_type,
                released=False,
                segment=None,
                vlan_id=None,
                site=None,
                dhcp_scope_removed=False,
            )
        entry = lookup.entry

        # Step 2 — the DHCP scope, BEFORE the segment returns to the pool. The
        # scope is keyed by the mask-stripped network address (same
        # derivation as the initialize-segment id; the DHCP API's 400 is the
        # validator of record for what it accepts).
        self._phase = "removing-dhcp-scope"
        network = entry.segment.split("/", 1)[0]
        scope = await workflow.execute_activity(
            get_dhcp_scope,
            network,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_DHCP_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if scope.found:
            workflow.logger.info(
                "DHCP scope %s survived the Argo CD cascade — deleting it", network
            )
            await workflow.execute_activity(
                delete_dhcp_scope,
                network,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_DHCP_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            # Verify after mutate, before recording (§6): the segment must not
            # go back to the pool on the strength of a 204 alone.
            after_delete = await workflow.execute_activity(
                get_dhcp_scope,
                network,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_DHCP_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if after_delete.found:
                raise ApplicationError(
                    f"DHCP scope {network} is still present after the delete — "
                    f"something re-created it, almost certainly a Crossplane "
                    f"Request for cluster {cluster} that still exists (its "
                    "Argo CD Application is not gone yet). The segment was NOT "
                    "released; re-run once the Application has been deleted",
                    type=DhcpScopeStillPresentError.__name__,
                )

        # Step 3 — release, after re-reading who holds the segment: the
        # Segments Manager's release checks no ownership, and the DHCP phase
        # (retries included) sits between the lookup and here.
        self._phase = "releasing-segment"
        current = await workflow.execute_activity(
            get_segment,
            entry.segment,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if current.cluster_name is not None and current.cluster_name != cluster:
            raise ApplicationError(
                f"Segment {entry.segment} is now held by cluster "
                f"{current.cluster_name!r}, not {cluster!r} — it was released and "
                "re-allocated since the lookup. Nothing was released",
                type=AllocationOwnerMismatchError.__name__,
            )
        if current.status == "Available":
            # Released by someone else since the lookup: already done is
            # success (§6). The read-back below still confirms the state.
            workflow.logger.info(
                "Segment %s is already Available — nothing to release", entry.segment
            )
        else:
            await workflow.execute_activity(
                release_segment,
                entry.segment,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )

        # Step 4 — verify by read-back: the segment is back in the shared
        # pool with nothing of the allocation left on it.
        self._phase = "verifying-release"
        released = await workflow.execute_activity(
            get_segment,
            entry.segment,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        checks = [
            ("status", "Available", released.status),
            ("cluster_name", None, released.cluster_name),
            ("type", None, released.type),
        ]
        mismatches = [
            f"{field}: expected {expected!r}, read back {actual!r}"
            for field, expected, actual in checks
            if expected != actual
        ]
        if mismatches:
            raise ApplicationError(
                f"Release of {entry.segment} did not verify: " + "; ".join(mismatches),
                type=ReleaseNotConfirmedError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "Segment %s (vlan=%d) released from cluster %s (DHCP scope %s %s)",
            entry.segment,
            entry.vlan_id,
            cluster,
            network,
            "deleted by this run" if scope.found else "already gone",
        )
        return ReleaseSegmentResult(
            cluster=cluster,
            type=segment_type,
            released=True,
            segment=entry.segment,
            vlan_id=entry.vlan_id,
            site=entry.site,
            dhcp_scope_removed=scope.found,
        )
