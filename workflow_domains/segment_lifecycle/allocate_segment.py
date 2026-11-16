"""allocate-segment — reserves a VLAN segment for a hosted cluster in the
Segments Manager and verifies the reservation landed, from one durable run.

The SECOND workflow of the `segment-lifecycle` domain: it reuses the running
`segment-lifecycle-worker` limb, its activity queue and its ConfigMap — only
this module, its own workflow queue and its route are new. It replaced the
GitLab CI bash pipeline that used to allocate VLANs on merge-request events:
that script had no durable retry, never verified the allocation landed, and
degraded to "cluster created without a VLAN" whenever the Segments Manager
was unreachable.

Shape of the run:

  1. validating-site      — the site is the caller's input, cross-checked
                            against GET /api/sites. provision-cluster passes
                            the day1 folder the cluster file goes in
                            (sites/<site>/mces/<mce>/), so the repo layout is
                            still the source of truth; a site the Segments
                            Manager does not know fails here, before anything
                            is allocated.
  2. allocating-segment   — POST /api/segments/allocate, idempotent per
                            (cluster, site, type) server-side. Available
                            segments carry no type: the Segments Manager hands
                            out any Available one at the site and stamps the
                            requested type onto it in the same update.
  3. verifying-allocation — read the segment back and require
                            status=Allocated + the exact cluster + the exact
                            vlan + the requested type (the allocation wrote
                            it). The run ends here: the allocation is on
                            record in the Segments Manager, verified.

Where it runs: as an AWAITED CHILD of provision-cluster (the cluster-lifecycle
domain), which then writes the vlanId + dhcp_values block into the cluster
file it publishes. Until 2026-11-11 this run located the cluster file on a
day1 branch the caller named and pushed the block itself, as a step inside
the day1 CI pipeline; the file is provision-cluster's now, so the git half
went with it (`values_branch` is refused at the API, ignored on an old
run's input). The standalone route stays for a manual allocation, on the same
id, so a manual run and a child dedup against each other.

There is deliberately NO wait for the DHCP scope (there was one — CLAUDE.md §4
has the history). Argo CD reads main only, so the scope appears only after a
human merges the cluster file — after this run, outside it.

HC only for now: any other type fails up front with UnsupportedSegmentType.
Future types get their own functions when they arrive.

On cancellation there is deliberately no compensating cleanup: the
allocation is kept (a re-run idempotently reclaims it), and this workflow has
no external pending-display to clear.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
    from shared.exceptions import (
        AllocationNotConfirmed,
        SegmentNotFoundError,
        SegmentPoolExhaustedError,
        SegmentsManagerAuthError,
        SegmentValidationError,
        SiteMissingError,
        UnknownSite,
        UnsupportedSegmentType,
    )
    from shared.interfaces.segment_lifecycle import (
        allocate_segment,
        get_segment,
        get_valid_sites,
    )
    from shared.models.segment_lifecycle import (
        AllocateSegmentProgress,
        AllocateSegmentResult,
        AllocateSegmentRunArgs,
        SegmentAllocationRequest,
        SegmentType,
    )

# Same budget rules as initialize-segment: bounded attempts (with the HTTP
# client timing out below), UNBOUNDED retries so transient outages are
# out-waited, and every known-permanent error classified — as CLASSES, so a
# wrong name is an ImportError at worker start, never a silent "retryable".
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    SegmentsManagerAuthError,
    SegmentNotFoundError,
    # The Segments Manager answers 400 for a bad cluster name — its own
    # validation, which the UnknownSite pre-check below does not cover.
    SegmentValidationError,
    SegmentPoolExhaustedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


@workflow.defn
class AllocateSegmentWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> AllocateSegmentProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return AllocateSegmentProgress(phase=self._phase)

    @workflow.run
    async def run(self, run_args: AllocateSegmentRunArgs) -> AllocateSegmentResult:
        allocate_input = run_args.input
        cluster = allocate_input.cluster
        workflow.logger.info(
            "Allocating a %s segment for cluster=%s at site=%s",
            allocate_input.type.value,
            cluster,
            allocate_input.site,
        )
        # Strict first check (§7): this workflow supports HC allocation only.
        # Deterministic failures raised from workflow code are plain
        # ApplicationErrors — non_retryable would be inert here, workflow
        # failures are never retried.
        if allocate_input.type != SegmentType.HC:
            raise ApplicationError(
                f"allocate-segment supports type=HC only (got "
                f"{allocate_input.type.value}); future segment types get "
                "their own functions when they arrive",
                type=UnsupportedSegmentType.__name__,
            )
        # The API requires the site (AllocateSegmentRequest); the boundary
        # model defaults it to None only so history recorded before the field
        # existed still decodes. A run without one has nothing to allocate at,
        # and there is nothing to derive it from any more.
        site = allocate_input.site
        if site is None:
            raise ApplicationError(
                "allocate-segment needs the site to allocate at (site) — the "
                "API requires it, so this run was not started through the router",
                type=SiteMissingError.__name__,
            )

        # Step 1 — the site must be one the Segments Manager knows.
        self._phase = "validating-site"
        valid_sites = await workflow.execute_activity(
            get_valid_sites,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if site not in valid_sites:
            raise ApplicationError(
                f"Site {site!r} (the day1 folder cluster {cluster}'s file goes in) "
                f"is not one the Segments Manager knows (valid: {sorted(valid_sites)})",
                type=UnknownSite.__name__,
            )

        # Step 2 — reserve the segment (idempotent per cluster/site/type).
        self._phase = "allocating-segment"
        allocation = await workflow.execute_activity(
            allocate_segment,
            SegmentAllocationRequest(cluster=cluster, site=site, type=allocate_input.type),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Step 3 — verify by read-back BEFORE anything records it: the cluster
        # file provision-cluster writes must never carry a vlan the Segments
        # Manager does not confirm as reserved for this exact cluster.
        self._phase = "verifying-allocation"
        entry = await workflow.execute_activity(
            get_segment,
            allocation.segment,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        checks = [
            ("status", "Allocated", entry.status),
            ("cluster_name", cluster, entry.cluster_name),
            ("vlan_id", allocation.vlan_id, entry.vlan_id),
        ]
        # The allocation also WROTE the type (Available segments have none), so
        # it is read back like the rest — but only when the read-back carries
        # the field at all. A payload WITHOUT the key comes from before it
        # existed: either a run replaying history recorded by the previous
        # code (it must take the path it originally took), or a limb one
        # rollout behind this brain (build.yml ships the brain first). Neither
        # is evidence about the allocation. A reported null always has the key,
        # so it still fails the check.
        if "type" in entry.model_fields_set:
            checks.append(("type", allocate_input.type.value, entry.type))
        mismatches = [
            f"{field}: expected {expected!r}, read back {actual!r}"
            for field, expected, actual in checks
            if expected != actual
        ]
        if mismatches:
            raise ApplicationError(
                f"Allocation of {allocation.segment} did not verify: "
                + "; ".join(mismatches),
                type=AllocationNotConfirmed.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "Segment %s (vlan=%d) allocated to %s at %s and verified",
            allocation.segment,
            allocation.vlan_id,
            cluster,
            site,
        )
        return AllocateSegmentResult(
            cluster=cluster,
            site=site,
            type=allocate_input.type,
            vlan_id=allocation.vlan_id,
            segment=allocation.segment,
            epg_name=allocation.epg_name,
        )
