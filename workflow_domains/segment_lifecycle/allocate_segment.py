"""allocate-segment — reserves a VLAN segment for a hosted cluster in the
Segments Manager and writes the vlanId + dhcp_values block into the cluster's
values file, on the values-repo branch the run names, from one durable run.

The SECOND workflow of the `segment-lifecycle` domain: it reuses the running
`segment-lifecycle-worker` limb, its activity queue and its ConfigMap — only
this module, its own workflow queue and its route are new. It replaces the
GitLab CI bash pipeline that used to allocate VLANs on merge-request events:
that script had no durable retry, never verified the allocation landed, and
degraded to "cluster created without a VLAN" whenever the Segments Manager
was unreachable.

Shape of the run:

  1. resolving-site       — the site is DERIVED from where the cluster's
                            values file sits in the repo (sites/<site>/...),
                            cross-checked against GET /api/sites. There is
                            deliberately no `site` input: the repo layout is
                            the source of truth, and a caller can never claim
                            a site the cluster does not live in.
  2. allocating-segment   — POST /api/segments/allocate, idempotent per
                            (cluster, site, type) server-side. Available
                            segments carry no type: the Segments Manager hands
                            out any Available one at the site and stamps the
                            requested type onto it in the same update.
  3. verifying-allocation — read the segment back and require
                            status=Allocated + the exact cluster + the exact
                            vlan + the requested type (the allocation wrote
                            it). Verification sits BEFORE the git write, so
                            a vlan is never recorded in the values repo
                            unless the Segments Manager confirms the
                            reservation.
  4. updating-values-repo — append the marker block (vlanId + dhcp_values),
                            commit with [skip ci], push to `values_branch`.
                            Git stays the single source of truth; a re-run
                            finds the block already present and pushes
                            nothing.

Where it runs: as a step INSIDE the day1 values repo's CI pipeline, which runs
on a temporary branch only (never on main). The pipeline passes its own branch
as `values_branch` — the new cluster's file exists only there — and waits for
this run to complete before generating the MachineConfig files from the vlanId
it recorded. A human merges the branch afterwards. The run ends at the push.

There is deliberately NO wait for the DHCP scope (there was one — CLAUDE.md §4
has the history). Argo CD reads main only, so the scope appears only after the
merge, and the merge comes after the pipeline, which is waiting on this run: a
scope wait here would deadlock the flow it is a step of.

HC only for now: any other type fails up front with UnsupportedSegmentType.
Without that gate a non-HC run would still die — only hostedCluster files
are named <cluster>.yaml, so locate_cluster_file finds nothing — but as a
misleading ClusterFileNotFoundError. Future types get their own functions
when they arrive.

On cancellation there is deliberately no compensating cleanup: the
allocation is kept (a re-run idempotently reclaims it and converges the git
write), and this workflow has no external pending-display to clear — the
initialize-segment failure-note machinery has no counterpart here.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
    from shared.interfaces.segment_lifecycle import (
        allocate_segment,
        append_allocation_to_cluster_values,
        get_segment,
        get_valid_sites,
        locate_cluster_file,
    )
    from shared.models.segment_lifecycle import (
        AllocateSegmentProgress,
        AllocateSegmentResult,
        AllocateSegmentRunArgs,
        ClusterFileLookupRequest,
        ClusterValuesAppendRequest,
        SegmentAllocationRequest,
        SegmentType,
    )

# Same budget rules as initialize-segment: bounded attempts (with the HTTP
# client timing out below), UNBOUNDED retries so transient outages are
# out-waited, and every known-permanent error classified. Git activities
# (clone + push) get a larger per-attempt budget.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
_GIT_ACTIVITY_TIMEOUT = timedelta(seconds=180)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[
        "SegmentsManagerAuthError",
        "SegmentNotFoundError",
        # The Segments Manager answers 400 for a bad cluster name — its own
        # validation, which the UnknownSite pre-check below does not cover.
        "SegmentValidationError",
        "ClusterFileNotFoundError",
        "AmbiguousClusterFileError",
        "SegmentPoolExhaustedError",
        "ClusterValuesConflictError",
        # The branch the caller named is not on the remote — a typo or a
        # branch deleted after the trigger; retrying cannot make it appear.
        "ValuesBranchNotFoundError",
    ],
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
            "Allocating a %s segment for cluster=%s",
            allocate_input.type.value,
            cluster,
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
                type="UnsupportedSegmentType",
            )
        # The API requires the branch (AllocateSegmentRequest); the boundary
        # model defaults it to None only so history recorded before the field
        # existed still decodes. A run without one has no branch to record on,
        # and there is no configured fallback — that would push to main.
        values_branch = allocate_input.values_branch
        if values_branch is None:
            raise ApplicationError(
                "allocate-segment needs the values-repo branch to record the "
                "allocation on (values_branch) — the API requires it, so this "
                "run was not started through the router",
                type="ValuesBranchMissing",
            )

        # Step 1 — resolve the site from the values repo, cross-checked
        # against the Segments Manager. Independent lookups, fanned out.
        self._phase = "resolving-site"
        valid_sites, location = await asyncio.gather(
            workflow.execute_activity(
                get_valid_sites,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            ),
            workflow.execute_activity(
                locate_cluster_file,
                ClusterFileLookupRequest(cluster=cluster, values_branch=values_branch),
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            ),
        )
        if location.site not in valid_sites:
            raise ApplicationError(
                f"Cluster {cluster}'s values file sits under site "
                f"{location.site!r} ({location.relative_path}), which the "
                f"Segments Manager does not know (valid: {sorted(valid_sites)})",
                type="UnknownSite",
            )

        # Step 2 — reserve the segment (idempotent per cluster/site/type).
        self._phase = "allocating-segment"
        allocation = await workflow.execute_activity(
            allocate_segment,
            SegmentAllocationRequest(
                cluster=cluster, site=location.site, type=allocate_input.type
            ),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Step 3 — verify by read-back BEFORE anything is written to git: the
        # values repo must never record a vlan the Segments Manager does not
        # confirm as reserved for this exact cluster.
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
                type="AllocationNotConfirmed",
            )

        # Step 4 — record the allocation in the values repo (idempotent: a
        # re-run finds this vlan/network already recorded and pushes nothing,
        # leaving any operator-tuned detail in that block alone).
        self._phase = "updating-values-repo"
        commit_ref = await workflow.execute_activity(
            append_allocation_to_cluster_values,
            ClusterValuesAppendRequest(
                cluster=cluster,
                relative_path=location.relative_path,
                vlan_id=allocation.vlan_id,
                segment=allocation.segment,
                type=allocate_input.type,
                values_branch=values_branch,
            ),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        self._phase = "completed"
        workflow.logger.info(
            "Segment %s (vlan=%d) allocated to %s and recorded on branch %s "
            "(network %s)",
            allocation.segment,
            allocation.vlan_id,
            cluster,
            values_branch,
            commit_ref.dhcp_values.network,
        )
        return AllocateSegmentResult(
            cluster=cluster,
            site=location.site,
            type=allocate_input.type,
            vlan_id=allocation.vlan_id,
            segment=allocation.segment,
            epg_name=allocation.epg_name,
            values_branch=values_branch,
            commit_sha=commit_ref.commit_sha,
            values_updated=commit_ref.changed,
        )
