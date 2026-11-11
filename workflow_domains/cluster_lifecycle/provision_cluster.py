"""provision-cluster — a hosted cluster from the YAML a person wrote: its segment, its
day1 file on a branch, the merge request, the wait for a person to merge it, and its
registration in the day2 sig repos.

The orchestrator is the ENTRY POINT for a new cluster (CLAUDE.md §4). The caller
hands over the COMPLETE hosted-cluster file (owner, mastertag, nodepools with
their config names — the slim shape every cluster file has before the day1 CI
chain works on it), the name of the cluster and the MCE folder it goes in.

Shape of the run:

  1. inspecting-target      — day1 `main` is read: the MCE folder is found by
                              name, the cluster must have no file there yet,
                              and the text is judged against the merged value
                              layers the way the chart renders them. Every
                              reason is in one failure. Nothing written.
  2. allocating-segment     — allocate-segment as an AWAITED CHILD on its own
                              id (`allocate-segment-HC-<cluster>`) and queue:
                              the Segments Manager reserves a /24 at the
                              folder's site and the child verifies it by
                              read-back. A child already running is a named
                              failure; a child's failure carries its type.
  3. publishing-cluster-file — the person's text plus the allocation block
                              (vlanId, dhcp_values with the type's exclusion
                              policy), pushed to `provision-cluster/<cluster>`
                              — created from main by the push, NO `[skip ci]`:
                              the day1 CI chain runs on the branch, skips
                              allocation (vlanId is there), renders the
                              MachineConfigs and pushes its own `[skip ci]`
                              commit with `mcConfigMap` on top. A re-run finds
                              the branch, compares the file SEMANTICALLY
                              (that appended key set aside) and pushes nothing.
  4. opening-merge-request  — the open merge request from the branch to main,
                              found or created (GitLab: remove_source_branch;
                              GitHub: a pull request).
  5. awaiting-merge         — a HUMAN wait (CLAUDE.md §5): the merge request
                              is read every _MERGE_POLL on a durable timer,
                              with no deadline ever; every _POLLS_PER_RUN
                              polls, or when the server suggests it, the run
                              CONTINUES AS NEW carrying its state, so history
                              stays bounded for as long as the review takes.
                              Closed without merging is a failure.
  6. registering-day2       — only AFTER the merge: for each sig the value
                              layers list under day2.copy_to_sigs, the folder
                              sites/<site>/<env>/mces/<mce>/<cluster>/ in that
                              sig's repo, on its default branch (already there
                              is success). After the merge because day2 renders
                              mastertag from day1 main: a folder created before
                              breaks the sig's whole render.
  7. completed              — the file, the branch, the allocation, the merge
                              request, the day2 folders.

What the run never does: render a MachineConfig (the day1 chain does, on the
branch, as it always has), write `mcConfigMap`, push to `main`, merge, sync the
Application (a person does, by hand, in production), or undo anything on
failure — the allocation is idempotent, the branch and the merge request
converge on a re-run, the day2 folders are "already there = success".

A run's input is decoded from history on every replay: the YAML text travels
verbatim, never re-serialised. No `schedule_to_start_timeout` on any activity
here: Temporal never retries it, and a run spans a merge wait of days — a limb
pod away for five minutes must not fail it (§5).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError, WorkflowAlreadyStartedError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import provision_cluster_branch
    from shared.consts import ALLOCATE_SEGMENT_WORKFLOW_QUEUE, CLUSTER_LIFECYCLE_ACTIVITY_QUEUE
    from shared.exceptions import (
        AllocateSegmentAlreadyRunningError,
        AllocateSegmentFailedError,
        AmbiguousMceFolderError,
        ClusterAlreadyProvisionedError,
        ClusterFileConflictError,
        ClusterValuesRejectedError,
        Day2EnvMissingError,
        GitProviderAuthError,
        GitProviderRequestRejectedError,
        MceFolderNotFoundError,
        MergeRequestClosedError,
    )
    from shared.interfaces.cluster_lifecycle import (
        ensure_merge_request,
        get_merge_request_state,
        inspect_cluster_target,
        publish_cluster_file,
        register_cluster_in_day2_sig,
    )
    from shared.models.cluster_lifecycle import (
        ClusterFilePublishRequest,
        ClusterFileRef,
        ClusterTarget,
        ClusterTargetRequest,
        Day2SigRegistration,
        Day2SigRegistrationRequest,
        MergeRequestRef,
        MergeRequestRequest,
        MergeRequestState,
        ProvisionClusterInput,
        ProvisionClusterProgress,
        ProvisionClusterResult,
        ProvisionClusterRunArgs,
        ProvisionClusterState,
    )
    from shared.models.segment_lifecycle import (
        AllocateSegmentInput,
        AllocateSegmentResult,
        AllocateSegmentRunArgs,
        SegmentType,
    )
    from shared.workflow_ids import allocate_segment_workflow_id
    from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow

# One HTTP call to the git provider (60 s in the limb) fits well inside this.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# A clone and a push, as every git activity in the repo.
_GIT_ACTIVITY_TIMEOUT = timedelta(seconds=180)
# The merge wait's durable timer. A constant: changing it is a
# non-deterministic change for runs in flight (CLAUDE.md §5).
_MERGE_POLL = timedelta(minutes=5)
# Polls per run before continuing as new (4 hours at _MERGE_POLL): a review
# that takes days is many short histories, never one long one.
_POLLS_PER_RUN = 48

# Activity-raised failures no retry can fix, as CLASSES (CLAUDE.md §5).
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    MceFolderNotFoundError,
    AmbiguousMceFolderError,
    ClusterAlreadyProvisionedError,
    ClusterValuesRejectedError,
    ClusterFileConflictError,
    GitProviderAuthError,
    GitProviderRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


def merge_request_title(cluster: str, mce: str) -> str:
    return f"Provision hosted cluster {cluster} on {mce}"


def merge_request_description(
    target: ClusterTarget, allocation: AllocateSegmentResult, published: ClusterFileRef, workflow_id: str
) -> str:
    """What a reviewer reads before merging: what was written, and what follows the merge."""
    return (
        f"provision-cluster run `{workflow_id}` adds `{published.relative_path}`.\n\n"
        f"- site: `{target.site}` · MCE folder: `{target.mce_dir}` · clusterName: `{target.cluster_name}`\n"
        f"- mastertag: `{target.mastertag}` · MachineConfigs: {', '.join(f'`{n}`' for n in target.config_names) or 'none'}\n"
        f"- segment: `{allocation.segment}` · vlan `{allocation.vlan_id}` · EPG `{allocation.epg_name}` "
        f"(verified in the Segments Manager)\n"
        f"- day2 sigs: {', '.join(f'`{s}`' for s in target.day2_sigs) or 'none'}\n\n"
        "The branch's CI pipeline renders the MachineConfigs into this file in its own commit "
        "(`CI: Update mcConfigMap from mc-files [skip ci]`): merge only once the pipeline is green "
        "and that commit is on the branch.\n\n"
        "After the merge: sync the cluster's Application by hand; the run then registers the "
        "cluster in the day2 sigs above and completes."
    )


@workflow.defn
class ProvisionClusterWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._input: ProvisionClusterInput | None = None
        self._state = ProvisionClusterState()
        self._child_id: str | None = None
        self._merge_request_state: str | None = None

    @workflow.query
    def progress(self) -> ProvisionClusterProgress:
        state = self._state
        return ProvisionClusterProgress(
            phase=self._phase,
            allocate_segment_workflow_id=self._child_id,
            branch=state.published.branch if state.published else None,
            merge_request_url=state.merge_request.url if state.merge_request else None,
            merge_request_state=self._merge_request_state,
            waited_since=state.waited_since,
            polls=state.polls,
        )

    @workflow.run
    async def run(self, run_args: ProvisionClusterRunArgs) -> ProvisionClusterResult:
        request = run_args.input
        self._input = request
        # A continued run restores what the previous one did and skips it:
        # each phase's outcome is in the state, so nothing is done twice.
        self._state = run_args.state.model_copy(deep=True) if run_args.state else ProvisionClusterState()
        state = self._state
        cluster = request.cluster
        branch = provision_cluster_branch(cluster)
        self._child_id = allocate_segment_workflow_id(SegmentType.HC, cluster)

        # Step 1 — where the cluster goes, and whether its text would render there.
        if state.target is None:
            self._phase = "inspecting-target"
            state.target = await workflow.execute_activity(
                inspect_cluster_target,
                ClusterTargetRequest(cluster=cluster, mce=request.mce, values=request.values),
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        target = state.target

        # Step 2 — the segment, from allocate-segment as an awaited child.
        if state.allocation is None:
            self._phase = "allocating-segment"
            state.allocation = await self._allocate(cluster, target.site)
        allocation = state.allocation

        # Step 3 — the file on its branch.
        if state.published is None:
            self._phase = "publishing-cluster-file"
            state.published = await workflow.execute_activity(
                publish_cluster_file,
                ClusterFilePublishRequest(
                    cluster=cluster, mce_dir=target.mce_dir, branch=branch, values=request.values,
                    vlan_id=allocation.vlan_id, segment=allocation.segment, type=allocation.type,
                ),
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        published = state.published

        # Step 4 — the merge request, found or created.
        if state.merge_request is None:
            self._phase = "opening-merge-request"
            state.merge_request = await workflow.execute_activity(
                ensure_merge_request,
                MergeRequestRequest(
                    branch=published.branch,
                    title=merge_request_title(cluster, request.mce),
                    description=merge_request_description(target, allocation, published, workflow.info().workflow_id),
                ),
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        merge_request = state.merge_request

        # Step 5 — a person merges. No deadline, ever.
        merged = await self._await_merge(request, merge_request)

        # Step 6 — the day2 sig repos, after the merge.
        registrations: list[Day2SigRegistration] = []
        if target.day2_sigs:
            if not target.day2_env:
                raise ApplicationError(
                    f"the target lists day2 sigs {target.day2_sigs} for {cluster} but no environment "
                    "folder for them — inspect_cluster_target derives both together, so the limb that "
                    "answered predates the field or has a bug. The merged cluster file stands; nothing "
                    "was pushed to day2",
                    type=Day2EnvMissingError.__name__,
                )
            self._phase = "registering-day2"
            registrations = list(
                await asyncio.gather(
                    *(
                        workflow.execute_activity(
                            register_cluster_in_day2_sig,
                            Day2SigRegistrationRequest(
                                sig=sig, site=target.site, env=target.day2_env, mce=request.mce, cluster=cluster
                            ),
                            task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                            start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
                            retry_policy=_RETRY_POLICY,
                        )
                        for sig in target.day2_sigs
                    )
                )
            )

        self._phase = "completed"
        workflow.logger.info(
            "%s provisioned: %s on %s (vlan %d, %s), merge request %s merged%s, day2 %s",
            cluster, published.relative_path, published.branch, allocation.vlan_id, allocation.segment,
            merge_request.url, f" ({merged.merged_sha})" if merged.merged_sha else "",
            ", ".join(f"{r.sig}:{'created' if r.created else 'present'}" for r in registrations) or "none",
        )
        return ProvisionClusterResult(
            cluster=cluster,
            cluster_name=target.cluster_name,
            site=target.site,
            mce=request.mce,
            mastertag=target.mastertag,
            config_names=list(target.config_names),
            vlan_id=allocation.vlan_id,
            segment=allocation.segment,
            epg_name=allocation.epg_name,
            network=published.dhcp_values.network,
            branch=published.branch,
            cluster_file=published.relative_path,
            commit_sha=published.commit_sha,
            file_changed=published.changed,
            merge_request_url=merge_request.url,
            merge_request_created=merge_request.created,
            merged_sha=merged.merged_sha,
            day2=registrations,
        )

    async def _allocate(self, cluster: str, site: str) -> AllocateSegmentResult:
        """allocate-segment as an awaited child, on its own id and queue.

        ABANDON on parent close: the child verifies an allocation by read-back
        and must never be cut off between the allocate and the verify; a
        parent that fails leaves the child to finish on its own id, where a
        re-run of this cluster finds the same allocation (idempotent per
        cluster, site, type).
        """
        child_id = self._child_id or allocate_segment_workflow_id(SegmentType.HC, cluster)
        try:
            return await workflow.execute_child_workflow(
                AllocateSegmentWorkflow.run,
                AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=cluster, site=site, type=SegmentType.HC)),
                id=child_id,
                task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            raise ApplicationError(
                f"allocate-segment is already running for {cluster} as {child_id} (a manual run, or an "
                "earlier provision-cluster's child). Let it finish, then re-run this cluster: the "
                "allocation is idempotent and the re-run picks it up",
                type=AllocateSegmentAlreadyRunningError.__name__,
            ) from None
        except ChildWorkflowError as err:
            # The child's own failure: an ApplicationError it raised, or the
            # ApplicationError behind the ActivityError of its failed activity.
            cause = err.cause
            while isinstance(cause, ActivityError) and cause.cause is not None:
                cause = cause.cause
            detail = (
                f"{cause.type}: {cause.message}"
                if isinstance(cause, ApplicationError)
                else f"{type(cause).__name__ if cause else 'ChildWorkflowError'}: {cause or err}"
            )
            raise ApplicationError(
                f"allocate-segment {child_id} failed allocating a segment for {cluster} at site {site} — "
                f"{detail}. Its history has the detail; nothing was pushed to day1, and re-running this "
                "cluster runs the allocation again",
                type=AllocateSegmentFailedError.__name__,
            ) from err

    async def _await_merge(self, request: ProvisionClusterInput, merge_request: MergeRequestRef) -> MergeRequestState:
        """Poll the merge request until a person merged it; continue as new to keep history short."""
        state = self._state
        if state.waited_since is None:
            state.waited_since = workflow.now()
        polls_this_run = 0
        while True:
            self._phase = "awaiting-merge"
            observed: MergeRequestState = await workflow.execute_activity(
                get_merge_request_state,
                merge_request,
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            self._merge_request_state = observed.state
            state.polls += 1
            polls_this_run += 1
            if observed.state == "merged":
                return observed
            if observed.state == "closed":
                raise ApplicationError(
                    f"merge request {merge_request.url} for {request.cluster} was closed without merging: "
                    f"a person decided against the cluster. The branch and the segment allocation stand; "
                    "re-running the same cluster re-opens a merge request for the same branch",
                    type=MergeRequestClosedError.__name__,
                )
            await workflow.sleep(_MERGE_POLL)  # durable, replay-safe
            if polls_this_run >= _POLLS_PER_RUN or workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(ProvisionClusterRunArgs(input=request, state=state))
