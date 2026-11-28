"""create-upi-cluster — an OpenShift UPI `platform: none` cluster on vSphere, from its name to Argo CD.

The ENTRY POINT for a new UPI cluster (CLAUDE.md §4). An MCE is one KIND of UPI
cluster (`kind: mce`, the default), and the only kind with a GitOps step: it is
added to day1's list of MCEs. Designed with the operator 2026-11-27: the team's
bootstrap-installer stays the ONE install implementation, run as Kubernetes Jobs
on the hub by the upi-provisioning limb; the operator's runbook ("Creation MCE
4.20") is what it automates.

Shape of the run:

  1. allocating-segment  — allocate-segment as AWAITED CHILDREN on their own ids:
                           the node network (MCE for an MCE, UPI otherwise), then
                           an MCE's INVENTORY network (the BareMetalHost boot
                           network add-server-to-mce resolves by cluster name).
  2. preparing           — Job `prepare`: IPs from the segment (infra .1-.3,
                           masters .4-.6, bootstrap .7, gateway .254 by default),
                           install-config, ignition, the *.apps DNS record; the
                           admin kubeconfig into a hub Secret.
  3. installing          — Job `install`: terraform apply (the VMs, their DNS
                           records) and bootstrap-installer's CSR loop.
  4. verifying-cluster   — every expected IP a Ready Node, ClusterVersion
                           Available, this host's nodes' CSRs approved meanwhile.
  5. registering-argocd  — the hub Argo CD cluster Secret `cluster-<cluster>`,
                           destination name = the cluster's name, read back.
  6. (mce) publishing-mce-folder → opening-merge-request → awaiting-merge:
                           `sites/<site>/mces/<cluster>/` (values.yaml,
                           version.yaml = the installed version) on
                           `create-upi-cluster/<cluster>`, its merge request, and
                           a HUMAN wait for the merge, no deadline (provision-
                           cluster's shape: durable timer + continue-as-new). Once
                           merged, the hub's `<site>-mces` ApplicationSet manages
                           the MCE; its day2 then deploys `in-cluster`.
  7. completed.

Re-runs: each phase's outcome is in the run's state (carried across
continue-as-new), so a continued run skips what is done. A NEW run of a cluster
after a failure starts from the allocation (idempotent), makes NEW Jobs (the
run's own token is in their names), and its `prepare` refuses a work volume whose
terraform state still lists VMs — a failed install is destroyed first. Past the
merge a re-run finds the folder at main (`on_main`) and opens nothing.

What the run never does: destroy anything, write a hosted cluster, push day1
`main`, merge, or deploy the cluster's own day2 (Argo CD does, after the merge).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError, WorkflowAlreadyStartedError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import create_upi_cluster_branch
    from shared.consts import (
        ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
        CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
        UPI_PROVISIONING_ACTIVITY_QUEUE,
    )
    from shared.exceptions import (
        AllocateSegmentAlreadyRunningError,
        AllocateSegmentFailedError,
        ArgoClusterSecretConflictError,
        Day1SiteNotFoundError,
        GitProviderAuthError,
        GitProviderRequestRejectedError,
        HubAccessError,
        InstallerConfigRejectedError,
        InstallerJobFailedError,
        MceFolderConflictError,
        MceFolderNotFoundError,
        MergeRequestClosedError,
        UpiClusterAccessError,
        UpiClusterNotReadyError,
    )
    from shared.interfaces.cluster_lifecycle import ensure_merge_request, get_merge_request_state, publish_mce_folder
    from shared.interfaces.upi_provisioning import (
        await_upi_cluster_ready,
        register_upi_cluster_in_argocd,
        run_installer_step,
    )
    from shared.models.cluster_lifecycle import MergeRequestRef, MergeRequestRequest, MergeRequestState
    from shared.models.segment_lifecycle import (
        AllocateSegmentInput,
        AllocateSegmentResult,
        AllocateSegmentRunArgs,
        SegmentType,
    )
    from shared.models.upi_lifecycle import (
        ArgoRegistrationRequest,
        ClusterReadyRequest,
        CreateUpiClusterInput,
        CreateUpiClusterProgress,
        CreateUpiClusterResult,
        CreateUpiClusterRunArgs,
        CreateUpiClusterState,
        InstallerStep,
        InstallerStepRequest,
        InstallerStepResult,
        MceFolderPublishRequest,
        SegmentAllocations,
        UpiClusterKind,
        node_segment_type,
    )
    from shared.workflow_ids import allocate_segment_workflow_id
    from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow

# A Job's whole wait is ONE heartbeating activity: the Job's own
# activeDeadlineSeconds (4 h by default) ends a hung installer first.
_JOB_TIMEOUT = timedelta(hours=5)
_HEARTBEAT = timedelta(minutes=2)
# The cluster's own convergence after the install Job: nodes Ready, operators up.
_READY_TIMEOUT_SECONDS = 2 * 3600
_READY_ACTIVITY_TIMEOUT = timedelta(seconds=_READY_TIMEOUT_SECONDS + 600)
_SHORT_TIMEOUT = timedelta(minutes=3)
_GIT_ACTIVITY_TIMEOUT = timedelta(seconds=180)
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# The merge wait (provision-cluster's numbers — constants: changing one is a
# non-deterministic change for runs in flight).
_MERGE_POLL = timedelta(minutes=5)
_POLLS_PER_RUN = 48

_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    InstallerConfigRejectedError,
    InstallerJobFailedError,
    HubAccessError,
    UpiClusterAccessError,
    UpiClusterNotReadyError,
    ArgoClusterSecretConflictError,
    Day1SiteNotFoundError,
    MceFolderConflictError,
    MceFolderNotFoundError,
    GitProviderAuthError,
    GitProviderRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


def merge_request_description(request: CreateUpiClusterInput, state: CreateUpiClusterState, workflow_id: str) -> str:
    segments = state.segments
    status = state.status
    folder = state.mce_folder
    lines = [
        f"create-upi-cluster run `{workflow_id}` adds the MCE `{request.cluster}` at site `{request.site}`: "
        f"`{folder.folder if folder else ''}`.",
        "",
        f"- API: `{status.api_url if status else ''}` · OpenShift `{status.version if status else ''}` · "
        f"{len(status.nodes) if status else 0} nodes Ready",
    ]
    if segments:
        lines.append(f"- node segment `{segments.node.segment}` (vlan {segments.node.vlan_id}, EPG `{segments.node.epg_name}`)")
        if segments.inventory:
            lines.append(f"- inventory segment `{segments.inventory.segment}` (vlan {segments.inventory.vlan_id})")
    lines += [
        f"- registered in the hub's Argo CD as `{request.cluster}` (Secret `{state.argo.secret_name if state.argo else ''}`)",
        "",
        "After the merge the hub's `<site>-mces` ApplicationSet makes this MCE's Application, "
        "with the cluster as its destination.",
    ]
    return "\n".join(lines)


@workflow.defn
class CreateUpiClusterWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._state = CreateUpiClusterState()
        self._job_name: str | None = None
        self._merge_request_state: str | None = None

    @workflow.query
    def progress(self) -> CreateUpiClusterProgress:
        state = self._state
        node = state.segments.node if state.segments else None
        return CreateUpiClusterProgress(
            phase=self._phase,
            segment=node.segment if node else None,
            vlan_id=node.vlan_id if node else None,
            job_name=self._job_name,
            api_url=state.status.api_url if state.status else None,
            nodes_ready=sum(1 for n in state.status.nodes if n.ready) if state.status else None,
            merge_request_url=state.merge_request.url if state.merge_request else None,
            merge_request_state=self._merge_request_state,
            waited_since=state.waited_since,
        )

    @workflow.run
    async def run(self, run_args: CreateUpiClusterRunArgs) -> CreateUpiClusterResult:
        request = run_args.input
        self._state = run_args.state.model_copy(deep=True) if run_args.state else CreateUpiClusterState()
        state = self._state
        cluster = request.cluster
        if state.run_token is None:
            state.run_token = workflow.uuid4().hex[:8]

        # Step 1 — the networks, from allocate-segment as awaited children.
        if state.segments is None:
            self._phase = "allocating-segment"
            node = await self._allocate(cluster, request.site, node_segment_type(request.kind))
            inventory = None
            if request.kind == UpiClusterKind.MCE:
                inventory = await self._allocate(cluster, request.site, SegmentType.INVENTORY)
            state.segments = SegmentAllocations(node=node, inventory=inventory)
        node_segment = state.segments.node

        # Steps 2 and 3 — bootstrap-installer, as two Jobs on the hub.
        if state.prepared is None:
            self._phase = "preparing"
            state.prepared = await self._installer_step(request, InstallerStep.PREPARE, node_segment)
        if state.installed is None:
            self._phase = "installing"
            state.installed = await self._installer_step(request, InstallerStep.INSTALL, node_segment)
        self._job_name = None

        # Step 4 — the cluster serves.
        if state.status is None:
            self._phase = "verifying-cluster"
            state.status = await workflow.execute_activity(
                await_upi_cluster_ready,
                ClusterReadyRequest(cluster=cluster, timeout_seconds=_READY_TIMEOUT_SECONDS),
                task_queue=UPI_PROVISIONING_ACTIVITY_QUEUE,
                start_to_close_timeout=_READY_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT,
                retry_policy=_RETRY_POLICY,
            )

        # Step 5 — the hub's Argo CD knows it, by its name.
        if state.argo is None:
            self._phase = "registering-argocd"
            state.argo = await workflow.execute_activity(
                register_upi_cluster_in_argocd,
                ArgoRegistrationRequest(cluster=cluster, kind=request.kind),
                task_queue=UPI_PROVISIONING_ACTIVITY_QUEUE,
                start_to_close_timeout=_SHORT_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )

        # Step 6 — an MCE joins day1's list of MCEs, by a merge request a person merges.
        if request.kind == UpiClusterKind.MCE:
            await self._register_mce(request)

        self._phase = "completed"
        inventory = state.segments.inventory
        workflow.logger.info(
            "%s created: %s, %d nodes, Argo CD %s%s", cluster, state.status.api_url, len(state.status.nodes),
            state.argo.secret_name, f", day1 {state.mce_folder.folder}" if state.mce_folder else "",
        )
        return CreateUpiClusterResult(
            cluster=cluster,
            site=request.site,
            kind=request.kind,
            segment=node_segment.segment,
            vlan_id=node_segment.vlan_id,
            epg_name=node_segment.epg_name,
            inventory_segment=inventory.segment if inventory else None,
            inventory_vlan_id=inventory.vlan_id if inventory else None,
            api_url=state.status.api_url,
            version=state.status.version,
            nodes=list(state.status.nodes),
            argo_secret=state.argo.secret_name,
            mce_folder=state.mce_folder.folder if state.mce_folder else None,
            merge_request_url=state.merge_request.url if state.merge_request else None,
            merged_sha=state.merged_sha,
        )

    async def _installer_step(
        self, request: CreateUpiClusterInput, step: InstallerStep, segment: AllocateSegmentResult
    ) -> InstallerStepResult:
        token = self._state.run_token or ""
        self._job_name = f"upi-{request.cluster}-{step.value}-{token}"
        return await workflow.execute_activity(
            run_installer_step,
            InstallerStepRequest(
                cluster=request.cluster, site=request.site, step=step, run_token=token,
                segment=segment.segment, vlan_id=segment.vlan_id, epg_name=segment.epg_name,
                vm_network=request.vm_network, install_config=request.install_config,
            ),
            task_queue=UPI_PROVISIONING_ACTIVITY_QUEUE,
            start_to_close_timeout=_JOB_TIMEOUT,
            heartbeat_timeout=_HEARTBEAT,
            retry_policy=_RETRY_POLICY,
        )

    async def _register_mce(self, request: CreateUpiClusterInput) -> None:
        state = self._state
        status = state.status
        if state.mce_folder is None:
            self._phase = "publishing-mce-folder"
            state.mce_folder = await workflow.execute_activity(
                publish_mce_folder,
                MceFolderPublishRequest(
                    mce=request.cluster, site=request.site, branch=create_upi_cluster_branch(request.cluster),
                    values=request.mce_values, mastertag=(status.version if status and status.version else "unknown"),
                ),
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_GIT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        if state.mce_folder.on_main:
            return
        if state.merge_request is None:
            self._phase = "opening-merge-request"
            state.merge_request = await workflow.execute_activity(
                ensure_merge_request,
                MergeRequestRequest(
                    branch=state.mce_folder.branch,
                    title=f"Add MCE {request.cluster} at {request.site}",
                    description=merge_request_description(request, state, workflow.info().workflow_id),
                ),
                task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        if state.merged_sha is None:
            merged = await self._await_merge(request, state.merge_request)
            state.merged_sha = merged.merged_sha or ""

    async def _allocate(self, cluster: str, site: str, segment_type: SegmentType) -> AllocateSegmentResult:
        """allocate-segment as an awaited child (provision-cluster's `_allocate`, per type)."""
        child_id = allocate_segment_workflow_id(segment_type, cluster)
        try:
            return await workflow.execute_child_workflow(
                AllocateSegmentWorkflow.run,
                AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=cluster, site=site, type=segment_type)),
                id=child_id,
                task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            raise ApplicationError(
                f"allocate-segment is already running for {cluster} as {child_id}. Let it finish, then "
                "re-run this cluster: the allocation is idempotent and the re-run picks it up",
                type=AllocateSegmentAlreadyRunningError.__name__,
            ) from None
        except ChildWorkflowError as err:
            cause = err.cause
            while isinstance(cause, ActivityError) and cause.cause is not None:
                cause = cause.cause
            detail = (
                f"{cause.type}: {cause.message}"
                if isinstance(cause, ApplicationError)
                else f"{type(cause).__name__ if cause else 'ChildWorkflowError'}: {cause or err}"
            )
            raise ApplicationError(
                f"allocate-segment {child_id} failed allocating a {segment_type.value} segment for {cluster} at "
                f"site {site} — {detail}. Nothing was installed; re-running this cluster runs the allocation again",
                type=AllocateSegmentFailedError.__name__,
            ) from err

    async def _await_merge(self, request: CreateUpiClusterInput, merge_request: MergeRequestRef) -> MergeRequestState:
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
                    f"merge request {merge_request.url} adding MCE {request.cluster} was closed without merging. "
                    "The cluster stands, registered in Argo CD; re-running it re-opens a merge request for the "
                    "same branch",
                    type=MergeRequestClosedError.__name__,
                )
            await workflow.sleep(_MERGE_POLL)
            if polls_this_run >= _POLLS_PER_RUN or workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(CreateUpiClusterRunArgs(input=request, state=state))
