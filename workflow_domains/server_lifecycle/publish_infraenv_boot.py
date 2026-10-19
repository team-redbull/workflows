"""publish-infraenv-boot — put an InfraEnv's CURRENT iPXE script on its site's PXE VM.

Given an MCE and an InfraEnv, (re)writes the InfraEnv's boot entry,
`/var/www/pxe-map/<mce_cluster>/<infra_env>.ipxe`, on the PXE VM of the MCE's
site — the file every IPMI host install-server mapped to that InfraEnv chains
to. install-server publishes it itself, per host; this workflow is the same
publish on demand, tied to no host: to seed the entry before the first install,
to repair one deleted or corrupted on the VM, or to point it at a regenerated
image without installing anything.

Shape of the run:

  1. resolving-site     — the MCE's inventory segment in the Segments Manager
                          (on the SEGMENT-LIFECYCLE queue, where that
                          credential lives) gives the site, and with it the PXE
                          VM — the same rule install-server maps hosts by.
  2. validating-infraenv — the InfraEnv must exist on the MCE; missing fails
                          the run before anything is written.
  3. publishing         — publish_infra_env_boot, polled every 10 s until the
                          InfraEnv shows a READY image (ImageCreated True, an
                          ipxeScript, a createdTime), then written with that
                          createdTime as its version. Bounded at 10 min: no
                          ready image by then fails with
                          InfraEnvImageNotUpdatedError, quoting what was
                          awaited. A VM already holding a NEWER image keeps it
                          (409 there) and the run completes as `superseded` —
                          the entry is current either way, and this run can
                          never roll it back.

Nothing is written on the MCE. The URL is read and written inside the
activity, so it never enters history.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import (
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        server_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        AmbiguousInventorySegmentError,
        BmhPrerequisiteMissingError,
        InfraEnvImageNotUpdatedError,
        InfraEnvNotFoundError,
        InventorySegmentMismatchError,
        InventorySegmentNotFoundError,
        PxeMapAuthError,
        PxeMapRequestRejectedError,
        PxeSiteNotConfiguredError,
        SegmentsManagerAuthError,
    )
    from shared.interfaces.segment_lifecycle import get_inventory_segment
    from shared.interfaces.server_lifecycle import check_infra_env, publish_infra_env_boot
    from shared.models.server_lifecycle import (
        InfraEnvBootRequest,
        InfraEnvBootState,
        InfraEnvRef,
        PublishInfraEnvBootProgress,
        PublishInfraEnvBootResult,
        PublishInfraEnvBootRunArgs,
    )

_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# The same "no worker polls this MCE's queue" signal install-server uses.
_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)

# Activity-raised failures no retry can fix, as CLASSES (CLAUDE.md §5).
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    InventorySegmentNotFoundError,
    AmbiguousInventorySegmentError,
    SegmentsManagerAuthError,
    InfraEnvNotFoundError,
    BmhPrerequisiteMissingError,
    PxeSiteNotConfiguredError,
    PxeMapAuthError,
    PxeMapRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# Machine convergence, so a real deadline — install-server's, for the same
# reason: an InfraEnv mid-regeneration has a ready image within seconds, and
# one without for ten minutes is not regenerating. Changing either constant is
# a non-deterministic change for in-flight runs.
_IMAGE_DEADLINE = timedelta(minutes=10)
_IMAGE_POLL_INTERVAL = timedelta(seconds=10)


@workflow.defn
class PublishInfraEnvBootWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._activity_queue: str | None = None

    @workflow.query
    def progress(self) -> PublishInfraEnvBootProgress:
        return PublishInfraEnvBootProgress(
            phase=self._phase, activity_queue=self._activity_queue
        )

    @workflow.run
    async def run(self, run_args: PublishInfraEnvBootRunArgs) -> PublishInfraEnvBootResult:
        request = run_args.input
        activity_queue = server_lifecycle_activity_queue(request.mce_cluster)
        self._activity_queue = activity_queue

        # Step 1 — the site, from the MCE's own inventory segment.
        self._phase = "resolving-site"
        segment = await workflow.execute_activity(
            get_inventory_segment,
            request.mce_cluster,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # Re-checked for install-server's reason: the match is client-side,
        # and the wrong segment would publish to another site's PXE VM.
        if segment.cluster_name != request.mce_cluster:
            raise ApplicationError(
                f"Segments Manager returned segment {segment.segment} allocated "
                f"to {segment.cluster_name!r}, not to the requested MCE "
                f"{request.mce_cluster!r}",
                type=InventorySegmentMismatchError.__name__,
            )

        # Step 2 — the InfraEnv must exist before anything is written.
        self._phase = "validating-infraenv"
        await self._call(
            check_infra_env,
            InfraEnvRef(name=request.infra_env, namespace=request.namespace),
            activity_queue,
        )

        # Step 3 — publish its current image as soon as it has a ready one.
        self._phase = "publishing"
        boot = await self._await_publish(
            InfraEnvBootRequest(
                namespace=request.namespace,
                infra_env=request.infra_env,
                mce_cluster=request.mce_cluster,
                site=segment.site,
            ),
            activity_queue,
        )
        if not boot.published:
            minutes = int(_IMAGE_DEADLINE.total_seconds() // 60)
            raise ApplicationError(
                f"InfraEnv {request.namespace}/{request.infra_env} on MCE "
                f"{request.mce_cluster} showed no ready discovery image within "
                f"{minutes} min: {boot.detail}. Nothing was written to the "
                f"{segment.site} PXE VM — check the InfraEnv's ImageCreated "
                "condition and the assisted-service log",
                type=InfraEnvImageNotUpdatedError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "Boot entry %s/%s on the %s PXE VM %s (image of %s)",
            request.mce_cluster,
            request.infra_env,
            segment.site,
            "already held a newer image" if boot.superseded else "published",
            boot.image_created_at,
        )
        return PublishInfraEnvBootResult(
            mce_cluster=request.mce_cluster,
            infra_env=request.infra_env,
            namespace=request.namespace,
            site=segment.site,
            image_created_at=boot.image_created_at,
            superseded=boot.superseded,
        )

    async def _call(self, activity_fn, arg, task_queue: str):
        """One call on the target MCE's queue, under the domain's budget."""
        return await workflow.execute_activity(
            activity_fn,
            arg,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _await_publish(
        self, request: InfraEnvBootRequest, task_queue: str
    ) -> InfraEnvBootState:
        """One publish attempt per poll until it writes, bounded by the deadline."""
        started_at = workflow.now()
        while True:
            state: InfraEnvBootState = await self._call(
                publish_infra_env_boot, request, task_queue
            )
            if state.published or workflow.now() - started_at >= _IMAGE_DEADLINE:
                return state
            await workflow.sleep(_IMAGE_POLL_INTERVAL)  # durable, replay-safe
