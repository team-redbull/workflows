from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_ALLOCATION_ACTIVITY_QUEUE
    from shared.exceptions import DeploymentApiError
    from shared.interfaces.segment_allocation import *
    from shared.models.segment_allocation import *

# Network-bound activities: retry transient failures, but keep each attempt bounded.
_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)

# Poll cadence for the deployment to reach "CREATED". The wait between polls is a
# workflow timer (durable, replay-safe) — never sleep inside an activity.
_POLL_INTERVAL = timedelta(seconds=10)
_POLL_TIMEOUT = timedelta(minutes=30)

_CREATED_STATUS = "CREATED"


@workflow.defn
class SegmentAllocationWorkflow:
    @workflow.run
    async def run(self, allocation_input: SegmentAllocationInput) -> SegmentAllocationResult:
        workflow.logger.info(
            "Allocating segment for cluster=%s site=%s",
            allocation_input.cluster_name,
            allocation_input.site,
        )

        # Step 1: create the deployment and poll until the segment is allocated.
        uuid: str = await workflow.execute_activity(
            create_deployment,
            allocation_input,
            task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        deadline = workflow.now() + _POLL_TIMEOUT
        while True:
            deployment: DeploymentStatus = await workflow.execute_activity(
                get_deployment,
                uuid,
                task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if deployment.status == _CREATED_STATUS:
                break
            if workflow.now() >= deadline:
                raise DeploymentApiError(
                    f"Deployment {uuid} not CREATED within {_POLL_TIMEOUT} "
                    f"(last status={deployment.status})"
                )
            await workflow.sleep(_POLL_INTERVAL)

        # Strict: a CREATED deployment must carry the allocated segment.
        if not deployment.segment:
            raise DeploymentApiError(
                f"Deployment {uuid} is CREATED but has no additionalInfo.segment"
            )
        segment = deployment.segment

        await workflow.execute_activity(
            commit_segment_to_git,
            args=[allocation_input, segment],
            task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        result = SegmentAllocationResult(
            cluster_name=allocation_input.cluster_name,
            site=allocation_input.site,
            uuid=uuid,
            segment=segment,
            allocated_at=workflow.now(),
        )
        workflow.logger.info(
            "Allocated segment=%s (uuid=%s) to cluster=%s",
            result.segment,
            result.uuid,
            result.cluster_name,
        )
        return result
