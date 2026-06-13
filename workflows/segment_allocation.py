from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_ALLOCATION_ACTIVITY_QUEUE
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


@workflow.defn
class SegmentAllocationWorkflow:
    @workflow.run
    async def run(self, allocation_input: SegmentAllocationInput) -> SegmentAllocationResult:
        workflow.logger.info(
            "Allocating segment for cluster=%s site=%s",
            allocation_input.cluster_name,
            allocation_input.site,
        )

        available_segment: SegmentSpec | None = await workflow.execute_activity(
            get_available_segment,
            allocation_input.site,
            task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        if available_segment is None:
            workflow.logger.info(
                "No available segment at site=%s; generating a new one",
                allocation_input.site,
            )
            spec: SegmentSpec = await workflow.execute_activity(
                request_segment,
                allocation_input.site,
                task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            await workflow.execute_activity(
                register_segment,
                spec,
                task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )

        result: SegmentAllocationResult = await workflow.execute_activity(
            allocate_segment,
            allocation_input,
            task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        workflow.logger.info(
            "Allocated vlan=%s segment=%s to cluster=%s",
            result.vlan_id,
            result.segment,
            result.cluster_name,
        )
        return result
