"""Connectivity workflow — opens firewall rules for a segment via the next API,
then flips the segment Locked -> Available in the Segments Manager.

Phase 1 implements the HC behavior (HC <-> every same-site MCE segment, both
directions); the input accepts any segment type and unsupported ones fail
loudly. Every supported type currently peers with the MCE segments in its
own site.

Completion of next requests depends on a HUMAN approval and can take minutes,
hours or more — the workflow therefore polls indefinitely (durable timers with
backoff) and rolls its history over with continue_as_new; it never fails on a
slow approval.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import CONNECTIVITY_ACTIVITY_QUEUE
    from shared.interfaces.connectivity import (
        check_connectivity_requests,
        list_mce_segments,
        publish_request_ids,
        submit_open_rules,
        unlock_segment,
        validate_segment_exists,
    )
    from shared.models.connectivity import (
        ConnectivityInput,
        ConnectivityProgress,
        ConnectivityRequestRef,
        ConnectivityRequestsUpdate,
        ConnectivityResult,
        ConnectivityResumeState,
        ConnectivityRunArgs,
        OpenRulesRequest,
        SegmentType,
    )

# Network-bound activities: retry transient failures, but keep each attempt bounded.
_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
# A missing segment is deterministic — retrying cannot fix it.
_VALIDATE_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
    non_retryable_error_types=["SegmentNotFoundError"],
)

# Types connectivity is implemented for. Every supported type peers with the
# MCE segments (single peer per type). Phase 2+: add members here plus their
# PORTS_* config in the activity layer.
_SUPPORTED_TYPES = frozenset({SegmentType.HC})

# Endless-poll pacing: back off from 15s to a 5-minute cap while waiting for
# the human approval, and continue_as_new periodically so workflow history
# stays bounded no matter how long approval takes.
_POLL_INITIAL = timedelta(seconds=15)
_POLL_MAX = timedelta(minutes=5)
_POLLS_PER_RUN = 300


@workflow.defn
class ConnectivityWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._total_requests = 0
        self._pending_requests = 0
        self._submitted_at: datetime | None = None

    @workflow.query
    def progress(self) -> ConnectivityProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return ConnectivityProgress(
            phase=self._phase,
            total_requests=self._total_requests,
            pending_requests=self._pending_requests,
        )

    @workflow.run
    async def run(self, run_args: ConnectivityRunArgs) -> ConnectivityResult:
        connectivity_input = run_args.input
        resume = run_args.resume
        if resume is None:
            request_ids, pending, peer_count = await self._open_rules(connectivity_input)
        else:
            # Resumed after continue_as_new: rules are already submitted,
            # jump straight back into polling.
            request_ids = resume.request_ids
            pending = resume.pending_ids
            peer_count = resume.peer_segment_count
            self._submitted_at = resume.submitted_at

        self._total_requests = len(request_ids)
        self._pending_requests = len(pending)

        # Step 4 — poll until every request completes. Approval is human-driven
        # (can take hours+), so there is deliberately NO deadline: back off to
        # _POLL_MAX and roll history over every _POLLS_PER_RUN cycles.
        self._phase = "awaiting-completion"
        interval = _POLL_INITIAL if resume is None else _POLL_MAX
        polls = 0
        while pending:
            if polls >= _POLLS_PER_RUN:
                workflow.logger.info(
                    "Continuing as new after %d poll cycles (%d requests pending)",
                    polls,
                    len(pending),
                )
                workflow.continue_as_new(
                    ConnectivityRunArgs(
                        input=connectivity_input,
                        resume=ConnectivityResumeState(
                            request_ids=request_ids,
                            pending_ids=pending,
                            peer_segment_count=peer_count,
                            submitted_at=self._submitted_at,
                        ),
                    )
                )
            await workflow.sleep(interval)  # durable, replay-safe timer
            still_pending = await workflow.execute_activity(
                check_connectivity_requests,
                pending,
                task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if still_pending != pending:
                # Completed ids drop off the Segments Manager display; the
                # final empty update deletes the display entirely.
                await self._publish_request_ids(
                    connectivity_input.segment, still_pending
                )
            pending = still_pending
            self._pending_requests = len(pending)
            polls += 1
            interval = min(interval * 2, _POLL_MAX)
            workflow.logger.info(
                "Polling: %d/%d requests still pending",
                len(pending),
                len(request_ids),
            )

        # Step 5 — all rules open: unlock the segment (Locked -> Available).
        self._phase = "unlocking-segment"
        await workflow.execute_activity(
            unlock_segment,
            connectivity_input.segment,
            task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        self._phase = "completed"
        workflow.logger.info(
            "Connectivity complete for segment=%s: %d rules opened, segment unlocked",
            connectivity_input.segment,
            len(request_ids),
        )
        return ConnectivityResult(
            segment=connectivity_input.segment,
            type=connectivity_input.type,
            peer_segment_count=peer_count,
            request_ids=request_ids,
        )

    async def _open_rules(
        self, connectivity_input: ConnectivityInput
    ) -> tuple[list[int], list[int], int]:
        """Steps 1-3 of a fresh run: validate, list peers, fan out submissions."""
        workflow.logger.info(
            "Opening connectivity for segment=%s type=%s",
            connectivity_input.segment,
            connectivity_input.type.value,
        )
        if connectivity_input.type not in _SUPPORTED_TYPES:
            raise ApplicationError(
                f"Connectivity for type={connectivity_input.type.value} is not "
                f"supported yet (supported: {sorted(t.value for t in _SUPPORTED_TYPES)})",
                type="UnsupportedSegmentType",
                non_retryable=True,
            )

        # Step 1 — fail fast before opening any firewall rules.
        self._phase = "validating-segment"
        await workflow.execute_activity(
            validate_segment_exists,
            connectivity_input,
            task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_VALIDATE_RETRY_POLICY,
        )

        # Step 2 — same-site MCE peer CIDRs; an empty pool is a misconfiguration.
        self._phase = "listing-mce-segments"
        mce_segments: list[str] = await workflow.execute_activity(
            list_mce_segments,
            connectivity_input,
            task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if not mce_segments:
            raise ApplicationError(
                "No same-site MCE segments found in the Segments Manager — nothing "
                "to open connectivity against",
                type="NoMceSegments",
                non_retryable=True,
            )

        # Step 3 — two requests per MCE segment (both directions), all in
        # parallel. asyncio.gather over execute_activity is deterministic and
        # sandbox-safe; result order matches submission order.
        self._phase = "submitting-rules"
        submissions = []
        for mce_segment in mce_segments:
            for rule in (
                OpenRulesRequest(
                    source_segment=connectivity_input.segment,
                    destination_segment=mce_segment,
                    source_type=connectivity_input.type,
                    destination_type=SegmentType.MCE,
                ),
                OpenRulesRequest(
                    source_segment=mce_segment,
                    destination_segment=connectivity_input.segment,
                    source_type=SegmentType.MCE,
                    destination_type=connectivity_input.type,
                ),
            ):
                submissions.append(
                    workflow.execute_activity(
                        submit_open_rules,
                        rule,
                        task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=_RETRY_POLICY,
                    )
                )
        refs: list[ConnectivityRequestRef] = list(await asyncio.gather(*submissions))
        request_ids = [ref.id for ref in refs]
        self._submitted_at = workflow.now()
        workflow.logger.info(
            "Submitted %d open-rules requests for %d MCE segment(s)",
            len(request_ids),
            len(mce_segments),
        )

        # Surface the freshly submitted ids beside the segment's status in the
        # Segments Manager UI; they stay visible until the requests complete.
        await self._publish_request_ids(connectivity_input.segment, request_ids)

        return request_ids, list(request_ids), len(mce_segments)

    async def _publish_request_ids(
        self, segment: str, request_ids: list[int]
    ) -> None:
        """Mirror the still-pending request ids into the Segments Manager UI."""
        await workflow.execute_activity(
            publish_request_ids,
            ConnectivityRequestsUpdate(
                segment=segment, request_ids=request_ids, submitted_at=self._submitted_at
            ),
            task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
