"""initialize-segment — creates a segment in the Segments Manager. One step,
and the segment is Available the moment the run completes.

The FIRST workflow of the `segment-lifecycle` domain, named for what it does
rather than for the domain, so a sibling can join the domain without a naming
collision. Domain-scoped things — the activity queue, the limb deployment, the
models, the `/workflows/segment-lifecycle` API prefix — stay named after the
DOMAIN and are shared; workflow-scoped things (this class, its task queue, its
workflow ids, its RunArgs/Progress/Result models) carry the workflow name.

This workflow is a segment's single ENTRY POINT: an operator POSTs the segment
definition to the trigger API (workflow_domains/api.py) and the workflow calls
the Segments Manager itself. It used to work the other way round — the Segments
Manager created the segment and then fired a best-effort HTTP trigger at us —
which left creation outside Temporal: invisible in the UI, and silently skipped
whenever that call failed.

The Segments Manager is the VALIDATOR OF RECORD: whether a site is known, a
CIDR fits its pool, a VLAN is free and nothing overlaps are all its rules, and
this workflow deliberately re-derives none of them. It passes the definition
through and classifies the answer.

The definition carries NO type. A segment is not born as any kind: the type
is allocation state, stamped on by allocate-segment when the segment is
reserved and cleared when it is released. So a created segment joins one
shared Available pool that serves every type.

Why a Temporal workflow for a single activity, rather than a bare HTTP call:
  * durable, UNBOUNDED retries — a Segments Manager outage is out-waited, not
    failed, and the operator's request survives an orchestrator restart;
  * ONE place that owns the dedup id (initialize-segment-<network>), so
    a duplicate trigger is a 409 rather than a second creation attempt;
  * one run per segment, each with its own status the caller can poll — which
    is what lets the bulk route start N of them and report per segment.

This workflow used to own a whole firewall-approval flow as well: peer
discovery, open-rules submission against the next service, request-id mirroring
into the Segments Manager UI, an endless human-approval poll with
continue_as_new, and a final unlock flipping the segment Locked -> Available.
Every firewall between segments is open now, so all of it — and the `Locked`
status it existed to clear — is gone. A segment is born Available.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
    from shared.interfaces.segment_lifecycle import create_segment
    from shared.models.segment_lifecycle import (
        InitializeSegmentProgress,
        InitializeSegmentResult,
        InitializeSegmentRunArgs,
    )

# Network-bound activity: keep each attempt bounded (90s, with the HTTP client
# timing out well below that), but retry UNBOUNDED — a transient outage of the
# Segments Manager is out-waited, never fatal. Deterministic failures must be
# classified: either listed here by type, or raised by the activity as a
# non-retryable ApplicationError. An UNCLASSIFIED deterministic failure retries
# every minute forever (workflow stuck RUNNING, visible in the Temporal UI).
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# Exactly the classified-permanent errors create_segment raises: the definition
# was rejected, the CIDR is stored with different attributes, or the token is
# wrong. Everything else it can raise (SegmentsManagerError) is transient by
# construction and retried.
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[
        "SegmentValidationError",
        "SegmentConflictError",
        "SegmentsManagerAuthError",
    ],
)


@workflow.defn
class InitializeSegmentWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> InitializeSegmentProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return InitializeSegmentProgress(phase=self._phase)

    @workflow.run
    async def run(self, run_args: InitializeSegmentRunArgs) -> InitializeSegmentResult:
        segment_input = run_args.input
        workflow.logger.info(
            "Creating segment=%s (site=%s)",
            segment_input.segment,
            segment_input.site,
        )

        # The whole run. Idempotent activity-side: a re-trigger for a segment
        # that already exists with this definition completes rather than
        # failing, so an operator re-submitting the same POST is harmless.
        self._phase = "creating-segment"
        await workflow.execute_activity(
            create_segment,
            segment_input,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        self._phase = "completed"
        workflow.logger.info(
            "Segment %s created and Available", segment_input.segment
        )
        return InitializeSegmentResult(segment=segment_input.segment)
