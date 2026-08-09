"""convert-segment — rebalances segment inventory between types: re-types up
to `quantity` AVAILABLE segments of a source type at a site, then re-runs the
open-segment-rules flow for each (the new type has different peers, so its
firewall connectivity must be established from scratch).

The THIRD workflow of the `segment-lifecycle` domain: it reuses the running
`segment-lifecycle-worker` limb, its activity queue and its ConfigMap — only
this module, its own workflow queue and its route are new.

AVAILABLE ONLY, and that is the design, not a filter detail. A Locked segment
has no established connectivity and may still have a LIVE
`open-segment-rules-<SRC>-<network>` run: re-typing it under a running sibling
would leave that run fighting the replacement over the request-ids display
(replace semantics, keyed by CIDR) and wrongly unlocking the segment when the
old approval lands. Handling that means cancelling the sibling and waiting out
its cleanup — and a cancel result cannot even tell you whether a run was live,
because Temporal accepts a cancel against an already-CLOSED execution and
reports it accepted, failing only for an id that never existed. Restricting
the selection to Available removes the whole problem at the source: every
match is a segment whose own run has already COMPLETED (that completion is
what unlocked it), so there is nothing to cancel, nothing to wait for, and no
stale-note race. The cost is deliberate — a Locked segment cannot be re-typed
by this workflow at all, including one whose run failed terminally.

Shape of the run (all MACHINE ops — bounded, fails loudly, no continue_as_new):

  1. validating-site      — GET /api/sites; an unknown site fails as
                            UnknownSite instead of silently matching nothing.
  2. searching-segments   — list every Available segment of the source type at
                            the site. Selection is the WORKFLOW's deterministic
                            policy over that recorded result: lowest vlan_id
                            first, cut at `quantity`. Fewer matches than
                            requested is NOT an error — the result reports the
                            shortfall.
  3. converting-segments  — sequentially per selected segment:
       a. convert the type in the Segments Manager (PUT /api/segments/type,
          expected_type=source as a compare-and-set against concurrent
          conversions). The manager re-locks the segment and clears the old
          type's connectivity fields atomically — a converted segment is
          born-Locked again, and never allocatable before its new rules open.
       b. start `open-segment-rules-<DEST>-<network>` as a DETACHED child
          (ParentClosePolicy.ABANDON). Exactly the bulk-route philosophy: one
          run per segment, each with its own dedup id, its own human approval
          and its own failure — which is also why this parent COMPLETES with
          a fan-out report instead of waiting days for N approvals. The
          child's create_segment accepts the existing (now destination-typed)
          segment idempotently and proceeds to open rules and unlock.

Cancellation mid-loop needs no compensation: conversions already performed
stand (their ABANDONed children run on, each re-run's search skips them —
they no longer match the source type), and nothing pending is half-applied.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError

from workflow_domains.segment_lifecycle.open_segment_rules import (
    OpenSegmentRulesWorkflow,
)

with workflow.unsafe.imports_passed_through():
    from shared.consts import (
        OPEN_SEGMENT_RULES_WORKFLOW_QUEUE,
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    )
    from shared.interfaces.segment_lifecycle import (
        convert_segment_type,
        get_valid_sites,
        list_convertible_segments,
    )
    from shared.models.segment_lifecycle import (
        ConvertedSegmentReport,
        ConvertibleSegment,
        ConvertibleSegmentsQuery,
        ConvertSegmentInput,
        ConvertSegmentProgress,
        ConvertSegmentResult,
        ConvertSegmentRunArgs,
        OpenSegmentRulesInput,
        OpenSegmentRulesRunArgs,
        SegmentTypeUpdate,
    )
    from shared.workflow_ids import open_segment_rules_workflow_id

# Same budget rules as the sibling workflows: bounded attempts (the HTTP
# client times out below), UNBOUNDED retries so transient outages are
# out-waited, and every known-permanent error classified.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[
        "SegmentsManagerAuthError",
        "SegmentNotFoundError",
        "SegmentConversionConflictError",
    ],
)


def _selection_order(segment: ConvertibleSegment) -> int:
    """Deterministic conversion preference: lowest vlan_id first.

    Every match is Available (the search returns nothing else), so vlan_id is
    the only discriminator left — and it is a total order over the CIDR-unique
    segments of one site, which keeps the selection replay-stable.
    """
    return segment.vlan_id


@workflow.defn
class ConvertSegmentWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._matched = 0
        self._selected = 0
        self._converted: list[ConvertedSegmentReport] = []

    @workflow.query
    def progress(self) -> ConvertSegmentProgress:
        """Cheap progress surface for the async caller (GET status endpoint).
        Carries the per-segment reports so a mid-loop failure still shows
        which segments WERE converted (their child runs proceed regardless)."""
        return ConvertSegmentProgress(
            phase=self._phase,
            matched=self._matched,
            selected=self._selected,
            converted=self._converted,
        )

    @workflow.run
    async def run(self, run_args: ConvertSegmentRunArgs) -> ConvertSegmentResult:
        convert_input = run_args.input
        workflow.logger.info(
            "Converting up to %d %s segment(s) to %s at site=%s",
            convert_input.quantity,
            convert_input.source_type.value,
            convert_input.destination_type.value,
            convert_input.site,
        )

        # Step 1 — strict site check (§7): a typo'd site must fail loudly as
        # what it is, not complete as "matched 0 segments, shortfall N".
        self._phase = "validating-site"
        valid_sites = await workflow.execute_activity(
            get_valid_sites,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if convert_input.site not in valid_sites:
            raise ApplicationError(
                f"Site {convert_input.site!r} is not known to the Segments "
                f"Manager (valid: {sorted(valid_sites)})",
                type="UnknownSite",
            )

        # Step 2 — find and select. The activity returns every AVAILABLE hit;
        # the ordering policy and the quantity cut are workflow code — a
        # deterministic pure function of the recorded result.
        self._phase = "searching-segments"
        hits = await workflow.execute_activity(
            list_convertible_segments,
            ConvertibleSegmentsQuery(
                site=convert_input.site, type=convert_input.source_type
            ),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        selected = sorted(hits, key=_selection_order)[: convert_input.quantity]
        self._matched = len(hits)
        self._selected = len(selected)
        if len(hits) < convert_input.quantity:
            workflow.logger.info(
                "Only %d of the requested %d %s segment(s) exist at %s — "
                "converting all of them (shortfall reported in the result)",
                len(hits),
                convert_input.quantity,
                convert_input.source_type.value,
                convert_input.site,
            )

        # Step 3 — convert sequentially: deterministic order, no thundering
        # herd of starts, and a conflict stops the run at a clean boundary
        # (everything before it fully converted, nothing after touched).
        self._phase = "converting-segments"
        for segment in selected:
            self._converted.append(await self._convert_one(convert_input, segment))

        self._phase = "completed"
        shortfall = max(0, convert_input.quantity - self._matched)
        workflow.logger.info(
            "Converted %d segment(s) %s -> %s at %s (shortfall %d)",
            len(self._converted),
            convert_input.source_type.value,
            convert_input.destination_type.value,
            convert_input.site,
            shortfall,
        )
        return ConvertSegmentResult(
            site=convert_input.site,
            source_type=convert_input.source_type,
            destination_type=convert_input.destination_type,
            requested_quantity=convert_input.quantity,
            matched=self._matched,
            shortfall=shortfall,
            converted=self._converted,
        )

    async def _convert_one(
        self, convert_input: ConvertSegmentInput, segment: ConvertibleSegment
    ) -> ConvertedSegmentReport:
        """Convert the type, then start the destination-type replacement run.

        No stale-run cancellation step: the search returns Available segments
        only, and an Available segment's own open-segment-rules run has
        COMPLETED — that completion is what unlocked it. See the module
        docstring for why that restriction is the design rather than a filter.
        """
        # 3a — the atomic re-type + re-lock + connectivity-field clear.
        await workflow.execute_activity(
            convert_segment_type,
            SegmentTypeUpdate(
                segment=segment.segment,
                type=convert_input.destination_type,
                expected_type=convert_input.source_type,
            ),
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # 3b — the replacement run, detached: ABANDON keeps it alive however
        # this parent ends, because it answers to its own id/approval/failure
        # exactly as if the bulk route had started it. An id collision means a
        # destination-type run for this segment already exists (e.g. a re-run
        # after a mid-loop failure) — that run IS the desired outcome, so it
        # is reported rather than treated as an error.
        child_id = open_segment_rules_workflow_id(
            convert_input.destination_type, segment.segment
        )
        open_rules_status = "started"
        try:
            await workflow.start_child_workflow(
                OpenSegmentRulesWorkflow.run,
                OpenSegmentRulesRunArgs(
                    input=OpenSegmentRulesInput(
                        segment=segment.segment,
                        type=convert_input.destination_type,
                        site=convert_input.site,
                        vlan_id=segment.vlan_id,
                        epg_name=segment.epg_name,
                        dhcp=segment.dhcp,
                    )
                ),
                id=child_id,
                task_queue=OPEN_SEGMENT_RULES_WORKFLOW_QUEUE,
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            open_rules_status = "already_running"
            workflow.logger.info(
                "open-segment-rules run %s already exists for %s",
                child_id,
                segment.segment,
            )

        return ConvertedSegmentReport(
            segment=segment.segment,
            vlan_id=segment.vlan_id,
            open_segment_rules_workflow_id=child_id,
            open_rules_status=open_rules_status,
        )
