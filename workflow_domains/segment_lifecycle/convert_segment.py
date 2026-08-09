"""convert-segment — rebalances segment inventory between types: re-types up
to `quantity` Available/Locked segments of a source type at a site, then
re-runs the open-segment-rules flow for each (the new type has different
peers, so its firewall connectivity must be established from scratch).

The THIRD workflow of the `segment-lifecycle` domain: it reuses the running
`segment-lifecycle-worker` limb, its activity queue and its ConfigMap — only
this module, its own workflow queue and its route are new.

Shape of the run (all MACHINE ops — bounded, fails loudly, no continue_as_new):

  1. validating-site      — GET /api/sites; an unknown site fails as
                            UnknownSite instead of silently matching nothing.
  2. searching-segments   — list every Available/Locked segment of the source
                            type at the site. Selection is the WORKFLOW's
                            deterministic policy over that recorded result:
                            Locked first (they are not usable inventory yet, so
                            converting them preserves ready Available capacity),
                            lowest vlan_id within each group, first `quantity`.
                            Fewer matches than requested is NOT an error — the
                            result reports the shortfall.
  3. converting-segments  — sequentially per selected segment:
       a. best-effort CANCEL the stale `open-segment-rules-<SRC>-<network>`
          run. A Locked match usually has one, still polling the OLD type's
          firewall request ids — left alive it would fight the replacement
          run over the request-ids display (replace semantics, keyed by CIDR)
          and wrongly unlock the segment when the old approval lands.
          Not-found is the benign, common case (every Available match).
       b. convert the type in the Segments Manager (PUT /api/segments/type,
          expected_type=source as a compare-and-set against concurrent
          conversions). The manager re-locks the segment and clears the old
          type's connectivity fields atomically — a converted segment is
          born-Locked again, and never allocatable before its new rules open.
       c. start `open-segment-rules-<DEST>-<network>` as a DETACHED child
          (ParentClosePolicy.ABANDON). Exactly the bulk-route philosophy: one
          run per segment, each with its own dedup id, its own human approval
          and its own failure — which is also why this parent COMPLETES with
          a fan-out report instead of waiting days for N approvals. The
          child's create_segment accepts the existing (now destination-typed)
          segment idempotently and proceeds to open rules and unlock.

Benign, self-healing race in 3a: the cancelled run's shielded cleanup
publishes a failure note, which may land AFTER 3b cleared the fields (the
grace timer makes that rare). The stale note then lives only until the child
publishes its first non-empty request-id set — the Segments Manager clears
the failure note on every non-empty publish.

Cancellation mid-loop needs no compensation: conversions already performed
stand (their ABANDONed children run on, each re-run's search skips them —
they no longer match the source type), and nothing pending is half-applied.
"""

from __future__ import annotations

import asyncio
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

# The cancel handshake is BOUNDED (asyncio.wait_for maps to a durable workflow
# timer): the cancel request command is submitted immediately either way, and
# an acceptance that has not come back within this window is treated exactly
# like not-found — the conversion proceeds, and if a stale run does exist the
# already-submitted request still cancels it whenever the server processes it
# (only the report's cancelled_previous_run flag and the grace pause are
# skipped). Waiting unboundedly here would hang the whole conversion on one
# slow handshake.
_CANCEL_RESOLUTION_TIMEOUT = timedelta(seconds=30)

# Durable pause between cancelling a stale source-type run and converting the
# segment, so the cancelled run's best-effort cleanup (clear ids + failure
# note) usually lands BEFORE the conversion wipes those fields — shrinking the
# benign stale-note race described in the module docstring. Only taken when a
# cancel was actually delivered. Changing it is a non-deterministic change for
# in-flight runs.
_CANCEL_CLEANUP_GRACE = timedelta(seconds=30)


def _selection_order(segment: ConvertibleSegment) -> tuple[int, int]:
    """Deterministic conversion preference: Locked before Available (spend
    not-yet-usable inventory before ready capacity), lowest vlan_id within
    each group."""
    return (0 if segment.status == "Locked" else 1, segment.vlan_id)


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

        # Step 2 — find and select. The activity returns every convertible
        # (Available/Locked) hit; the ordering policy and the quantity cut are
        # workflow code — a deterministic pure function of the recorded result.
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
        # herd of cancels/starts, and a conflict stops the run at a clean
        # boundary (everything before it fully converted, nothing after
        # touched).
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
        """Cancel the stale source-type run, convert, start the replacement."""
        # 3a — the stale run can only be addressed by its deterministic id;
        # not-found (no such run — every Available match) surfaces as an
        # exception on the cancel request and is the normal case.
        stale_run_id = open_segment_rules_workflow_id(
            convert_input.source_type, segment.segment
        )
        cancelled = False
        try:
            await asyncio.wait_for(
                workflow.get_external_workflow_handle(stale_run_id).cancel(),
                timeout=_CANCEL_RESOLUTION_TIMEOUT.total_seconds(),
            )
            cancelled = True
            workflow.logger.info(
                "Cancelled stale run %s before converting %s",
                stale_run_id,
                segment.segment,
            )
        except asyncio.TimeoutError:
            workflow.logger.info(
                "Cancel handshake for %s unresolved after %ds — proceeding "
                "(the request stays submitted)",
                stale_run_id,
                int(_CANCEL_RESOLUTION_TIMEOUT.total_seconds()),
            )
        except Exception:
            workflow.logger.info(
                "No running %s to cancel for %s", stale_run_id, segment.segment
            )
        if cancelled:
            # Let the cancelled run's best-effort cleanup land before the
            # conversion wipes the same fields (durable, replay-safe timer).
            await workflow.sleep(_CANCEL_CLEANUP_GRACE)

        # 3b — the atomic re-type + re-lock + connectivity-field clear.
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

        # 3c — the replacement run, detached: ABANDON keeps it alive however
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
            previous_status=segment.status,
            cancelled_previous_run=cancelled,
            open_segment_rules_workflow_id=child_id,
            open_rules_status=open_rules_status,
        )
