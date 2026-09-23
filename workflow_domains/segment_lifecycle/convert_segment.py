"""convert-segment — rebalances segment inventory between types: re-types up
to `quantity` AVAILABLE segments of a source type at a site.

The THIRD workflow of the `segment-lifecycle` domain: it reuses the running
`segment-lifecycle-worker` limb, its activity queue and its ConfigMap — only
this module, its own workflow queue and its route are new.

CONVERTIBLE = Available AND no cluster assigned, and the activity asserts both
on every hit rather than trusting the filter it asked for. Allocated segments
are in use by definition; an Available segment carrying a cluster is a Segments
Manager invariant violation, which fails the run rather than being converted
under whoever holds it. The Segments Manager applies the same guard server-side
on the conversion itself, so the two agree.

Re-typing is the WHOLE operation. A converted segment stays Available and is
immediately allocatable as its new type — nothing has to be established for it
first. (This workflow used to start an initialize-segment child per converted
segment: the new type had different firewall peers, so connectivity had to be
re-opened from scratch, and the segment sat Locked until a human approved it.
Every firewall is open now, so the fan-out and the wait are both gone.)

Shape of the run (all MACHINE ops — bounded, fails loudly, no continue_as_new):

  1. validating-site      — GET /api/sites; an unknown site fails as
                            UnknownSite instead of silently matching nothing.
  2. searching-segments   — list every convertible segment of the source type
                            at the site. Selection is the WORKFLOW's
                            deterministic policy over that recorded result:
                            lowest vlan_id first, cut at `quantity`. Fewer
                            matches than requested is NOT an error — the result
                            reports the shortfall.
  3. converting-segments  — sequentially per selected segment: PUT
                            /api/segments/type with expected_type=source as a
                            compare-and-set against concurrent conversions.

The compare-and-set is what makes two conversions racing for one segment safe
(HC->MCE against HC->PXE): the loser gets a 409 rather than silently hijacking
a segment the winner already re-typed. A plain Temporal retry stays safe too —
a stored type already equal to the NEW type short-circuits as success before
the CAS check.

Cancellation mid-loop needs no compensation: conversions already performed
stand, and nothing pending is half-applied. A re-run is naturally convergent —
converted segments no longer match the source type, so the search skips them.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
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
        SegmentTypeUpdate,
    )

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

    Every match is equally convertible (the search returns nothing else), so
    vlan_id is the only discriminator left — and it is a total order over the
    CIDR-unique segments of one site, which keeps the selection replay-stable.
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
        which segments WERE converted (those conversions stand)."""
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

        # Step 2 — find and select. The activity returns every convertible hit;
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

        # Step 3 — convert sequentially: deterministic order, and a conflict
        # stops the run at a clean boundary (everything before it fully
        # converted, nothing after touched).
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
        """Re-type one segment. That is the entire conversion: it stays
        Available and is allocatable as its new type the moment this returns.

        expected_type is the compare-and-set guard — a concurrent conversion
        that re-typed this segment first turns the call into a 409
        (SegmentConversionConflictError, non-retryable) rather than a silent
        hijack.
        """
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
        return ConvertedSegmentReport(
            segment=segment.segment, vlan_id=segment.vlan_id
        )
