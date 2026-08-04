"""open-segment-rules — creates a segment in the Segments Manager, opens its
firewall rules via the next API, then flips it Locked -> Available.

The FIRST workflow of the `segment-connectivity` domain, named for what it does
rather than for the domain, so a sibling (e.g. a close-segment-rules that tears
the rules down when a segment is decommissioned) can join the domain without a
naming collision. Domain-scoped things — the activity queue, the limb
deployment, the models, the `/workflows/segment-connectivity` API prefix — stay
named after the DOMAIN and are shared; workflow-scoped things (this class, its
task queue, its workflow ids, its RunArgs/ResumeState/Progress/Result models)
carry the workflow name.

This workflow owns a segment's WHOLE lifecycle and is its single entry point:
an operator POSTs the segment definition to the trigger API (workflows/api.py)
and the workflow calls the Segments Manager itself (create_segment, step 1).
It used to work the other way round — the Segments Manager created the segment
and then fired a best-effort HTTP trigger at us — which left creation outside
Temporal: invisible in the UI, and silently skipped whenever that call failed.
Now every step is one durable, replayable run.

HC, INVENTORY, PXE and MCE segments each peer with every same-site segment of
the OTHER types the port policy defines for them (list_peer_segments + a
bidirectional OpenRulesRequest per peer). Today HC/INVENTORY/PXE each peer
only with MCE, and MCE peers with all three of them — symmetric by
construction: adding a new MCE segment discovers and opens rules against
every existing same-site HC/INVENTORY/PXE segment, exactly as adding a new
HC/INVENTORY/PXE segment already discovers same-site MCE segments. The input
accepts any segment type; unsupported ones fail loudly.

MCE segments additionally get one mandatory, one-directional MCE -> BMC rule
per run (submit_bmc_open_rules), independent of peer discovery: BMC is a
static, ConfigMap-sourced network per site (not Segments-Manager-tracked),
so this never peers back and is submitted unconditionally whenever the input
type is MCE.

Completion of next requests depends on a HUMAN approval and can take minutes,
hours or more — the workflow therefore polls indefinitely (durable timers at a
constant, operator-configured interval) and rolls its history over with
continue_as_new every _CONTINUE_AS_NEW_AFTER; it never fails on a slow
approval. The same philosophy extends to activity retries: attempts are
UNBOUNDED so transient outages of the Segments Manager or the next service are
simply out-waited; only errors classified non-retryable (or raised as
non-retryable ApplicationError by an activity) fail the workflow. When one
does — or the workflow is cancelled — a failure note is best-effort published
to the Segments Manager so the segment is not left silently Locked.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE
    from shared.interfaces.segment_connectivity import (
        check_next_requests,
        create_segment,
        get_bmc_segment,
        get_next_checking_request_interval,
        list_peer_segments,
        publish_segment_connectivity_failure,
        publish_request_ids,
        submit_bmc_open_rules,
        submit_open_rules,
        unlock_segment,
    )
    from shared.models.segment_connectivity import (
        BmcOpenRulesRequest,
        SegmentConnectivityFailureNotice,
        OpenSegmentRulesInput,
        OpenSegmentRulesProgress,
        NextRequestRef,
        SegmentConnectivityRequestsUpdate,
        OpenSegmentRulesResult,
        OpenSegmentRulesResumeState,
        OpenSegmentRulesRunArgs,
        OpenRulesRequest,
        PeerSegmentsQuery,
        SegmentRef,
        SegmentType,
    )

# Network-bound activities: keep each attempt bounded (90s, with the HTTP
# client timing out well below that), but retry UNBOUNDED — transient outages
# of a dependency are out-waited, never fatal. Deterministic failures must be
# classified: either listed here by type, or raised by the activity as a
# non-retryable ApplicationError. An UNCLASSIFIED deterministic failure retries
# every minute forever (workflow stuck RUNNING, visible in the Temporal UI).
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# The full classified-permanent set, shared by both policies below so a newly
# classified error can never be added to one and forgotten in the other.
_NON_RETRYABLE_ERROR_TYPES = [
    "SegmentNotFoundError",
    "SegmentValidationError",
    "SegmentConflictError",
    "SegmentsManagerAuthError",
    "BmcSegmentNotConfiguredError",
]
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=_NON_RETRYABLE_ERROR_TYPES,
)
# The failure note is best-effort cleanup on a workflow that is already dying:
# a few bounded attempts, then give up (the workflow swallows the error).
_FAILURE_NOTE_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
    non_retryable_error_types=_NON_RETRYABLE_ERROR_TYPES,
)

# Types connectivity is implemented for. Which OTHER types each one peers
# with is derived entirely from the activity layer's PORTS_* profiles
# (_PORT_PROFILES) — HC/INVENTORY/PXE currently peer only with MCE, and MCE
# peers with all three of them (symmetric, driven by config not code). New
# peer-based types: add the member here plus its PORTS_<SRC>_TO_<DST> /
# PORTS_<DST>_TO_<SRC> config in the activity layer.
_SUPPORTED_TYPES = frozenset(
    {SegmentType.HC, SegmentType.INVENTORY, SegmentType.PXE, SegmentType.MCE}
)

# Endless-poll pacing: the interval itself is operator-configured (fast
# locally against the mock, slow in prod against the real human-driven
# approval — see get_next_checking_request_interval), constant per poll.
# continue_as_new rolls history over after a fixed wall-clock duration so it
# stays bounded no matter how long approval takes, independent of interval.
_CONTINUE_AS_NEW_AFTER = timedelta(hours=48)


@workflow.defn
class OpenSegmentRulesWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._total_requests = 0
        self._pending_request_ids: list[int] = []

    @workflow.query
    def progress(self) -> OpenSegmentRulesProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return OpenSegmentRulesProgress(
            phase=self._phase,
            total_requests=self._total_requests,
            pending_requests=len(self._pending_request_ids),
        )

    @workflow.run
    async def run(self, run_args: OpenSegmentRulesRunArgs) -> OpenSegmentRulesResult:
        rules_input = run_args.input
        resume = run_args.resume
        if resume is None:
            workflow.logger.info(
                "Creating segment=%s (type=%s, site=%s) and opening its connectivity",
                rules_input.segment,
                rules_input.type.value,
                rules_input.site,
            )
            if rules_input.type not in _SUPPORTED_TYPES:
                raise ApplicationError(
                    f"Connectivity for type={rules_input.type.value} is not "
                    f"supported yet (supported: {sorted(t.value for t in _SUPPORTED_TYPES)})",
                    type="UnsupportedSegmentType",
                )

            # Step 1 — create the segment (born Locked) in the Segments
            # Manager, which is also what validates the definition. Idempotent,
            # so a re-trigger for an already-created segment simply proceeds to
            # open its rules. Failures here need no failure note: nothing has
            # been submitted, and there may be no segment to annotate.
            self._phase = "creating-segment"
            await workflow.execute_activity(
                create_segment,
                rules_input,
                task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )

        # Once the segment exists, failures are terminal-by-classification only
        # (or a cancellation): surface them in the Segments Manager UI before
        # propagating, so the segment is not left silently Locked.
        # ContinueAsNewError deliberately passes through uncaught.
        try:
            return await self._run_created(rules_input, resume)
        except (ActivityError, ApplicationError, asyncio.CancelledError) as exc:
            await self._publish_failure(rules_input.segment, exc)
            raise

    async def _run_created(
        self,
        rules_input: OpenSegmentRulesInput,
        resume: OpenSegmentRulesResumeState | None,
    ) -> OpenSegmentRulesResult:
        if resume is None:
            state = await self._open_rules(rules_input)
        else:
            # Resumed after continue_as_new: rules are already submitted,
            # jump straight back into polling.
            state = resume

        request_ids = state.request_ids
        pending_request_ids = list(state.pending_request_ids)
        submitted_at = state.submitted_at
        self._total_requests = len(request_ids)
        self._pending_request_ids = pending_request_ids

        # Step 3 — poll until every request completes. Approval is human-driven
        # (can take hours+), so there is deliberately NO deadline: poll at a
        # constant, operator-configured interval and roll history over via
        # continue_as_new after _CONTINUE_AS_NEW_AFTER of wall-clock time.
        self._phase = "awaiting-completion"
        interval_seconds = await workflow.execute_activity(
            get_next_checking_request_interval,
            task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        interval = timedelta(seconds=interval_seconds)
        run_started_at = workflow.now()
        polls = 0
        while pending_request_ids:
            if workflow.now() - run_started_at >= _CONTINUE_AS_NEW_AFTER:
                workflow.logger.info(
                    "Continuing as new after %d poll cycles (%d requests pending)",
                    polls,
                    len(pending_request_ids),
                )
                workflow.continue_as_new(
                    OpenSegmentRulesRunArgs(
                        input=rules_input,
                        resume=OpenSegmentRulesResumeState(
                            request_ids=request_ids,
                            pending_request_ids=pending_request_ids,
                            peer_segment_count=state.peer_segment_count,
                            submitted_at=submitted_at,
                        ),
                    )
                )
            await workflow.sleep(interval)  # durable, replay-safe timer
            still_pending_request_ids = await workflow.execute_activity(
                check_next_requests,
                pending_request_ids,
                task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if still_pending_request_ids != pending_request_ids:
                # Completed ids drop off the Segments Manager display; the
                # final empty update deletes the display entirely.
                await self._publish_request_ids(
                    rules_input.segment, still_pending_request_ids, submitted_at
                )
            pending_request_ids = still_pending_request_ids
            self._pending_request_ids = pending_request_ids
            polls += 1
            workflow.logger.info(
                "Polling: %d/%d requests still pending",
                len(pending_request_ids),
                len(request_ids),
            )

        # Step 4 — all rules open: unlock the segment (Locked -> Available).
        self._phase = "unlocking-segment"
        await workflow.execute_activity(
            unlock_segment,
            rules_input.segment,
            task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        self._phase = "completed"
        workflow.logger.info(
            "Connectivity complete for segment=%s: %d rules opened, segment unlocked",
            rules_input.segment,
            len(request_ids),
        )
        return OpenSegmentRulesResult(
            segment=rules_input.segment,
            type=rules_input.type,
            peer_segment_count=state.peer_segment_count,
            request_ids=request_ids,
        )

    async def _open_rules(
        self, rules_input: OpenSegmentRulesInput
    ) -> OpenSegmentRulesResumeState:
        """Step 2 of a fresh run: list peers, fan out submissions, publish ids.

        The site is taken straight from the input rather than read back from
        the Segments Manager: create_segment has already reconciled the two
        (it fails on a stored segment whose site disagrees), so a second fetch
        could only return the same value.
        """
        site = rules_input.site
        # Same-site peer CIDRs across every type this segment's type peers
        # with (derived from the activity layer's port profiles).
        self._phase = "listing-peer-segments"
        peer_segments: list[SegmentRef] = await workflow.execute_activity(
            list_peer_segments,
            PeerSegmentsQuery(source_type=rules_input.type, site=site),
            task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Every MCE segment also gets one mandatory, one-directional rule to
        # its site's static BMC network — unconditional, independent of
        # whether any peers were found above (BMC is not a discovered peer).
        # Resolved before building any submission below so a missing BMC
        # config fails before anything is submitted, not partway through.
        bmc_segment: str | None = None
        if rules_input.type == SegmentType.MCE:
            bmc_segment = await workflow.execute_activity(
                get_bmc_segment,
                site,
                task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )

        # Two requests per peer segment (both directions), all in parallel.
        # asyncio.gather over execute_activity is deterministic and
        # sandbox-safe; result order matches submission order.
        self._phase = "submitting-rules"
        submissions = []
        for peer in peer_segments:
            for rule in (
                OpenRulesRequest(
                    source_segment=rules_input.segment,
                    destination_segment=peer.segment,
                    source_type=rules_input.type,
                    destination_type=peer.type,
                ),
                OpenRulesRequest(
                    source_segment=peer.segment,
                    destination_segment=rules_input.segment,
                    source_type=peer.type,
                    destination_type=rules_input.type,
                ),
            ):
                submissions.append(
                    workflow.execute_activity(
                        submit_open_rules,
                        rule,
                        task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=_RETRY_POLICY,
                    )
                )

        if bmc_segment is not None:
            submissions.append(
                workflow.execute_activity(
                    submit_bmc_open_rules,
                    BmcOpenRulesRequest(
                        mce_segment=rules_input.segment, bmc_segment=bmc_segment
                    ),
                    task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=_RETRY_POLICY,
                )
            )

        if not submissions:
            raise ApplicationError(
                f"No same-site peer segments found in the Segments Manager for "
                f"type={rules_input.type.value} — nothing to open "
                "connectivity against",
                type="NoPeerSegments",
            )

        refs: list[NextRequestRef] = list(await asyncio.gather(*submissions))
        request_ids = [ref.id for ref in refs]
        submitted_at = workflow.now()
        workflow.logger.info(
            "Submitted %d open-rules requests for %d peer segment(s)",
            len(request_ids),
            len(peer_segments),
        )

        # Surface the freshly submitted ids beside the segment's status in the
        # Segments Manager UI; they stay visible until the requests complete.
        await self._publish_request_ids(
            rules_input.segment, request_ids, submitted_at
        )

        return OpenSegmentRulesResumeState(
            request_ids=request_ids,
            pending_request_ids=list(request_ids),
            peer_segment_count=len(peer_segments),
            submitted_at=submitted_at,
        )

    async def _publish_request_ids(
        self, segment: str, request_ids: list[int], submitted_at: datetime
    ) -> None:
        """Mirror the still-pending request ids into the Segments Manager UI."""
        await workflow.execute_activity(
            publish_request_ids,
            SegmentConnectivityRequestsUpdate(
                segment=segment, request_ids=request_ids, submitted_at=submitted_at
            ),
            task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _publish_failure(self, segment: str, exc: BaseException) -> None:
        """Best-effort: surface the terminal failure beside the segment in the
        Segments Manager UI. Never raises — the original failure/cancellation
        must propagate unchanged, even if the manager is what's down."""
        if is_cancelled_exception(exc):
            reason = "the workflow was cancelled"
        else:
            # ActivityError's own message is generic; its cause carries the
            # real failure raised by the activity.
            cause = getattr(exc, "cause", None)
            reason = str(cause) if cause is not None else str(exc)
        # Names the WORKFLOW, not the domain: the endpoint and the UI display
        # are domain-level and shared, so the message is the only thing telling
        # an operator which of the domain's workflows died on this segment.
        message = f"open-segment-rules workflow failed: {reason}"
        if self._pending_request_ids:
            message += f" (orphaned next request ids: {self._pending_request_ids})"
        try:
            # Shielded so the cleanup survives the very cancellation it may be
            # handling (saga-style compensation pattern).
            await asyncio.shield(
                asyncio.ensure_future(
                    workflow.execute_activity(
                        publish_segment_connectivity_failure,
                        SegmentConnectivityFailureNotice(segment=segment, message=message),
                        task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=_FAILURE_NOTE_RETRY_POLICY,
                    )
                )
            )
        except Exception:
            workflow.logger.warning(
                "Could not publish failure note for segment %s", segment, exc_info=True
            )
