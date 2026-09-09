"""Workflow tests: real InitializeSegmentWorkflow, mock activities, time-skipping env.

The time-skipping environment makes the poll loop's constant interval and
retry backoffs run in milliseconds. The workflow routes activities to
SEGMENT_LIFECYCLE_ACTIVITY_QUEUE explicitly, so each test runs TWO workers — one per
queue — exactly like the real brain/limb split.
"""

from __future__ import annotations

import asyncio
import itertools
import uuid
from datetime import datetime, timezone

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, INITIALIZE_SEGMENT_WORKFLOW_QUEUE
from shared.exceptions import BmcSegmentNotConfiguredError, SegmentValidationError
from shared.models.segment_lifecycle import (
    BmcOpenRulesRequest,
    BmcRuleDirection,
    BmcSegments,
    BmcVendor,
    SegmentConnectivityFailureNotice,
    InitializeSegmentInput,
    NextRequestRef,
    SegmentConnectivityRequestsUpdate,
    InitializeSegmentResumeState,
    InitializeSegmentRunArgs,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
    SegmentType,
)
from workflow_domains.segment_lifecycle.initialize_segment import (
    InitializeSegmentWorkflow,
)

SEGMENT = "10.0.0.0/24"
SITE = "site-a"


def _input(segment_type: SegmentType) -> InitializeSegmentInput:
    """A complete segment definition — the workflow now CREATES the segment, so
    its input carries every field the Segments Manager needs, not just a
    reference to an existing one."""
    return InitializeSegmentInput(
        segment=SEGMENT,
        type=segment_type,
        site=SITE,
        vlan_id=100,
        epg_name="EPG_TEST_01",
    )


HC_INPUT = _input(SegmentType.HC)
MCE_INPUT = _input(SegmentType.MCE)


def make_mock_activities(
    *,
    peer_segments: tuple[SegmentRef, ...] = (SegmentRef(segment="10.1.0.0/24", type=SegmentType.MCE),),
    bmc_segments: BmcSegments | None = BmcSegments(
        dell="10.98.0.0/16", cisco="10.99.0.0/16"
    ),
    check_script: list[list[int]] | None = None,
    check_always_pending: bool = False,
    create_fail_times: int = 0,
    create_error: Exception | None = None,
    open_connectivity: bool = False,
):
    """Build the full mock activity set + a call recorder.

    check_script: per-poll return values; once exhausted (and not
    check_always_pending) every subsequent poll returns [] (all complete).
    bmc_segments=None simulates an unconfigured site (get_bmc_segments raises).
    open_connectivity=True makes the site one whose connectivity is always
    open (SITES_WITH_OPEN_CONNECTIVITY), so the whole next flow is skipped.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "create_segment",
            "site_has_open_connectivity",
            "list_peer_segments",
            "submit_open_rules",
            "get_bmc_segments",
            "submit_bmc_open_rules",
            "publish_request_ids",
            "check_next_requests",
            "get_next_checking_request_interval",
            "unlock_segment",
            "publish_segment_connectivity_failure",
        )
    }
    script = list(check_script or [])
    ids = itertools.count(1)
    create_failures_left = [create_fail_times]

    @activity.defn
    async def create_segment(rules_input: InitializeSegmentInput) -> None:
        calls["create_segment"].append(rules_input)
        if create_error is not None:
            raise create_error
        if create_failures_left[0] > 0:
            create_failures_left[0] -= 1
            raise RuntimeError("simulated transient Segments Manager outage")

    @activity.defn
    async def site_has_open_connectivity(site_arg: str) -> bool:
        calls["site_has_open_connectivity"].append(site_arg)
        return open_connectivity

    @activity.defn
    async def list_peer_segments(query: PeerSegmentsQuery) -> list[SegmentRef]:
        calls["list_peer_segments"].append(query)
        return list(peer_segments)

    @activity.defn
    async def submit_open_rules(request: OpenRulesRequest) -> NextRequestRef:
        calls["submit_open_rules"].append(request)
        return NextRequestRef(id=next(ids), status="pending")

    @activity.defn
    async def get_bmc_segments(site_arg: str) -> BmcSegments:
        calls["get_bmc_segments"].append(site_arg)
        if bmc_segments is None:
            raise BmcSegmentNotConfiguredError(f"No BMC segments configured for site={site_arg}")
        return bmc_segments

    @activity.defn
    async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> NextRequestRef:
        calls["submit_bmc_open_rules"].append(request)
        return NextRequestRef(id=next(ids), status="pending")

    @activity.defn
    async def publish_request_ids(update: SegmentConnectivityRequestsUpdate) -> None:
        calls["publish_request_ids"].append(update)

    @activity.defn
    async def check_next_requests(request_ids: list[int]) -> list[int]:
        calls["check_next_requests"].append(list(request_ids))
        if script:
            return script.pop(0)
        if check_always_pending:
            return list(request_ids)
        return []

    @activity.defn
    async def get_next_checking_request_interval() -> int:
        calls["get_next_checking_request_interval"].append(None)
        return 1

    @activity.defn
    async def unlock_segment(segment: str) -> None:
        calls["unlock_segment"].append(segment)

    @activity.defn
    async def publish_segment_connectivity_failure(notice: SegmentConnectivityFailureNotice) -> None:
        calls["publish_segment_connectivity_failure"].append(notice)

    return calls, [
        create_segment,
        site_has_open_connectivity,
        list_peer_segments,
        submit_open_rules,
        get_bmc_segments,
        submit_bmc_open_rules,
        publish_request_ids,
        check_next_requests,
        get_next_checking_request_interval,
        unlock_segment,
        publish_segment_connectivity_failure,
    ]


class _Harness:
    """One time-skipping env + the two production-shaped workers."""

    def __init__(self, mock_activities):
        self._mock_activities = mock_activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workflow_worker = Worker(
            client,
            task_queue=INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
            workflows=[InitializeSegmentWorkflow],
        )
        self._activity_worker = Worker(
            client,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            activities=self._mock_activities,
        )
        await self._workflow_worker.__aenter__()
        await self._activity_worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info):
        await self._activity_worker.__aexit__(*exc_info)
        await self._workflow_worker.__aexit__(*exc_info)
        await self._env.shutdown()


def _workflow_cause(exc_info) -> BaseException:
    """Unwrap WorkflowFailureError -> (ActivityError ->) the root ApplicationError."""
    cause = exc_info.value.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    return cause


async def _execute(client: Client, args: InitializeSegmentRunArgs):
    return await asyncio.wait_for(
        client.execute_workflow(
            InitializeSegmentWorkflow.run,
            args,
            id=f"test-{uuid.uuid4()}",
            task_queue=INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
        ),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


async def test_happy_path_submits_polls_publishes_and_unlocks():
    calls, mocks = make_mock_activities(check_script=[[2], []])
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert result.type == SegmentType.HC
    assert result.peer_segment_count == 1
    # 2 directions x 1 MCE segment; the mock's ids race, so order-insensitive.
    assert sorted(result.request_ids) == [1, 2]
    assert result.open_connectivity_site is False
    # Asked exactly once, before any peer is listed.
    assert calls["site_has_open_connectivity"] == [SITE]
    assert calls["list_peer_segments"] == [
        PeerSegmentsQuery(source_type=SegmentType.HC, site=SITE)
    ]
    assert len(calls["submit_open_rules"]) == 2
    # The BMC leg is MCE-only: an HC input never touches it.
    assert calls["get_bmc_segments"] == []
    assert calls["submit_bmc_open_rules"] == []
    assert calls["unlock_segment"] == [SEGMENT]
    # Publish trail: all ids after submit, shrink to [2], then the clearing [].
    published = [u.request_ids for u in calls["publish_request_ids"]]
    assert published == [result.request_ids, [2], []]
    # submitted_at captured once and reused verbatim on every republish.
    assert len({u.submitted_at for u in calls["publish_request_ids"]}) == 1
    assert calls["publish_segment_connectivity_failure"] == []


async def test_open_connectivity_site_creates_and_unlocks_without_any_next_request():
    """A site whose connectivity is always open: the segment is created and
    unlocked in one run, and the next service is never involved — no peer
    discovery, no submission, no polling, and no request-ids display (nothing
    is ever pending for an operator to wait on)."""
    calls, mocks = make_mock_activities(open_connectivity=True)
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert result.open_connectivity_site is True
    assert result.request_ids == []
    assert result.peer_segment_count == 0
    # The segment IS still created here — the workflow stays the single entry
    # point; only the connectivity half is skipped.
    assert calls["create_segment"] == [HC_INPUT]
    assert calls["site_has_open_connectivity"] == [SITE]
    assert calls["unlock_segment"] == [SEGMENT]
    for skipped in (
        "list_peer_segments",
        "submit_open_rules",
        "get_bmc_segments",
        "submit_bmc_open_rules",
        "publish_request_ids",
        "check_next_requests",
        "get_next_checking_request_interval",
        "publish_segment_connectivity_failure",
    ):
        assert calls[skipped] == [], skipped


async def test_open_connectivity_site_skips_the_next_flow_for_mce_too():
    """MCE is the type with mandatory BMC rules on the normal path — an open
    site must skip those as well, not just peer discovery."""
    calls, mocks = make_mock_activities(open_connectivity=True)
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=MCE_INPUT))

    assert result.open_connectivity_site is True
    assert calls["get_bmc_segments"] == []
    assert calls["submit_bmc_open_rules"] == []
    assert calls["unlock_segment"] == [SEGMENT]


async def test_open_connectivity_site_re_run_is_idempotent():
    """Re-running for a segment that already exists and is already unlocked is
    a no-op success — both activities absorb it server-side, so the open path
    needs no state of its own to be safely repeatable."""
    calls, mocks = make_mock_activities(open_connectivity=True)
    async with _Harness(mocks) as client:
        first = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))
        second = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert first == second
    assert calls["create_segment"] == [HC_INPUT, HC_INPUT]
    assert calls["unlock_segment"] == [SEGMENT, SEGMENT]


async def test_open_connectivity_site_still_rejects_an_unsupported_type():
    """The open-site short-circuit sits AFTER the type gate: PXE is rejected
    everywhere, so an open site cannot become a back door for a type this
    workflow does not support."""
    calls, mocks = make_mock_activities(open_connectivity=True)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=_input(SegmentType.PXE)))

    assert _workflow_cause(exc_info).type == "UnsupportedSegmentType"
    assert calls["create_segment"] == []
    assert calls["site_has_open_connectivity"] == []
    assert calls["unlock_segment"] == []


def test_supported_types_is_every_type_except_pxe():
    # The gate's coverage, locked in: PXE is a real SegmentType (the Segments
    # Manager still creates PXE segments) that this workflow deliberately does
    # NOT open connectivity for. If a 5th SegmentType is ever added without an
    # explicit decision here, this fails loudly — exactly as the gate intends.
    from workflow_domains.segment_lifecycle.initialize_segment import _SUPPORTED_TYPES

    assert _SUPPORTED_TYPES == frozenset(SegmentType) - {SegmentType.PXE}


async def test_pxe_input_is_rejected_before_the_segment_is_created():
    """The unsupported-type gate runs BEFORE create_segment and OUTSIDE the
    failure-note try block: a PXE run must leave nothing behind — no segment
    in the Segments Manager, and no failure note about a segment that was
    never created."""
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=_input(SegmentType.PXE)))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnsupportedSegmentType"
    assert calls["create_segment"] == []
    assert calls["list_peer_segments"] == []
    assert calls["submit_open_rules"] == []
    assert calls["publish_segment_connectivity_failure"] == []


async def test_creation_is_the_first_step_and_carries_the_full_definition():
    calls, mocks = make_mock_activities(check_script=[[]])
    async with _Harness(mocks) as client:
        await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    # The segment is created by the workflow, before anything is submitted.
    assert calls["create_segment"] == [HC_INPUT]
    assert calls["create_segment"][0].vlan_id == 100
    assert calls["create_segment"][0].epg_name == "EPG_TEST_01"


async def test_rejected_definition_fails_without_retry_or_note():
    calls, mocks = make_mock_activities(
        create_error=SegmentValidationError("VLAN 100 already exists at site 'site-a'")
    )
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "SegmentValidationError"
    # Non-retryable classification: exactly one attempt, no failure note
    # (creation phase — nothing was submitted, and there may be no segment
    # to annotate).
    assert len(calls["create_segment"]) == 1
    assert calls["publish_segment_connectivity_failure"] == []


async def test_transient_activity_failures_are_outwaited():
    calls, mocks = make_mock_activities(create_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert sorted(result.request_ids) == [1, 2]
    assert len(calls["create_segment"]) == 3  # 2 transient failures + success


async def test_empty_peer_pool_fails_and_publishes_failure_note():
    calls, mocks = make_mock_activities(peer_segments=())
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "NoPeerSegments"
    assert calls["submit_open_rules"] == []
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT
    assert "No same-site peer segments" in notice.message


async def test_mce_source_peers_with_hc_and_inventory():
    peers = (
        SegmentRef(segment="10.1.0.0/24", type=SegmentType.HC),
        SegmentRef(segment="10.2.0.0/24", type=SegmentType.INVENTORY),
    )
    calls, mocks = make_mock_activities(peer_segments=peers, check_script=[[]])
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=MCE_INPUT))

    assert result.peer_segment_count == 2
    assert calls["list_peer_segments"] == [
        PeerSegmentsQuery(source_type=SegmentType.MCE, site=SITE)
    ]
    assert len(calls["submit_open_rules"]) == 4
    pairs = {(r.source_type, r.destination_type) for r in calls["submit_open_rules"]}
    assert pairs == {
        (SegmentType.MCE, SegmentType.HC),
        (SegmentType.HC, SegmentType.MCE),
        (SegmentType.MCE, SegmentType.INVENTORY),
        (SegmentType.INVENTORY, SegmentType.MCE),
    }
    # Plus the mandatory BMC legs — one request per configured hardware vendor
    # per direction, all four (this site has both) from a single
    # get_bmc_segments call.
    assert calls["get_bmc_segments"] == [SITE]
    # Set comparison: the submissions run concurrently, so the order they are
    # RECORDED in races (same reason the ids above are sorted). The fixed
    # SCHEDULING order that replay depends on is BmcSegments.pairs()'s job.
    assert {
        (r.vendor, r.direction, r.mce_segment, r.bmc_segment)
        for r in calls["submit_bmc_open_rules"]
    } == {
        (BmcVendor.DELL, d, SEGMENT, "10.98.0.0/16")
        for d in BmcRuleDirection
    } | {
        (BmcVendor.CISCO, d, SEGMENT, "10.99.0.0/16")
        for d in BmcRuleDirection
    }
    assert len(result.request_ids) == 8


async def test_mce_source_with_no_peers_still_submits_bmc_rules():
    calls, mocks = make_mock_activities(peer_segments=(), check_script=[[]])
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=MCE_INPUT))

    assert result.peer_segment_count == 0
    # Two vendors x two directions at this site — the BMC legs alone keep the
    # run alive.
    assert len(result.request_ids) == 4
    assert calls["submit_open_rules"] == []
    assert {(r.vendor, r.direction) for r in calls["submit_bmc_open_rules"]} == {
        (vendor, direction)
        for vendor in BmcVendor
        for direction in BmcRuleDirection
    }
    assert calls["publish_segment_connectivity_failure"] == []


async def test_mce_source_at_a_single_vendor_site_opens_that_vendor_only():
    """A site with only Dell hardware carries only `dell-bmc`, and the MCE run
    opens both directions against it — two BMC requests, not four. The absent
    vendor is a site shape, not a config gap: nothing fails."""
    calls, mocks = make_mock_activities(
        peer_segments=(),
        bmc_segments=BmcSegments(dell="10.98.0.0/16"),
        check_script=[[]],
    )
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=MCE_INPUT))

    assert len(result.request_ids) == 2
    assert {
        (r.vendor, r.direction, r.bmc_segment)
        for r in calls["submit_bmc_open_rules"]
    } == {(BmcVendor.DELL, d, "10.98.0.0/16") for d in BmcRuleDirection}
    assert calls["publish_segment_connectivity_failure"] == []


async def test_mce_source_missing_bmc_config_fails_non_retryable():
    calls, mocks = make_mock_activities(bmc_segments=None)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=MCE_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "BmcSegmentNotConfiguredError"
    # Non-retryable classification: exactly one attempt.
    assert len(calls["get_bmc_segments"]) == 1
    assert calls["submit_bmc_open_rules"] == []
    assert calls["submit_open_rules"] == []
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT


async def test_resume_path_skips_submission_and_finishes():
    submitted_at = datetime(2026, 7, 18, 9, 30, tzinfo=timezone.utc)
    calls, mocks = make_mock_activities(check_script=[[12], []])
    resume = InitializeSegmentResumeState(
        request_ids=[11, 12],
        pending_request_ids=[11, 12],
        peer_segment_count=3,
        submitted_at=submitted_at,
    )
    async with _Harness(mocks) as client:
        result = await _execute(
            client, InitializeSegmentRunArgs(input=HC_INPUT, resume=resume)
        )

    assert result.request_ids == [11, 12]
    assert result.peer_segment_count == 3
    # Resume never re-creates, re-checks the site, or re-submits.
    assert calls["create_segment"] == []
    assert calls["site_has_open_connectivity"] == []
    assert calls["list_peer_segments"] == []
    assert calls["submit_open_rules"] == []
    # The original submission time survives continue_as_new.
    published = [(u.request_ids, u.submitted_at) for u in calls["publish_request_ids"]]
    assert published == [([12], submitted_at), ([], submitted_at)]
    assert calls["unlock_segment"] == [SEGMENT]


async def test_cancellation_publishes_failure_note_with_orphaned_ids():
    calls, mocks = make_mock_activities(check_always_pending=True)
    async with _Harness(mocks) as client:
        handle = await client.start_workflow(
            InitializeSegmentWorkflow.run,
            InitializeSegmentRunArgs(input=HC_INPUT),
            id=f"test-{uuid.uuid4()}",
            task_queue=INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
        )
        # Let it get past submission into the poll loop before cancelling.
        async def _wait_for_polling():
            while True:
                progress = await handle.query(InitializeSegmentWorkflow.progress)
                if progress.phase == "awaiting-completion":
                    return
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_wait_for_polling(), timeout=30)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as exc_info:
            await asyncio.wait_for(handle.result(), timeout=30)

    assert isinstance(exc_info.value.cause, CancelledError)
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT
    assert "cancelled" in notice.message
    assert "orphaned next request ids" in notice.message  # ids survive in the note
    assert calls["unlock_segment"] == []  # segment must stay Locked
