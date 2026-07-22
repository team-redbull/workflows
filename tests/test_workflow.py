"""Workflow tests: real SegmentConnectivityWorkflow, mock activities, time-skipping env.

The time-skipping environment makes the poll loop's constant interval and
retry backoffs run in milliseconds. The workflow routes activities to
SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE explicitly, so each test runs TWO workers — one per
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

from shared.consts import SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE, SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE
from shared.exceptions import BmcSegmentNotConfiguredError, SegmentNotFoundError
from shared.models.segment_connectivity import (
    BmcOpenRulesRequest,
    SegmentConnectivityFailureNotice,
    SegmentConnectivityInput,
    SegmentConnectivityRequestRef,
    SegmentConnectivityRequestsUpdate,
    SegmentConnectivityResumeState,
    SegmentConnectivityRunArgs,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
    SegmentType,
)
from workflows.segment_connectivity import SegmentConnectivityWorkflow

SEGMENT = "10.0.0.0/24"
HC_INPUT = SegmentConnectivityInput(segment=SEGMENT, type=SegmentType.HC)
MCE_INPUT = SegmentConnectivityInput(segment=SEGMENT, type=SegmentType.MCE)


def make_mock_activities(
    *,
    site: str = "site-a",
    peer_segments: tuple[SegmentRef, ...] = (SegmentRef(segment="10.1.0.0/24", type=SegmentType.MCE),),
    bmc_segment: str | None = "10.99.0.0/16",
    check_script: list[list[int]] | None = None,
    check_always_pending: bool = False,
    site_fail_times: int = 0,
    site_error: Exception | None = None,
):
    """Build the full mock activity set + a call recorder.

    check_script: per-poll return values; once exhausted (and not
    check_always_pending) every subsequent poll returns [] (all complete).
    bmc_segment=None simulates an unconfigured site (get_bmc_segment raises).
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_segment_site",
            "list_peer_segments",
            "submit_open_rules",
            "get_bmc_segment",
            "submit_bmc_open_rules",
            "publish_request_ids",
            "check_segment_connectivity_requests",
            "get_next_checking_request_interval",
            "unlock_segment",
            "publish_segment_connectivity_failure",
        )
    }
    script = list(check_script or [])
    ids = itertools.count(1)
    site_failures_left = [site_fail_times]

    @activity.defn
    async def get_segment_site(connectivity_input: SegmentConnectivityInput) -> str:
        calls["get_segment_site"].append(connectivity_input)
        if site_error is not None:
            raise site_error
        if site_failures_left[0] > 0:
            site_failures_left[0] -= 1
            raise RuntimeError("simulated transient Segments Manager outage")
        return site

    @activity.defn
    async def list_peer_segments(query: PeerSegmentsQuery) -> list[SegmentRef]:
        calls["list_peer_segments"].append(query)
        return list(peer_segments)

    @activity.defn
    async def submit_open_rules(request: OpenRulesRequest) -> SegmentConnectivityRequestRef:
        calls["submit_open_rules"].append(request)
        return SegmentConnectivityRequestRef(id=next(ids), status="pending")

    @activity.defn
    async def get_bmc_segment(site_arg: str) -> str:
        calls["get_bmc_segment"].append(site_arg)
        if bmc_segment is None:
            raise BmcSegmentNotConfiguredError(f"No BMC segment configured for site={site_arg}")
        return bmc_segment

    @activity.defn
    async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> SegmentConnectivityRequestRef:
        calls["submit_bmc_open_rules"].append(request)
        return SegmentConnectivityRequestRef(id=next(ids), status="pending")

    @activity.defn
    async def publish_request_ids(update: SegmentConnectivityRequestsUpdate) -> None:
        calls["publish_request_ids"].append(update)

    @activity.defn
    async def check_segment_connectivity_requests(request_ids: list[int]) -> list[int]:
        calls["check_segment_connectivity_requests"].append(list(request_ids))
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
        get_segment_site,
        list_peer_segments,
        submit_open_rules,
        get_bmc_segment,
        submit_bmc_open_rules,
        publish_request_ids,
        check_segment_connectivity_requests,
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
            task_queue=SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
            workflows=[SegmentConnectivityWorkflow],
        )
        self._activity_worker = Worker(
            client,
            task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
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


async def _execute(client: Client, args: SegmentConnectivityRunArgs):
    return await asyncio.wait_for(
        client.execute_workflow(
            SegmentConnectivityWorkflow.run,
            args,
            id=f"test-{uuid.uuid4()}",
            task_queue=SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
        ),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


async def test_happy_path_submits_polls_publishes_and_unlocks():
    calls, mocks = make_mock_activities(check_script=[[2], []])
    async with _Harness(mocks) as client:
        result = await _execute(client, SegmentConnectivityRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert result.type == SegmentType.HC
    assert result.peer_segment_count == 1
    # 2 directions x 1 MCE segment; the mock's ids race, so order-insensitive.
    assert sorted(result.request_ids) == [1, 2]
    assert calls["list_peer_segments"] == [
        PeerSegmentsQuery(source_type=SegmentType.HC, site="site-a")
    ]
    assert len(calls["submit_open_rules"]) == 2
    # The BMC leg is MCE-only: an HC input never touches it.
    assert calls["get_bmc_segment"] == []
    assert calls["submit_bmc_open_rules"] == []
    assert calls["unlock_segment"] == [SEGMENT]
    # Publish trail: all ids after submit, shrink to [2], then the clearing [].
    published = [u.request_ids for u in calls["publish_request_ids"]]
    assert published == [result.request_ids, [2], []]
    # submitted_at captured once and reused verbatim on every republish.
    assert len({u.submitted_at for u in calls["publish_request_ids"]}) == 1
    assert calls["publish_segment_connectivity_failure"] == []


def test_supported_types_covers_every_segment_type():
    # All 4 real SegmentType members are supported today, so the
    # "UnsupportedSegmentType" fail-fast path can no longer be exercised
    # through the public API with a real SegmentType value (the enum is
    # closed, and Temporal's workflow sandbox re-executes this module in
    # isolation, so monkeypatching _SUPPORTED_TYPES from the test process
    # doesn't reach the sandboxed copy either). This locks in the gate's
    # current coverage instead: if a 5th SegmentType is ever added without
    # updating _SUPPORTED_TYPES, this test fails loudly and prompts an
    # explicit decision, exactly as the gate is meant to.
    from workflows.segment_connectivity import _SUPPORTED_TYPES

    assert _SUPPORTED_TYPES == frozenset(SegmentType)


async def test_segment_not_found_fails_without_retry_or_note():
    calls, mocks = make_mock_activities(site_error=SegmentNotFoundError("missing"))
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, SegmentConnectivityRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "SegmentNotFoundError"
    # Non-retryable classification: exactly one attempt, no failure note
    # (validation phase — nothing was submitted).
    assert len(calls["get_segment_site"]) == 1
    assert calls["publish_segment_connectivity_failure"] == []


async def test_transient_activity_failures_are_outwaited():
    calls, mocks = make_mock_activities(site_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client, SegmentConnectivityRunArgs(input=HC_INPUT))

    assert sorted(result.request_ids) == [1, 2]
    assert len(calls["get_segment_site"]) == 3  # 2 transient failures + success


async def test_empty_peer_pool_fails_and_publishes_failure_note():
    calls, mocks = make_mock_activities(peer_segments=())
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, SegmentConnectivityRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "NoPeerSegments"
    assert calls["submit_open_rules"] == []
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT
    assert "No same-site peer segments" in notice.message


async def test_mce_source_peers_with_hc_inventory_and_pxe():
    peers = (
        SegmentRef(segment="10.1.0.0/24", type=SegmentType.HC),
        SegmentRef(segment="10.2.0.0/24", type=SegmentType.INVENTORY),
        SegmentRef(segment="10.3.0.0/24", type=SegmentType.PXE),
    )
    calls, mocks = make_mock_activities(peer_segments=peers, check_script=[[]])
    async with _Harness(mocks) as client:
        result = await _execute(client, SegmentConnectivityRunArgs(input=MCE_INPUT))

    assert result.peer_segment_count == 3
    assert calls["list_peer_segments"] == [
        PeerSegmentsQuery(source_type=SegmentType.MCE, site="site-a")
    ]
    assert len(calls["submit_open_rules"]) == 6
    pairs = {(r.source_type, r.destination_type) for r in calls["submit_open_rules"]}
    assert pairs == {
        (SegmentType.MCE, SegmentType.HC),
        (SegmentType.HC, SegmentType.MCE),
        (SegmentType.MCE, SegmentType.INVENTORY),
        (SegmentType.INVENTORY, SegmentType.MCE),
        (SegmentType.MCE, SegmentType.PXE),
        (SegmentType.PXE, SegmentType.MCE),
    }
    # Plus the mandatory one-directional BMC leg.
    assert calls["get_bmc_segment"] == ["site-a"]
    (bmc_request,) = calls["submit_bmc_open_rules"]
    assert bmc_request.mce_segment == SEGMENT
    assert bmc_request.bmc_segment == "10.99.0.0/16"
    assert len(result.request_ids) == 7


async def test_mce_source_with_no_peers_still_submits_bmc_rule():
    calls, mocks = make_mock_activities(peer_segments=(), check_script=[[]])
    async with _Harness(mocks) as client:
        result = await _execute(client, SegmentConnectivityRunArgs(input=MCE_INPUT))

    assert result.peer_segment_count == 0
    assert len(result.request_ids) == 1
    assert calls["submit_open_rules"] == []
    assert len(calls["submit_bmc_open_rules"]) == 1
    assert calls["publish_segment_connectivity_failure"] == []


async def test_mce_source_missing_bmc_config_fails_non_retryable():
    calls, mocks = make_mock_activities(bmc_segment=None)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, SegmentConnectivityRunArgs(input=MCE_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "BmcSegmentNotConfiguredError"
    # Non-retryable classification: exactly one attempt.
    assert len(calls["get_bmc_segment"]) == 1
    assert calls["submit_bmc_open_rules"] == []
    assert calls["submit_open_rules"] == []
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT


async def test_resume_path_skips_submission_and_finishes():
    submitted_at = datetime(2026, 7, 18, 9, 30, tzinfo=timezone.utc)
    calls, mocks = make_mock_activities(check_script=[[12], []])
    resume = SegmentConnectivityResumeState(
        request_ids=[11, 12],
        pending_request_ids=[11, 12],
        peer_segment_count=3,
        submitted_at=submitted_at,
    )
    async with _Harness(mocks) as client:
        result = await _execute(
            client, SegmentConnectivityRunArgs(input=HC_INPUT, resume=resume)
        )

    assert result.request_ids == [11, 12]
    assert result.peer_segment_count == 3
    # Resume never re-validates or re-submits.
    assert calls["get_segment_site"] == []
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
            SegmentConnectivityWorkflow.run,
            SegmentConnectivityRunArgs(input=HC_INPUT),
            id=f"test-{uuid.uuid4()}",
            task_queue=SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
        )
        # Let it get past submission into the poll loop before cancelling.
        async def _wait_for_polling():
            while True:
                progress = await handle.query(SegmentConnectivityWorkflow.progress)
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
