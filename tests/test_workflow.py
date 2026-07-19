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
from shared.exceptions import SegmentNotFoundError
from shared.models.segment_connectivity import (
    SegmentConnectivityFailureNotice,
    SegmentConnectivityInput,
    SegmentConnectivityRequestRef,
    SegmentConnectivityRequestsUpdate,
    SegmentConnectivityResumeState,
    SegmentConnectivityRunArgs,
    OpenRulesRequest,
    SegmentType,
)
from workflows.segment_connectivity import SegmentConnectivityWorkflow

SEGMENT = "10.0.0.0/24"
HC_INPUT = SegmentConnectivityInput(segment=SEGMENT, type=SegmentType.HC)


def make_mock_activities(
    *,
    site: str = "site-a",
    mce_segments: tuple[str, ...] = ("10.1.0.0/24",),
    check_script: list[list[int]] | None = None,
    check_always_pending: bool = False,
    site_fail_times: int = 0,
    site_error: Exception | None = None,
):
    """Build the full mock activity set + a call recorder.

    check_script: per-poll return values; once exhausted (and not
    check_always_pending) every subsequent poll returns [] (all complete).
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_segment_site",
            "list_mce_segments",
            "submit_open_rules",
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
    async def list_mce_segments(site_arg: str) -> list[str]:
        calls["list_mce_segments"].append(site_arg)
        return list(mce_segments)

    @activity.defn
    async def submit_open_rules(request: OpenRulesRequest) -> SegmentConnectivityRequestRef:
        calls["submit_open_rules"].append(request)
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
        list_mce_segments,
        submit_open_rules,
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
    assert calls["list_mce_segments"] == ["site-a"]
    assert len(calls["submit_open_rules"]) == 2
    assert calls["unlock_segment"] == [SEGMENT]
    # Publish trail: all ids after submit, shrink to [2], then the clearing [].
    published = [u.request_ids for u in calls["publish_request_ids"]]
    assert published == [result.request_ids, [2], []]
    # submitted_at captured once and reused verbatim on every republish.
    assert len({u.submitted_at for u in calls["publish_request_ids"]}) == 1
    assert calls["publish_segment_connectivity_failure"] == []


async def test_unsupported_type_fails_fast_before_any_activity():
    calls, mocks = make_mock_activities()
    mce_input = SegmentConnectivityInput(segment=SEGMENT, type=SegmentType.MCE)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, SegmentConnectivityRunArgs(input=mce_input))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnsupportedSegmentType"
    assert calls["get_segment_site"] == []
    assert calls["publish_segment_connectivity_failure"] == []  # pre-validation: no note


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


async def test_empty_mce_pool_fails_and_publishes_failure_note():
    calls, mocks = make_mock_activities(mce_segments=())
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, SegmentConnectivityRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "NoMceSegments"
    assert calls["submit_open_rules"] == []
    (notice,) = calls["publish_segment_connectivity_failure"]
    assert notice.segment == SEGMENT
    assert "No same-site MCE segments" in notice.message


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
    assert calls["list_mce_segments"] == []
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
