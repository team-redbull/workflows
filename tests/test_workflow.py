"""Workflow tests: real InitializeSegmentWorkflow, mock activities, time-skipping env.

The workflow is one activity now, so what is worth pinning is not a sequence
but the CLASSIFICATION around it: which failures are terminal, which are merely
waited out, and that nothing schedules a timer any more.

The time-skipping environment makes the retry backoffs run in milliseconds. The
workflow routes its activity to SEGMENT_LIFECYCLE_ACTIVITY_QUEUE explicitly, so
each test runs TWO workers — one per queue — exactly like the real brain/limb
split.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, INITIALIZE_SEGMENT_WORKFLOW_QUEUE
from shared.exceptions import (
    SegmentConflictError,
    SegmentsManagerAuthError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    InitializeSegmentInput,
    InitializeSegmentRunArgs,
    SegmentType,
)
from workflow_domains.segment_lifecycle.initialize_segment import (
    InitializeSegmentWorkflow,
)

SEGMENT = "10.0.0.0/24"
SITE = "site-a"


def _input(segment_type: SegmentType) -> InitializeSegmentInput:
    """A complete segment definition — the workflow CREATES the segment, so its
    input carries every field the Segments Manager needs, not just a reference
    to an existing one."""
    return InitializeSegmentInput(
        segment=SEGMENT,
        type=segment_type,
        site=SITE,
        vlan_id=100,
        epg_name="EPG_TEST_01",
    )


HC_INPUT = _input(SegmentType.HC)


def make_mock_activities(
    *,
    create_fail_times: int = 0,
    create_error: Exception | None = None,
):
    """Build the mock activity set + a call recorder.

    create_fail_times: how many attempts raise a TRANSIENT error before
    succeeding. create_error: a classified error raised on every attempt.
    """
    calls: dict[str, list] = {"create_segment": []}
    create_failures_left = [create_fail_times]

    @activity.defn
    async def create_segment(segment_input: InitializeSegmentInput) -> None:
        calls["create_segment"].append(segment_input)
        if create_error is not None:
            raise create_error
        if create_failures_left[0] > 0:
            create_failures_left[0] -= 1
            raise RuntimeError("simulated transient Segments Manager outage")

    return calls, [create_segment]


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


async def _start(client: Client, args: InitializeSegmentRunArgs):
    return await client.start_workflow(
        InitializeSegmentWorkflow.run,
        args,
        id=f"test-{uuid.uuid4()}",
        task_queue=INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
    )


async def _execute(client: Client, args: InitializeSegmentRunArgs):
    handle = await _start(client, args)
    return await asyncio.wait_for(
        handle.result(),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


async def test_happy_path_creates_the_segment_and_completes():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert result.type == SegmentType.HC
    # The FULL definition reaches the activity — the Segments Manager is the
    # validator of record, so nothing may be dropped on the way there.
    assert calls["create_segment"] == [HC_INPUT]


@pytest.mark.parametrize("segment_type", list(SegmentType))
async def test_every_segment_type_is_accepted(segment_type):
    """No type gate: PXE included.

    The gate existed only because no firewall rules were defined for PXE, and
    a PXE run would have died half-way through opening them. With no rules to
    open, every type the Segments Manager knows is simply created.
    """
    segment_input = _input(segment_type)
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=segment_input))

    assert result.type == segment_type
    assert calls["create_segment"] == [segment_input]


@pytest.mark.parametrize(
    "error",
    [
        SegmentValidationError("the Segments Manager rejected the definition"),
        SegmentConflictError("that CIDR is stored with different attributes"),
        SegmentsManagerAuthError("bad SEGMENTS_MANAGER_API_TOKEN"),
    ],
    ids=["validation", "conflict", "auth"],
)
async def test_classified_failures_fail_the_run_without_retrying(error):
    """The three errors create_segment raises deterministically.

    Each must be in the workflow's non_retryable_error_types: an unclassified
    permanent error would retry every minute FOREVER, leaving the run RUNNING
    rather than FAILED — the failure mode that hides a bad definition from an
    operator watching the Temporal UI.
    """
    calls, mocks = make_mock_activities(create_error=error)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert _workflow_cause(exc_info).type == type(error).__name__
    # Attempted exactly once — no retry.
    assert len(calls["create_segment"]) == 1


async def test_a_transient_failure_is_out_waited():
    """Retries are UNBOUNDED: an unclassified error is treated as an outage to
    be out-waited, not a reason to fail the operator's request."""
    calls, mocks = make_mock_activities(create_fail_times=3)
    async with _Harness(mocks) as client:
        result = await _execute(client, InitializeSegmentRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert len(calls["create_segment"]) == 4  # 3 failures + the success


async def test_run_has_no_timers_at_all():
    """Nothing waits any more.

    The endless human-approval poll — durable timers plus continue_as_new — is
    gone with the firewall flow, so a run records not one timer. Pinned so it
    cannot creep back in unnoticed.
    """
    _, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        handle = await _start(client, InitializeSegmentRunArgs(input=HC_INPUT))
        await asyncio.wait_for(handle.result(), timeout=60)
        events = [event async for event in handle.fetch_history_events()]

    assert not any(event.HasField("timer_started_event_attributes") for event in events)
    assert not any(
        event.HasField("workflow_execution_continued_as_new_event_attributes")
        for event in events
    )


async def test_progress_query_reports_the_terminal_phase():
    _, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        handle = await _start(client, InitializeSegmentRunArgs(input=HC_INPUT))
        await asyncio.wait_for(handle.result(), timeout=60)
        progress = await handle.query(InitializeSegmentWorkflow.progress)

    assert progress.phase == "completed"


async def test_progress_query_reports_the_creating_phase_while_blocked():
    """`creating-segment` is observable, not just a value set on the way past:
    it is what the status endpoint shows while a Segments Manager outage is
    being out-waited."""
    calls, mocks = make_mock_activities(create_error=RuntimeError("outage"))
    async with _Harness(mocks) as client:
        handle = await _start(client, InitializeSegmentRunArgs(input=HC_INPUT))
        while not calls["create_segment"]:
            await asyncio.sleep(0.05)
        progress = await handle.query(InitializeSegmentWorkflow.progress)
        await handle.cancel()

    assert progress.phase == "creating-segment"
