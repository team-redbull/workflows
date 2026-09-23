"""Workflow tests: real ConvertSegmentWorkflow, mock activities, time-skipping env.

Same harness shape as test_allocate_segment_workflow.py — two workers, one per
queue, exactly like the real brain/limb split.

Re-typing is the whole operation now, so these tests pin that the run is
exactly a search plus N conversions: no child workflows, no timers, nothing
started on any other queue.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    CONVERT_SEGMENT_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
)
from shared.exceptions import SegmentConversionConflictError
from shared.models.segment_lifecycle import (
    ConvertibleSegment,
    ConvertibleSegmentsQuery,
    ConvertSegmentInput,
    ConvertSegmentRunArgs,
    SegmentType,
    SegmentTypeUpdate,
)
from workflow_domains.segment_lifecycle.convert_segment import (
    ConvertSegmentWorkflow,
    _selection_order,
)

SITE = "site1"


CONVERT_INPUT = ConvertSegmentInput(
    site=SITE,
    source_type=SegmentType.HC,
    destination_type=SegmentType.MCE,
    quantity=3,
)


def _hit(segment: str, vlan_id: int) -> ConvertibleSegment:
    """A search hit: Available and unassigned, the only thing
    list_convertible_segments returns (it asserts both)."""
    return ConvertibleSegment(
        segment=segment,
        vlan_id=vlan_id,
        status="Available",
        cluster_name=None,
    )


def make_mock_activities(
    *,
    valid_sites: tuple[str, ...] = (SITE, "site2"),
    hits: list[ConvertibleSegment] | None = None,
    convert_error_on: str | None = None,
):
    """Build the mock activity set + a call recorder.

    convert_error_on: segment CIDR whose conversion raises the non-retryable
    SegmentConversionConflictError (the Segments Manager's 409).
    """
    calls: dict[str, list] = {
        name: []
        for name in ("get_valid_sites", "list_convertible_segments", "convert_segment_type")
    }
    resolved_hits = list(hits or [])

    @activity.defn
    async def get_valid_sites() -> list[str]:
        calls["get_valid_sites"].append(None)
        return list(valid_sites)

    @activity.defn
    async def list_convertible_segments(
        query: ConvertibleSegmentsQuery,
    ) -> list[ConvertibleSegment]:
        calls["list_convertible_segments"].append(query)
        return resolved_hits

    @activity.defn
    async def convert_segment_type(update: SegmentTypeUpdate) -> None:
        calls["convert_segment_type"].append(update)
        if update.segment == convert_error_on:
            raise SegmentConversionConflictError(
                f"Segments Manager refused to convert {update.segment}"
            )

    return calls, [get_valid_sites, list_convertible_segments, convert_segment_type]


class _Harness:
    """One time-skipping env + the two production-shaped workers."""

    def __init__(self, mock_activities):
        self._mock_activities = mock_activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        # Sandboxed, exactly as the brain runs it: nothing here patches the
        # workflow module's constants any more, so there is no reason to give
        # up that fidelity.
        self._workflow_worker = Worker(
            client,
            task_queue=CONVERT_SEGMENT_WORKFLOW_QUEUE,
            workflows=[ConvertSegmentWorkflow],
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
    """Unwrap WorkflowFailureError -> (ActivityError ->) the root failure."""
    cause = exc_info.value.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    return cause


async def _start(client: Client, args: ConvertSegmentRunArgs):
    return await client.start_workflow(
        ConvertSegmentWorkflow.run,
        args,
        id=f"test-{uuid.uuid4()}",
        task_queue=CONVERT_SEGMENT_WORKFLOW_QUEUE,
    )


async def _execute(client: Client, args: ConvertSegmentRunArgs):
    handle = await _start(client, args)
    return await asyncio.wait_for(
        handle.result(),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


def test_selection_prefers_lowest_vlan():
    hits = [
        _hit("10.0.10.0/24", 10),
        _hit("10.0.40.0/24", 40),
        _hit("10.0.20.0/24", 20),
        _hit("10.0.30.0/24", 30),
    ]
    ordered = sorted(hits, key=_selection_order)
    assert [s.vlan_id for s in ordered] == [10, 20, 30, 40]


def test_input_rejects_identical_source_and_destination():
    with pytest.raises(ValueError):
        ConvertSegmentInput(
            site=SITE,
            source_type=SegmentType.HC,
            destination_type=SegmentType.HC,
            quantity=1,
        )


async def test_happy_path_converts_lowest_vlans_and_starts_no_children():
    hits = [
        _hit("10.0.10.0/24", 10),
        _hit("10.0.40.0/24", 40),
        _hit("10.0.20.0/24", 20),
        _hit("10.0.30.0/24", 30),
    ]
    calls, mocks = make_mock_activities(hits=hits)
    async with _Harness(mocks) as client:
        handle = await _start(client, ConvertSegmentRunArgs(input=CONVERT_INPUT))
        result = await asyncio.wait_for(handle.result(), timeout=60)
        events = [event async for event in handle.fetch_history_events()]

    assert result.matched == 4
    assert result.shortfall == 0
    # Lowest vlan first, cut at quantity=3.
    assert [r.vlan_id for r in result.converted] == [10, 20, 30]
    assert [r.segment for r in result.converted] == [
        "10.0.10.0/24",
        "10.0.20.0/24",
        "10.0.30.0/24",
    ]
    # Re-typing is the WHOLE operation: nothing is spawned on any other queue.
    assert not any(
        event.HasField("start_child_workflow_execution_initiated_event_attributes")
        for event in events
    )
    # The search asked for the SOURCE type; every conversion named the source
    # as the compare-and-set guard and the destination as the new type.
    assert calls["list_convertible_segments"] == [
        ConvertibleSegmentsQuery(site=SITE, type=SegmentType.HC)
    ]
    assert [
        (u.segment, u.type, u.expected_type) for u in calls["convert_segment_type"]
    ] == [
        ("10.0.10.0/24", SegmentType.MCE, SegmentType.HC),
        ("10.0.20.0/24", SegmentType.MCE, SegmentType.HC),
        ("10.0.30.0/24", SegmentType.MCE, SegmentType.HC),
    ]


async def test_shortfall_converts_everything_that_matched():
    hits = [_hit("10.0.30.0/24", 30), _hit("10.0.10.0/24", 10)]
    calls, mocks = make_mock_activities(hits=hits)
    async with _Harness(mocks) as client:
        result = await _execute(
            client,
            ConvertSegmentRunArgs(input=CONVERT_INPUT.model_copy(update={"quantity": 5})),
        )

    assert result.requested_quantity == 5
    assert result.matched == 2
    assert result.shortfall == 3
    assert len(result.converted) == 2
    assert len(calls["convert_segment_type"]) == 2


async def test_zero_matches_completes_with_full_shortfall():
    calls, mocks = make_mock_activities(hits=[])
    async with _Harness(mocks) as client:
        result = await _execute(client, ConvertSegmentRunArgs(input=CONVERT_INPUT))

    assert result.matched == 0
    assert result.shortfall == CONVERT_INPUT.quantity
    assert result.converted == []
    assert calls["convert_segment_type"] == []


async def test_unknown_site_fails_before_searching():
    calls, mocks = make_mock_activities(valid_sites=("site2",))
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, ConvertSegmentRunArgs(input=CONVERT_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnknownSite"
    assert calls["list_convertible_segments"] == []
    assert calls["convert_segment_type"] == []


async def test_run_has_no_timers_at_all():
    """A conversion loop is pure machine work: it records not one timer.

    No approval to wait on, no cancel handshake, no grace pause — an earlier
    shape paid a 30 s durable pause per segment, and this pins that none of it
    can come back unnoticed."""
    hits = [_hit("10.0.10.0/24", 10), _hit("10.0.20.0/24", 20), _hit("10.0.30.0/24", 30)]
    _, mocks = make_mock_activities(hits=hits)
    async with _Harness(mocks) as client:
        handle = await _start(client, ConvertSegmentRunArgs(input=CONVERT_INPUT))
        await asyncio.wait_for(handle.result(), timeout=60)
        events = [event async for event in handle.fetch_history_events()]

    assert not any(event.HasField("timer_started_event_attributes") for event in events)


async def test_conversion_conflict_fails_loudly_but_earlier_conversions_stand():
    hits = [_hit("10.0.30.0/24", 30), _hit("10.0.40.0/24", 40)]
    calls, mocks = make_mock_activities(hits=hits, convert_error_on="10.0.40.0/24")
    async with _Harness(mocks) as client:
        handle = await _start(client, ConvertSegmentRunArgs(input=CONVERT_INPUT))
        with pytest.raises(WorkflowFailureError) as exc_info:
            await asyncio.wait_for(handle.result(), timeout=60)

        cause = _workflow_cause(exc_info)
        assert isinstance(cause, ApplicationError)
        assert cause.type == "SegmentConversionConflictError"
        # Non-retryable: the conflicting segment was attempted exactly once
        # (after the first segment's successful conversion).
        assert len(calls["convert_segment_type"]) == 2

        # The failed run still shows what WAS converted, via the progress
        # query — those conversions stand.
        progress = await handle.query(ConvertSegmentWorkflow.progress)
        assert [r.vlan_id for r in progress.converted] == [30]
        assert [r.segment for r in progress.converted] == ["10.0.30.0/24"]
