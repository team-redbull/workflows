"""Workflow tests: real AllocateSegmentWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_workflow.py: two workers — one per queue —
exactly like the real brain/limb split, with the time-skipping environment
collapsing the retry backoffs to milliseconds. Since 2026-11-11 the run
allocates and verifies only: the site is its input (provision-cluster passes
the day1 folder's), and nothing touches git.
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

from shared.consts import ALLOCATE_SEGMENT_WORKFLOW_QUEUE, SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
from shared.exceptions import SegmentPoolExhaustedError, SegmentsManagerAuthError, SegmentValidationError
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentResult,
    AllocateSegmentRunArgs,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
    SegmentType,
)
from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow

CLUSTER = "ocp4-e2e-alloc-site1-a"
SITE = "site1"
SEGMENT = "10.20.90.0/24"
VLAN_ID = 23

# type defaults to HC
HC_INPUT = AllocateSegmentInput(cluster=CLUSTER, site=SITE)


def make_mock_activities(
    *,
    valid_sites: tuple[str, ...] = (SITE, "site2"),
    allocate_fail_times: int = 0,
    allocate_error: Exception | None = None,
    entry_overrides: dict | None = None,
    entry_omits_type: bool = False,
):
    """Build the full mock activity set + a call recorder.

    entry_overrides: fields of the read-back SegmentEntry to distort — the
    default read-back matches the allocation exactly.
    entry_omits_type: return the read-back WITHOUT a `type` key at all, the
    way a limb built before SegmentEntry.type existed (or history recorded by
    the previous brain) serializes it.
    """
    calls: dict[str, list] = {name: [] for name in ("get_valid_sites", "allocate_segment", "get_segment")}
    entry_fields = {
        "segment": SEGMENT,
        "site": SITE,
        "vlan_id": VLAN_ID,
        "status": "Allocated",
        "type": SegmentType.HC.value,
        "cluster_name": CLUSTER,
        **(entry_overrides or {}),
    }
    allocate_failures_left = [allocate_fail_times]

    @activity.defn
    async def get_valid_sites() -> list[str]:
        calls["get_valid_sites"].append(None)
        return list(valid_sites)

    @activity.defn
    async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
        calls["allocate_segment"].append(request)
        if allocate_error is not None:
            raise allocate_error
        if allocate_failures_left[0] > 0:
            allocate_failures_left[0] -= 1
            raise RuntimeError("simulated transient Segments Manager outage")
        return SegmentAllocation(vlan_id=VLAN_ID, segment=SEGMENT, epg_name="EPG_HC_23")

    @activity.defn
    async def get_segment(segment: str) -> SegmentEntry:
        calls["get_segment"].append(segment)
        if entry_omits_type:
            # A plain dict serializes exactly as it is — no `type` key.
            return {k: v for k, v in entry_fields.items() if k != "type"}
        return SegmentEntry(**entry_fields)

    return calls, [get_valid_sites, allocate_segment, get_segment]


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
            task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
            workflows=[AllocateSegmentWorkflow],
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


async def _execute(client: Client, args: AllocateSegmentRunArgs):
    return await asyncio.wait_for(
        client.execute_workflow(
            AllocateSegmentWorkflow.run,
            args,
            id=f"test-{uuid.uuid4()}",
            task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
        ),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


async def test_happy_path_validates_the_site_allocates_and_verifies():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result == AllocateSegmentResult(
        cluster=CLUSTER, site=SITE, type=SegmentType.HC, vlan_id=VLAN_ID, segment=SEGMENT, epg_name="EPG_HC_23"
    )
    # Nothing about git in the result any more: provision-cluster records the
    # allocation in the cluster file it publishes. Pin the shape whole — the
    # parent reads it.
    assert set(AllocateSegmentResult.model_fields) == {"cluster", "site", "type", "vlan_id", "segment", "epg_name"}
    assert calls["get_valid_sites"] == [None]
    # The allocation request carried the caller's site, verbatim.
    assert calls["allocate_segment"] == [SegmentAllocationRequest(cluster=CLUSTER, site=SITE, type=SegmentType.HC)]
    # Verification read back the allocated CIDR.
    assert calls["get_segment"] == [SEGMENT]


async def test_missing_site_is_rejected_before_any_activity():
    """The API requires the site; the boundary model defaults it to None only
    for history. A run that still arrives without one fails at once — there is
    nothing to derive a site from any more."""
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=CLUSTER)))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "SiteMissingError"
    assert all(recorded == [] for recorded in calls.values())


async def test_a_retired_values_branch_on_an_old_input_is_ignored():
    """History recorded before 2026-11-11 carries `values_branch`; it decodes
    and the run neither reads it nor fails on it."""
    calls, mocks = make_mock_activities()
    old_input = AllocateSegmentInput.model_validate(
        {"cluster": CLUSTER, "site": SITE, "type": "HC", "values_branch": "feature/old"}
    )
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=old_input))
    assert result.vlan_id == VLAN_ID
    assert len(calls["allocate_segment"]) == 1


async def test_non_hc_type_is_rejected_before_any_activity():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(
                client,
                AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=CLUSTER, site=SITE, type=SegmentType.MCE)),
            )

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnsupportedSegmentType"
    assert all(recorded == [] for recorded in calls.values())


async def test_unknown_site_fails_with_no_allocation_attempted():
    calls, mocks = make_mock_activities(valid_sites=("site2", "site3"))
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnknownSite"
    assert SITE in str(cause) and "site2" in str(cause)
    assert calls["allocate_segment"] == []
    assert calls["get_segment"] == []


@pytest.mark.parametrize(
    "error",
    [
        SegmentPoolExhaustedError("no Available segment at site1"),
        SegmentValidationError("bad cluster name"),
        SegmentsManagerAuthError("401"),
    ],
    ids=lambda e: type(e).__name__,
)
async def test_each_permanent_allocation_error_fails_after_exactly_one_attempt(error):
    # Attempt count is the proof of the non-retryable classification: an
    # unclassified error here would retry forever and time the test out.
    calls, mocks = make_mock_activities(allocate_error=error)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == type(error).__name__
    assert len(calls["allocate_segment"]) == 1
    assert calls["get_segment"] == []


async def test_read_back_mismatch_fails_the_run():
    calls, mocks = make_mock_activities(entry_overrides={"cluster_name": "some-other-cluster"})
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "AllocationNotConfirmed"
    assert "cluster_name" in str(cause)
    assert calls["get_segment"] == [SEGMENT]


@pytest.mark.parametrize("read_back_type", ["MCE", None])
async def test_read_back_type_mismatch_fails_the_run(read_back_type):
    """The allocation stamps the type onto the segment, so the read-back must
    show the requested one. A null is a failure too: an Allocated segment
    always has a type."""
    calls, mocks = make_mock_activities(entry_overrides={"type": read_back_type})
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "AllocationNotConfirmed"
    assert "type: expected 'HC'" in str(cause)


async def test_read_back_without_a_type_field_is_not_held_against_the_run():
    """A read-back with no `type` key predates the field — history replayed
    from the previous brain, or a limb one rollout behind (build.yml ships the
    brain first). It says nothing about the allocation, so it must not fail the
    run; the rest of the read-back is still verified."""
    calls, mocks = make_mock_activities(entry_omits_type=True)
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.vlan_id == VLAN_ID
    assert len(calls["get_segment"]) == 1


async def test_transient_activity_failures_are_outwaited():
    calls, mocks = make_mock_activities(allocate_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert len(calls["allocate_segment"]) == 3  # 2 transient failures + success


async def test_progress_reports_the_phases_in_order():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        handle = await client.start_workflow(
            AllocateSegmentWorkflow.run, AllocateSegmentRunArgs(input=HC_INPUT),
            id=f"test-{uuid.uuid4()}", task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
        )
        await asyncio.wait_for(handle.result(), timeout=60)
        progress = await handle.query(AllocateSegmentWorkflow.progress)
    assert progress.phase == "completed"
