"""Workflow tests: real AllocateSegmentWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_workflow.py: two workers — one per queue —
exactly like the real brain/limb split, with the time-skipping environment
collapsing the retry backoffs to milliseconds.
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
from shared.exceptions import ClusterFileNotFoundError, ValuesBranchNotFoundError
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentResult,
    AllocateSegmentRunArgs,
    ClusterFileLocation,
    ClusterFileLookupRequest,
    ClusterValuesAppendRequest,
    DhcpExclusion,
    DhcpValues,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
    SegmentType,
    ValuesCommitRef,
)
from workflow_domains.segment_lifecycle.allocate_segment import (
    AllocateSegmentWorkflow,
)

CLUSTER = "ocp4-e2e-alloc-site1-a"
SITE = "site1"
RELATIVE_PATH = f"sites/{SITE}/mces/mce-a/hostedClusters/{CLUSTER}.yaml"
SEGMENT = "10.20.90.0/24"
VLAN_ID = 23
# The day1 pipeline's branch — where the run records the allocation.
VALUES_BRANCH = f"feature/{CLUSTER}"

# type defaults to HC
HC_INPUT = AllocateSegmentInput(cluster=CLUSTER, values_branch=VALUES_BRANCH)

DHCP_VALUES = DhcpValues(
    network="10.20.90.0",
    exclusions=[
        DhcpExclusion(start_address="10.20.90.1", end_address="10.20.90.10"),
        DhcpExclusion(start_address="10.20.90.241", end_address="10.20.90.254"),
    ],
)


def make_mock_activities(
    *,
    valid_sites: tuple[str, ...] = (SITE, "site2"),
    location: ClusterFileLocation | None = None,
    locate_error: Exception | None = None,
    allocate_fail_times: int = 0,
    entry_overrides: dict | None = None,
    entry_omits_type: bool = False,
    append_changed: bool = True,
):
    """Build the full mock activity set + a call recorder.

    entry_overrides: fields of the read-back SegmentEntry to distort — the
    default read-back matches the allocation exactly.
    entry_omits_type: return the read-back WITHOUT a `type` key at all, the
    way a limb built before SegmentEntry.type existed (or history recorded by
    the previous brain) serializes it.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_valid_sites",
            "locate_cluster_file",
            "allocate_segment",
            "get_segment",
            "append_allocation_to_cluster_values",
        )
    }
    resolved_location = location or ClusterFileLocation(
        site=SITE, relative_path=RELATIVE_PATH
    )
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
    async def locate_cluster_file(request: ClusterFileLookupRequest) -> ClusterFileLocation:
        calls["locate_cluster_file"].append(request)
        if locate_error is not None:
            raise locate_error
        return resolved_location

    @activity.defn
    async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
        calls["allocate_segment"].append(request)
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

    @activity.defn
    async def append_allocation_to_cluster_values(
        request: ClusterValuesAppendRequest,
    ) -> ValuesCommitRef:
        calls["append_allocation_to_cluster_values"].append(request)
        if append_changed:
            return ValuesCommitRef(
                commit_sha="a" * 40, changed=True, dhcp_values=DHCP_VALUES
            )
        return ValuesCommitRef(commit_sha=None, changed=False, dhcp_values=DHCP_VALUES)

    return calls, [
        get_valid_sites,
        locate_cluster_file,
        allocate_segment,
        get_segment,
        append_allocation_to_cluster_values,
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


async def test_happy_path_allocates_verifies_and_pushes_to_the_branch():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.cluster == CLUSTER
    assert result.site == SITE
    assert result.type == SegmentType.HC
    assert result.vlan_id == VLAN_ID
    assert result.segment == SEGMENT
    assert result.values_branch == VALUES_BRANCH
    assert result.commit_sha == "a" * 40
    assert result.values_updated is True
    # The run ends at the push: the scope appears only after a human merges
    # the branch (Argo reads main only), so the result makes no claim on it.
    # The day1 pipeline parses this shape — pin it whole.
    assert set(AllocateSegmentResult.model_fields) == {
        "cluster", "site", "type", "vlan_id", "segment", "epg_name",
        "values_branch", "commit_sha", "values_updated",
    }
    # The cluster file was looked up on the run's branch — the only place a
    # new cluster's file exists.
    assert calls["locate_cluster_file"] == [
        ClusterFileLookupRequest(cluster=CLUSTER, values_branch=VALUES_BRANCH)
    ]
    # The allocation request carried the DERIVED site, never a caller's claim.
    assert calls["allocate_segment"] == [
        SegmentAllocationRequest(cluster=CLUSTER, site=SITE, type=SegmentType.HC)
    ]
    # Verification read back the allocated CIDR before any git write.
    assert calls["get_segment"] == [SEGMENT]
    (append_request,) = calls["append_allocation_to_cluster_values"]
    assert append_request.relative_path == RELATIVE_PATH
    assert append_request.vlan_id == VLAN_ID
    # The type travels with the append: the exclusion policy is per type, and
    # the activity selects that type's ranges out of the ConfigMap.
    assert append_request.type == SegmentType.HC
    # ...and the block is pushed to the same branch it was located on.
    assert append_request.values_branch == VALUES_BRANCH


async def test_missing_values_branch_is_rejected_before_any_activity():
    """The API requires the branch; the boundary model defaults it to None only
    for history. A run that still arrives without one fails at once — there is
    no configured fallback, which would push to main."""
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(
                client, AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=CLUSTER))
            )

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "ValuesBranchMissing"
    assert all(recorded == [] for recorded in calls.values())


async def test_nonexistent_branch_fails_after_exactly_one_attempt():
    # Attempt count is the proof of the non-retryable classification: a typo'd
    # or deleted branch must fail the run, not retry every minute forever.
    calls, mocks = make_mock_activities(
        locate_error=ValuesBranchNotFoundError(f"Branch {VALUES_BRANCH!r} does not exist")
    )
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "ValuesBranchNotFoundError"
    assert len(calls["locate_cluster_file"]) == 1
    assert calls["allocate_segment"] == []


async def test_non_hc_type_is_rejected_before_any_activity():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(
                client,
                AllocateSegmentRunArgs(
                    input=AllocateSegmentInput(
                        cluster=CLUSTER, type=SegmentType.MCE, values_branch=VALUES_BRANCH
                    )
                ),
            )

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnsupportedSegmentType"
    assert all(recorded == [] for recorded in calls.values())


async def test_unknown_site_fails_with_no_allocation_attempted():
    calls, mocks = make_mock_activities(
        location=ClusterFileLocation(
            site="site-x", relative_path=f"sites/site-x/mces/m/hostedClusters/{CLUSTER}.yaml"
        )
    )
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "UnknownSite"
    assert calls["allocate_segment"] == []
    assert calls["append_allocation_to_cluster_values"] == []


async def test_missing_cluster_file_fails_after_exactly_one_attempt():
    # Attempt count is the proof of the non-retryable classification: an
    # unclassified error here would retry forever and time the test out.
    calls, mocks = make_mock_activities(
        locate_error=ClusterFileNotFoundError(f"No {CLUSTER}.yaml under sites/")
    )
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "ClusterFileNotFoundError"
    assert len(calls["locate_cluster_file"]) == 1
    assert calls["allocate_segment"] == []


async def test_read_back_mismatch_fails_with_no_git_commit():
    calls, mocks = make_mock_activities(
        entry_overrides={"cluster_name": "some-other-cluster"}
    )
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "AllocationNotConfirmed"
    assert "cluster_name" in str(cause)
    # Verification sits BEFORE the git write: nothing was pushed.
    assert calls["append_allocation_to_cluster_values"] == []


@pytest.mark.parametrize("read_back_type", ["MCE", None])
async def test_read_back_type_mismatch_fails_with_no_git_commit(read_back_type):
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
    assert calls["append_allocation_to_cluster_values"] == []


async def test_read_back_without_a_type_field_is_not_held_against_the_run():
    """A read-back with no `type` key predates the field — history replayed
    from the previous brain, or a limb one rollout behind (build.yml ships the
    brain first). It says nothing about the allocation, so it must not fail the
    run; the rest of the read-back is still verified."""
    calls, mocks = make_mock_activities(entry_omits_type=True)
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.values_updated is True
    assert len(calls["append_allocation_to_cluster_values"]) == 1


async def test_transient_activity_failures_are_outwaited():
    calls, mocks = make_mock_activities(allocate_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.segment == SEGMENT
    assert len(calls["allocate_segment"]) == 3  # 2 transient failures + success


async def test_rerun_over_already_appended_file_reports_values_unchanged():
    calls, mocks = make_mock_activities(append_changed=False)
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.values_updated is False
    assert result.commit_sha is None
    # A no-op re-run still reports the branch that carries the block.
    assert result.values_branch == VALUES_BRANCH

