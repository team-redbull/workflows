"""Workflow tests: real AllocateSegmentWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_workflow.py: two workers — one per queue —
exactly like the real brain/limb split, with the time-skipping environment
collapsing the DHCP convergence timers and retry backoffs to milliseconds.
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
from shared.exceptions import ClusterFileNotFoundError
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentRunArgs,
    ClusterFileLocation,
    ClusterValuesAppendRequest,
    DhcpExclusion,
    DhcpScopeState,
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

HC_INPUT = AllocateSegmentInput(cluster=CLUSTER)  # type defaults to HC

DHCP_VALUES = DhcpValues(
    network="10.20.90.0",
    exclusions=[
        DhcpExclusion(start_address="10.20.90.1", end_address="10.20.90.10"),
        DhcpExclusion(start_address="10.20.90.241", end_address="10.20.90.254"),
    ],
)
# Convergence is checked against the exclusions the run pushed — the scope's
# distribution range is the DHCP API's own derivation, not ours.
CONVERGED_SCOPE = DhcpScopeState(found=True, exclusions=DHCP_VALUES.exclusions)


def make_mock_activities(
    *,
    valid_sites: tuple[str, ...] = (SITE, "site2"),
    location: ClusterFileLocation | None = None,
    locate_error: Exception | None = None,
    allocate_fail_times: int = 0,
    entry_overrides: dict | None = None,
    append_changed: bool = True,
    scope_script: list[DhcpScopeState] | None = None,
    scope_never_converges: bool = False,
):
    """Build the full mock activity set + a call recorder.

    scope_script: per-poll returns; once exhausted (and not
    scope_never_converges) every subsequent poll reports the converged scope.
    entry_overrides: fields of the read-back SegmentEntry to distort — the
    default read-back matches the allocation exactly.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "get_valid_sites",
            "locate_cluster_file",
            "allocate_segment",
            "get_segment",
            "append_allocation_to_cluster_values",
            "get_dhcp_scope",
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
        "cluster_name": CLUSTER,
        **(entry_overrides or {}),
    }
    script = list(scope_script or [])
    allocate_failures_left = [allocate_fail_times]

    @activity.defn
    async def get_valid_sites() -> list[str]:
        calls["get_valid_sites"].append(None)
        return list(valid_sites)

    @activity.defn
    async def locate_cluster_file(cluster: str) -> ClusterFileLocation:
        calls["locate_cluster_file"].append(cluster)
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

    @activity.defn
    async def get_dhcp_scope(network: str) -> DhcpScopeState:
        calls["get_dhcp_scope"].append(network)
        if script:
            return script.pop(0)
        if scope_never_converges:
            return DhcpScopeState(found=False)
        return CONVERGED_SCOPE

    return calls, [
        get_valid_sites,
        locate_cluster_file,
        allocate_segment,
        get_segment,
        append_allocation_to_cluster_values,
        get_dhcp_scope,
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


async def test_happy_path_allocates_verifies_pushes_and_awaits_dhcp():
    # First poll finds no scope yet (Crossplane still reconciling), second
    # converges — the normal shape of a fresh allocation.
    calls, mocks = make_mock_activities(scope_script=[DhcpScopeState(found=False)])
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    assert result.cluster == CLUSTER
    assert result.site == SITE
    assert result.type == SegmentType.HC
    assert result.vlan_id == VLAN_ID
    assert result.segment == SEGMENT
    assert result.commit_sha == "a" * 40
    assert result.values_updated is True
    assert result.dhcp_scope_ready is True
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
    # Both polls asked for the mask-stripped network address.
    assert calls["get_dhcp_scope"] == ["10.20.90.0", "10.20.90.0"]


async def test_non_hc_type_is_rejected_before_any_activity():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(
                client,
                AllocateSegmentRunArgs(
                    input=AllocateSegmentInput(cluster=CLUSTER, type=SegmentType.MCE)
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
    assert calls["get_dhcp_scope"] == []


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
    # The no-op path still returns the DhcpValues, so convergence is polled.
    assert calls["get_dhcp_scope"] == ["10.20.90.0"]


async def test_dhcp_scope_never_appearing_fails_bounded():
    calls, mocks = make_mock_activities(scope_never_converges=True)
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    assert cause.type == "DhcpScopeNotConverged"
    assert "never created" in str(cause)
    # 15 min deadline / 15 s durable timer: a poll at t=0 plus one per sleep,
    # the last at the deadline itself. Pins the pacing constants.
    assert len(calls["get_dhcp_scope"]) == 61


async def test_scope_with_wrong_exclusions_does_not_count_as_converged():
    # A scope existing is NOT enough — its exclusions must match the block we
    # wrote (Crossplane may still be carrying an older revision), else the
    # deadline calls it out.
    wrong = DhcpScopeState(
        found=True,
        exclusions=[
            DhcpExclusion(start_address="10.20.90.50", end_address="10.20.90.60")
        ],
    )
    calls, mocks = make_mock_activities(
        scope_script=[wrong], scope_never_converges=False
    )
    async with _Harness(mocks) as client:
        result = await _execute(client, AllocateSegmentRunArgs(input=HC_INPUT))

    # Second poll (the converged default) finished the run.
    assert len(calls["get_dhcp_scope"]) == 2
    assert result.dhcp_scope_ready is True
