"""Workflow tests: real ReleaseSegmentWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_allocate_segment_workflow.py: two workers —
one per queue — exactly like the real brain/limb split, with the
time-skipping environment collapsing the retry backoffs to milliseconds.
"""

from __future__ import annotations

import ast
import asyncio
import pathlib
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared import exceptions
from shared.consts import RELEASE_SEGMENT_WORKFLOW_QUEUE, SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
from shared.exceptions import (
    AmbiguousAllocationError,
    DhcpApiAuthError,
    DhcpApiError,
    DhcpScopeInvalidError,
    SegmentNotFoundError,
    SegmentsManagerAuthError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ClusterAllocationLookup,
    ClusterAllocationLookupRequest,
    DhcpScopeState,
    ReleaseSegmentInput,
    ReleaseSegmentResult,
    ReleaseSegmentRunArgs,
    SegmentEntry,
    SegmentType,
)
from workflow_domains.segment_lifecycle.release_segment import (
    ReleaseSegmentWorkflow,
    _RETRY_POLICY,
)

CLUSTER = "ocp4-prep-gone-site1-a"
SITE = "site1"
SEGMENT = "10.20.90.0/24"
NETWORK = "10.20.90.0"  # the mask-stripped address the DHCP API is keyed by
VLAN_ID = 23

HC_INPUT = ReleaseSegmentInput(cluster=CLUSTER)  # type defaults to HC

ALLOCATED = {
    "segment": SEGMENT,
    "site": SITE,
    "vlan_id": VLAN_ID,
    "status": "Allocated",
    "type": SegmentType.HC.value,
    "cluster_name": CLUSTER,
}
RELEASED = {**ALLOCATED, "status": "Available", "type": None, "cluster_name": None}


def make_mock_activities(
    *,
    found: bool = True,
    lookup_error: Exception | None = None,
    scope_found: tuple[bool, ...] = (True, False),
    scope_get_error: Exception | None = None,
    scope_get_fail_times: int = 0,
    delete_error: Exception | None = None,
    pre_release_overrides: dict | None = None,
    get_segment_error: Exception | None = None,
    release_error: Exception | None = None,
    release_fail_times: int = 0,
    post_release_overrides: dict | None = None,
):
    """Build the full mock activity set + a call recorder.

    scope_found: what successive get_dhcp_scope calls report — the default is
    a surviving scope that the delete removes (found, then gone).
    get_segment returns the pre-release read first, the post-release read
    after that; the *_overrides distort either one.
    """
    calls: dict[str, list] = {
        name: []
        for name in (
            "find_cluster_allocation",
            "get_dhcp_scope",
            "delete_dhcp_scope",
            "get_segment",
            "release_segment",
        )
    }
    scope_answers = list(scope_found)
    scope_failures_left = [scope_get_fail_times]
    release_failures_left = [release_fail_times]
    pre = {**ALLOCATED, **(pre_release_overrides or {})}
    post = {**RELEASED, **(post_release_overrides or {})}

    @activity.defn
    async def find_cluster_allocation(
        request: ClusterAllocationLookupRequest,
    ) -> ClusterAllocationLookup:
        calls["find_cluster_allocation"].append(request)
        if lookup_error is not None:
            raise lookup_error
        if not found:
            return ClusterAllocationLookup(found=False)
        return ClusterAllocationLookup(found=True, entry=SegmentEntry(**ALLOCATED))

    @activity.defn
    async def get_dhcp_scope(network: str) -> DhcpScopeState:
        calls["get_dhcp_scope"].append(network)
        if scope_get_error is not None:
            raise scope_get_error
        if scope_failures_left[0] > 0:
            scope_failures_left[0] -= 1
            raise DhcpApiError("simulated transient DHCP API outage (503)")
        # The last answer repeats: a scope that will not go away stays.
        answer = scope_answers.pop(0) if len(scope_answers) > 1 else scope_answers[0]
        return DhcpScopeState(found=answer)

    @activity.defn
    async def delete_dhcp_scope(network: str) -> None:
        calls["delete_dhcp_scope"].append(network)
        if delete_error is not None:
            raise delete_error

    @activity.defn
    async def get_segment(segment: str) -> SegmentEntry:
        calls["get_segment"].append(segment)
        if get_segment_error is not None:
            raise get_segment_error
        read = pre if len(calls["get_segment"]) == 1 else post
        return SegmentEntry(**read)

    @activity.defn
    async def release_segment(segment: str) -> None:
        calls["release_segment"].append(segment)
        if release_error is not None:
            raise release_error
        if release_failures_left[0] > 0:
            release_failures_left[0] -= 1
            raise RuntimeError("simulated transient Segments Manager outage")

    return calls, [
        find_cluster_allocation,
        get_dhcp_scope,
        delete_dhcp_scope,
        get_segment,
        release_segment,
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
            task_queue=RELEASE_SEGMENT_WORKFLOW_QUEUE,
            workflows=[ReleaseSegmentWorkflow],
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


async def _execute(client: Client, release_input: ReleaseSegmentInput = HC_INPUT):
    return await asyncio.wait_for(
        client.execute_workflow(
            ReleaseSegmentWorkflow.run,
            ReleaseSegmentRunArgs(input=release_input),
            id=f"test-{uuid.uuid4()}",
            task_queue=RELEASE_SEGMENT_WORKFLOW_QUEUE,
        ),
        timeout=60,  # real seconds — a misclassified error would retry forever
    )


async def _expect_failure(mocks, release_input: ReleaseSegmentInput = HC_INPUT) -> str:
    async with _Harness(mocks) as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await _execute(client, release_input)
    cause = _workflow_cause(exc_info)
    assert isinstance(cause, ApplicationError)
    return cause.type


# --- happy paths ------------------------------------------------------------


async def test_surviving_scope_is_deleted_then_the_segment_released():
    calls, mocks = make_mock_activities()
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result == ReleaseSegmentResult(
        cluster=CLUSTER,
        type=SegmentType.HC,
        released=True,
        segment=SEGMENT,
        vlan_id=VLAN_ID,
        site=SITE,
        dhcp_scope_removed=True,
    )
    # The hook and the runs API consumers read this shape — pin it whole.
    assert set(ReleaseSegmentResult.model_fields) == {
        "cluster", "type", "released", "segment", "vlan_id", "site",
        "dhcp_scope_removed",
    }
    assert calls["find_cluster_allocation"] == [
        ClusterAllocationLookupRequest(cluster=CLUSTER, type=SegmentType.HC)
    ]
    # The DHCP API is keyed by the mask-stripped network; the delete is read
    # back before the segment goes anywhere.
    assert calls["get_dhcp_scope"] == [NETWORK, NETWORK]
    assert calls["delete_dhcp_scope"] == [NETWORK]
    # Owner re-read before the release, state read-back after it.
    assert calls["get_segment"] == [SEGMENT, SEGMENT]
    assert calls["release_segment"] == [SEGMENT]


async def test_scope_already_gone_releases_without_a_delete():
    """The normal case: the Argo CD cascade removed the scope before the run."""
    calls, mocks = make_mock_activities(scope_found=(False,))
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result.released is True
    assert result.dhcp_scope_removed is False
    assert calls["get_dhcp_scope"] == [NETWORK]
    assert calls["delete_dhcp_scope"] == []
    assert calls["release_segment"] == [SEGMENT]


async def test_no_allocation_completes_as_a_no_op():
    """Already released, or never allocated: a repeated trigger stays green."""
    calls, mocks = make_mock_activities(found=False)
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result == ReleaseSegmentResult(
        cluster=CLUSTER,
        type=SegmentType.HC,
        released=False,
        segment=None,
        vlan_id=None,
        site=None,
        dhcp_scope_removed=False,
    )
    assert len(calls["find_cluster_allocation"]) == 1
    for name in ("get_dhcp_scope", "delete_dhcp_scope", "get_segment", "release_segment"):
        assert calls[name] == [], name


async def test_segment_already_available_is_not_released_again():
    """Released by someone else between the lookup and the release: already
    done is success, and the read-back still confirms it."""
    calls, mocks = make_mock_activities(
        scope_found=(False,),
        pre_release_overrides={"status": "Available", "type": None, "cluster_name": None},
    )
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result.released is True
    assert calls["release_segment"] == []
    assert calls["get_segment"] == [SEGMENT, SEGMENT]


# --- up-front gates and read-back failures ----------------------------------


async def test_non_hc_type_is_rejected_before_any_activity():
    calls, mocks = make_mock_activities()
    failure = await _expect_failure(
        mocks, ReleaseSegmentInput(cluster=CLUSTER, type=SegmentType.MCE)
    )
    assert failure == "UnsupportedSegmentType"
    assert all(recorded == [] for recorded in calls.values())


async def test_a_scope_that_survives_the_delete_stops_the_release():
    """A scope re-created after our DELETE means a Request CR still exists —
    the segment must not go back to the pool with a live scope on it."""
    calls, mocks = make_mock_activities(scope_found=(True, True))
    assert await _expect_failure(mocks) == "DhcpScopeStillPresentError"
    assert calls["delete_dhcp_scope"] == [NETWORK]
    assert calls["get_segment"] == []
    assert calls["release_segment"] == []


async def test_segment_re_allocated_to_another_cluster_is_not_released():
    calls, mocks = make_mock_activities(
        scope_found=(False,),
        pre_release_overrides={"cluster_name": "ocp4-prep-other-site1-a"},
    )
    assert await _expect_failure(mocks) == "AllocationOwnerMismatchError"
    assert calls["release_segment"] == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "Allocated"},
        {"cluster_name": CLUSTER},
        {"type": SegmentType.HC.value},
    ],
    ids=["status", "cluster_name", "type"],
)
async def test_release_that_does_not_read_back_fails(overrides):
    calls, mocks = make_mock_activities(
        scope_found=(False,), post_release_overrides=overrides
    )
    assert await _expect_failure(mocks) == "ReleaseNotConfirmedError"
    assert calls["release_segment"] == [SEGMENT]


# --- classification: every non-retryable type fails after ONE attempt -------


def test_every_classified_error_is_covered_below():
    """The cases below must cover the whole non_retryable list — a type added
    there without a test here fails this guard."""
    covered = {error_type for error_type, _activity in _CLASSIFIED_CASES}
    assert covered == set(_RETRY_POLICY.non_retryable_error_types)


_CLASSIFIED_CASES = [
    ("AmbiguousAllocationError", "find_cluster_allocation"),
    ("SegmentsManagerAuthError", "release_segment"),
    ("SegmentValidationError", "release_segment"),
    ("SegmentNotFoundError", "get_segment"),
    ("DhcpApiAuthError", "delete_dhcp_scope"),
    ("DhcpScopeInvalidError", "get_dhcp_scope"),
]

_ERRORS = {
    "AmbiguousAllocationError": AmbiguousAllocationError("two segments"),
    "SegmentsManagerAuthError": SegmentsManagerAuthError("401"),
    "SegmentValidationError": SegmentValidationError("422"),
    "SegmentNotFoundError": SegmentNotFoundError("gone"),
    "DhcpApiAuthError": DhcpApiAuthError("401"),
    "DhcpScopeInvalidError": DhcpScopeInvalidError("400 INVALID_SCOPE"),
}

_KNOB = {
    "find_cluster_allocation": "lookup_error",
    "release_segment": "release_error",
    "get_segment": "get_segment_error",
    "delete_dhcp_scope": "delete_error",
    "get_dhcp_scope": "scope_get_error",
}


@pytest.mark.parametrize(("error_type", "activity_name"), _CLASSIFIED_CASES)
async def test_classified_error_fails_the_run_after_one_attempt(error_type, activity_name):
    # Attempt count is the proof of the non-retryable classification: an
    # unclassified error would retry forever and time the test out.
    calls, mocks = make_mock_activities(**{_KNOB[activity_name]: _ERRORS[error_type]})
    assert await _expect_failure(mocks) == error_type
    assert len(calls[activity_name]) == 1
    if activity_name != "release_segment":
        assert calls["release_segment"] == []


# --- transient errors are out-waited -----------------------------------------


async def test_transient_release_failure_is_retried():
    calls, mocks = make_mock_activities(scope_found=(False,), release_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result.released is True
    assert len(calls["release_segment"]) == 3


async def test_transient_dhcp_api_outage_is_retried():
    calls, mocks = make_mock_activities(scope_found=(False,), scope_get_fail_times=2)
    async with _Harness(mocks) as client:
        result = await _execute(client)

    assert result.released is True
    assert len(calls["get_dhcp_scope"]) == 3



# --- the exception rule (shared/exceptions.py): structural, not reviewed -----

_RELEASE_SEGMENT = (
    pathlib.Path(__file__).resolve().parent.parent
    / "workflow_domains/segment_lifecycle/release_segment.py"
)


def test_the_non_retryable_list_is_built_from_classes():
    """A string literal is the whole failure mode: it reads correctly and,
    misspelt, silently means "retry forever"."""
    tree = ast.parse(_RELEASE_SEGMENT.read_text())
    listed = next(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword) and node.arg == "non_retryable_error_types"
    )
    assert not isinstance(listed, ast.List)
    for name in _RETRY_POLICY.non_retryable_error_types:
        assert isinstance(getattr(exceptions, name, None), type), name


def test_no_workflow_raised_failure_is_in_the_non_retryable_list():
    # Inert there (workflow failures are never retried) — and an inert entry
    # reads as a protection that does not exist.
    workflow_raised = {
        name
        for name, obj in vars(exceptions).items()
        if isinstance(obj, type) and (obj.__doc__ or "").startswith("WORKFLOW-RAISED")
    }
    assert not workflow_raised & set(_RETRY_POLICY.non_retryable_error_types)


def test_every_failure_raised_from_the_workflow_takes_its_type_from_a_class():
    tree = ast.parse(_RELEASE_SEGMENT.read_text())
    types = [
        keyword.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
        for keyword in node.keywords
        if keyword.arg == "type"
    ]
    assert len(types) == 4
    for value in types:
        assert isinstance(value, ast.Attribute) and value.attr == "__name__", ast.unparse(value)
        error = getattr(exceptions, value.value.id)
        assert (error.__doc__ or "").startswith("WORKFLOW-RAISED"), error.__name__
