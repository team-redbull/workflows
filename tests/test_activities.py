"""Activity unit tests: real activity code, HTTP mocked at the httpx layer.

Covers the strict-validation and error-classification contracts: 401/403 ->
SegmentsManagerAuthError (non-retryable), 404 -> SegmentNotFoundError
(non-retryable), everything else -> retryable SegmentsManagerError. The git
activities' plumbing is tested against real bare repos in test_values_repo.py;
here only their branch guard, which runs before any git.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from temporalio.contrib.pydantic import pydantic_data_converter

from activities.segment_lifecycle import values_repo
from activities.segment_lifecycle.activities import (
    append_allocation_to_cluster_values,
    create_segment,
    get_segment,
    locate_cluster_file,
)
from shared.exceptions import (
    SegmentConflictError,
    SegmentNotFoundError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ClusterFileLookupRequest,
    ClusterValuesAppendRequest,
    InitializeSegmentInput,
    SegmentType,
)

SM = "http://segments-manager.test"

SEGMENT_INPUT = InitializeSegmentInput(
    segment="10.0.0.0/24",
    site="site-a",
    vlan_id=100,
    epg_name="EPG_TEST_01",
)
# What the Segments Manager stores for SEGMENT_INPUT once created — Available
# immediately and typeless: the type is stamped on at allocation.
STORED_SEGMENT = {
    "segment": "10.0.0.0/24",
    "type": None,
    "site": "site-a",
    "vlan_id": 100,
    "epg_name": "EPG_TEST_01",
    "dhcp": True,
    "status": "Available",
    "cluster_name": None,
}


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


# --- create_segment ---


@respx.mock
async def test_create_segment_posts_the_full_definition(env):
    route = respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(200, json={"message": "Segment created", "id": "abc"})
    )
    await env.run(create_segment, SEGMENT_INPUT)

    assert route.called
    import json

    body = json.loads(route.calls.last.request.content)
    # Exactly the Segments Manager's Segment schema (which is extra="forbid").
    # No type: the manager rejects one on create.
    assert body == {
        "segment": "10.0.0.0/24",
        "site": "site-a",
        "vlan_id": 100,
        "epg_name": "EPG_TEST_01",
        "dhcp": True,
    }
    assert route.calls.last.request.headers["Authorization"].startswith("Bearer ")


@respx.mock
async def test_create_segment_existing_identical_segment_is_success(env):
    """A retried-but-already-applied create (or an operator re-running
    connectivity for an existing segment) must converge, not fail."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "VLAN 100 already exists at site 'site-a'"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(200, json=STORED_SEGMENT)
    )
    await env.run(create_segment, SEGMENT_INPUT)  # no raise


@respx.mock
async def test_create_segment_existing_segment_with_different_dhcp_is_success(env):
    """dhcp is editable after creation, so a differing flag is a later edit,
    not a conflicting definition."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(200, json={**STORED_SEGMENT, "dhcp": False})
    )
    await env.run(create_segment, SEGMENT_INPUT)  # no raise


@respx.mock
async def test_create_segment_existing_segment_since_allocated_is_success(env):
    """Type, cluster and status are allocation state the manager sets and
    clears — a segment allocated since it was created still matches the
    definition that created it."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(
            200,
            json={
                **STORED_SEGMENT,
                "type": "HC",
                "status": "Allocated",
                "cluster_name": "cluster-a",
            },
        )
    )
    await env.run(create_segment, SEGMENT_INPUT)  # no raise


@respx.mock
async def test_create_segment_existing_segment_with_different_identity_conflicts(env):
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(200, json={**STORED_SEGMENT, "vlan_id": 999})
    )
    with pytest.raises(SegmentConflictError) as exc_info:
        await env.run(create_segment, SEGMENT_INPUT)
    assert "vlan_id" in str(exc_info.value)


@respx.mock
async def test_create_segment_rejected_definition_is_validation_error(env):
    """A 400 with no such segment stored = the definition itself was bad. The
    manager's own wording is carried through — it is the validator of record."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            400, json={"detail": "Segment 10.0.0.0/24 overlaps with existing segment"}
        )
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(404, json={"detail": "Segment not found"})
    )
    with pytest.raises(SegmentValidationError) as exc_info:
        await env.run(create_segment, SEGMENT_INPUT)
    assert "overlaps with existing segment" in str(exc_info.value)


@respx.mock
async def test_create_segment_auth_failure_is_classified(env):
    respx.post(f"{SM}/api/segments").mock(return_value=httpx.Response(401))
    with pytest.raises(SegmentsManagerAuthError):
        await env.run(create_segment, SEGMENT_INPUT)


@respx.mock
async def test_create_segment_server_error_is_retryable_type(env):
    respx.post(f"{SM}/api/segments").mock(return_value=httpx.Response(503))
    with pytest.raises(SegmentsManagerError):
        await env.run(create_segment, SEGMENT_INPUT)


@respx.mock
async def test_create_segment_unreadable_lookup_stays_retryable(env):
    """A conflict we cannot yet classify (the lookup itself failed) must retry,
    not guess at a terminal failure."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(return_value=httpx.Response(503))
    with pytest.raises(SegmentsManagerError):
        await env.run(create_segment, SEGMENT_INPUT)


# --- get_segment ---


@pytest.mark.parametrize("stored_type", ["HC", None])
@respx.mock
async def test_get_segment_always_reports_the_type_key(env, stored_type):
    """allocate-segment verifies the read-back type only when the payload
    HAS a `type` key — its absence means a limb that predates the field. So
    this limb must emit the key on every read-back, a null one included, or
    the check would silently switch itself off."""
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(
            200,
            json={
                **STORED_SEGMENT,
                "type": stored_type,
                "status": "Allocated" if stored_type else "Available",
            },
        )
    )
    entry = await env.run(get_segment, "10.0.0.0/24")

    assert entry.type == stored_type
    [payload] = pydantic_data_converter.payload_converter.to_payloads([entry])
    assert b'"type":' in payload.data


@respx.mock
async def test_get_segment_404_is_not_found(env):
    respx.get(f"{SM}/api/segments/by-segment").mock(return_value=httpx.Response(404))
    with pytest.raises(SegmentNotFoundError):
        await env.run(get_segment, "10.0.0.0/24")


# --- git activities: the branch guard ---


@pytest.fixture
def git_must_not_run(monkeypatch):
    """Any git call fails the test: the guard must stop the request first."""

    async def _forbidden(**_kwargs):
        raise AssertionError("git ran for a request without a values_branch")

    monkeypatch.setattr(values_repo, "locate_cluster_file", _forbidden)
    monkeypatch.setattr(values_repo, "append_allocation", _forbidden)


def _assert_values_branch_missing(exc_info) -> None:
    error = exc_info.value
    assert isinstance(error, ApplicationError)
    assert error.type == "ValuesBranchMissing"
    # Non-retryable in the activity itself: retrying cannot invent a branch,
    # and there is no configured fallback (it would push to main).
    assert error.non_retryable is True


async def test_locate_cluster_file_without_a_branch_is_non_retryable(env, git_must_not_run):
    with pytest.raises(ApplicationError) as exc_info:
        await env.run(locate_cluster_file, ClusterFileLookupRequest(cluster="c1"))
    _assert_values_branch_missing(exc_info)


async def test_append_allocation_without_a_branch_is_non_retryable(env, git_must_not_run):
    request = ClusterValuesAppendRequest(
        cluster="c1",
        relative_path="sites/site1/mces/m/hostedClusters/c1.yaml",
        vlan_id=23,
        segment="10.20.90.0/24",
        type=SegmentType.HC,
    )
    with pytest.raises(ApplicationError) as exc_info:
        await env.run(append_allocation_to_cluster_values, request)
    _assert_values_branch_missing(exc_info)
