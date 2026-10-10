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
    delete_dhcp_scope,
    find_cluster_allocation,
    get_dhcp_scope,
    get_segment,
    locate_cluster_file,
    release_segment,
)
from shared.exceptions import (
    AmbiguousAllocationError,
    DhcpApiAuthError,
    DhcpApiError,
    DhcpScopeInvalidError,
    SegmentConflictError,
    SegmentNotFoundError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ClusterAllocationLookupRequest,
    ClusterFileLookupRequest,
    ClusterValuesAppendRequest,
    DhcpExclusion,
    InitializeSegmentInput,
    SegmentType,
)

SM = "http://segments-manager.test"
DHCP = "http://dhcp.test"

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



# --- release-segment: find_cluster_allocation ---

CLUSTER = "ocp4-prep-gone-site1-a"


def _allocated(segment: str, cluster: str, type_: str = "HC", vlan_id: int = 100) -> dict:
    return {
        **STORED_SEGMENT,
        "_id": f"id-{segment}",
        "segment": segment,
        "vlan_id": vlan_id,
        "status": "Allocated",
        "type": type_,
        "cluster_name": cluster,
        "allocated_at": "2026-10-10T00:00:00",
    }


@respx.mock
async def test_find_cluster_allocation_matches_the_exact_cluster_only(env):
    route = respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            200,
            json=[
                _allocated("10.0.1.0/24", "ocp4-prep-other-site1-a", vlan_id=101),
                # A name that CONTAINS ours — the manager's /search would hit it.
                _allocated("10.0.2.0/24", f"{CLUSTER}-2", vlan_id=102),
                _allocated("10.0.3.0/24", CLUSTER, vlan_id=103),
                # Same cluster, another type — not this lookup's allocation.
                _allocated("10.0.4.0/24", CLUSTER, type_="PXE", vlan_id=104),
            ],
        )
    )
    lookup = await env.run(
        find_cluster_allocation,
        ClusterAllocationLookupRequest(cluster=CLUSTER, type=SegmentType.HC),
    )

    assert lookup.found is True
    assert lookup.entry.segment == "10.0.3.0/24"
    assert lookup.entry.vlan_id == 103
    # The filter is server-side for status/type and fresh (no stale cache) —
    # and never `cluster_name`, which the route silently ignores.
    params = dict(route.calls.last.request.url.params)
    assert params == {"status": "Allocated", "type": "HC", "fresh": "true"}
    # Public GET: no credential sent.
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
async def test_find_cluster_allocation_without_a_match_is_not_found(env):
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(200, json=[_allocated("10.0.1.0/24", "someone-else")])
    )
    lookup = await env.run(
        find_cluster_allocation,
        ClusterAllocationLookupRequest(cluster=CLUSTER, type=SegmentType.HC),
    )
    assert lookup.found is False
    assert lookup.entry is None


@respx.mock
async def test_find_cluster_allocation_with_two_matches_is_ambiguous(env):
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            200,
            json=[_allocated("10.0.1.0/24", CLUSTER), _allocated("10.0.2.0/24", CLUSTER)],
        )
    )
    with pytest.raises(AmbiguousAllocationError, match="10.0.1.0/24, 10.0.2.0/24"):
        await env.run(
            find_cluster_allocation,
            ClusterAllocationLookupRequest(cluster=CLUSTER, type=SegmentType.HC),
        )


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (httpx.Response(401), SegmentsManagerAuthError),
        (httpx.Response(503), SegmentsManagerError),
        (httpx.Response(200, json={"detail": "not a list"}), SegmentsManagerError),
    ],
    ids=["401", "503", "non-list body"],
)
@respx.mock
async def test_find_cluster_allocation_classifies_failures(env, response, error):
    respx.get(f"{SM}/api/segments").mock(return_value=response)
    with pytest.raises(error):
        await env.run(
            find_cluster_allocation,
            ClusterAllocationLookupRequest(cluster=CLUSTER, type=SegmentType.HC),
        )


# --- release-segment: release_segment ---


@pytest.mark.parametrize("message", ["Segment released successfully", "Segment already released"])
@respx.mock
async def test_release_segment_posts_the_cidr_with_the_token(env, message):
    """Both 200 answers are success — "already released" is the idempotent
    path a Temporal retry lands on."""
    route = respx.post(f"{SM}/api/segments/release").mock(
        return_value=httpx.Response(200, json={"message": message})
    )
    await env.run(release_segment, "10.0.0.0/24")

    import json

    # Exactly the manager's SegmentRelease (extra="forbid"): no cluster, no type.
    assert json.loads(route.calls.last.request.content) == {"segment": "10.0.0.0/24"}
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-token"


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (404, SegmentNotFoundError),
        (400, SegmentValidationError),
        (422, SegmentValidationError),
        (401, SegmentsManagerAuthError),
        # A lost release race: retried, and the retry answers "already released".
        (500, SegmentsManagerError),
    ],
)
@respx.mock
async def test_release_segment_classifies_failures(env, status, error):
    respx.post(f"{SM}/api/segments/release").mock(
        return_value=httpx.Response(status, json={"detail": "nope"})
    )
    with pytest.raises(error):
        await env.run(release_segment, "10.0.0.0/24")


# --- release-segment: the DHCP scope API ---


@respx.mock
async def test_get_dhcp_scope_reports_an_existing_scope_anonymously(env):
    route = respx.get(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(
        return_value=httpx.Response(
            200,
            json={
                "scopeName": "OCP4-PREP-GONE-SITE1-A",
                "exclusions": [{"startAddress": "10.0.0.1", "endAddress": "10.0.0.10"}],
            },
        )
    )
    state = await env.run(get_dhcp_scope, "10.0.0.0")

    assert state.found is True
    assert state.exclusions == [DhcpExclusion(start_address="10.0.0.1", end_address="10.0.0.10")]
    # The API leaves scope GETs anonymous; the token goes on the write only.
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
async def test_get_dhcp_scope_404_is_not_found_never_an_error(env):
    respx.get(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(
        return_value=httpx.Response(
            404, json={"error": {"code": "SCOPE_NOT_FOUND", "message": "no scope"}}
        )
    )
    state = await env.run(get_dhcp_scope, "10.0.0.0")
    assert state.found is False


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (
            httpx.Response(400, json={"error": {"code": "INVALID_SCOPE", "message": "bad"}}),
            DhcpScopeInvalidError,
        ),
        # No DHCP backend configured: transient, out-waited.
        (httpx.Response(503), DhcpApiError),
        (httpx.Response(500), DhcpApiError),
        (httpx.Response(504), DhcpApiError),
        (httpx.Response(200, text="<html>proxy</html>"), DhcpApiError),
    ],
    ids=["400", "503", "500", "504", "malformed body"],
)
@respx.mock
async def test_get_dhcp_scope_classifies_failures(env, response, error):
    respx.get(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(return_value=response)
    with pytest.raises(error):
        await env.run(get_dhcp_scope, "10.0.0.0")


@respx.mock
async def test_get_dhcp_scope_network_error_is_transient(env):
    respx.get(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    with pytest.raises(DhcpApiError):
        await env.run(get_dhcp_scope, "10.0.0.0")


@respx.mock
async def test_delete_dhcp_scope_sends_the_token(env):
    """204 is the answer whether or not the scope existed — idempotent."""
    route = respx.delete(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(
        return_value=httpx.Response(204, headers={"X-Deleted-Scope": "10.0.0.0"})
    )
    await env.run(delete_dhcp_scope, "10.0.0.0")
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-dhcp-token"


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, DhcpApiAuthError),
        (403, DhcpApiAuthError),
        (400, DhcpScopeInvalidError),
        (500, DhcpApiError),
        (503, DhcpApiError),
        (504, DhcpApiError),
    ],
)
@respx.mock
async def test_delete_dhcp_scope_classifies_failures(env, status, error):
    respx.delete(f"{DHCP}/api/v1/scopes/10.0.0.0").mock(
        return_value=httpx.Response(status, json={"error": {"code": "X", "message": "nope"}})
    )
    with pytest.raises(error):
        await env.run(delete_dhcp_scope, "10.0.0.0")
