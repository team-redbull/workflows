"""Activity unit tests: real activity code, HTTP mocked at the httpx layer.

Covers the strict-validation and error-classification contracts: 401/403 ->
SegmentsManagerAuthError (non-retryable), 404 -> SegmentNotFoundError
(non-retryable), 409 on a conversion -> SegmentConversionConflictError
(non-retryable), everything else -> retryable SegmentsManagerError.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from temporalio.testing import ActivityEnvironment

from activities.segment_lifecycle.activities import (
    convert_segment_type,
    create_segment,
    list_convertible_segments,
)
from shared.exceptions import (
    SegmentConflictError,
    SegmentConversionConflictError,
    SegmentNotFoundError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ConvertibleSegmentsQuery,
    InitializeSegmentInput,
    SegmentTypeUpdate,
    SegmentType,
)

SM = "http://segments-manager.test"

HC_INPUT = InitializeSegmentInput(
    segment="10.0.0.0/24",
    type=SegmentType.HC,
    site="site-a",
    vlan_id=100,
    epg_name="EPG_TEST_01",
)
# What the Segments Manager stores for HC_INPUT once created — Available
# immediately: there is no Locked status any more.
STORED_HC_SEGMENT = {
    "segment": "10.0.0.0/24",
    "type": "HC",
    "site": "site-a",
    "vlan_id": 100,
    "epg_name": "EPG_TEST_01",
    "dhcp": True,
    "status": "Available",
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
    await env.run(create_segment, HC_INPUT)

    assert route.called
    import json

    body = json.loads(route.calls.last.request.content)
    # Exactly the Segments Manager's Segment schema (which is extra="forbid").
    assert body == {
        "segment": "10.0.0.0/24",
        "type": "HC",
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
        return_value=httpx.Response(200, json=STORED_HC_SEGMENT)
    )
    await env.run(create_segment, HC_INPUT)  # no raise


@respx.mock
async def test_create_segment_existing_segment_with_different_dhcp_is_success(env):
    """dhcp is editable after creation, so a differing flag is a later edit,
    not a conflicting definition."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(200, json={**STORED_HC_SEGMENT, "dhcp": False})
    )
    await env.run(create_segment, HC_INPUT)  # no raise


@respx.mock
async def test_create_segment_existing_segment_with_different_identity_conflicts(env):
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(
        return_value=httpx.Response(200, json={**STORED_HC_SEGMENT, "vlan_id": 999})
    )
    with pytest.raises(SegmentConflictError) as exc_info:
        await env.run(create_segment, HC_INPUT)
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
        await env.run(create_segment, HC_INPUT)
    assert "overlaps with existing segment" in str(exc_info.value)


@respx.mock
async def test_create_segment_auth_failure_is_classified(env):
    respx.post(f"{SM}/api/segments").mock(return_value=httpx.Response(401))
    with pytest.raises(SegmentsManagerAuthError):
        await env.run(create_segment, HC_INPUT)


@respx.mock
async def test_create_segment_server_error_is_retryable_type(env):
    respx.post(f"{SM}/api/segments").mock(return_value=httpx.Response(503))
    with pytest.raises(SegmentsManagerError):
        await env.run(create_segment, HC_INPUT)


@respx.mock
async def test_create_segment_unreadable_lookup_stays_retryable(env):
    """A conflict we cannot yet classify (the lookup itself failed) must retry,
    not guess at a terminal failure."""
    respx.post(f"{SM}/api/segments").mock(
        return_value=httpx.Response(400, json={"detail": "already exists"})
    )
    respx.get(f"{SM}/api/segments/by-segment").mock(return_value=httpx.Response(503))
    with pytest.raises(SegmentsManagerError):
        await env.run(create_segment, HC_INPUT)


# --- convert-segment: list_convertible_segments / convert_segment_type ---


def _stored(
    segment: str, vlan_id: int, status: str, cluster_name: str | None = None
) -> dict:
    return {
        "segment": segment,
        "type": "HC",
        "site": "site-a",
        "vlan_id": vlan_id,
        "epg_name": f"EPG_HC_{vlan_id}",
        "dhcp": True,
        "status": status,
        "cluster_name": cluster_name,
    }


@respx.mock
async def test_list_convertible_segments_asks_the_server_for_available_only(env):
    """All three filters are the server's now that "convertible" means one
    status — so the request itself must carry status=Available."""
    route = respx.get(
        f"{SM}/api/segments",
        params={"site": "site-a", "type": "HC", "status": "Available"},
    ).mock(
        return_value=httpx.Response(
            200,
            json=[
                _stored("10.0.10.0/24", 10, "Available"),
                _stored("10.0.30.0/24", 30, "Available"),
            ],
        )
    )
    hits = await env.run(
        list_convertible_segments,
        ConvertibleSegmentsQuery(site="site-a", type=SegmentType.HC),
    )
    assert route.called
    assert [(h.segment, h.vlan_id, h.status, h.cluster_name) for h in hits] == [
        ("10.0.10.0/24", 10, "Available", None),
        ("10.0.30.0/24", 30, "Available", None),
    ]


@respx.mock
@pytest.mark.parametrize("wrong_status", ["Allocated", "Reserved"])
async def test_list_convertible_segments_rejects_a_wrong_status(env, wrong_status):
    """Strict, not tolerant (§7): a segment the server should have filtered out
    would enter the conversion loop, so it fails the activity rather than being
    quietly skipped."""
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            200,
            json=[
                _stored("10.0.10.0/24", 10, "Available"),
                _stored("10.0.20.0/24", 20, wrong_status),
            ],
        )
    )
    with pytest.raises(SegmentsManagerError):
        await env.run(
            list_convertible_segments,
            ConvertibleSegmentsQuery(site="site-a", type=SegmentType.HC),
        )


@respx.mock
@pytest.mark.parametrize("cluster_name", ["hub-cluster-01", " "])
async def test_list_convertible_segments_rejects_a_cluster_assigned_hit(
    env, cluster_name
):
    """The OTHER half of "convertible", asserted for the same reason as the
    status: an Available segment carrying a cluster is a Segments Manager
    invariant violation (allocation assigns the cluster and sets the status in
    one update), and converting it would re-type a segment a cluster is
    actually using.
    """
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            200,
            json=[
                _stored("10.0.10.0/24", 10, "Available"),
                _stored("10.0.20.0/24", 20, "Available", cluster_name=cluster_name),
            ],
        )
    )
    with pytest.raises(SegmentsManagerError, match="assigned to cluster"):
        await env.run(
            list_convertible_segments,
            ConvertibleSegmentsQuery(site="site-a", type=SegmentType.HC),
        )


@respx.mock
@pytest.mark.parametrize("empty", [None, ""])
async def test_list_convertible_segments_accepts_an_empty_cluster_name(env, empty):
    """Unassigned is both None and "" — the manager has used both spellings,
    and neither means "a cluster holds this"."""
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(
            200,
            json=[_stored("10.0.10.0/24", 10, "Available", cluster_name=empty)],
        )
    )
    hits = await env.run(
        list_convertible_segments,
        ConvertibleSegmentsQuery(site="site-a", type=SegmentType.HC),
    )
    assert [h.segment for h in hits] == ["10.0.10.0/24"]


@respx.mock
async def test_list_convertible_segments_malformed_entry_is_retryable(env):
    respx.get(f"{SM}/api/segments").mock(
        return_value=httpx.Response(200, json=[{"status": "Available"}])
    )
    with pytest.raises(SegmentsManagerError):
        await env.run(
            list_convertible_segments,
            ConvertibleSegmentsQuery(site="site-a", type=SegmentType.HC),
        )


@respx.mock
async def test_convert_segment_type_puts_new_and_expected_type(env):
    route = respx.put(f"{SM}/api/segments/type").mock(
        return_value=httpx.Response(200, json={"message": "Segment type updated"})
    )
    await env.run(
        convert_segment_type,
        SegmentTypeUpdate(
            segment="10.0.30.0/24",
            type=SegmentType.MCE,
            expected_type=SegmentType.HC,
        ),
    )
    import json

    body = json.loads(route.calls.last.request.content)
    assert body == {
        "segment": "10.0.30.0/24",
        "type": "MCE",
        "expected_type": "HC",
    }
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-token"


@respx.mock
async def test_convert_segment_type_conflict_is_classified(env):
    respx.put(f"{SM}/api/segments/type").mock(
        return_value=httpx.Response(
            409, json={"detail": "Cannot convert allocated segment"}
        )
    )
    with pytest.raises(SegmentConversionConflictError) as exc_info:
        await env.run(
            convert_segment_type,
            SegmentTypeUpdate(
                segment="10.0.30.0/24",
                type=SegmentType.MCE,
                expected_type=SegmentType.HC,
            ),
        )
    # The manager's own wording is what the operator has to act on.
    assert "Cannot convert allocated segment" in str(exc_info.value)


@respx.mock
async def test_convert_segment_type_404_is_not_found(env):
    respx.put(f"{SM}/api/segments/type").mock(return_value=httpx.Response(404))
    with pytest.raises(SegmentNotFoundError):
        await env.run(
            convert_segment_type,
            SegmentTypeUpdate(
                segment="10.0.30.0/24",
                type=SegmentType.MCE,
                expected_type=SegmentType.HC,
            ),
        )


@respx.mock
async def test_convert_segment_type_forbidden_is_auth_error(env):
    respx.put(f"{SM}/api/segments/type").mock(return_value=httpx.Response(403))
    with pytest.raises(SegmentsManagerAuthError):
        await env.run(
            convert_segment_type,
            SegmentTypeUpdate(
                segment="10.0.30.0/24",
                type=SegmentType.MCE,
                expected_type=SegmentType.HC,
            ),
        )
