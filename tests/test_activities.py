"""Activity unit tests: real activity code, HTTP mocked at the httpx layer.

Covers the strict-validation and error-classification contracts: 401/403 ->
SegmentsManagerAuthError (non-retryable), 404 -> SegmentNotFoundError
(non-retryable), unexpected next status -> non-retryable ApplicationError,
everything else -> retryable SegmentsManagerError/NextApiError.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from activities.segment_connectivity.activities import (
    _expand_ports,
    _peer_types,
    check_segment_connectivity_requests,
    create_segment,
    get_bmc_segment,
    get_next_checking_request_interval,
    list_peer_segments,
    publish_segment_connectivity_failure,
    publish_request_ids,
    submit_bmc_open_rules,
    submit_open_rules,
    unlock_segment,
)
from shared.exceptions import (
    BmcSegmentNotConfiguredError,
    NextApiError,
    SegmentConflictError,
    SegmentNotFoundError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentValidationError,
)
from shared.models.segment_connectivity import (
    BmcOpenRulesRequest,
    SegmentConnectivityFailureNotice,
    OpenSegmentRulesInput,
    SegmentConnectivityRequestsUpdate,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
    SegmentType,
)

SM = "http://segments-manager.test"
NEXT = "http://next.test"

HC_INPUT = OpenSegmentRulesInput(
    segment="10.0.0.0/24",
    type=SegmentType.HC,
    site="site-a",
    vlan_id=100,
    epg_name="EPG_TEST_01",
)
# What the Segments Manager stores for HC_INPUT once created.
STORED_HC_SEGMENT = {
    "segment": "10.0.0.0/24",
    "type": "HC",
    "site": "site-a",
    "vlan_id": 100,
    "epg_name": "EPG_TEST_01",
    "dhcp": True,
    "status": "Locked",
}


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


def test_expand_ports_ranges_and_single_ports():
    assert _expand_ports({"tcp": ["30000-32767"], "udp": ["9000"]}) == [
        {
            "type": "range",
            "port_range_start": 30000,
            "port_range_end": 32767,
            "protocol": "TCP",
        },
        {"type": "port", "port": 9000, "protocol": "UDP"},
    ]


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


# --- _peer_types / list_peer_segments ---


def test_peer_types_derived_from_port_profiles():
    assert _peer_types(SegmentType.HC) == [SegmentType.MCE]
    assert _peer_types(SegmentType.INVENTORY) == [SegmentType.MCE]
    assert _peer_types(SegmentType.PXE) == [SegmentType.MCE]
    assert _peer_types(SegmentType.MCE) == [
        SegmentType.HC,
        SegmentType.INVENTORY,
        SegmentType.PXE,
    ]


@respx.mock
async def test_list_peer_segments_hc_source_queries_mce_and_filters_by_site(env):
    respx.get(f"{SM}/api/segments", params={"type": "MCE"}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"segment": "10.1.0.0/24", "site": "site-a"},
                {"segment": "10.2.0.0/24", "site": "site-b"},
                {"segment": "10.3.0.0/24", "site": "site-a"},
            ],
        )
    )
    result = await env.run(
        list_peer_segments, PeerSegmentsQuery(source_type=SegmentType.HC, site="site-a")
    )
    assert result == [
        SegmentRef(segment="10.1.0.0/24", type=SegmentType.MCE),
        SegmentRef(segment="10.3.0.0/24", type=SegmentType.MCE),
    ]


@respx.mock
async def test_list_peer_segments_mce_source_queries_all_peer_types_and_merges(env):
    respx.get(f"{SM}/api/segments", params={"type": "HC"}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"segment": "10.1.0.0/24", "site": "site-a"},
                {"segment": "10.9.0.0/24", "site": "site-b"},
            ],
        )
    )
    respx.get(f"{SM}/api/segments", params={"type": "INVENTORY"}).mock(
        return_value=httpx.Response(200, json=[{"segment": "10.2.0.0/24", "site": "site-a"}])
    )
    respx.get(f"{SM}/api/segments", params={"type": "PXE"}).mock(
        return_value=httpx.Response(200, json=[{"segment": "10.3.0.0/24", "site": "site-a"}])
    )
    result = await env.run(
        list_peer_segments, PeerSegmentsQuery(source_type=SegmentType.MCE, site="site-a")
    )
    assert sorted(result, key=lambda r: r.segment) == [
        SegmentRef(segment="10.1.0.0/24", type=SegmentType.HC),
        SegmentRef(segment="10.2.0.0/24", type=SegmentType.INVENTORY),
        SegmentRef(segment="10.3.0.0/24", type=SegmentType.PXE),
    ]


# --- submit_open_rules ---


@respx.mock
async def test_submit_open_rules_builds_payload_and_returns_ref(env):
    respx.post(f"{NEXT}/token-renewal-uri").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1"})
    )
    open_rules = respx.post(f"{NEXT}/open-rules-uri").mock(
        return_value=httpx.Response(201, json={"id": 42, "status": "pending"})
    )

    ref = await env.run(
        submit_open_rules,
        OpenRulesRequest(
            source_segment="10.0.0.0/24",
            destination_segment="10.1.0.0/24",
            source_type=SegmentType.HC,
            destination_type=SegmentType.MCE,
        ),
    )

    assert ref.id == 42
    request = open_rules.calls.last.request
    assert request.headers["Authorization"] == "Bearer tok-1"
    import json

    payload = json.loads(request.content)
    assert payload["properties"]["source"]["addresses"] == [
        {"type": "segment", "segment": "10.0.0.0/24"}
    ]
    # PORTS_HC_TO_MCE from conftest: tcp 30000-32767 + udp 9000.
    assert payload["properties"]["ports"] == [
        {
            "type": "range",
            "port_range_start": 30000,
            "port_range_end": 32767,
            "protocol": "TCP",
        },
        {"type": "port", "port": 9000, "protocol": "UDP"},
    ]


async def test_submit_open_rules_missing_port_profile_is_non_retryable(env):
    with pytest.raises(ApplicationError) as exc_info:
        await env.run(
            submit_open_rules,
            OpenRulesRequest(
                source_segment="10.0.0.0/24",
                destination_segment="10.1.0.0/24",
                source_type=SegmentType.HC,
                destination_type=SegmentType.HC,  # no HC->HC profile configured
            ),
        )
    assert exc_info.value.type == "PortProfileMissing"
    assert exc_info.value.non_retryable is True


# --- get_bmc_segment / submit_bmc_open_rules ---


async def test_get_bmc_segment_returns_configured_cidr(env):
    # BMC_SEGMENTS_BY_SITE from conftest: {"site-a": "10.99.0.0/16"}.
    assert await env.run(get_bmc_segment, "site-a") == "10.99.0.0/16"


async def test_get_bmc_segment_missing_site_raises(env):
    with pytest.raises(BmcSegmentNotConfiguredError):
        await env.run(get_bmc_segment, "site-unknown")


@respx.mock
async def test_submit_bmc_open_rules_builds_one_directional_payload(env):
    respx.post(f"{NEXT}/token-renewal-uri").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1"})
    )
    open_rules = respx.post(f"{NEXT}/open-rules-uri").mock(
        return_value=httpx.Response(201, json={"id": 99, "status": "pending"})
    )

    ref = await env.run(
        submit_bmc_open_rules,
        BmcOpenRulesRequest(mce_segment="10.0.0.0/24", bmc_segment="10.99.0.0/16"),
    )

    assert ref.id == 99
    import json

    payload = json.loads(open_rules.calls.last.request.content)
    assert payload["properties"]["source"]["system_name"] == "mce"
    assert payload["properties"]["source"]["addresses"] == [
        {"type": "segment", "segment": "10.0.0.0/24"}
    ]
    assert payload["properties"]["destination"]["system_name"] == "bmc"
    assert payload["properties"]["destination"]["addresses"] == [
        {"type": "segment", "segment": "10.99.0.0/16"}
    ]
    # PORTS_MCE_TO_BMC from conftest: tcp 623.
    assert payload["properties"]["ports"] == [
        {"type": "port", "port": 623, "protocol": "TCP"}
    ]


# --- check_segment_connectivity_requests ---


@respx.mock
async def test_check_connectivity_requests_returns_pending_ids(env):
    respx.post(f"{NEXT}/token-renewal-uri").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1"})
    )
    respx.get(f"{NEXT}/check-request-status/1").mock(
        return_value=httpx.Response(200, json={"status": "complete"})
    )
    respx.get(f"{NEXT}/check-request-status/2").mock(
        return_value=httpx.Response(200, json={"status": "pending"})
    )
    assert await env.run(check_segment_connectivity_requests, [1, 2]) == [2]


@respx.mock
async def test_check_connectivity_requests_unexpected_status_is_non_retryable(env):
    respx.post(f"{NEXT}/token-renewal-uri").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1"})
    )
    respx.get(f"{NEXT}/check-request-status/1").mock(
        return_value=httpx.Response(200, json={"status": "rejected"})
    )
    with pytest.raises(ApplicationError) as exc_info:
        await env.run(check_segment_connectivity_requests, [1])
    assert exc_info.value.type == "UnexpectedRequestStatus"
    assert exc_info.value.non_retryable is True


@respx.mock
async def test_check_connectivity_requests_next_error_is_retryable_type(env):
    respx.post(f"{NEXT}/token-renewal-uri").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1"})
    )
    respx.get(f"{NEXT}/check-request-status/1").mock(return_value=httpx.Response(502))
    with pytest.raises(NextApiError):
        await env.run(check_segment_connectivity_requests, [1])


# --- get_next_checking_request_interval ---


async def test_get_next_checking_request_interval_returns_configured_value(env):
    assert await env.run(get_next_checking_request_interval) == 15  # from conftest


# --- publish_request_ids / unlock_segment ---


@respx.mock
async def test_publish_request_ids_puts_replacement_list(env):
    route = respx.put(f"{SM}/api/segments/segment-connectivity-requests").mock(
        return_value=httpx.Response(200)
    )
    from datetime import datetime, timezone

    submitted = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)
    await env.run(
        publish_request_ids,
        SegmentConnectivityRequestsUpdate(
            segment="10.0.0.0/24", request_ids=[1, 2], submitted_at=submitted
        ),
    )
    import json

    body = json.loads(route.calls.last.request.content)
    assert body["request_ids"] == [1, 2]
    assert body["submitted_at"] == submitted.isoformat()
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-token"


@respx.mock
async def test_publish_request_ids_404_is_not_found(env):
    respx.put(f"{SM}/api/segments/segment-connectivity-requests").mock(
        return_value=httpx.Response(404)
    )
    from datetime import datetime, timezone

    with pytest.raises(SegmentNotFoundError):
        await env.run(
            publish_request_ids,
            SegmentConnectivityRequestsUpdate(
                segment="10.0.0.0/24",
                request_ids=[],
                submitted_at=datetime.now(timezone.utc),
            ),
        )


@respx.mock
async def test_unlock_segment_forbidden_is_auth_error(env):
    respx.post(f"{SM}/api/segments/unlock").mock(return_value=httpx.Response(403))
    with pytest.raises(SegmentsManagerAuthError):
        await env.run(unlock_segment, "10.0.0.0/24")


# --- publish_segment_connectivity_failure ---


@respx.mock
async def test_publish_connectivity_failure_clears_ids_then_publishes_note(env):
    clear = respx.put(f"{SM}/api/segments/segment-connectivity-requests").mock(
        return_value=httpx.Response(200)
    )
    note = respx.put(f"{SM}/api/segments/segment-connectivity-failure").mock(
        return_value=httpx.Response(200)
    )
    await env.run(
        publish_segment_connectivity_failure,
        SegmentConnectivityFailureNotice(segment="10.0.0.0/24", message="boom"),
    )
    import json

    assert json.loads(clear.calls.last.request.content)["request_ids"] == []
    assert json.loads(note.calls.last.request.content)["message"] == "boom"


@respx.mock
async def test_publish_connectivity_failure_missing_note_endpoint_still_clears(env):
    """Until the Segments Manager grows the note endpoint, the activity must
    clear the display first and only then fail (the workflow swallows it)."""
    clear = respx.put(f"{SM}/api/segments/segment-connectivity-requests").mock(
        return_value=httpx.Response(200)
    )
    respx.put(f"{SM}/api/segments/segment-connectivity-failure").mock(
        return_value=httpx.Response(404)
    )
    with pytest.raises(SegmentsManagerError):
        await env.run(
            publish_segment_connectivity_failure,
            SegmentConnectivityFailureNotice(segment="10.0.0.0/24", message="boom"),
        )
    assert clear.called
