"""The server-lifecycle limb's two technology layers, tested without Temporal.

Both are reachable only through an activity in production, and neither had any
coverage before they were split out of activities.py: `fetch_available_servers`
takes its endpoint and token as parameters, and cluster_api holds no settings at
all, so each can be driven here with no environment and no worker.

What is checked is mostly CLASSIFICATION. Every failure these layers raise is
sorted into permanent or transient by its type, and the workflow's retry policy
is unbounded — so a status sorted wrongly does not fail a run, it retries every
minute forever with the run sitting RUNNING. That is the failure mode with no
symptom, which is why it is tested directly rather than through the workflow.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest
import respx
from kubernetes import client as k8s_client

from activities.server_lifecycle import cluster_api
from activities.server_lifecycle.server_scan import (
    fetch_available_servers,
    install_candidate_by_name,
    release_server,
    reserve_server,
)
from shared.exceptions import (
    AmbiguousServerNameError,
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    BmhResourceError,
    ServerNotAvailableError,
    ServerReservedError,
    ServerScanAuthError,
    ServerScanError,
    ServerScanRequestInvalidError,
)
from shared.models.server_lifecycle import (
    AcquireServerRequest,
    LinkState,
    ReleaseServerRequest,
    ReserveServerRequest,
)

SS = "http://server-scan.test/api/v1"
AVAILABLE = f"{SS}/servers/available"

PATTERN_REQUEST = AcquireServerRequest(
    pattern="^ocp-dell-r650-tlv", count=3, health="HEALTHY"
)
NAME_REQUEST = AcquireServerRequest(
    name="ocp-dell-r650-tlv-64c-1024gb-DEL0000485",
    count=1,
    health="HEALTHY",
)

_ITEM = {
    "id": "srv_1",
    "name": "ocp-dell-r650-tlv-64c-1024gb-DEL0000485",
    "vendor": "dell",
    "source_provider": "OPENMANAGE",
    "bmc_vendor": "DELL",
    "bmc": {"host": "10.11.1.229", "host_is_ip": True, "scheme": "redfish"},
    "nic_macs": ["aa:bb:cc:dd:ee:01"] * 16,
    "interfaces": [
        {
            "name": "NIC.Integrated.1-1-1",
            "mac": "aa:bb:cc:dd:ee:01",
            "location": "1/1/1",
            "link_state": "UP",
        }
    ],
    "health_overall": "HEALTHY",
    "live_recheck_performed": True,
}


def _payload(*items: dict) -> dict:
    return {"items": list(items), "mode": "pattern", "requested": len(items)}


RESERVATION = f"{SS}/servers/srv_1/reservation"

RESERVE = ReserveServerRequest(
    server_id="srv_1",
    server_name="ocp-dell-r650-tlv-64c-1024gb-DEL0000485",
    holder="add-server-to-mce",
    workflow_id="add-server-to-mce-dell-r650-tlv",
    mce_cluster="ocp4-mce-tlv-01",
    infra_env="dell-r650-tlv",
    namespace="multicluster-engine",
    ttl_seconds=7200,
)
RELEASE = ReleaseServerRequest(
    server_id="srv_1",
    server_name=RESERVE.server_name,
    holder="add-server-to-mce",
    workflow_id="add-server-to-mce-dell-r650-tlv",
)


def _problem(status: int, detail: str, **details: object) -> httpx.Response:
    """server-scan's RFC 9457 envelope, as its exception handler writes it."""
    return httpx.Response(
        status, json={"status": status, "detail": detail, "details": details}
    )


class TestTheInstallLock:
    @respx.mock
    async def test_a_reserve_sends_who_where_and_for_how_long(self):
        route = respx.post(RESERVATION).mock(
            return_value=httpx.Response(
                200, json={"id": "srv_1", "reservation": {"expires_at": "2026-10-02T12:00:00Z"}}
            )
        )
        held = await reserve_server(SS, "admin-token", RESERVE)

        assert held.held is True
        assert held.expires_at == "2026-10-02T12:00:00Z"
        request = route.calls.last.request
        assert request.headers["Authorization"] == "Bearer admin-token"
        # The name is for messages only: the lock is keyed on the id in the path.
        assert json.loads(request.content) == {
            "holder": "add-server-to-mce",
            "workflow_id": "add-server-to-mce-dell-r650-tlv",
            "mce_cluster": "ocp4-mce-tlv-01",
            "infra_env": "dell-r650-tlv",
            "namespace": "multicluster-engine",
            "ttl_seconds": 7200,
        }

    @respx.mock
    async def test_a_lock_held_by_another_run_is_permanent_and_names_it(self):
        respx.post(RESERVATION).mock(
            return_value=_problem(
                409,
                "reserved",
                held_by="add-server-to-mce",
                held_for_mce="ocp4-mce-other",
                workflow_id="add-server-to-mce-other",
                expires_at="2026-10-02T12:00:00Z",
            )
        )
        with pytest.raises(ServerReservedError, match="ocp4-mce-other"):
            await reserve_server(SS, "", RESERVE)

    @respx.mock
    async def test_a_lost_revision_race_stays_transient(self):
        # No `held_by`: some other write won (often a collector run), and a
        # retry either takes the lock or turns into the refusal above.
        respx.post(RESERVATION).mock(
            return_value=_problem(409, "modified by another caller", requested_by="add-server-to-mce")
        )
        with pytest.raises(ServerScanError) as excinfo:
            await reserve_server(SS, "", RESERVE)
        assert not isinstance(excinfo.value, ServerReservedError)

    @respx.mock
    async def test_a_server_gone_from_the_inventory_is_permanent(self):
        respx.post(RESERVATION).mock(return_value=_problem(404, "No server"))
        with pytest.raises(ServerNotAvailableError):
            await reserve_server(SS, "", RESERVE)

    @respx.mock
    @pytest.mark.parametrize("status", [401, 403])
    async def test_a_token_without_admin_is_permanent_and_says_so(self, status):
        respx.post(RESERVATION).mock(return_value=_problem(status, "forbidden"))
        with pytest.raises(ServerScanAuthError, match="ADMIN"):
            await reserve_server(SS, "viewer-token", RESERVE)

    @respx.mock
    @pytest.mark.parametrize("status", [400, 422])
    async def test_a_rejected_body_is_permanent(self, status):
        respx.post(RESERVATION).mock(return_value=_problem(status, "ttl_seconds"))
        with pytest.raises(ServerScanRequestInvalidError, match="ttl_seconds"):
            await reserve_server(SS, "", RESERVE)

    @respx.mock
    async def test_a_server_error_on_reserve_stays_transient(self):
        respx.post(RESERVATION).mock(return_value=httpx.Response(503, text="upstream"))
        with pytest.raises(ServerScanError):
            await reserve_server(SS, "", RESERVE)

    @respx.mock
    async def test_a_release_names_its_holder_so_it_never_clears_anothers_lock(self):
        route = respx.delete(RESERVATION).mock(
            return_value=httpx.Response(200, json={"id": "srv_1"})
        )
        released = await release_server(SS, "", RELEASE)

        assert released.held is False
        assert released.detail is None
        # Without both, server-scan treats a release as an operator override.
        assert json.loads(route.calls.last.request.content) == {
            "holder": "add-server-to-mce",
            "workflow_id": "add-server-to-mce-dell-r650-tlv",
        }

    @respx.mock
    async def test_a_server_gone_from_the_inventory_is_released(self):
        respx.delete(RESERVATION).mock(return_value=_problem(404, "No server"))
        released = await release_server(SS, "", RELEASE)
        assert released.held is False
        assert released.detail

    @respx.mock
    async def test_another_holders_lock_is_left_alone(self):
        # server-scan's ConflictError for a lock that is not ours carries held_by.
        respx.delete(RESERVATION).mock(
            return_value=_problem(
                409, "reserved by 'add-server-to-mce' for MCE 'ocp4-mce-other', not by 'add-server-to-mce'",
                server_id="srv_1", held_by="add-server-to-mce", held_for_mce="ocp4-mce-other",
            )
        )
        released = await release_server(SS, "", RELEASE)
        assert released.held is False
        assert "left alone" in released.detail

    @respx.mock
    async def test_a_lost_revision_race_on_release_is_retried_not_taken_as_released(self):
        # server-scan's "modified by another caller while being released": a
        # collector or membership write won, and the lock is still ours and live.
        respx.delete(RESERVATION).mock(
            return_value=_problem(
                409, "Server 'srv_1' was modified by another caller while being released.",
                server_id="srv_1",
            )
        )
        with pytest.raises(ServerScanError) as excinfo:
            await release_server(SS, "", RELEASE)
        assert not isinstance(excinfo.value, (ServerScanAuthError, ServerScanRequestInvalidError))

    @respx.mock
    async def test_a_release_without_admin_is_permanent(self):
        respx.delete(RESERVATION).mock(return_value=_problem(403, "forbidden"))
        with pytest.raises(ServerScanAuthError):
            await release_server(SS, "viewer-token", RELEASE)


class TestTheInventoryQuery:
    @respx.mock
    async def test_a_pool_draw_asks_for_the_pattern_the_count_and_both_gates(self):
        route = respx.get(AVAILABLE).mock(
            return_value=httpx.Response(200, json=_payload(_ITEM))
        )
        await fetch_available_servers(SS, "tok", PATTERN_REQUEST)
        params = route.calls[0].request.url.params
        assert params["pattern"] == "^ocp-dell-r650-tlv"
        assert params["count"] == "3"
        assert params["health"] == "HEALTHY"
        # server-scan's own network gate already requires two links up.
        assert "min_nic_macs" not in params
        assert route.calls[0].request.headers["Authorization"] == "Bearer tok"

    @respx.mock
    async def test_a_named_lookup_sends_no_pattern_and_no_count(self):
        # A name is a pool of one; `count` would be meaningless beside it, and
        # the endpoint's contract treats the two modes as exclusive.
        route = respx.get(AVAILABLE).mock(
            return_value=httpx.Response(200, json=_payload(_ITEM))
        )
        await fetch_available_servers(SS, "tok", NAME_REQUEST)
        params = route.calls[0].request.url.params
        assert params["name"] == NAME_REQUEST.name
        assert "pattern" not in params
        assert "count" not in params

    @respx.mock
    async def test_no_token_sends_no_authorization_header(self):
        # server-scan can run with auth disabled, as it does on the sandbox.
        route = respx.get(AVAILABLE).mock(
            return_value=httpx.Response(200, json=_payload(_ITEM))
        )
        await fetch_available_servers(SS, "", PATTERN_REQUEST)
        assert "Authorization" not in route.calls[0].request.headers

    @respx.mock
    async def test_a_candidate_keeps_its_interfaces_and_drops_the_mac_list(self):
        respx.get(AVAILABLE).mock(
            return_value=httpx.Response(200, json=_payload(_ITEM))
        )
        [server] = await fetch_available_servers(SS, "tok", PATTERN_REQUEST)
        assert [i.mac for i in server.interfaces] == ["aa:bb:cc:dd:ee:01"]
        assert server.interfaces[0].physical_port_key() == "1/1"
        # 16 partition MACs were reported and none of them is modelled: bonding
        # from that list can pair two partitions of one physical port.
        assert not hasattr(server, "nic_macs")

    @respx.mock
    async def test_an_interface_with_no_link_state_reads_as_unknown(self):
        # Exactly what the server-scan build predating this work returns — it has
        # no link_state on AvailableInterface at all. UNKNOWN is what makes the
        # bond rule refuse it loudly instead of treating absent as up.
        item = {**_ITEM, "interfaces": [{"name": "nic1", "mac": "aa:bb:cc:dd:ee:01"}]}
        respx.get(AVAILABLE).mock(return_value=httpx.Response(200, json=_payload(item)))
        [server] = await fetch_available_servers(SS, "tok", PATTERN_REQUEST)
        assert server.interfaces[0].link_state is LinkState.UNKNOWN


class TestTheInventoryQuerysFailures:
    @respx.mock
    @pytest.mark.parametrize("status", [401, 403])
    async def test_a_rejected_credential_is_permanent(self, status):
        respx.get(AVAILABLE).mock(return_value=httpx.Response(status))
        with pytest.raises(ServerScanAuthError):
            await fetch_available_servers(SS, "bad", PATTERN_REQUEST)

    @respx.mock
    async def test_nothing_assignable_carries_server_scans_own_detail(self):
        # The detail is the whole value of the 404: it distinguishes "no server
        # name matches" from "every match is claimed, unreachable or CRITICAL".
        respx.get(AVAILABLE).mock(
            return_value=httpx.Response(
                404, json={"detail": "all 4 matches are INSTALLED"}
            )
        )
        with pytest.raises(ServerNotAvailableError, match="all 4 matches are INSTALLED"):
            await fetch_available_servers(SS, "tok", PATTERN_REQUEST)

    @respx.mock
    async def test_an_ambiguous_name_is_permanent_and_names_the_server(self):
        respx.get(AVAILABLE).mock(
            return_value=httpx.Response(409, json={"detail": "srv_1, srv_2"})
        )
        with pytest.raises(AmbiguousServerNameError, match="srv_1, srv_2"):
            await fetch_available_servers(SS, "tok", NAME_REQUEST)

    @respx.mock
    async def test_a_server_error_stays_transient(self):
        # Not classified permanent anywhere: a 5xx is what unbounded retries
        # exist to out-wait.
        respx.get(AVAILABLE).mock(return_value=httpx.Response(503, text="upstream"))
        with pytest.raises(ServerScanError):
            await fetch_available_servers(SS, "tok", PATTERN_REQUEST)

    @respx.mock
    @pytest.mark.parametrize(
        ("response", "error"),
        [
            (httpx.Response(500, text="z" * (3 << 20)), ServerScanError),
            (httpx.Response(503, json={"detail": "w" * (3 << 20)}), ServerScanError),
            (httpx.Response(404, json={"detail": "v" * (3 << 20)}), ServerNotAvailableError),
        ],
        ids=["5xx-page", "5xx-json-detail", "404-detail"],
    )
    async def test_a_huge_answer_is_cut_before_it_becomes_the_error(self, response, error):
        # History records an activity error verbatim, and Temporal turns one
        # over 2 MiB into a non-retryable "Failure exceeds size limit." — so a
        # 5xx that should have been out-waited failed the run (2026-11-09).
        respx.get(AVAILABLE).mock(return_value=response)
        with pytest.raises(error) as excinfo:
            await fetch_available_servers(SS, "tok", PATTERN_REQUEST)
        message = str(excinfo.value)
        assert len(message) < 1_000
        assert "more characters cut" in message

    @respx.mock
    async def test_an_empty_draw_is_treated_as_nothing_assignable(self):
        # A 200 with no items is server-scan's honest partial-fulfilment answer,
        # but for one install it means the same thing as a 404.
        respx.get(AVAILABLE).mock(return_value=httpx.Response(200, json=_payload()))
        with pytest.raises(ServerNotAvailableError):
            await fetch_available_servers(SS, "tok", PATTERN_REQUEST)


def _api_error(status: int, message: str | None = None) -> k8s_client.ApiException:
    exc = k8s_client.ApiException(status=status, reason="Some HTTP Phrase")
    if message is not None:
        exc.body = json.dumps({"kind": "Status", "status": "Failure", "message": message})
    return exc


class TestTheApiServersOwnMessage:
    def test_the_status_bodys_message_wins_over_the_http_phrase(self):
        # The live failure this exists for: a Secret named after an uppercase
        # vendor serial. `reason` was "Unprocessable Entity" and useless; the
        # sentence below was the entire diagnosis.
        rfc1123 = "a lowercase RFC 1123 subdomain must consist of lower case..."
        assert cluster_api._detail(_api_error(422, rfc1123)) == rfc1123

    def test_no_body_falls_back_to_the_phrase(self):
        assert cluster_api._detail(_api_error(500)) == "Some HTTP Phrase"

    def test_a_body_that_is_not_json_is_carried_verbatim_and_bounded(self):
        exc = _api_error(500)
        exc.body = "<html>" + "x" * 10_000
        detail = cluster_api._detail(exc)
        assert detail.startswith("<html>")
        assert len(detail) <= 500


class TestStatusClassification:
    @pytest.mark.parametrize("status", [403, 404])
    def test_a_deployment_gap_is_permanent(self, status):
        # 404 = no CRD or no namespace; 403 = no RBAC. No retry closes either.
        error = cluster_api._classify(_api_error(status), "create", "BareMetalHost", "a")
        assert isinstance(error, BmhPrerequisiteMissingError)

    @pytest.mark.parametrize("status", [400, 422])
    def test_a_rejected_body_is_permanent_and_quotes_the_api_server(self, status):
        error = cluster_api._classify(
            _api_error(status, "spec.bmc.address: Invalid value"),
            "create",
            "BareMetalHost",
            "a",
        )
        assert isinstance(error, BmhRequestInvalidError)
        assert "spec.bmc.address: Invalid value" in str(error)

    def test_a_rejected_token_stays_transient(self):
        # Deliberate: a projected ServiceAccount token is rotated under the pod,
        # so 401 is the case retrying is the right answer to.
        assert isinstance(
            cluster_api._classify(_api_error(401), "create", "Secret", "a"),
            BmhResourceError,
        )

    def test_a_server_error_stays_transient(self):
        assert isinstance(
            cluster_api._classify(_api_error(500), "create", "Secret", "a"),
            BmhResourceError,
        )


class TestCreateIsIdempotent:
    def test_a_clean_create_reports_changed(self):
        result = cluster_api._create_if_absent(lambda: None, "Secret", "s")
        assert result.changed is True

    def test_an_existing_resource_with_no_comparison_is_success(self):
        # The Secret's case: comparing one means reading a credential back, and
        # this never rotates a credential an operator may have corrected.
        def _conflict() -> None:
            raise _api_error(409)

        result = cluster_api._create_if_absent(_conflict, "Secret", "s")
        assert result.changed is False

    def test_an_existing_resource_that_matches_is_success(self):
        def _conflict() -> None:
            raise _api_error(409)

        result = cluster_api._create_if_absent(
            _conflict, "BareMetalHost", "h", lambda: []
        )
        assert result.changed is False

    def test_an_existing_resource_that_disagrees_names_the_field(self):
        # Reporting success here would describe an installation that is not the
        # one on the cluster.
        def _conflict() -> None:
            raise _api_error(409)

        with pytest.raises(BmhConflictError, match="spec.bootMACAddress"):
            cluster_api._create_if_absent(
                _conflict,
                "BareMetalHost",
                "h",
                lambda: ["spec.bootMACAddress: want 'a', found 'b'"],
            )

    def test_a_read_back_that_fails_is_classified_not_leaked(self):
        # Without this the raw ApiException escapes: an unrecognised type, so
        # unbounded retries apply and the run sits RUNNING rather than failing.
        def _conflict() -> None:
            raise _api_error(409)

        def _forbidden() -> list[str]:
            raise _api_error(403)

        with pytest.raises(BmhPrerequisiteMissingError):
            cluster_api._create_if_absent(_conflict, "BareMetalHost", "h", _forbidden)

    def test_a_create_that_fails_for_any_other_reason_is_classified(self):
        def _invalid() -> None:
            raise _api_error(422, "metadata.name: Invalid value")

        with pytest.raises(BmhRequestInvalidError):
            cluster_api._create_if_absent(_invalid, "Secret", "s")


@pytest.fixture(autouse=True)
def _never_load_a_real_kubeconfig(monkeypatch):
    """cluster_api loads kube config per operation; these tests make no calls."""
    monkeypatch.setattr(cluster_api, "_load_kube", lambda: None)


# --- the stored record of a named server /servers/available refused -----------

_NAMED = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"


def _detail(**overrides: object) -> dict:
    """A `GET /servers/{id}` ServerDetail, trimmed to what the read uses (server-scan's schema)."""
    detail: dict = {
        "id": "srv_1",
        "name": _NAMED,
        "identity": {"vendor": "dell", "serial": "DEL0000485", "nic_macs": ["aa:bb:cc:dd:ee:01"] * 8},
        "source_provider": "OPENMANAGE",
        "site_id": "tlv",
        "network": {
            "bmc": {"host": "10.11.1.229", "host_is_ip": True, "scheme": "redfish", "mac": "x"},
            "interfaces": [
                {"name": "NIC.Integrated.1-1-1", "mac": "aa:bb:cc:dd:ee:01", "location": "1/1/1",
                 "link_state": "UP", "speed_mbps": 25000},
                {"name": "NIC.Integrated.1-2-1", "mac": "aa:bb:cc:dd:ee:02", "location": "1/2/1",
                 "link_state": "DOWN", "speed_mbps": 25000},
            ],
        },
        "health": {"overall": "MAJOR", "network": "MAJOR", "storage": "MAJOR", "cpu": "HEALTHY",
                   "evaluated_at": "2026-10-31T08:00:00Z", "active_policy_keys": ["raid"]},
        "reachable": True,
        "maintenance": {"enabled": False, "reason": None},
        "reservation": {"holder": None},
        "openshift": {"lifecycle_state": "AVAILABLE", "cluster_name": None},
    }
    detail.update(overrides)
    return detail


def _scan_serves(*details: dict) -> None:
    respx.get(f"{SS}/servers").mock(
        return_value=httpx.Response(200, json={"items": [{"id": d["id"], "name": d["name"]} for d in details]})
    )
    for d in details:
        respx.get(f"{SS}/servers/{d['id']}").mock(return_value=httpx.Response(200, json=d))


class TestTheStoredRecordOfANamedServer:
    """install_candidate_by_name: why /servers/available refused a named machine."""

    @respx.mock
    async def test_it_projects_the_detail_as_servers_available_would(self):
        _scan_serves(_detail())
        record = await install_candidate_by_name(SS, "t", _NAMED.lower())

        server = record.server
        assert server is not None
        assert (server.id, server.name, server.vendor, server.bmc_vendor) == (
            "srv_1", _NAMED, "dell", "DELL",
        )
        assert server.bmc.host == "10.11.1.229" and server.bmc.host_is_ip is True
        # interfaces[], never nic_macs — with their link state.
        assert [(i.mac, i.link_state) for i in server.interfaces] == [
            ("aa:bb:cc:dd:ee:01", LinkState.UP),
            ("aa:bb:cc:dd:ee:02", LinkState.DOWN),
        ]
        assert server.live_recheck_performed is False
        assert server.health_overall == "MAJOR"
        assert record.health == {"overall": "MAJOR", "cpu": "HEALTHY", "storage": "MAJOR", "network": "MAJOR"}
        assert record.reachable is True
        assert record.membership.found and record.membership.lifecycle_state == "AVAILABLE"
        assert record.lock.live is False

    @pytest.mark.parametrize(
        ("vendor", "provider", "bmc_vendor"),
        [
            ("cisco", "UCS_CENTRAL", "CISCO"),
            ("cisco", "INTERSIGHT", "INTERSIGHT"),
            ("hp", "ONEVIEW", "HP"),
            ("dell", "OPENMANAGE", "DELL"),
            ("standalone", "REDFISH_STANDALONE", None),
        ],
    )
    @respx.mock
    async def test_the_driver_vocabulary_is_server_scans_own(self, vendor, provider, bmc_vendor):
        _scan_serves(_detail(identity={"vendor": vendor}, source_provider=provider))
        record = await install_candidate_by_name(SS, "t", _NAMED)
        assert record.server is not None and record.server.bmc_vendor == bmc_vendor

    @respx.mock
    async def test_a_lock_still_ahead_is_live_and_one_past_is_not(self):
        held = {"holder": "add-server-to-mce", "workflow_id": "run-a", "mce_cluster": "ocp4-a",
                "expires_at": "2026-10-31T12:00:00Z"}
        _scan_serves(_detail(reservation=held))
        before = datetime(2026, 10, 31, 11, 0, tzinfo=UTC)
        after = datetime(2026, 10, 31, 13, 0, tzinfo=UTC)

        live = await install_candidate_by_name(SS, "t", _NAMED, now=before)
        assert live.lock.live is True
        assert (live.lock.holder, live.lock.workflow_id, live.lock.mce_cluster) == (
            "add-server-to-mce", "run-a", "ocp4-a",
        )
        assert (await install_candidate_by_name(SS, "t", _NAMED, now=after)).lock.live is False

    @respx.mock
    async def test_a_held_lock_with_no_expiry_is_live(self):
        # `reserve` never writes one, so it is a hand-edit; server-scan honours it.
        _scan_serves(_detail(reservation={"holder": "operator", "expires_at": None}))
        assert (await install_candidate_by_name(SS, "t", _NAMED)).lock.live is True

    @respx.mock
    async def test_a_name_server_scan_does_not_know(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": []}))
        record = await install_candidate_by_name(SS, "t", _NAMED)
        assert record.membership.found is False and record.server is None

    @respx.mock
    async def test_two_documents_of_one_name_are_ambiguous(self):
        _scan_serves(_detail(), _detail(id="srv_9"))
        with pytest.raises(AmbiguousServerNameError, match="which machine to install"):
            await install_candidate_by_name(SS, "t", _NAMED)

    @respx.mock
    async def test_a_search_hit_with_another_name_is_not_this_server(self):
        _scan_serves(_detail(name=f"{_NAMED}-2"))
        assert (await install_candidate_by_name(SS, "t", _NAMED)).membership.found is False
