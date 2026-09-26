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

import httpx
import pytest
import respx
from kubernetes import client as k8s_client

from activities.server_lifecycle import cluster_api
from activities.server_lifecycle.server_scan import fetch_available_servers
from shared.exceptions import (
    AmbiguousServerNameError,
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    BmhResourceError,
    ServerNotAvailableError,
    ServerScanAuthError,
    ServerScanError,
)
from shared.models.server_lifecycle import AcquireServerRequest, LinkState

SS = "http://server-scan.test/api/v1"
AVAILABLE = f"{SS}/servers/available"

PATTERN_REQUEST = AcquireServerRequest(
    pattern="^ocp-dell-r650-tlv", count=3, health="HEALTHY", min_nic_macs=2
)
NAME_REQUEST = AcquireServerRequest(
    name="ocp-dell-r650-tlv-64c-1024gb-DEL0000485",
    count=1,
    health="HEALTHY",
    min_nic_macs=2,
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
        assert params["min_nic_macs"] == "2"
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
