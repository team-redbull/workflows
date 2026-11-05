"""The vm-lifecycle limb: the two agent clients, the capacity fan-out, the MAC, the
heartbeating ready wait (activities/vm_lifecycle/).

Which failures are classified how is the point of most tests here: the
workflow retries without bound, so a permanent answer must be its own class
and a transient one must stay retryable. The token never appears in a message.
"""

from __future__ import annotations

import dataclasses
import shutil
import ssl
import subprocess
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from temporalio.testing import ActivityEnvironment

from activities.vm_lifecycle import activities, kv_capacity_api, kv_vm_api
from activities.vm_lifecycle.kv_capacity_api import KvProbeFailure
from shared.exceptions import (
    KvAgentAuthError,
    KvCapacityUnavailableError,
    KvVmApiUnavailableError,
    VmDisappearedError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
)
from shared.models.vm_lifecycle import (
    KvCapacityReport,
    KvClusterRef,
    KvPlacementRequest,
    KvVmCreateRequest,
    VmCreateRequest,
    VmDeleteRequest,
    VmReadyRequest,
)

TOKEN = "s3cret-fleet-token"
URL = "https://kv-capacity.apps.kubevirt-s-a1.test"
VM_URL = "https://kv-vm-api.apps.kubevirt-s-a1.test"
GIB = 1024**3


def _ref(n: int) -> KvClusterRef:
    return KvClusterRef(
        name=f"ocp4-kubevirt-s-a{n}", cluster_name=f"kubevirt-s-a{n}", hosted_cluster=f"openshift-kubevirt-s-a{n}",
        site="s", zone="a",
    )


def _report(n: int = 1, macs=("02:a1:b2:00:00:01",)) -> dict:
    return {
        "cluster": f"kubevirt-s-a{n}", "observed_at": "2026-11-05T10:00:00Z",
        "nodes": [{"name": "n1", "schedulable": True, "allocatable_cpu_millicores": 64000,
                   "allocatable_memory_bytes": 512 * GIB, "requested_cpu_millicores": 4000,
                   "requested_memory_bytes": 8 * GIB}],
        "halted_reservations": [],
        "vms": [{"namespace": "inventory", "name": f"vm{i}", "mac": mac} for i, mac in enumerate(macs)],
    }


def _state(*, phase="Succeeded", bound=True, admitted=True) -> dict:
    return {"name": "ocp-x201-zone-a-8-0a1b2c", "namespace": "inventory", "mac": "02:a1:b2:0a:1b:2c", "cpu": 8,
            "memory_gib": 64, "run_strategy": "Halted", "admitted": admitted,
            "root_disk": {"name": "ocp-x201-zone-a-8-0a1b2c-root", "phase": phase, "bound": bound}}


async def _capacity(token: str = TOKEN):
    return await kv_capacity_api.read_capacity(URL, token, verify=False, timeout=5)


class TestTheCapacityClient:
    @respx.mock
    async def test_a_report_comes_back_typed_with_the_token_in_the_header_only(self):
        route = respx.get(f"{URL}/v1/capacity").mock(return_value=httpx.Response(200, json=_report()))
        report = await _capacity()
        assert isinstance(report, KvCapacityReport)
        assert report.nodes[0].allocatable_cpu_millicores == 64000
        assert route.calls.last.request.headers["Authorization"] == f"Bearer {TOKEN}"
        assert TOKEN not in str(route.calls.last.request.url)

    @respx.mock
    async def test_no_token_no_header(self):
        route = respx.get(f"{URL}/v1/capacity").mock(return_value=httpx.Response(200, json=_report()))
        await _capacity(token="")
        assert "Authorization" not in route.calls.last.request.headers

    @pytest.mark.parametrize(
        ("answer", "reason"),
        [
            (httpx.Response(401), "refused-token"),
            (httpx.Response(403), "refused-token"),
            (httpx.Response(404), "no-agent"),
            (httpx.Response(503, text="Application is not available"), "no-agent"),
            (httpx.Response(500), "unreachable"),
            (httpx.Response(502), "unreachable"),
            (httpx.Response(422, json={"error": "bad"}), "malformed"),
            (httpx.Response(200, json={"cluster": "x"}), "malformed"),
            (httpx.Response(200, content=b"<html>"), "malformed"),
        ],
    )
    @respx.mock
    async def test_every_answer_that_is_not_a_report_is_a_classified_failure(self, answer, reason):
        respx.get(f"{URL}/v1/capacity").mock(return_value=answer)
        failure = await _capacity()
        assert isinstance(failure, KvProbeFailure)
        assert failure.reason == reason
        assert TOKEN not in failure.detail

    @pytest.mark.parametrize("error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"),
                                       httpx.RemoteProtocolError("reset")])
    @respx.mock
    async def test_transport_errors_are_unreachable_never_raised(self, error):
        respx.get(f"{URL}/v1/capacity").mock(side_effect=error)
        failure = await _capacity()
        assert failure.reason == "unreachable"


async def _put(**overrides) -> object:
    request = KvVmCreateRequest(namespace="inventory", cpu=8, memory_gib=64, mac="02:a1:b2:0a:1b:2c")
    return await kv_vm_api.put_vm(VM_URL, TOKEN, name="ocp-x201-zone-a-8-0a1b2c", request=request, verify=False, timeout=5)


class TestTheVmClient:
    @pytest.mark.parametrize(("status", "created"), [(201, True), (200, False)])
    @respx.mock
    async def test_put_answers_created_or_already_there(self, status, created):
        body = {"name": "ocp-x201-zone-a-8-0a1b2c", "namespace": "inventory", "mac": "02:a1:b2:0a:1b:2c",
                "cpu": 8, "memory_gib": 64, "created": created}
        route = respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(status, json=body))
        response = await _put()
        assert response.created is created
        sent = route.calls.last.request
        assert sent.headers["Authorization"] == f"Bearer {TOKEN}"
        assert b'"mac":"02:a1:b2:0a:1b:2c"' in sent.content

    @respx.mock
    async def test_a_409_is_an_identity_conflict_quoting_what_is_held(self):
        respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(409, json={
            "reason": "a VM of this name exists with another MAC", "existing": _state() | {"mac": "02:a1:b2:ff:ff:ff"},
        }))
        with pytest.raises(VmIdentityConflictError, match="another MAC.*02:a1:b2:ff:ff:ff"):
            await _put()

    @pytest.mark.parametrize(
        ("answer", "error"),
        [
            (httpx.Response(404, json={"error": "namespace-not-found", "detail": "no inventory"}), VmNamespaceNotFoundError),
            (httpx.Response(422, json={"error": "invalid", "detail": "memory must be a number"}), VmRequestRejectedError),
            (httpx.Response(401), KvAgentAuthError),
            (httpx.Response(403), KvAgentAuthError),
            (httpx.Response(500), KvVmApiUnavailableError),
            (httpx.Response(503), KvVmApiUnavailableError),
            (httpx.Response(200, content=b"not json"), KvVmApiUnavailableError),
        ],
    )
    @respx.mock
    async def test_every_other_put_answer_is_classified(self, answer, error):
        respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=answer)
        with pytest.raises(error) as raised:
            await _put()
        assert TOKEN not in str(raised.value)

    @respx.mock
    async def test_the_agents_own_wording_is_carried(self):
        respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(
            return_value=httpx.Response(422, json={"error": "invalid", "detail": "memory must be a number"}))
        with pytest.raises(VmRequestRejectedError, match="invalid: memory must be a number"):
            await _put()

    @pytest.mark.parametrize("error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow")])
    @respx.mock
    async def test_transport_errors_are_retryable(self, error):
        respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(side_effect=error)
        with pytest.raises(KvVmApiUnavailableError):
            await _put()

    @respx.mock
    async def test_get_returns_the_state_or_none(self):
        route = respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(200, json=_state()))
        state = await kv_vm_api.get_vm(VM_URL, TOKEN, name="ocp-x201-zone-a-8-0a1b2c", namespace="inventory", verify=False, timeout=5)
        assert state.root_disk.bound and state.admitted
        assert route.calls.last.request.url.params["namespace"] == "inventory"
        route.mock(return_value=httpx.Response(404, json={"error": "not-found"}))
        assert await kv_vm_api.get_vm(VM_URL, TOKEN, name="ocp-x201-zone-a-8-0a1b2c", namespace="inventory", verify=False, timeout=5) is None

    @pytest.mark.parametrize(("status", "deleted"), [(204, True), (404, False)])
    @respx.mock
    async def test_delete_is_deleted_or_already_gone(self, status, deleted):
        route = respx.delete(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(status))
        assert await kv_vm_api.delete_vm(VM_URL, TOKEN, name="ocp-x201-zone-a-8-0a1b2c", namespace="inventory",
                                         mac="02:a1:b2:0a:1b:2c", verify=False, timeout=5) is deleted
        params = route.calls.last.request.url.params
        assert (params["namespace"], params["mac"]) == ("inventory", "02:a1:b2:0a:1b:2c")

    @pytest.mark.parametrize(
        ("answer", "error"),
        [
            (httpx.Response(409, json={"reason": "the VM's MAC is not the one given"}), VmIdentityConflictError),
            (httpx.Response(401), KvAgentAuthError),
            (httpx.Response(500), KvVmApiUnavailableError),
        ],
    )
    @respx.mock
    async def test_delete_failures_are_classified(self, answer, error):
        respx.delete(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=answer)
        with pytest.raises(error):
            await kv_vm_api.delete_vm(VM_URL, TOKEN, name="ocp-x201-zone-a-8-0a1b2c", namespace="inventory",
                                      mac="02:a1:b2:0a:1b:2c", verify=False, timeout=5)


class TestTheActivitySettings:
    def test_the_mac_is_the_prefix_plus_the_suffix(self):
        assert activities.mac_for("0a1b2c") == "02:a1:b2:0a:1b:2c"

    def test_no_ca_means_no_verification_and_a_ca_means_a_context(self, tmp_path):
        assert activities.tls_verify("") is False
        openssl = shutil.which("openssl")
        if openssl is None:
            pytest.skip("openssl is needed to mint a CA for this test")
        ca = tmp_path / "ca.crt"
        subprocess.run(
            [openssl, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
             "-keyout", str(tmp_path / "ca.key"), "-out", str(ca), "-days", "1", "-subj", "/CN=test-ca"],
            check=True, capture_output=True,
        )
        assert isinstance(activities.tls_verify(str(ca)), ssl.SSLContext)

    def test_the_http_budgets_sit_under_the_workflows_timeouts(self):
        from workflow_domains.vm_lifecycle import create_vm

        assert activities.HTTP_TIMEOUT_SECONDS < create_vm._ACTIVITY_TIMEOUT.total_seconds()
        assert activities.FANOUT_TIMEOUT_SECONDS < create_vm._RANK_TIMEOUT.total_seconds()
        assert activities.PROBE_TIMEOUT_SECONDS < activities.FANOUT_TIMEOUT_SECONDS
        assert activities.READY_POLL_SECONDS < create_vm._READY_HEARTBEAT_TIMEOUT.total_seconds() / 2
        assert create_vm._READY_DEADLINE_MINUTES * 60 < create_vm._READY_TIMEOUT.total_seconds()


def _env(heartbeat_details: list | None = None) -> ActivityEnvironment:
    env = ActivityEnvironment()
    if heartbeat_details is not None:
        env.info = dataclasses.replace(env.info, heartbeat_details=heartbeat_details)
    return env


class TestRankKvClusters:
    @respx.mock
    async def test_the_fan_out_reads_every_cluster_and_unions_their_macs(self):
        respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(
            return_value=httpx.Response(200, json=_report(1, macs=("02:A1:B2:00:00:01",))))
        respx.get("https://kv-capacity.apps.kubevirt-s-a2.test/v1/capacity").mock(side_effect=httpx.ConnectError("x"))
        respx.get("https://kv-capacity.apps.kubevirt-s-a3.test/v1/capacity").mock(
            return_value=httpx.Response(200, json=_report(3, macs=("02:a1:b2:00:00:02", "02:a1:b2:00:00:01"))))
        table = await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1), _ref(2), _ref(3)], cpu=8, memory_gib=64))
        assert table.chosen in {"ocp4-kubevirt-s-a1", "ocp4-kubevirt-s-a3"}
        assert {row.name: row.skip_reason for row in table.ranked}["ocp4-kubevirt-s-a2"] == "unreachable"
        assert table.known_macs == ["02:a1:b2:00:00:01", "02:a1:b2:00:00:02"]
        assert table.buffer_percent == 30
        assert {row.agent_host for row in table.ranked} == {
            "kv-capacity.apps.kubevirt-s-a1.test", "kv-capacity.apps.kubevirt-s-a2.test", "kv-capacity.apps.kubevirt-s-a3.test"}

    @respx.mock
    async def test_the_url_is_rendered_from_the_template_and_carries_the_configured_token(self):
        route = respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(
            return_value=httpx.Response(200, json=_report()))
        await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1)], cpu=8, memory_gib=64))
        assert route.calls.last.request.headers["Authorization"] == "Bearer test-kv-capacity-token"

    @respx.mock
    async def test_nothing_readable_is_retried(self):
        respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(side_effect=httpx.ConnectError("x"))
        respx.get("https://kv-capacity.apps.kubevirt-s-a2.test/v1/capacity").mock(return_value=httpx.Response(503))
        with pytest.raises(KvCapacityUnavailableError, match="ocp4-kubevirt-s-a1.*ocp4-kubevirt-s-a2"):
            await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1), _ref(2)], cpu=8, memory_gib=64))

    @respx.mock
    async def test_every_cluster_refusing_the_token_is_permanent(self):
        respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(return_value=httpx.Response(401))
        respx.get("https://kv-capacity.apps.kubevirt-s-a2.test/v1/capacity").mock(return_value=httpx.Response(403))
        with pytest.raises(KvAgentAuthError):
            await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1), _ref(2)], cpu=8, memory_gib=64))

    @respx.mock
    async def test_one_refusal_among_readable_clusters_is_a_skip(self):
        respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(return_value=httpx.Response(401))
        respx.get("https://kv-capacity.apps.kubevirt-s-a2.test/v1/capacity").mock(return_value=httpx.Response(200, json=_report(2)))
        table = await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1), _ref(2)], cpu=8, memory_gib=64))
        assert table.chosen == "ocp4-kubevirt-s-a2"
        assert {row.name: row.skip_reason for row in table.ranked}["ocp4-kubevirt-s-a1"] == "refused-token"

    @respx.mock
    async def test_a_cluster_the_fan_out_budget_cuts_off_is_unreachable(self, monkeypatch):
        monkeypatch.setattr(activities, "FANOUT_TIMEOUT_SECONDS", 0.05)

        async def slow(request):
            import asyncio
            await asyncio.sleep(1)
            return httpx.Response(200, json=_report(1))

        respx.get("https://kv-capacity.apps.kubevirt-s-a1.test/v1/capacity").mock(side_effect=slow)
        respx.get("https://kv-capacity.apps.kubevirt-s-a2.test/v1/capacity").mock(return_value=httpx.Response(200, json=_report(2)))
        table = await _env().run(activities.rank_kv_clusters, KvPlacementRequest(candidates=[_ref(1), _ref(2)], cpu=8, memory_gib=64))
        rows = {row.name: row for row in table.ranked}
        assert rows["ocp4-kubevirt-s-a1"].skip_reason == "unreachable" and "fan-out" in rows["ocp4-kubevirt-s-a1"].skip_detail
        assert table.chosen == "ocp4-kubevirt-s-a2"


class TestCreateAndDelete:
    @respx.mock
    async def test_create_composes_the_mac_and_reports_the_cluster(self):
        body = {"name": "ocp-x201-zone-a-8-0a1b2c", "namespace": "inventory", "mac": "02:a1:b2:0a:1b:2c",
                "cpu": 8, "memory_gib": 64, "created": True}
        route = respx.put(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(201, json=body))
        created = await _env().run(activities.create_vm_on_cluster, VmCreateRequest(
            kv_cluster=_ref(1), namespace="inventory", name="ocp-x201-zone-a-8-0a1b2c", cpu=8, memory_gib=64, suffix_hex="0a1b2c"))
        sent = route.calls.last.request
        assert sent.headers["Authorization"] == "Bearer test-kv-vm-api-token"
        assert b'"mac":"02:a1:b2:0a:1b:2c"' in sent.content and b'"labels":{}' in sent.content
        assert (created.mac, created.created, created.kv_cluster) == ("02:a1:b2:0a:1b:2c", True, "ocp4-kubevirt-s-a1")

    @respx.mock
    async def test_delete_sends_the_runs_mac(self):
        route = respx.delete(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(204))
        deleted = await _env().run(activities.delete_vm_on_cluster, VmDeleteRequest(
            kv_cluster=_ref(1), namespace="inventory", name="ocp-x201-zone-a-8-0a1b2c", mac="02:a1:b2:0a:1b:2c"))
        assert deleted.deleted is True
        assert route.calls.last.request.url.params["mac"] == "02:a1:b2:0a:1b:2c"


class TestAwaitVmReady:
    def _request(self, deadline_minutes: int = 10) -> VmReadyRequest:
        return VmReadyRequest(kv_cluster=_ref(1), namespace="inventory", name="ocp-x201-zone-a-8-0a1b2c",
                              deadline_minutes=deadline_minutes)

    @respx.mock
    async def test_it_polls_and_heartbeats_until_the_disk_is_bound(self, monkeypatch):
        monkeypatch.setattr(activities, "READY_POLL_SECONDS", 0.01)
        route = respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c")
        route.side_effect = [
            httpx.Response(200, json=_state(phase="Pending", bound=False, admitted=False)),
            httpx.Response(200, json=_state(phase="ImportInProgress", bound=True)),
            httpx.Response(200, json=_state()),
        ]
        beats: list = []
        env = _env()
        env.on_heartbeat = lambda *details: beats.append(details)
        outcome = await env.run(activities.await_vm_ready, self._request())
        assert (outcome.result, outcome.polls, outcome.root_disk_phase, outcome.admitted) == ("done", 3, "Succeeded", True)
        assert len(beats) == 3
        assert beats[-1][0]["polls"] == 3 and "started_at" in beats[-1][0]

    @respx.mock
    async def test_a_retry_resumes_the_clock_from_the_heartbeat(self):
        respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(
            return_value=httpx.Response(200, json=_state(phase="Pending", bound=False)))
        started = (datetime.now(UTC) - timedelta(minutes=11)).isoformat()
        outcome = await _env([{"started_at": started, "polls": 70}]).run(activities.await_vm_ready, self._request())
        assert outcome.result == "deadline"
        assert outcome.polls == 71
        assert (outcome.root_disk_phase, outcome.root_disk_bound) == ("Pending", False)
        assert "Pending" in outcome.detail

    @respx.mock
    async def test_an_unreadable_heartbeat_starts_the_clock_afresh(self):
        respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(200, json=_state()))
        outcome = await _env(["garbage"]).run(activities.await_vm_ready, self._request())
        assert (outcome.result, outcome.polls) == ("done", 1)

    @respx.mock
    async def test_a_vm_that_vanished_is_permanent(self):
        respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(return_value=httpx.Response(404))
        with pytest.raises(VmDisappearedError):
            await _env().run(activities.await_vm_ready, self._request())

    @respx.mock
    async def test_an_agent_outage_is_raised_retryably(self):
        respx.get(f"{VM_URL}/v1/vms/ocp-x201-zone-a-8-0a1b2c").mock(side_effect=httpx.ConnectError("x"))
        with pytest.raises(KvVmApiUnavailableError):
            await _env().run(activities.await_vm_ready, self._request())
