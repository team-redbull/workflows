"""The upgrade-agent limb: the agent service's HTTP client and the heartbeating activity.

Which failures raise (so Temporal's two attempts apply) and which come back as
needs_human (the interface's promise for a malformed reply) is the point of
most tests here.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import respx
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from activities.upgrade_agent import activities, agent_api
from shared.exceptions import UpgradeAgentRefusedError, UpgradeAgentUnavailableError
from shared.models.cluster_upgrade import (
    AgentBudget,
    AgentRequest,
    AgentVerdict,
    CheckResult,
    ClusterProfile,
    EvidenceBundle,
    StepKind,
    StepReason,
    UpgradeStep,
)

URL = "http://upgrade-agent.test:8080"
INVOKE = f"{URL}/v1/invoke"


def _request(**overrides) -> AgentRequest:
    fields = dict(
        invocation_id="upgrade-cluster-demo-hc/0b8e", workflow_id="upgrade-cluster-demo-hc",
        mce_cluster="ocp4-mce-test", cluster="demo-hc", profile=ClusterProfile.GENERIC, phase="preflight",
        step=UpgradeStep(kind=StepKind.CONTROL_PLANE, version="4.20.18", reason=StepReason.TARGET),
        gate="preflight",
        failing_checks=[CheckResult(name="cluster-operators", passed=False,
                                    observed="image-registry (Degraded=True: ImagePrunerJobFailed)")],
        evidence=EvidenceBundle(sections={"clusteroperators": "image-registry (Degraded=True: ImagePrunerJobFailed)"}),
        hosted_cluster="openshift-demo-hc", hosted_cluster_namespace="hcp-demo-hc",
    )
    fields.update(overrides)
    return AgentRequest(**fields)


def _verdict(**overrides) -> dict:
    body = {"diagnosis": "one image-pruner run failed", "confidence": "verified", "verdict": "needs_human",
            "actions_taken": [{"tool": "hosted_list", "args": {"kind": "Job"}, "tier": 0,
                               "result": "1 Job", "at": "2026-10-31T00:10:00Z"}],
            "actions_proposed": [{"action_id": "3fa1c2d4-1", "kind": "rerun_cronjob",
                                  "target": "openshift-image-registry/image-pruner", "rationale": "KCS 6988893"}]}
    body.update(overrides)
    return body


async def _invoke(request: AgentRequest | None = None, token: str = "t") -> AgentVerdict:
    return await agent_api.invoke(URL, token, request or _request(), timeout=5)


class TestTheClient:
    @respx.mock
    async def test_it_posts_the_request_with_the_token_and_returns_the_verdict(self):
        route = respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict()))
        verdict = await _invoke(token="agent-token")
        sent = route.calls.last.request
        assert sent.headers["Authorization"] == "Bearer agent-token"
        body = json.loads(sent.content)
        assert body["invocation_id"] == "upgrade-cluster-demo-hc/0b8e"
        assert body["hosted_cluster"] == "openshift-demo-hc"
        assert verdict.actions_proposed[0].target == "openshift-image-registry/image-pruner"
        assert verdict.actions_taken[0].tool == "hosted_list"

    @respx.mock
    async def test_no_token_no_header(self):
        route = respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict()))
        await _invoke(token="")
        assert "Authorization" not in route.calls.last.request.headers

    @pytest.mark.parametrize("error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"),
                                       httpx.RemoteProtocolError("reset")])
    @respx.mock
    async def test_an_agent_that_cannot_answer_raises_retryably(self, error):
        respx.post(INVOKE).mock(side_effect=error)
        with pytest.raises(UpgradeAgentUnavailableError):
            await _invoke()

    @pytest.mark.parametrize("status", [429, 500, 502, 503])
    @respx.mock
    async def test_busy_or_broken_is_retryable(self, status):
        respx.post(INVOKE).mock(return_value=httpx.Response(status))
        with pytest.raises(UpgradeAgentUnavailableError, match=f"HTTP {status}"):
            await _invoke()

    @pytest.mark.parametrize("status", [401, 403, 404, 422])
    @respx.mock
    async def test_a_refusal_is_its_own_error(self, status):
        respx.post(INVOKE).mock(return_value=httpx.Response(status, json={"detail": "nope"}))
        with pytest.raises(UpgradeAgentRefusedError, match=f"HTTP {status}"):
            await _invoke()

    @pytest.mark.parametrize("body", [b"not json", b'{"diagnosis": "x"}',
                                      json.dumps(_verdict(verdict="continue")).encode()])
    @respx.mock
    async def test_a_malformed_reply_is_needs_human_never_raised(self, body):
        respx.post(INVOKE).mock(return_value=httpx.Response(200, content=body))
        verdict = await _invoke()
        assert verdict.verdict == "needs_human"
        assert verdict.diagnosis.startswith("the upgrade agent's reply was not a verdict")
        assert verdict.actions_proposed == []

    @respx.mock
    async def test_a_proposal_outside_the_catalog_never_reaches_the_workflow(self):
        respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict(verdict="retry_gate", actions_proposed=[
            {"action_id": "a-1", "kind": "delete_namespace", "target": "openshift-image-registry"},
            {"action_id": "a-2", "kind": "delete_pod", "target": "payments/api-0"},
            {"action_id": "a-3", "kind": "rerun_cronjob", "target": "openshift-image-registry/image-pruner"},
        ])))
        verdict = await _invoke()
        assert [(a.action_id, a.kind) for a in verdict.actions_proposed] == [("a-3", "rerun_cronjob")]
        assert verdict.verdict == "needs_human"
        assert "delete_namespace" in verdict.diagnosis and "payments/api-0" in verdict.diagnosis

    @respx.mock
    async def test_action_ids_are_made_unique_and_url_safe(self):
        respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict(actions_proposed=[
            {"action_id": "a/1", "kind": "rerun_cronjob", "target": "openshift-image-registry/image-pruner"},
            {"action_id": "x", "kind": "delete_failed_job", "target": "openshift-image-registry/image-pruner-1"},
            {"action_id": "x", "kind": "delete_pod", "target": "openshift-console/console-1"},
        ])))
        ids = [a.action_id for a in (await _invoke()).actions_proposed]
        assert len(set(ids)) == 3 and all(i.replace("-", "").isalnum() for i in ids)

    @respx.mock
    async def test_a_clean_verdict_is_returned_unchanged(self):
        respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict()))
        verdict = await _invoke()
        assert verdict == AgentVerdict.model_validate(_verdict())


class TestTheActivity:
    def test_the_http_timeout_is_below_the_activitys_start_to_close(self):
        from workflow_domains.cluster_upgrade import dispatch

        for budget in (AgentBudget(), AgentBudget(wall_clock_seconds=60)):
            start_to_close = budget.wall_clock_seconds + dispatch.AGENT_TIMEOUT_MARGIN.total_seconds()
            assert budget.wall_clock_seconds < activities.http_timeout(_request(budget=budget)) < start_to_close
        assert activities.HEARTBEAT_EVERY_SECONDS < dispatch.AGENT_HEARTBEAT_TIMEOUT.total_seconds() / 2

    @respx.mock
    async def test_it_heartbeats_while_the_agent_works(self, monkeypatch):
        monkeypatch.setattr(activities, "HEARTBEAT_EVERY_SECONDS", 0.01)

        async def slow(request):
            await asyncio.sleep(0.1)
            return httpx.Response(200, json=_verdict())

        respx.post(INVOKE).mock(side_effect=slow)
        beats: list = []
        env = ActivityEnvironment()
        env.on_heartbeat = lambda *details: beats.append(details)

        verdict = await env.run(activities.invoke_upgrade_agent, _request())

        assert verdict.verdict == "needs_human"
        assert len(beats) >= 3

    @respx.mock
    async def test_a_refusal_is_non_retryable(self):
        respx.post(INVOKE).mock(return_value=httpx.Response(401, json={"detail": "missing or wrong bearer token"}))
        with pytest.raises(ApplicationError) as raised:
            await ActivityEnvironment().run(activities.invoke_upgrade_agent, _request())
        assert raised.value.type == UpgradeAgentRefusedError.__name__
        assert raised.value.non_retryable

    @respx.mock
    async def test_an_unreachable_agent_is_left_to_temporals_second_attempt(self):
        respx.post(INVOKE).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(UpgradeAgentUnavailableError):
            await ActivityEnvironment().run(activities.invoke_upgrade_agent, _request())

    @respx.mock
    async def test_it_uses_the_configured_endpoint_and_token(self):
        route = respx.post(INVOKE).mock(return_value=httpx.Response(200, json=_verdict()))
        await ActivityEnvironment().run(activities.invoke_upgrade_agent, _request())
        assert route.calls.last.request.headers["Authorization"] == "Bearer test-agent-token"
