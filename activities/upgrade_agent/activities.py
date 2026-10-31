"""The upgrade-agent limb's activity: one call to the agent service, heartbeating while it works.

The limb holds no Kubernetes credential (CLAUDE.md §2). The agent service
(k8s-agent's `upgrade-agent`, an agent-sandbox Sandbox in this MCE) holds the
model endpoint and the agent's own read-only identities; this side only
carries the request there and the verdict back.
"""

from __future__ import annotations

import asyncio

from temporalio import activity
from temporalio.exceptions import ApplicationError

from activities.upgrade_agent import agent_api
from shared.exceptions import UpgradeAgentRefusedError
from shared.models.cluster_upgrade import AgentRequest, AgentVerdict
from shared.settings import UpgradeAgentActivitySettings

_settings = UpgradeAgentActivitySettings()

# Well inside the activity's 60 s heartbeat_timeout (dispatch.AGENT_HEARTBEAT_TIMEOUT).
HEARTBEAT_EVERY_SECONDS = 20.0
# The HTTP timeout: the request's own wall clock plus this, which stays below
# the activity's start_to_close_timeout (wall clock + 120 s, dispatch.py), so a
# hung call fails here, retryably, before Temporal reaps the activity.
HTTP_MARGIN_SECONDS = 60


def http_timeout(request: AgentRequest) -> float:
    return float(request.budget.wall_clock_seconds + HTTP_MARGIN_SECONDS)


@activity.defn
async def invoke_upgrade_agent(request: AgentRequest) -> AgentVerdict:
    """One agent invocation, within `request.budget` (shared/interfaces/upgrade_agent.py)."""
    call = asyncio.create_task(
        agent_api.invoke(_settings.upgrade_agent_url, _settings.upgrade_agent_token, request,
                         timeout=http_timeout(request)))
    try:
        while True:
            done, _ = await asyncio.wait({call}, timeout=HEARTBEAT_EVERY_SECONDS)
            if done:
                break
            activity.heartbeat()
        verdict = call.result()
    except UpgradeAgentRefusedError as err:
        raise ApplicationError(str(err), type=UpgradeAgentRefusedError.__name__, non_retryable=True) from None
    finally:
        if not call.done():  # cancelled (an abort): stop the call, the service keeps its own run
            call.cancel()
    activity.logger.info(
        "upgrade agent %s for %s: %s, %d tool calls, %d proposals", request.invocation_id, request.cluster,
        verdict.verdict, len(verdict.actions_taken), len(verdict.actions_proposed),
    )
    return verdict
