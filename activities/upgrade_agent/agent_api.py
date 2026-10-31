"""The upgrade agent service's API: POST <url>/v1/invoke, AgentRequest in, AgentVerdict out.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). The service is
the k8s-agent folder's `upgrade-agent`, an agent-sandbox `Sandbox` in the same
MCE; it runs the model and its read-only tools, and answers within the
request's wall clock.

Three outcomes, chosen so Temporal's two attempts (dispatch.AGENT_RETRY_POLICY)
apply only where a second attempt can help:

  * the service could not answer — unreachable, timed out, 429, 5xx: raise
    UpgradeAgentUnavailableError (retryable). The retry carries the same
    invocation id, so the service returns or joins the first attempt's run;
  * it refused the request — any other 4xx (a wrong token, a contract it cannot
    decode): raise UpgradeAgentRefusedError, which the activity marks
    non-retryable;
  * it answered 2xx, but not with an AgentVerdict: RETURN needs_human saying so
    (the interface's promise: a malformed reply is never raised).

A verdict comes back through action_catalog.checked: what to keep of its
proposals is the catalog's policy, not this HTTP client's.

No retries here: Temporal owns them. The client lives for one call.
"""

from __future__ import annotations

import httpx
from pydantic import ValidationError

from activities.cluster_upgrade import action_catalog
from shared.exceptions import UpgradeAgentRefusedError, UpgradeAgentUnavailableError
from shared.models.cluster_upgrade import AgentRequest, AgentVerdict

INVOKE_PATH = "/v1/invoke"
_CONNECT_TIMEOUT = 10.0


def _malformed(why: str) -> AgentVerdict:
    return AgentVerdict(diagnosis=f"the upgrade agent's reply was not a verdict: {why}",
                        confidence="hypothesis", verdict="needs_human")


async def invoke(url: str, token: str, request: AgentRequest, *, timeout: float) -> AgentVerdict:
    """One call; `timeout` must stay below the activity's start_to_close_timeout (CLAUDE.md §5)."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=_CONNECT_TIMEOUT)) as client:
            response = await client.post(f"{url}{INVOKE_PATH}", json=request.model_dump(mode="json"), headers=headers)
    except httpx.TimeoutException as err:
        raise UpgradeAgentUnavailableError(f"the upgrade agent did not answer in time ({type(err).__name__})") from None
    except httpx.HTTPError as err:
        raise UpgradeAgentUnavailableError(f"the upgrade agent is unreachable ({type(err).__name__})") from None
    if response.status_code == 429 or response.status_code >= 500:
        raise UpgradeAgentUnavailableError(f"the upgrade agent answered HTTP {response.status_code}")
    if response.status_code >= 400:
        raise UpgradeAgentRefusedError(
            f"the upgrade agent refused the request: HTTP {response.status_code}: {response.text[:300]}")
    try:
        verdict = AgentVerdict.model_validate_json(response.content)
    except ValidationError as err:
        errors = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'body'}: {e['msg']}" for e in err.errors()[:3])
        return _malformed(errors)
    return action_catalog.checked(verdict)
