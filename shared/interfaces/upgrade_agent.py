"""Upgrade-agent activity signature — the domain's PER-MCE agent limb.

Registered by `upgrade-agent-worker` inside each MCE, polling
`upgrade_agent_activity_queue(<mce>)`. The worker holds no Kubernetes
credential: it POSTs the request to the upgrade agent service in the same MCE
(the k8s-agent folder: our own small service, run as a kubernetes-sigs
agent-sandbox `Sandbox`, talking to an OpenAI-compatible model). The service's
tools are read-only and run under the agent's own identities, fenced by RBAC
rather than by a prompt (DESIGN §8, §9). It replaces the kagent plan (D9).

The agent is invoked only when a gate fails or a watch stalls. It diagnoses
and proposes; control then returns to the same gate, and the workflow alone
decides what happens next.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.cluster_upgrade import AgentRequest, AgentVerdict


@activity.defn
async def invoke_upgrade_agent(request: AgentRequest) -> AgentVerdict:
    """One agent invocation, within `request.budget`.

    The service loads the Brain runbooks for the step and profile, runs the
    model, and answers; the reply is validated against AgentVerdict, and a
    malformed one is returned as `needs_human`, never raised. Raised: an agent
    that cannot answer (UpgradeAgentUnavailableError, retried once with the
    same invocation id) or that refuses the request (UpgradeAgentRefusedError,
    non-retryable). Heartbeats while the agent works.
    """
    ...
