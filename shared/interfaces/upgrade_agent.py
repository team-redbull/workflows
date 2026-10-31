"""Upgrade-agent activity signature — the domain's PER-MCE agent limb.

Registered by `upgrade-agent-worker` inside each MCE, polling
`upgrade_agent_activity_queue(<mce>)`. The worker holds no Kubernetes
credential: it calls the kagent `upgrade-agent` over A2A, and the agent's tools
run in the `upgrade-tools` server under the agent's own identity, fenced by
RBAC and admission policy rather than by a prompt (DESIGN §8, §9).

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

    Loads the Brain context for the step and profile, calls the agent, and
    validates its reply against AgentVerdict: a malformed reply is returned
    as `needs_human`, never raised. Heartbeats while the agent works.
    """
    ...
