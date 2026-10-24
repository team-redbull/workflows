"""What uninstall-server reads off CAPI and HyperShift objects — pure functions only.

The sibling of bmh_resources.py for the hosted-cluster side: no client, no
settings, no Temporal, so every rule here is tested on plain dicts. Each shape
was taken from the upstream code that writes it, because none of it is
documented as an API:

  * cluster-api-provider-agent (`controllers/agentmachine_controller.go`) puts
    an `agentMachineRef` label (the AgentMachine's name) and an
    `agentMachineRefNamespace` annotation on the Agent it binds, sets
    `status.agentRef` {name, namespace} on the AgentMachine, and holds the
    Machine on a `pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine`
    hook while it unbinds. On a Machine deletion it clears the Agent's label,
    annotation and `spec.clusterDeploymentName` FIRST, then waits for the
    Agent to reach an unbound state before releasing the hook — so for that
    whole stretch only the AgentMachine's `agentRef` still links the two.
  * HyperShift (`hypershift-operator/.../platform/agent/agent.go`) creates the
    AgentCluster — and so the ClusterDeployment an Agent binds to — in the
    hosted control-plane namespace, named after the HostedCluster; that
    namespace is `<hc-namespace>-<hc-name with dots as dashes>`
    (`pkg/manifests.HostedControlPlaneNamespace`). An Agent's
    `clusterDeploymentName.namespace` is therefore the CONTROL-PLANE
    namespace, never the HostedCluster's own.
"""

from __future__ import annotations

from typing import Any

from shared.models.server_lifecycle import (
    AgentBindingState,
    HostedClusterRef,
    MachineState,
)

MACHINE_GROUP = "cluster.x-k8s.io"
MACHINE_PLURAL = "machines"
MACHINE_KIND = "Machine"

AGENT_MACHINE_GROUP = "capi-provider.agent-install.openshift.io"
AGENT_MACHINE_VERSION = "v1beta1"
AGENT_MACHINE_PLURAL = "agentmachines"
AGENT_MACHINE_KIND = "AgentMachine"

HOSTED_CLUSTER_GROUP = "hypershift.openshift.io"
HOSTED_CLUSTER_VERSION = "v1beta1"
HOSTED_CLUSTER_PLURAL = "hostedclusters"
HOSTED_CLUSTER_KIND = "HostedCluster"

AGENT_MACHINE_REF_LABEL = "agentMachineRef"
AGENT_MACHINE_REF_NAMESPACE_ANNOTATION = "agentMachineRefNamespace"

_DELETE_HOOK_PREFIXES = (
    "pre-drain.delete.hook.machine.cluster.x-k8s.io",
    "pre-terminate.delete.hook.machine.cluster.x-k8s.io",
)
# v1beta1's per-step deletion conditions: each one False/Unknown while its step
# has not succeeded yet.
_DELETION_STEP_CONDITIONS = (
    "PreDrainDeleteHookSucceeded",
    "DrainingSucceeded",
    "VolumeDetachSucceeded",
    "PreTerminateDeleteHookSucceeded",
)
# v1beta2's single condition: True WHILE deleting, its reason naming the step.
_DELETING_CONDITION = "Deleting"


def _metadata(obj: dict[str, Any]) -> dict[str, Any]:
    return obj.get("metadata") or {}


def split_api_version(api_version: str) -> tuple[str, str]:
    """`cluster.x-k8s.io/v1beta1` -> (`cluster.x-k8s.io`, `v1beta1`)."""
    group, _, version = api_version.partition("/")
    return group, version


def agent_machine_for(
    agent: dict[str, Any], agent_namespace: str, agent_machines: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """The AgentMachine behind one Agent, or None for an inventory host.

    By the provider's label first (with its namespace annotation, since
    AgentMachine names are only unique within a control-plane namespace), then
    by an AgentMachine whose `status.agentRef` names the Agent — the only link
    left once the provider has started unbinding it.
    """
    metadata = _metadata(agent)
    name = metadata.get("name")
    label = (metadata.get("labels") or {}).get(AGENT_MACHINE_REF_LABEL)
    label_namespace = (metadata.get("annotations") or {}).get(
        AGENT_MACHINE_REF_NAMESPACE_ANNOTATION
    )
    if label:
        for agent_machine in agent_machines:
            am = _metadata(agent_machine)
            if am.get("name") == label and label_namespace in (None, am.get("namespace")):
                return agent_machine
    for agent_machine in agent_machines:
        ref = (agent_machine.get("status") or {}).get("agentRef") or {}
        if ref.get("name") == name and (ref.get("namespace") or agent_namespace) == agent_namespace:
            return agent_machine
    return None


def qualified_name(obj: dict[str, Any]) -> str:
    metadata = _metadata(obj)
    return f"{metadata.get('namespace')}/{metadata.get('name')}"


def machine_owner(agent_machine: dict[str, Any]) -> tuple[str, str] | None:
    """(name, apiVersion) of the Machine that owns an AgentMachine, if it has one yet."""
    for owner in _metadata(agent_machine).get("ownerReferences") or []:
        group, _ = split_api_version(owner.get("apiVersion") or "")
        if owner.get("kind") == MACHINE_KIND and group == MACHINE_GROUP and owner.get("name"):
            return owner["name"], owner["apiVersion"]
    return None


def _conditions(status: dict[str, Any]) -> list[dict[str, Any]]:
    """Every condition list a Machine may carry, whichever CAPI API version wrote it.

    v1beta1 Machines keep theirs in `status.conditions` and, from CAPI 1.9, the
    v1beta2 ones in `status.v1beta2.conditions`; v1beta2 Machines keep theirs in
    `status.conditions` and the old ones under `status.deprecated.v1beta1`.
    """
    lists = [
        status.get("conditions"),
        (status.get("v1beta2") or {}).get("conditions"),
        ((status.get("deprecated") or {}).get("v1beta1") or {}).get("conditions"),
    ]
    return [c for conditions in lists for c in conditions or [] if isinstance(c, dict)]


def _describe(condition: dict[str, Any]) -> str:
    reason = f" ({condition['reason']})" if condition.get("reason") else ""
    message = f": {condition['message']}" if condition.get("message") else ""
    return f"{condition.get('type')}={condition.get('status')}{reason}{message}"


def machine_state(machine: dict[str, Any] | None) -> MachineState:
    """A Machine as uninstall-server watches it; found=False for None."""
    if machine is None:
        return MachineState(found=False)
    metadata = _metadata(machine)
    status = machine.get("status") or {}
    deletion_timestamp = metadata.get("deletionTimestamp") or None
    pending: list[str] = []
    seen: set[str] = set()
    for condition in _conditions(status):
        kind = condition.get("type")
        unfinished_step = kind in _DELETION_STEP_CONDITIONS and condition.get("status") != "True"
        deleting = kind == _DELETING_CONDITION and condition.get("status") == "True"
        described = _describe(condition)
        if (unfinished_step or deleting) and described not in seen:
            seen.add(described)
            pending.append(described)
    return MachineState(
        found=True,
        deleting=deletion_timestamp is not None,
        deletion_timestamp=deletion_timestamp,
        node_name=(status.get("nodeRef") or {}).get("name") or None,
        phase=status.get("phase") or None,
        pending_hooks=sorted(
            key
            for key in (metadata.get("annotations") or {})
            if key.startswith(_DELETE_HOOK_PREFIXES)
        ),
        conditions=pending,
    )


def hosted_control_plane_namespace(namespace: str, name: str) -> str:
    """HyperShift's rule: `<hc-namespace>-<hc-name>`, dots in the name as dashes."""
    return f"{namespace}-{name.replace('.', '-')}"


def hosted_cluster_for(
    hcp_namespace: str, hosted_clusters: list[dict[str, Any]]
) -> HostedClusterRef | None:
    """The HostedCluster whose control plane lives in `hcp_namespace`, if any.

    Matched by HyperShift's own naming rule rather than by trimming the
    namespace: `clusters-a-b` could be HostedCluster `clusters/a-b` or
    `clusters-a/b`, and only the list of HostedClusters says which exists.
    """
    for hosted_cluster in hosted_clusters:
        metadata = _metadata(hosted_cluster)
        namespace, name = metadata.get("namespace"), metadata.get("name")
        if namespace and name and hosted_control_plane_namespace(namespace, name) == hcp_namespace:
            return HostedClusterRef(namespace=namespace, name=name, hcp_namespace=hcp_namespace)
    return None


def agent_state(agent: dict[str, Any]) -> str | None:
    """`status.debugInfo.state`: `known-unbound`, `unbinding`, `added-to-existing-cluster`…"""
    return ((agent.get("status") or {}).get("debugInfo") or {}).get("state") or None


def agent_binding_state(agent: dict[str, Any] | None) -> AgentBindingState:
    """One Agent's binding as read now; found=False for None."""
    if agent is None:
        return AgentBindingState(found=False)
    ref = (agent.get("spec") or {}).get("clusterDeploymentName") or {}
    labels = _metadata(agent).get("labels") or {}
    return AgentBindingState(
        found=True,
        cluster_deployment=(
            f"{ref.get('namespace') or '-'}/{ref['name']}" if ref.get("name") else None
        ),
        agent_machine_label=bool(labels.get(AGENT_MACHINE_REF_LABEL)),
        state=agent_state(agent),
        approved=(agent.get("spec") or {}).get("approved"),
    )
