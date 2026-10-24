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
  * The same controller unbinds ONLY from a Machine's pre-terminate hook
    (`handleDeletionHook`). With no Machine — none owns the AgentMachine, or
    the owner is gone — it does nothing for the Agent: an AgentMachine whose
    owner is missing has its `agentmachine.agent-install.openshift.io/deprovision`
    finalizer dropped WITHOUT the unbind, and one with no owner at all keeps
    the finalizer for ever. A Machine whose AgentMachine is gone keeps the
    provider's hook for ever. These are the leftovers a broken HyperShift
    deletion leaves, and why uninstall-server does the provider's writes
    itself when the provider cannot (`agent_release_patch`, the hook, the
    finalizer).
  * It sets the AgentMachine's — and so the Machine's — `spec.providerID` to
    `agent://<agent name>`, and CAPI links a Machine to the Node whose
    `spec.providerID` matches. The Node's name is the Agent's hostname
    (`spec.hostname`, else `status.inventory.hostname`, as `getAddresses`
    reports it).
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

# The provider's own hold on a Machine, and on its AgentMachine.
PROVIDER_MACHINE_HOOK = "pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine"
AGENT_MACHINE_FINALIZER = "agentmachine.agent-install.openshift.io/deprovision"
_PROVIDER_ID_PREFIX = "agent://"

# Where the provider releases its hook: the Agent is back in some unbound state,
# or assisted-service has given up on the reclaim (handleDeletionHook's
# removeHookStates).
PROVIDER_RELEASE_STATES = frozenset({
    "discovering-unbound",
    "known-unbound",
    "disconnected-unbound",
    "insufficient-unbound",
    "disabled-unbound",
    "unbinding-pending-user-action",
    "error",
})

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
# Where the provider's hook takes over: past the drain, waiting on the
# pre-terminate hooks — as handleDeletionHook itself reads it.
_WAITING_FOR_PRE_TERMINATE_HOOK = "WaitingForPreTerminateHook"
_PRE_TERMINATE_STEP = "PreTerminateDeleteHookSucceeded"


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


def provider_id(agent_name: str) -> str:
    """The providerID the Agent provider gives an Agent's Machine, and CAPI its Node."""
    return f"{_PROVIDER_ID_PREFIX}{agent_name}"


def agent_hostname(agent: dict[str, Any]) -> str | None:
    """The hostname the Agent's Node registers under: requested, else discovered."""
    requested = (agent.get("spec") or {}).get("hostname")
    discovered = ((agent.get("status") or {}).get("inventory") or {}).get("hostname")
    return requested or discovered or None


def machine_agent_machine(machine: dict[str, Any]) -> str | None:
    """`<namespace>/<name>` of the AgentMachine a Machine's infrastructureRef names, if any.

    v1beta1 refs carry `apiVersion` (+ optional namespace), v1beta2 refs
    `apiGroup`; both carry `kind` and `name`, which is all that is compared.
    """
    ref = (machine.get("spec") or {}).get("infrastructureRef") or {}
    if ref.get("kind") != AGENT_MACHINE_KIND or not ref.get("name"):
        return None
    namespace = ref.get("namespace") or _metadata(machine).get("namespace")
    return f"{namespace}/{ref['name']}"


def machines_for(
    agent_name: str,
    agent_machine: dict[str, Any] | None,
    machines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Every Machine that belongs to this Agent, by any of the three links.

    The AgentMachine's owner, a Machine whose infrastructureRef names that
    AgentMachine (an ownerReference can be lost), and a Machine whose
    providerID is the Agent's — the one link left once the AgentMachine is
    gone. A healthy node has exactly one; more is a contradiction the caller
    refuses.
    """
    wanted_owner = None
    am_qualified = None
    if agent_machine is not None:
        owner = machine_owner(agent_machine)
        am_namespace = _metadata(agent_machine).get("namespace")
        wanted_owner = (am_namespace, owner[0]) if owner else None
        am_qualified = qualified_name(agent_machine)
    wanted_provider_id = provider_id(agent_name)
    found: dict[str, dict[str, Any]] = {}
    for machine in machines:
        metadata = _metadata(machine)
        key = (metadata.get("namespace"), metadata.get("name"))
        linked = (
            key == wanted_owner
            or (am_qualified is not None and machine_agent_machine(machine) == am_qualified)
            or (machine.get("spec") or {}).get("providerID") == wanted_provider_id
        )
        if linked:
            found[qualified_name(machine)] = machine
    return [found[name] for name in sorted(found)]


def agent_machines_naming(
    agent: dict[str, Any] | None,
    agent_name: str,
    agent_namespace: str,
    agent_machines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Every AgentMachine that names this Agent — by its `agentRef`, or the Agent's own label.

    `agent_machine_for` picks THE one behind an Agent; this is the
    verification's question, "is anything left?", so it returns all of them.
    `agent` may be None (deleted): then only `agentRef`s can name it.
    """
    labelled = None
    if agent is not None:
        metadata = _metadata(agent)
        label = (metadata.get("labels") or {}).get(AGENT_MACHINE_REF_LABEL)
        namespace = (metadata.get("annotations") or {}).get(AGENT_MACHINE_REF_NAMESPACE_ANNOTATION)
        labelled = (label, namespace) if label else None
    found = []
    for agent_machine in agent_machines:
        am = _metadata(agent_machine)
        ref = (agent_machine.get("status") or {}).get("agentRef") or {}
        by_ref = ref.get("name") == agent_name and (ref.get("namespace") or agent_namespace) == agent_namespace
        by_label = labelled is not None and am.get("name") == labelled[0] and labelled[1] in (
            None,
            am.get("namespace"),
        )
        if by_ref or by_label:
            found.append(agent_machine)
    return found


def agent_release_patch(resource_version: str | None) -> dict[str, Any]:
    """The provider's own unbind of an Agent, as a merge patch.

    Exactly what `handleDeletionHook` writes — the `agentMachineRef` label and
    namespace annotation removed, `machineConfigPool` emptied, the ignition
    token reference and headers dropped, `clusterDeploymentName` cleared — so
    assisted-service sees the same release it would have seen from the
    provider. With the resourceVersion it was read at, a concurrent change is
    a 409 and the activity re-reads before writing again.
    """
    metadata: dict[str, Any] = {
        "labels": {AGENT_MACHINE_REF_LABEL: None},
        "annotations": {AGENT_MACHINE_REF_NAMESPACE_ANNOTATION: None},
    }
    if resource_version:
        metadata["resourceVersion"] = resource_version
    return {
        "metadata": metadata,
        "spec": {
            "machineConfigPool": "",
            "ignitionEndpointTokenReference": None,
            "ignitionEndpointHTTPHeaders": None,
            "clusterDeploymentName": None,
        },
    }


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
    at_hook = False
    for condition in _conditions(status):
        kind = condition.get("type")
        unfinished_step = kind in _DELETION_STEP_CONDITIONS and condition.get("status") != "True"
        deleting = kind == _DELETING_CONDITION and condition.get("status") == "True"
        if (deleting and condition.get("reason") == _WAITING_FOR_PRE_TERMINATE_HOOK) or (
            kind == _PRE_TERMINATE_STEP and condition.get("status") != "True"
        ):
            at_hook = True
        described = _describe(condition)
        if (unfinished_step or deleting) and described not in seen:
            seen.add(described)
            pending.append(described)
    annotations = metadata.get("annotations") or {}
    return MachineState(
        provider_hook=PROVIDER_MACHINE_HOOK in annotations,
        at_pre_terminate_hook=deletion_timestamp is not None and at_hook,
        agent_machine=machine_agent_machine(machine),
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


def labelled_agent_machine(agent: dict[str, Any] | None) -> str | None:
    """`<namespace>/<name>` of the AgentMachine the provider's label on this Agent names.

    The provider writes both when it takes an Agent (`updateFoundAgent`): the
    `agentMachineRef` label carries the AgentMachine's NAME, the
    `agentMachineRefNamespace` annotation its namespace — and removes both when
    it releases it. None when unlabelled. A missing annotation reads as `-`,
    the same on every read, so two reads of one Agent compare.
    """
    if agent is None:
        return None
    metadata = _metadata(agent)
    name = (metadata.get("labels") or {}).get(AGENT_MACHINE_REF_LABEL)
    if not name:
        return None
    namespace = (metadata.get("annotations") or {}).get(AGENT_MACHINE_REF_NAMESPACE_ANNOTATION)
    return f"{namespace or '-'}/{name}"


def agent_binding_state(agent: dict[str, Any] | None) -> AgentBindingState:
    """One Agent's binding as read now; found=False for None.

    `agent_machine` is always passed, a None included, so the key is in the
    payload and the workflow can tell this limb from one predating the field.
    """
    if agent is None:
        return AgentBindingState(found=False, agent_machine=None)
    ref = (agent.get("spec") or {}).get("clusterDeploymentName") or {}
    labels = _metadata(agent).get("labels") or {}
    return AgentBindingState(
        found=True,
        cluster_deployment=(
            f"{ref.get('namespace') or '-'}/{ref['name']}" if ref.get("name") else None
        ),
        agent_machine_label=bool(labels.get(AGENT_MACHINE_REF_LABEL)),
        agent_machine=labelled_agent_machine(agent),
        state=agent_state(agent),
        approved=(agent.get("spec") or {}).get("approved"),
    )


# --- a hosted cluster's Nodes and Pods ------------------------------------------------
#
# Read through the hosted cluster's admin kubeconfig (hosted_cluster_api.py) and
# passed here as plain dicts.

_MIRROR_POD_ANNOTATION = "kubernetes.io/config.mirror"


def node_ready(node: dict[str, Any]) -> bool:
    """Whether the kubelet is posting Ready=True — i.e. something is running as this Node."""
    for condition in (node.get("status") or {}).get("conditions") or []:
        if isinstance(condition, dict) and condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False


def node_provider_id(node: dict[str, Any]) -> str | None:
    return (node.get("spec") or {}).get("providerID") or None


def node_matches(
    node: dict[str, Any], wanted_provider_id: str | None, names: set[str]
) -> list[str]:
    """Why this Node is this server's: `provider-id`, `name`, or nothing ([]).

    The providerID is CAPI's own link and decides alone. A name — the Machine's
    nodeRef, the Agent's hostname, the server's own name — counts only when
    the Node carries no providerID or this one: a Node of that name with
    ANOTHER Agent's providerID is another machine's.
    """
    reasons = []
    found = node_provider_id(node)
    if wanted_provider_id and found == wanted_provider_id:
        reasons.append("provider-id")
    name = _metadata(node).get("name")
    if name in names and (found is None or wanted_provider_id is None or found == wanted_provider_id):
        reasons.append("name")
    return reasons


def pod_to_evict(pod: dict[str, Any]) -> bool:
    """Whether a drain evicts this Pod — CAPI's drain rules, which kubectl's are.

    CAPI drains with IgnoreAllDaemonSets, DeleteEmptyDirData and Force, so
    everything goes except DaemonSet pods (their controller would put them
    straight back) and mirror pods (the kubelet's static pods, which the API
    cannot remove).
    """
    metadata = _metadata(pod)
    if _MIRROR_POD_ANNOTATION in (metadata.get("annotations") or {}):
        return False
    return not any(
        owner.get("kind") == "DaemonSet" and owner.get("controller")
        for owner in metadata.get("ownerReferences") or []
    )
