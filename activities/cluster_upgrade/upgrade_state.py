"""What the cluster-upgrade limb makes of the objects it reads — pure, no I/O.

Plain dicts in, as the API serves them (camelCase JSON), typed models out. The
modules that talk to an API server only fetch; every reading of a field lives
here, so each rule is a plain unit test against the shapes a live cluster
returned (tests/fixtures/cluster_upgrade/, captured on the sandbox).

Each rule cites where its meaning comes from (CONTEXT §8 in the
Cluster-AutoUpgrade workspace).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from activities.server_lifecycle.hosted_resources import pod_to_evict
from shared.models.cluster_upgrade import (
    AdminAckState,
    ApiUsage,
    ArgoApplicationState,
    CheckFailure,
    CheckResult,
    GateOutcome,
    GateResult,
    HostedClusterState,
    NodePoolState,
    ProgressSnapshot,
    WatchResult,
)
from shared.ocp_version import release_version

# The hosted CVO's admin-ack check writes this condition when it fails and
# other checks fail too; alone, its reason sits on the aggregate Upgradeable
# condition instead (setUpgradeableConditions, pkg/cvo/upgradeable.go,
# cluster-version-operator release-4.19).
_UPGRADEABLE = "Upgradeable"
_ADMIN_ACK_CONDITION = "UpgradeableAdminAckRequired"
_ADMIN_ACK_REQUIRED = "AdminAckRequired"
_MULTIPLE_REASONS = "MultipleReasons"
_ZONE_LABEL = "topology.kubernetes.io/zone"


def _conditions(obj: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [c for c in ((obj or {}).get("status") or {}).get("conditions") or [] if isinstance(c, dict)]


def _condition(obj: dict[str, Any] | None, kind: str) -> dict[str, Any] | None:
    return next((c for c in _conditions(obj) if c.get("type") == kind), None)


def _bare(version: Any) -> str | None:
    """A release as the models carry it, bare X.Y.Z; None for anything else."""
    if not isinstance(version, str) or not version:
        return None
    try:
        return release_version(version)
    except ValueError:
        return None


# --- the MCE side: HostedCluster and NodePools ------------------------------------


def completed_version(hosted_cluster: dict[str, Any]) -> str | None:
    """`status.version.history[0]` when its state is Completed: the version the control plane runs.

    Mid-upgrade the newest entry is Partial, and the HostedCluster's own
    Progressing is already False again (CONTEXT §8 fact 10), so a Completed
    entry is the only "it runs this" signal.
    """
    history = (((hosted_cluster.get("status") or {}).get("version") or {}).get("history")) or []
    if not history or not isinstance(history[0], dict) or history[0].get("state") != "Completed":
        return None
    return _bare(history[0].get("version"))


def nodepool_state(nodepool: dict[str, Any]) -> NodePoolState:
    spec = nodepool.get("spec") or {}
    management = spec.get("management") or {}
    in_place = management.get("inPlace") or {}
    max_unavailable = in_place.get("maxUnavailable")
    return NodePoolState(
        name=(nodepool.get("metadata") or {}).get("name", ""),
        version=_bare((nodepool.get("status") or {}).get("version")),
        replicas=spec.get("replicas"),
        ready_replicas=(nodepool.get("status") or {}).get("replicas"),
        upgrade_type=management.get("upgradeType"),
        # "1" or "33%": an int-or-string, carried as the string HyperShift would parse.
        max_unavailable=None if max_unavailable is None else str(max_unavailable),
        zone=(spec.get("nodeLabels") or {}).get(_ZONE_LABEL),
        conditions={c["type"]: str(c.get("status")) for c in _conditions(nodepool) if c.get("type")},
    )


def hosted_cluster_state(
    hosted_cluster: dict[str, Any] | None, nodepools: list[dict[str, Any]]
) -> HostedClusterState:
    """The HostedCluster and the NodePools whose `spec.clusterName` names it."""
    if hosted_cluster is None:
        return HostedClusterState(found=False)
    metadata = hosted_cluster.get("metadata") or {}
    name = metadata.get("name")
    progressing = _condition(hosted_cluster, "Progressing")
    mirrors = [
        mirror
        for source in (hosted_cluster.get("spec") or {}).get("imageContentSources") or []
        for mirror in (source or {}).get("mirrors") or []
    ]
    return HostedClusterState(
        found=True,
        namespace=metadata.get("namespace"),
        control_plane_version=completed_version(hosted_cluster),
        progressing=None if progressing is None else progressing.get("status") == "True",
        conditions={c["type"]: str(c.get("status")) for c in _conditions(hosted_cluster) if c.get("type")},
        image_content_mirrors=mirrors,
        nodepools=sorted(
            (nodepool_state(np) for np in nodepools if (np.get("spec") or {}).get("clusterName") == name),
            key=lambda state: state.name,
        ),
    )


# --- the hosted side: ClusterVersion and ClusterOperators -------------------------


def upgradeable(cluster_version: dict[str, Any]) -> dict[str, Any]:
    """The HostedHealth Upgradeable fields from the hosted ClusterVersion.

    No Upgradeable condition means upgradeable: HyperShift reports True then,
    "Condition not found in the CVO." (hcpstatus.go, release-4.17 and 4.20;
    CONTEXT §8 fact 11), and the 4.17 and 4.18 CVOs set none. With
    MultipleReasons, each reason comes from its own Upgradeable<Check>
    condition (fact 15). Such a condition exists only while its check fails,
    WHATEVER ITS STATUS: UpgradeableUpgradeInProgress is written True by
    design (upgradeInProgressUpgradeable, pkg/cvo/upgradeable.go, release-4.19;
    seen live on the sandbox), UpgradeableAdminAckRequired False.
    """
    aggregate = _condition(cluster_version, _UPGRADEABLE)
    if aggregate is None or aggregate.get("status") == "True":
        return {"cluster_version_upgradeable": True, "upgradeable_reason": None,
                "upgradeable_message": None, "upgradeable_reasons": []}
    reason = aggregate.get("reason")
    if reason == _MULTIPLE_REASONS:
        reasons = sorted(
            str(c.get("reason"))
            for c in _conditions(cluster_version)
            if str(c.get("type", "")).startswith(_UPGRADEABLE) and c.get("type") != _UPGRADEABLE
            and c.get("reason")
        )
    else:
        reasons = [reason] if reason else []
    return {
        "cluster_version_upgradeable": False,
        "upgradeable_reason": reason,
        "upgradeable_message": aggregate.get("message"),
        "upgradeable_reasons": reasons,
    }


def operators_unhealthy(cluster_operators: list[dict[str, Any]]) -> list[str]:
    """ClusterOperators not Available, or Degraded: "<name> (Available=False: <reason>)"."""
    unhealthy = []
    for operator in cluster_operators:
        name = (operator.get("metadata") or {}).get("name", "?")
        available = _condition(operator, "Available")
        degraded = _condition(operator, "Degraded")
        problems = []
        if available is None or available.get("status") != "True":
            problems.append(f"Available={(available or {}).get('status', 'missing')}"
                            + (f": {available.get('reason')}" if available and available.get("reason") else ""))
        if degraded is not None and degraded.get("status") == "True":
            problems.append("Degraded=True" + (f": {degraded.get('reason')}" if degraded.get("reason") else ""))
        if problems:
            unhealthy.append(f"{name} ({'; '.join(problems)})")
    return sorted(unhealthy)


# --- admin acks (D16) ----------------------------------------------------------------


def admin_ack_required(cluster_version: dict[str, Any]) -> str | None:
    """The CVO's admin-ack message while that check fails, else None.

    Its own UpgradeableAdminAckRequired condition when several checks fail;
    the aggregate Upgradeable's when it is the only one.
    """
    own = _condition(cluster_version, _ADMIN_ACK_CONDITION)
    if own is not None and own.get("status") == "False":
        return str(own.get("message") or own.get("reason") or _ADMIN_ACK_REQUIRED)
    aggregate = _condition(cluster_version, _UPGRADEABLE)
    if aggregate is not None and aggregate.get("status") == "False" and aggregate.get("reason") == _ADMIN_ACK_REQUIRED:
        return str(aggregate.get("message") or _ADMIN_ACK_REQUIRED)
    return None


def api_usage(api: str, api_request_count: dict[str, Any] | None) -> list[ApiUsage]:
    """Who called `api` in the last 24 hours, one entry per (user, user agent).

    From `status.last24h[].byNode[].byUser[]`, Red Hat's documented way to find
    a removed API's callers (KCS 6955985). Requests are summed over hours and
    nodes; verbs are the union. No APIRequestCount means nobody called it.
    """
    if not api_request_count:
        return []
    totals: dict[tuple[str, str], dict[str, Any]] = {}
    for hour in (api_request_count.get("status") or {}).get("last24h") or []:
        for node in (hour or {}).get("byNode") or []:
            for user in (node or {}).get("byUser") or []:
                key = (str(user.get("username") or ""), str(user.get("userAgent") or ""))
                entry = totals.setdefault(key, {"requests": 0, "verbs": set()})
                entry["requests"] += int(user.get("requestCount") or 0)
                entry["verbs"].update(str(v.get("verb")) for v in user.get("byVerb") or [] if v.get("verb"))
    return [
        ApiUsage(api=api, username=username, user_agent=agent, verbs=sorted(entry["verbs"]),
                 requests=entry["requests"])
        for (username, agent), entry in sorted(totals.items())
        if entry["requests"] > 0
    ]


def admin_ack_state(
    *,
    admin_gates: dict[str, Any] | None,
    admin_acks: dict[str, Any] | None,
    cluster_version: dict[str, Any],
    api_request_counts: dict[str, dict[str, Any] | None],
) -> AdminAckState:
    """The AdminAckState of a reachable hosted cluster, from what was read.

    `api_request_counts` maps each asked-about API to its APIRequestCount, or
    None when it has none.
    """
    return AdminAckState(
        reachable=True,
        gates={str(k): str(v) for k, v in ((admin_gates or {}).get("data") or {}).items()},
        acks={str(k): str(v) for k, v in ((admin_acks or {}).get("data") or {}).items()},
        usage=[use for api, count in api_request_counts.items() for use in api_usage(api, count)],
        admin_ack_required=admin_ack_required(cluster_version),
    )


def ack_patch(gate: str) -> dict[str, Any]:
    """The merge patch that acks one gate, and nothing else."""
    return {"data": {gate: "true"}}


# --- the rest of hosted health (pre-flight checks 6, 8-11) -------------------------

# The label HyperShift puts on every hosted Node, naming its NodePool.
NODEPOOL_LABEL = "hypershift.openshift.io/nodePool"
# PDBs here are the platform's, not a customer's (AGENT.md #12 routes customer
# ones to their owner). Platform namespaces are OpenShift's and Kubernetes' own.
_PLATFORM_NAMESPACE_PREFIXES = ("openshift-", "kube-")
_PLATFORM_NAMESPACES = ("openshift", "default")


def _ready(node: dict[str, Any]) -> bool:
    ready = _condition(node, "Ready")
    return ready is not None and ready.get("status") == "True"


def nodes_not_ready(nodes: list[dict[str, Any]]) -> list[str]:
    return sorted((n.get("metadata") or {}).get("name", "?") for n in nodes if not _ready(n))


def nodes_in_scope(nodes: list[dict[str, Any]], nodepools: list[str]) -> set[str]:
    """The Nodes of these NodePools (by HyperShift's label); every Node when none are named."""
    return {
        (n.get("metadata") or {}).get("name", "")
        for n in nodes
        if not nodepools or ((n.get("metadata") or {}).get("labels") or {}).get(NODEPOOL_LABEL) in nodepools
    }


def pending_csrs(csrs: list[dict[str, Any]]) -> int:
    """CertificateSigningRequests neither Approved nor Denied (nor Failed) yet."""
    return sum(1 for csr in csrs if not _conditions(csr))


def _platform(namespace: str) -> bool:
    return namespace in _PLATFORM_NAMESPACES or namespace.startswith(_PLATFORM_NAMESPACE_PREFIXES)


def _selects(selector: dict[str, Any] | None, labels: dict[str, str]) -> bool:
    """A LabelSelector against a Pod's labels; an empty selector selects every Pod (policy/v1)."""
    selector = selector or {}
    for key, value in (selector.get("matchLabels") or {}).items():
        if labels.get(key) != value:
            return False
    for expression in selector.get("matchExpressions") or []:
        key, operator, values = expression.get("key"), expression.get("operator"), expression.get("values") or []
        if operator == "In" and labels.get(key) not in values:
            return False
        if operator == "NotIn" and key in labels and labels[key] in values:
            return False
        if operator == "Exists" and key not in labels:
            return False
        if operator == "DoesNotExist" and key in labels:
            return False
    return True


def blocking_pdbs(pdbs: list[dict[str, Any]], pods: list[dict[str, Any]], scope: set[str]) -> list[str]:
    """Customer PDBs at disruptionsAllowed 0 that cover a Pod on an in-scope Node: "ns/name".

    Those would hold a drain for ever (nodeDrainTimeout 0, CONTEXT §8.13), and
    only their owner may change them (AGENT.md #12). Platform PDBs are left to
    the roll's own watch and the agent (D17).
    """
    blocking = []
    for pdb in pdbs:
        metadata = pdb.get("metadata") or {}
        namespace = metadata.get("namespace", "")
        if _platform(namespace) or (pdb.get("status") or {}).get("disruptionsAllowed", 1) != 0:
            continue
        selector = (pdb.get("spec") or {}).get("selector")
        if any(
            (pod.get("metadata") or {}).get("namespace") == namespace
            and (pod.get("spec") or {}).get("nodeName") in scope
            and _selects(selector, (pod.get("metadata") or {}).get("labels") or {})
            for pod in pods
        ):
            blocking.append(f"{namespace}/{metadata.get('name')}")
    return sorted(blocking)


def dead_webhooks(configurations: list[dict[str, Any]], endpoints: list[dict[str, Any]]) -> list[str]:
    """failurePolicy Fail webhooks whose Service has no ready address: "<configuration>/<webhook>".

    Such a webhook refuses every request it matches, which stalls an upgrade
    the moment an operator writes through it (AGENT.md #12). A webhook called by
    URL, not a Service, is not judged here.
    """
    ready = {
        ((e.get("metadata") or {}).get("namespace"), (e.get("metadata") or {}).get("name"))
        for e in endpoints
        if any(subset.get("addresses") for subset in e.get("subsets") or [])
    }
    dead = []
    for configuration in configurations:
        for webhook in configuration.get("webhooks") or []:
            service = ((webhook.get("clientConfig") or {}).get("service")) or {}
            if webhook.get("failurePolicy", "Fail") != "Fail" or not service:
                continue
            if (service.get("namespace"), service.get("name")) not in ready:
                dead.append(f"{(configuration.get('metadata') or {}).get('name')}/{webhook.get('name')}")
    return sorted(dead)


def idms_mirrors(image_digest_mirror_sets: list[dict[str, Any]]) -> list[str]:
    """Every mirror repository the hosted cluster's ImageDigestMirrorSets name (the day2 chart's)."""
    return sorted({
        mirror
        for idms in image_digest_mirror_sets
        for entry in (idms.get("spec") or {}).get("imageDigestMirrors") or []
        for mirror in entry.get("mirrors") or []
    })


def nodepools_short(nodepools: list[NodePoolState], names: list[str]) -> list[str]:
    """In-scope NodePools whose ready nodes are below their desired replicas: "name (ready/desired)"."""
    return [
        f"{np.name} ({np.ready_replicas or 0}/{np.replicas})"
        for np in nodepools
        if (not names or np.name in names) and np.replicas is not None and (np.ready_replicas or 0) < np.replicas
    ]


# --- the MCE: what its HyperShift can host -------------------------------------------


def supported_versions(data: dict[str, str] | None) -> tuple[bool, list[str]]:
    """(found, minors) from ConfigMap hypershift/supported-versions.

    Key `supported-versions`, JSON `{"versions": ["4.22", ...]}`, written by the
    HyperShift operator's supportedversion reconciler (CONTEXT §8 fact 5).
    """
    if data is None or "supported-versions" not in data:
        return False, []
    try:
        versions = json.loads(data["supported-versions"]).get("versions") or []
    except (ValueError, AttributeError):
        return False, []
    return True, [str(v) for v in versions]


# --- the MCE: the cluster's Argo CD Application ---------------------------------------


def argo_application_state(application: dict[str, Any] | None) -> ArgoApplicationState:
    """Sync and health of the cluster's Application, and which resources are out of sync ("Kind/name").

    Pre-flight check 5 needs it Synced and Healthy; a sync is refused when
    anything but the expected resource is out of sync (DESIGN §6.2 step 3).
    """
    if application is None:
        return ArgoApplicationState(found=False)
    status = application.get("status") or {}
    return ArgoApplicationState(
        found=True,
        sync_status=(status.get("sync") or {}).get("status"),
        health_status=(status.get("health") or {}).get("status"),
        revision=(status.get("sync") or {}).get("revision"),
        out_of_sync=sorted(
            f"{r.get('kind')}/{r.get('name')}"
            for r in status.get("resources") or [] if r.get("status") == "OutOfSync"
        ),
    )


def sync_refusal(application: dict[str, Any] | None, allowed: list[str]) -> str | None:
    """Why the run must not sync this Application now, or None.

    Anything out of sync beyond `allowed` is someone else's pending change:
    syncing would deploy it under the run's name (DESIGN §6.2 step 3).
    """
    state = argo_application_state(application)
    if not state.found:
        return "the Application does not exist"
    extra = [resource for resource in state.out_of_sync if resource not in allowed]
    if extra:
        return f"out of sync beyond {allowed or 'nothing'}: {', '.join(extra)}"
    return None


def synced_at(application: dict[str, Any] | None, revision: str) -> bool:
    """Already applied at exactly `revision`: Synced, and the last operation there Succeeded."""
    status = (application or {}).get("status") or {}
    operation = status.get("operationState") or {}
    return (
        (status.get("sync") or {}).get("status") == "Synced"
        and (status.get("sync") or {}).get("revision") == revision
        and operation.get("phase") in (None, "Succeeded")
    )


def operation_phase(application: dict[str, Any] | None, revision: str) -> str | None:
    """The phase of the operation syncing `revision` (Running, Succeeded, Failed, Error), or None for none."""
    operation = ((application or {}).get("status") or {}).get("operationState") or {}
    target = ((operation.get("operation") or {}).get("sync") or {}).get("revision") or (
        (operation.get("syncResult") or {}).get("revision")
    )
    return operation.get("phase") if target == revision else None


def sync_operation(revision: str, by: str) -> dict[str, Any]:
    """The merge patch that asks Argo CD to sync exactly `revision` (as `argocd app sync --revision` does)."""
    return {"operation": {"initiatedBy": {"username": by}, "sync": {"revision": revision}}}


# --- the watches: what progress looks like, and when to stop watching ----------------

# The HostedCluster conditions a control-plane watch reports and compares.
_CONTROL_PLANE_CONDITIONS = (
    "Available", "Progressing", "ValidReleaseInfo",
    "ClusterVersionAvailable", "ClusterVersionProgressing", "ClusterVersionSucceeding",
)


def _operator_version(operator: dict[str, Any]) -> str | None:
    """A ClusterOperator's own version: its `operator` entry in status.versions."""
    for entry in (operator.get("status") or {}).get("versions") or []:
        if entry.get("name") == "operator":
            return _bare(entry.get("version"))
    return None


def control_plane_progress(
    hosted_cluster: dict[str, Any] | None, cluster_operators: list[dict[str, Any]] | None, version: str
) -> tuple[bool, ProgressSnapshot]:
    """(done, what it looks like) for a control plane going to `version`.

    Done (DESIGN §6.2 step 4): history[0] is `version` and Completed,
    ClusterVersionSucceeding and ValidReleaseInfo True, not Progressing, and
    every hosted ClusterOperator at `version`. A short Available=False or
    ClusterVersionSucceeding=False is normal early in a step (CONTEXT §8 fact
    14): it changes the snapshot, it never ends the watch. `cluster_operators`
    None means the hosted API could not be read this time: not done, and the
    operator counts are left out of the snapshot rather than read as zero.
    """
    hosted_cluster = hosted_cluster or {}
    history = (((hosted_cluster.get("status") or {}).get("version") or {}).get("history")) or [{}]
    newest = history[0] if isinstance(history[0], dict) else {}
    conditions = {
        c["type"]: str(c.get("status")) for c in _conditions(hosted_cluster) if c.get("type") in _CONTROL_PLANE_CONDITIONS
    }
    at_version = total = None
    if cluster_operators is not None:
        total = len(cluster_operators)
        at_version = sum(1 for co in cluster_operators if _operator_version(co) == version)
    done = (
        completed_version(hosted_cluster) == version
        and conditions.get("ClusterVersionSucceeding") == "True"
        and conditions.get("ValidReleaseInfo") == "True"
        and conditions.get("Progressing") != "True"
        and total is not None and total > 0 and at_version == total
    )
    succeeding = _condition(hosted_cluster, "ClusterVersionSucceeding") or {}
    summary = (
        f"history[0] {newest.get('version', '?')} {newest.get('state', '?')}"
        + (f"; {at_version}/{total} operators at {version}" if total is not None else "; operators unread")
        + (f"; ClusterVersionSucceeding={succeeding.get('status')} ({succeeding.get('reason')})"
           if succeeding and succeeding.get("status") != "True" else "")
    )
    return done, ProgressSnapshot(summary=summary, conditions=conditions, updated=at_version, total=total)


def advance(previous: ProgressSnapshot | None, seen: ProgressSnapshot, now: datetime) -> ProgressSnapshot:
    """`seen`, with the watch's clocks carried from `previous`: started once, last changed when anything did."""
    started = previous.started_at if previous and previous.started_at else now
    changed = previous is None or (previous.summary, previous.conditions, previous.updated) != (
        seen.summary, seen.conditions, seen.updated
    )
    last_change = now if changed or not (previous and previous.last_change_at) else previous.last_change_at
    return seen.model_copy(update={"started_at": started, "last_change_at": last_change})


def watch_verdict(
    snapshot: ProgressSnapshot, done: bool, now: datetime, *, stall_minutes: int, deadline_minutes: int
) -> WatchResult | None:
    """How a watch ends now, or None to keep watching. Done wins over a stall or the deadline."""
    if done:
        return WatchResult.DONE
    if snapshot.started_at and now - snapshot.started_at >= timedelta(minutes=deadline_minutes):
        return WatchResult.DEADLINE
    if snapshot.last_change_at and now - snapshot.last_change_at >= timedelta(minutes=stall_minutes):
        return WatchResult.STALLED
    return None


# --- the NodePool watch, and a drain a PDB refuses (D17) ------------------------------

# On the CAPI MachineSet of an InPlace NodePool: the config every node must reach
# (inplaceupgrader.go, hosted-cluster-config-operator, release-4.20; seen live).
TARGET_CONFIG_ANNOTATION = "hypershift.openshift.io/nodePoolTargetConfigVersion"
# ...and the config it last finished rolling out; equal to the target at rest.
CURRENT_CONFIG_ANNOTATION = "hypershift.openshift.io/nodePoolCurrentConfigVersion"
# On each hosted Node, written by its machine-config daemon.
_NODE_CURRENT_CONFIG = "machineconfiguration.openshift.io/currentConfig"
_NODE_MCD_STATE = "machineconfiguration.openshift.io/state"
# The taint Kubernetes adds when a Node is cordoned; its timeAdded dates the drain.
_UNSCHEDULABLE_TAINT = "node.kubernetes.io/unschedulable"
_NODEPOOL_CONDITIONS = ("UpdatingVersion", "UpdatingConfig", "AllNodesHealthy", "Ready", "SupportedVersionSkew")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def cordoned_since(node: dict[str, Any]) -> datetime | None:
    """When the Node was cordoned (its unschedulable taint's timeAdded); None when it is not."""
    if not (node.get("spec") or {}).get("unschedulable"):
        return None
    for taint in (node.get("spec") or {}).get("taints") or []:
        if taint.get("key") == _UNSCHEDULABLE_TAINT:
            return _parse_time(taint.get("timeAdded"))
    return None


def pod_state(pod: dict[str, Any]) -> str:
    """A Pod's state in a word or two: the waiting reason of a container (CrashLoopBackOff), else its phase."""
    status = pod.get("status") or {}
    for container in status.get("containerStatuses") or []:
        waiting = (container.get("state") or {}).get("waiting") or {}
        if waiting.get("reason"):
            return str(waiting["reason"])
    ready = next((c for c in status.get("conditions") or [] if c.get("type") == "Ready"), {})
    phase = str(status.get("phase", "Unknown"))
    return phase if ready.get("status") == "True" else f"{phase}, not Ready"


def drain_blocker(
    node: dict[str, Any], pods: list[dict[str, Any]], pdbs: list[dict[str, Any]], now: datetime, minutes: int
) -> str | None:
    """Why this Node's drain has waited `minutes` on a refused eviction, or None.

    Cordoned that long, with Pods a drain must move still on it, covered by a
    PodDisruptionBudget at disruptionsAllowed 0: the eviction API refuses them
    and nodeDrainTimeout 0 waits for ever (CONTEXT §8.13). The text names the
    PDB and its Pods' states — what the agent needs to see (D17).
    """
    since = cordoned_since(node)
    if since is None or now - since < timedelta(minutes=minutes):
        return None
    name = (node.get("metadata") or {}).get("name")
    stuck = [
        pod for pod in pods
        if (pod.get("spec") or {}).get("nodeName") == name and pod_to_evict(pod)
        and not (pod.get("metadata") or {}).get("deletionTimestamp")
    ]
    reasons = []
    for pdb in pdbs:
        if (pdb.get("status") or {}).get("disruptionsAllowed", 1) != 0:
            continue
        namespace = (pdb.get("metadata") or {}).get("namespace")
        covered = [
            pod for pod in stuck
            if (pod.get("metadata") or {}).get("namespace") == namespace
            and _selects((pdb.get("spec") or {}).get("selector"), (pod.get("metadata") or {}).get("labels") or {})
        ]
        if covered:
            pods_text = ", ".join(f"{p['metadata']['name']} {pod_state(p)}" for p in covered)
            reasons.append(f"PDB {namespace}/{pdb['metadata']['name']} (disruptionsAllowed 0; {pods_text})")
    if not reasons:
        return None
    waited = int((now - since).total_seconds() // 60)
    return f"the drain of {name} has waited {waited} min on {'; '.join(reasons)}"


def nodepool_progress(
    nodepool: dict[str, Any] | None, nodes: list[dict[str, Any]] | None, target_config: str | None, version: str,
    current_config: str | None = None,
) -> tuple[bool, ProgressSnapshot]:
    """(done, what it looks like) for a NodePool rolling to `version`.

    Done (DESIGN §6.3): status.version is `version`, UpdatingVersion False,
    AllNodesHealthy True, and SupportedVersionSkew True where the operator
    reports it. A node counts as updated once its currentConfig is the
    MachineSet's target, or, with no currentConfig at all, while the MachineSet
    is at rest (current == target): a node no in-place upgrade has touched yet
    needs none then (nodeNeedsUpgrade, inplaceupgrader.go, release-4.19; seen
    live). `nodes` None: the hosted API was unreadable this poll.
    """
    nodepool = nodepool or {}
    conditions = {
        c["type"]: str(c.get("status")) for c in _conditions(nodepool) if c.get("type") in _NODEPOOL_CONDITIONS
    }
    done = (
        _bare((nodepool.get("status") or {}).get("version")) == version
        and conditions.get("UpdatingVersion") == "False"
        and conditions.get("AllNodesHealthy") == "True"
        and conditions.get("SupportedVersionSkew", "True") == "True"
    )
    updated = total = None
    states = []
    if nodes is not None:
        total = len(nodes)
        if target_config:
            at_rest = current_config == target_config
            updated = sum(
                1 for n in nodes
                if (config := ((n.get("metadata") or {}).get("annotations") or {}).get(_NODE_CURRENT_CONFIG)) == target_config
                or (not config and at_rest)
            )
        for n in nodes:
            state = ((n.get("metadata") or {}).get("annotations") or {}).get(_NODE_MCD_STATE)
            cordoned = bool((n.get("spec") or {}).get("unschedulable"))
            if cordoned or state not in (None, "Done"):
                states.append(f"{n['metadata']['name']} ({'cordoned, ' if cordoned else ''}{state or 'no state'})")
    summary = (
        f"status.version {_bare((nodepool.get('status') or {}).get('version')) or '?'}"
        + (f"; {updated}/{total} nodes at the target config" if updated is not None
           else "; nodes unread" if total is None else f"; {total} nodes")
        + (f"; {', '.join(sorted(states))}" if states else "")
        + (f"; UpdatingVersion={conditions['UpdatingVersion']}" if "UpdatingVersion" in conditions else "")
    )
    return done, ProgressSnapshot(summary=summary, conditions=conditions, updated=updated, total=total)


# --- the NodePool gate (DESIGN §6.3 step 3) --------------------------------------------

GATE_NODES = "nodepool-nodes"
GATE_PDBS = "pdbs"
GATE_STATEFULSETS = "statefulsets"
GATE_SOAK = "soak"


def _gate_nodes(nodepool: str, nodes: list[dict[str, Any]]) -> CheckResult:
    expected = f"every node of NodePool {nodepool} Ready and schedulable"
    if not nodes:
        return CheckResult(name=GATE_NODES, passed=False, expected=expected,
                           observed=f"no node carries {NODEPOOL_LABEL}={nodepool}", on_failure=CheckFailure.WAIT)
    bad = []
    for node in nodes:
        why = [word for word, hit in (("NotReady", not _ready(node)),
                                      ("cordoned", bool((node.get("spec") or {}).get("unschedulable")))) if hit]
        if why:
            bad.append(f"{(node.get('metadata') or {}).get('name', '?')} ({', '.join(why)})")
    return CheckResult(name=GATE_NODES, passed=not bad, expected=expected,
                       observed="; ".join(sorted(bad)) or f"{len(nodes)} nodes Ready and schedulable",
                       on_failure=CheckFailure.WAIT)


def _gate_pdbs(nodes: list[dict[str, Any]], pods: list[dict[str, Any]], pdbs: list[dict[str, Any]]) -> CheckResult:
    """PDBs at disruptionsAllowed 0 covering a Pod on the NodePool's nodes, or one not scheduled yet.

    A Pod the roll evicted may still be Pending somewhere; its PDB counts too.
    Platform PDBs included: the next NodePool's drain would meet them the same way.
    """
    scope = {(n.get("metadata") or {}).get("name") for n in nodes}
    low = []
    for pdb in pdbs:
        if (pdb.get("status") or {}).get("disruptionsAllowed", 1) >= 1:
            continue
        metadata = pdb.get("metadata") or {}
        namespace = metadata.get("namespace")
        selector = (pdb.get("spec") or {}).get("selector")
        if any(
            (pod.get("metadata") or {}).get("namespace") == namespace
            and (pod.get("spec") or {}).get("nodeName") in scope | {None}
            and _selects(selector, (pod.get("metadata") or {}).get("labels") or {})
            for pod in pods
        ):
            low.append(f"{namespace}/{metadata.get('name')}")
    return CheckResult(name=GATE_PDBS, passed=not low,
                       expected="PDBs covering this NodePool's Pods back to disruptionsAllowed >= 1",
                       observed="at 0: " + ", ".join(sorted(low)) if low else "none at 0",
                       on_failure=CheckFailure.WAIT)


def _gate_statefulsets(statefulsets: list[dict[str, Any]]) -> CheckResult:
    """Every StatefulSet in the cluster with readyReplicas == replicas (AGENT.md #17; not scoped
    to the NodePool, so one already short before the upgrade holds the gate for a person too)."""
    short = []
    for sts in statefulsets:
        want = (sts.get("spec") or {}).get("replicas", 1) or 0
        ready = (sts.get("status") or {}).get("readyReplicas", 0) or 0
        if want and ready < want:
            metadata = sts.get("metadata") or {}
            short.append(f"{metadata.get('namespace')}/{metadata.get('name')} ({ready}/{want} ready)")
    return CheckResult(name=GATE_STATEFULSETS, passed=not short, expected="every StatefulSet readyReplicas == replicas",
                       observed=", ".join(sorted(short)) or f"{len(statefulsets)} StatefulSets ready",
                       on_failure=CheckFailure.WAIT)


def gate_checks(
    nodepool: str, objects: dict[str, Any] | None, workload: bool, unreadable: str = "the hosted API was unreadable"
) -> list[CheckResult]:
    """The gate's platform and workload checks on what the hosted cluster shows now.

    `objects` None: nothing could be read this poll, which fails the platform
    check (saying why) rather than passing anything.
    """
    if objects is None:
        return [CheckResult(name=GATE_NODES, passed=False, expected=f"every node of NodePool {nodepool} Ready and schedulable",
                            observed=unreadable, on_failure=CheckFailure.WAIT)]
    checks = [_gate_nodes(nodepool, objects["nodes"])]
    if workload:
        checks.append(_gate_pdbs(objects["nodes"], objects["pods"], objects["pdbs"]))
        checks.append(_gate_statefulsets(objects["statefulsets"]))
    return checks


def gate_snapshot(checks: list[CheckResult]) -> ProgressSnapshot:
    """What the gate saw, for `advance`: the summary is fixed while every check passes, so the
    soak clock is the time since it last changed."""
    failing = [c for c in checks if not c.passed]
    summary = "; ".join(f"{c.name}: {c.observed}" for c in failing) or f"every check passes ({len(checks)})"
    return ProgressSnapshot(summary=summary, updated=len(checks) - len(failing), total=len(checks))


def gate_verdict(
    snapshot: ProgressSnapshot, checks: list[CheckResult], now: datetime, *, soak_minutes: int, deadline_minutes: int
) -> GateOutcome | None:
    """PASSED once every check has held for the soak, DEADLINE on the gate's own clock, else None."""
    failing = [c for c in checks if not c.passed]
    held = now - snapshot.last_change_at if snapshot.last_change_at else timedelta(0)
    if not failing and held >= timedelta(minutes=soak_minutes):
        return GateOutcome(result=GateResult.PASSED)
    if snapshot.started_at and now - snapshot.started_at >= timedelta(minutes=deadline_minutes):
        return GateOutcome(result=GateResult.DEADLINE, failing_checks=failing or [CheckResult(
            name=GATE_SOAK, passed=False, expected=f"every check passing for {soak_minutes} min",
            observed=f"passing for {int(held.total_seconds() // 60)} min", on_failure=CheckFailure.WAIT,
        )])
    return None


# --- the agent's evidence --------------------------------------------------------------

# Temporal's payload limit is 2 MB; the bundle stays well under it (DESIGN §8.1).
EVIDENCE_LIMIT_BYTES = 512 * 1024


def _conditions_text(obj: dict[str, Any] | None) -> str:
    return "\n".join(
        f"{c.get('type')}={c.get('status')} {c.get('reason') or ''}: {(c.get('message') or '')[:300]}".rstrip(": ")
        for c in _conditions(obj)
    )


def pods_not_ready(pods: list[dict[str, Any]]) -> list[str]:
    """"ns/name (state) on node" for every Pod that is not Running and Ready, or Succeeded."""
    lines = []
    for pod in pods:
        state = pod_state(pod)
        if state in ("Running", "Succeeded"):
            continue
        metadata = pod.get("metadata") or {}
        lines.append(f"{metadata.get('namespace')}/{metadata.get('name')} ({state}) "
                     f"on {(pod.get('spec') or {}).get('nodeName') or 'no node'}")
    return sorted(lines)


def warning_events(events: list[dict[str, Any]], limit: int = 50) -> list[str]:
    """The newest Warning events, one line each: when, object, reason, message."""
    def when(event):
        return event.get("lastTimestamp") or event.get("eventTime") or (event.get("metadata") or {}).get("creationTimestamp") or ""
    lines = []
    for event in sorted(events, key=when, reverse=True)[:limit]:
        involved = event.get("involvedObject") or {}
        lines.append(f"{when(event)} {involved.get('kind')}/{involved.get('namespace') or ''}/{involved.get('name')} "
                     f"{event.get('reason')} x{event.get('count') or 1}: {(event.get('message') or '')[:300]}")
    return lines


def drain_text(nodes: list[dict[str, Any]], pods: list[dict[str, Any]], pdbs: list[dict[str, Any]]) -> str:
    """Cordoned nodes, the Pods a drain still has to move off them, and the PDBs over those Pods (D17)."""
    lines = []
    for node in nodes:
        since = cordoned_since(node)
        if since is None:
            continue
        name = node["metadata"]["name"]
        lines.append(f"node {name} cordoned since {since.isoformat()}")
        for pod in pods:
            if (pod.get("spec") or {}).get("nodeName") == name and pod_to_evict(pod):
                lines.append(f"  pod {pod['metadata']['namespace']}/{pod['metadata']['name']} ({pod_state(pod)})")
    for pdb in pdbs:
        status = pdb.get("status") or {}
        lines.append(f"pdb {pdb['metadata']['namespace']}/{pdb['metadata']['name']}: disruptionsAllowed "
                     f"{status.get('disruptionsAllowed')}, healthy {status.get('currentHealthy')}/{status.get('expectedPods')}")
    return "\n".join(lines) or "no node of this NodePool is cordoned"


def evidence_sections(
    *,
    hosted_cluster: dict[str, Any] | None,
    nodepools: list[dict[str, Any]],
    cluster_operators: list[dict[str, Any]] | None,
    argo: ArgoApplicationState,
    data_plane_pods: list[dict[str, Any]] | None,
    warnings: list[dict[str, Any]] | None,
    drain: str | None,
) -> tuple[dict[str, str], bool]:
    """The bundle's sections, and whether it had to be cut to EVIDENCE_LIMIT_BYTES."""
    sections = {
        "hostedcluster": _conditions_text(hosted_cluster) or "not found",
        "nodepools": "\n\n".join(
            f"{(np.get('metadata') or {}).get('name')} at {(np.get('status') or {}).get('version')}:\n{_conditions_text(np)}"
            for np in nodepools
        ) or "none",
        "clusteroperators": "unreadable" if cluster_operators is None else (
            "\n".join(operators_unhealthy(cluster_operators)) or "all Available, none Degraded"),
        "argo": f"{argo.sync_status}/{argo.health_status} at {argo.revision}"
                + (f"; out of sync: {', '.join(argo.out_of_sync)}" if argo.out_of_sync else "")
                if argo.found else "Application not found",
        "data-plane": "unreadable" if data_plane_pods is None else (
            "\n".join(pods_not_ready(data_plane_pods)) or "every ingress, image-registry and console Pod Running"),
        "events": "unreadable" if warnings is None else ("\n".join(warning_events(warnings)) or "no Warning events"),
    }
    if drain is not None:
        sections["drain"] = drain
    truncated = False
    share = EVIDENCE_LIMIT_BYTES // len(sections)
    for key, text in sections.items():
        if len(text.encode()) > share:
            sections[key] = text.encode()[:share].decode(errors="ignore") + "\n[cut]"
            truncated = True
    return sections, truncated
