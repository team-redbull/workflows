"""Cluster-upgrade activities — the per-MCE limb (DESIGN §2). Thin: each one reads, then hands to upgrade_state.

Runs inside each MCE as the workflow ServiceAccount (`cluster-upgrade-worker`),
polling cluster_upgrade_activity_queue(<mce>). The MCE's own API is read through
activities/server_lifecycle/cluster_api.py; a hosted cluster's through its admin
kubeconfig, by hosted_upgrade_api.py — the workflow path only, never the agent's
(AGENT.md Part 4).

BUILT SO FAR (DESIGN §11 step 4, one piece at a time): the MCE state, the Argo
CD Application read and sync, the HostedCluster read, the hosted health read,
the admin-ack pair (D16), the control-plane and NodePool watches, and the
agent's evidence. The rest of shared/interfaces/cluster_upgrade.py follows;
until it is all here the limb has no worker_init, so nothing registers a partial
limb, and tests/test_cluster_upgrade_limb.py keeps what exists in step with the
interface.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import time
from datetime import UTC, datetime

from temporalio import activity

from activities.cluster_upgrade import action_catalog, hosted_upgrade_api, upgrade_state
from activities.server_lifecycle import cluster_api, hosted_cluster_api
from shared.exceptions import (
    ArgoSyncFailedError,
    ArgoSyncRefusedError,
    HostedClusterAccessError,
    HostedClusterUnreachableError,
)
from shared.models.cluster_upgrade import (
    ActionResult,
    AdminAckGiven,
    AdminAckRequest,
    AdminAckState,
    ArgoApplicationState,
    ArgoSyncRequest,
    ArgoSyncResult,
    CheckResult,
    EvidenceBundle,
    EvidenceRequest,
    GateOutcome,
    GiveAdminAckRequest,
    HostedClusterRequest,
    HostedClusterState,
    HostedHealth,
    HostedHealthRequest,
    MceRequest,
    MceState,
    NodePoolGateRequest,
    ProgressSnapshot,
    ProposedActionRequest,
    WatchControlPlaneRequest,
    WatchNodePoolRequest,
    WatchOutcome,
    WatchResult,
)

# Argo CD on the MCE: one Application per hosted cluster, named after it, made
# by the <mce>-hc ApplicationSet (CONTEXT §6.1).
_ARGO_NAMESPACE = "openshift-gitops"
# Who Argo CD records as having started the sync.
_SYNC_INITIATOR = "cluster-upgrade-worker"
# Poll the operation this often, and give up this long after asking — under the
# 5-minute start_to_close the workflow gives a sync (dispatch.SYNC_TIMEOUT).
_SYNC_POLL_SECONDS = 5
_SYNC_WAIT_SECONDS = 240
# A watch looks this often, and heartbeats each time (heartbeat_timeout 2 min,
# dispatch.WATCH_HEARTBEAT_TIMEOUT).
_WATCH_POLL_SECONDS = 30
_HYPERSHIFT_GROUP = "hypershift.openshift.io"
_HYPERSHIFT_VERSION = "v1beta1"
# The key HyperShift stores an admin kubeconfig under in its Secret.
_KUBECONFIG_KEY = "kubeconfig"


async def _hosted_cluster(cluster: str) -> dict | None:
    """The HostedCluster of this name on the MCE, in whatever namespace; None when there is none.

    Found by listing: day1 renders each into its own `hcp-<cluster>` namespace,
    and a request names only the cluster. Two of one name is a conflict a
    person resolves, never a guess.
    """
    found = [
        hc for hc in await cluster_api.list_cluster_custom_objects(
            group=_HYPERSHIFT_GROUP, version=_HYPERSHIFT_VERSION, plural="hostedclusters", kind="HostedCluster",
        )
        if (hc.get("metadata") or {}).get("name") == cluster
    ]
    if len(found) > 1:
        namespaces = sorted((hc.get("metadata") or {}).get("namespace", "?") for hc in found)
        raise HostedClusterAccessError(
            f"{len(found)} HostedClusters are named {cluster} on this MCE, in {', '.join(namespaces)}"
        )
    return found[0] if found else None


def _hcp_namespace(hosted_cluster: dict) -> str:
    """HyperShift's control-plane namespace: `<namespace>-<name, dots as dashes>`."""
    metadata = hosted_cluster.get("metadata") or {}
    return f"{metadata.get('namespace')}-{str(metadata.get('name')).replace('.', '-')}"


async def _kubeconfig(hosted_cluster: dict) -> str:
    """The hosted cluster's admin kubeconfig text, from the Secret its status names."""
    metadata = hosted_cluster.get("metadata") or {}
    namespace, label = metadata.get("namespace"), f"{metadata.get('namespace')}/{metadata.get('name')}"
    secret = ((hosted_cluster.get("status") or {}).get("kubeconfig") or {}).get("name")
    if not secret:
        raise HostedClusterAccessError(f"HostedCluster {label} publishes no status.kubeconfig yet")
    data = await cluster_api.read_secret_data(namespace, secret)
    encoded = (data or {}).get(_KUBECONFIG_KEY)
    if not encoded:
        raise HostedClusterAccessError(f"Secret {namespace}/{secret} of HostedCluster {label} holds no kubeconfig")
    try:
        return base64.b64decode(encoded, validate=True).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        raise HostedClusterAccessError(
            f"Secret {namespace}/{secret} of HostedCluster {label} holds no readable kubeconfig"
        ) from None


@activity.defn
async def read_mce_state(request: MceRequest) -> MceState:
    """ConfigMap `hypershift/supported-versions`, and the MCE's IDMS sources."""
    found, versions = upgrade_state.supported_versions(
        await cluster_api.read_config_map_data("hypershift", "supported-versions")
    )
    idms = await cluster_api.list_cluster_custom_objects(
        group="config.openshift.io", version="v1", plural="imagedigestmirrorsets", kind="ImageDigestMirrorSet",
    )
    return MceState(supported_versions_found=found, supported_versions=versions,
                    idms_mirrors=upgrade_state.idms_mirrors(idms))


@activity.defn
async def read_argo_application(request: HostedClusterRequest) -> ArgoApplicationState:
    """The cluster's Application in openshift-gitops: sync, health, what is out of sync."""
    return upgrade_state.argo_application_state(await _application(request.cluster))


async def _application(cluster: str) -> dict | None:
    return await cluster_api.read_custom_object(
        group="argoproj.io", version="v1alpha1", plural="applications", kind="Application",
        namespace=_ARGO_NAMESPACE, name=cluster,
    )


@activity.defn
async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
    """Sync the Application to exactly `request.revision`, and wait for the operation.

    Raises ArgoSyncRefusedError (permanent) when anything outside
    `allowed_out_of_sync` is out of sync. Idempotent: already synced at the
    revision is success, and an operation already running for it is waited
    for, not started twice.
    """
    label = f"{_ARGO_NAMESPACE}/{request.cluster}"
    application = await _application(request.cluster)
    if upgrade_state.synced_at(application, request.revision):
        return ArgoSyncResult(revision=request.revision, phase="Succeeded")
    refusal = upgrade_state.sync_refusal(application, request.allowed_out_of_sync)
    if refusal is not None:
        raise ArgoSyncRefusedError(f"Application {label}: {refusal}")
    if upgrade_state.operation_phase(application, request.revision) != "Running":
        await cluster_api.patch_custom_object(
            group="argoproj.io", version="v1alpha1", plural="applications", kind="Application",
            namespace=_ARGO_NAMESPACE, name=request.cluster,
            body=upgrade_state.sync_operation(request.revision, _SYNC_INITIATOR),
        )
        activity.logger.info("asked Argo CD to sync %s to %s", label, request.revision)
    deadline = time.monotonic() + _SYNC_WAIT_SECONDS
    while time.monotonic() < deadline:
        await asyncio.sleep(_SYNC_POLL_SECONDS)
        activity.heartbeat()
        application = await _application(request.cluster)
        phase = upgrade_state.operation_phase(application, request.revision)
        if phase == "Succeeded" or upgrade_state.synced_at(application, request.revision):
            return ArgoSyncResult(revision=request.revision, phase="Succeeded")
        if phase in ("Failed", "Error"):
            message = (((application or {}).get("status") or {}).get("operationState") or {}).get("message", "")
            raise ArgoSyncFailedError(f"Argo CD's sync of {label} to {request.revision} ended {phase}: {message}")
    raise ArgoSyncFailedError(
        f"Argo CD's sync of {label} to {request.revision} did not finish in {_SYNC_WAIT_SECONDS}s"
    )


@activity.defn
async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
    """The HostedCluster and its NodePools: versions, conditions, rollout settings."""
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        return upgrade_state.hosted_cluster_state(None, [])
    nodepools = await cluster_api.list_custom_objects(
        group=_HYPERSHIFT_GROUP, version=_HYPERSHIFT_VERSION, plural="nodepools", kind="NodePool",
        namespace=(hosted.get("metadata") or {}).get("namespace", ""),
    )
    return upgrade_state.hosted_cluster_state(hosted, nodepools)


@activity.defn
async def read_hosted_health(request: HostedHealthRequest) -> HostedHealth:
    """Through the hosted cluster's admin kubeconfig (the existing workflow-path pattern).

    An API server that cannot be reached is `reachable: False`, which the
    pre-flight shows a person, rather than a read retried for ever.
    """
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        return HostedHealth(reachable=False)
    kubeconfig = await _kubeconfig(hosted)
    server = hosted_cluster_api.kas_service_url(_hcp_namespace(hosted))
    try:
        upgrade = await hosted_upgrade_api.read_upgrade_objects(kubeconfig, server, request.cluster, [])
        health = await hosted_upgrade_api.read_health_objects(kubeconfig, server, request.cluster)
    except HostedClusterUnreachableError as err:
        activity.logger.warning("hosted cluster %s unreachable: %s", request.cluster, err)
        return HostedHealth(reachable=False)
    nodepools = upgrade_state.hosted_cluster_state(
        hosted,
        await cluster_api.list_custom_objects(
            group=_HYPERSHIFT_GROUP, version=_HYPERSHIFT_VERSION, plural="nodepools", kind="NodePool",
            namespace=(hosted.get("metadata") or {}).get("namespace", ""),
        ),
    ).nodepools
    scope = upgrade_state.nodes_in_scope(health["nodes"], request.nodepools)
    return HostedHealth(
        reachable=True,
        operators_unhealthy=upgrade_state.operators_unhealthy(upgrade["cluster_operators"]),
        **upgrade_state.upgradeable(upgrade["cluster_version"]),
        nodes_not_ready=upgrade_state.nodes_not_ready(health["nodes"]),
        nodepools_short=upgrade_state.nodepools_short(nodepools, request.nodepools),
        pending_csrs=upgrade_state.pending_csrs(health["csrs"]),
        blocking_pdbs=upgrade_state.blocking_pdbs(health["pdbs"], health["pods"], scope),
        dead_webhooks=upgrade_state.dead_webhooks(health["webhooks"], health["endpoints"]),
        idms_mirrors=upgrade_state.idms_mirrors(health["image_digest_mirror_sets"]),
    )


@activity.defn
async def read_admin_acks(request: AdminAckRequest) -> AdminAckState:
    """The hosted cluster's admin gates and acks, and who called `request.apis` in the last 24 hours."""
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        return AdminAckState(reachable=False)
    objects = await hosted_upgrade_api.read_upgrade_objects(
        await _kubeconfig(hosted),
        hosted_cluster_api.kas_service_url(_hcp_namespace(hosted)),
        request.cluster,
        request.apis,
    )
    state = upgrade_state.admin_ack_state(
        admin_gates=objects["admin_gates"],
        admin_acks=objects["admin_acks"],
        cluster_version=objects["cluster_version"],
        api_request_counts=objects["api_request_counts"],
    )
    activity.logger.info(
        "admin acks of %s: %d gates, %d acks, %d callers of %d APIs",
        request.cluster, len(state.gates), len(state.acks), len(state.usage), len(request.apis),
    )
    return state


@activity.defn
async def give_admin_ack(request: GiveAdminAckRequest) -> AdminAckGiven:
    """Merge `data.<gate>: "true"` into the hosted cluster's openshift-config/admin-acks.

    Raises AdminGateNotFoundError (permanent) for a gate its admin-gates does not hold.
    """
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        raise HostedClusterAccessError(f"no HostedCluster {request.cluster} on this MCE to ack {request.gate} in")
    changed = await hosted_upgrade_api.give_admin_ack(
        await _kubeconfig(hosted),
        hosted_cluster_api.kas_service_url(_hcp_namespace(hosted)),
        request.cluster,
        request.gate,
    )
    activity.logger.info("admin ack %s in %s: %s", request.gate, request.cluster, "written" if changed else "already true")
    return AdminAckGiven(gate=request.gate, changed=changed)


def _resumed_snapshot() -> ProgressSnapshot | None:
    """The last heartbeat's snapshot, when this attempt retries a watch: its clocks carry on."""
    details = activity.info().heartbeat_details
    if not details:
        return None
    try:
        return ProgressSnapshot.model_validate(details[0])
    except Exception:  # noqa: BLE001 — an unreadable detail starts the clocks afresh
        return None


async def _hosted_operators(hosted: dict, cluster: str) -> list[dict] | None:
    """The hosted ClusterOperators, or None when the hosted API cannot be read this time."""
    try:
        objects = await hosted_upgrade_api.read_upgrade_objects(
            await _kubeconfig(hosted), hosted_cluster_api.kas_service_url(_hcp_namespace(hosted)), cluster, [],
        )
    except (HostedClusterUnreachableError, HostedClusterAccessError) as err:
        activity.logger.info("hosted cluster %s unreadable this poll: %s", cluster, err)
        return None
    return objects["cluster_operators"]


@activity.defn
async def watch_control_plane(request: WatchControlPlaneRequest) -> WatchOutcome:
    """Until the HostedCluster runs `version`, nothing changes for the stall window, or the deadline.

    Done: version history[0] is `version` and Completed, ClusterVersionSucceeding
    and ValidReleaseInfo True, not Progressing, and every hosted ClusterOperator
    at `version`. Heartbeats every poll.
    """
    snapshot = _resumed_snapshot()
    while True:
        hosted = await _hosted_cluster(request.cluster)
        operators = None if hosted is None else await _hosted_operators(hosted, request.cluster)
        done, seen = upgrade_state.control_plane_progress(hosted, operators, request.version)
        now = datetime.now(UTC)
        snapshot = upgrade_state.advance(snapshot, seen, now)
        activity.heartbeat(snapshot.model_dump(mode="json"))
        verdict = upgrade_state.watch_verdict(
            snapshot, done, now, stall_minutes=request.stall_minutes, deadline_minutes=request.deadline_minutes,
        )
        if verdict is not None:
            activity.logger.info("control plane of %s → %s: %s (%s)", request.cluster, request.version,
                                 verdict.value, snapshot.summary)
            return WatchOutcome(result=verdict, snapshot=snapshot)
        await asyncio.sleep(_WATCH_POLL_SECONDS)


async def _nodepool_view(request: WatchNodePoolRequest) -> tuple[dict | None, dict, dict | None]:
    """(NodePool, the MachineSet's annotations, the hosted objects or None when unreadable)."""
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        return None, {}, None
    namespace = (hosted.get("metadata") or {}).get("namespace", "")
    nodepool = await cluster_api.read_custom_object(
        group=_HYPERSHIFT_GROUP, version=_HYPERSHIFT_VERSION, plural="nodepools", kind="NodePool",
        namespace=namespace, name=request.nodepool,
    )
    machine_set = await cluster_api.read_custom_object(
        group="cluster.x-k8s.io", version="v1beta1", plural="machinesets", kind="MachineSet",
        namespace=_hcp_namespace(hosted), name=request.nodepool,
    )
    annotations = ((machine_set or {}).get("metadata") or {}).get("annotations") or {}
    try:
        objects = await hosted_upgrade_api.read_nodepool_objects(
            await _kubeconfig(hosted), hosted_cluster_api.kas_service_url(_hcp_namespace(hosted)),
            request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL,
        )
    except (HostedClusterUnreachableError, HostedClusterAccessError) as err:
        activity.logger.info("hosted cluster %s unreadable this poll: %s", request.cluster, err)
        objects = None
    return nodepool, annotations, objects


@activity.defn
async def watch_nodepool(request: WatchNodePoolRequest) -> WatchOutcome:
    """Until the NodePool runs `version` with every node healthy, a stall, or the deadline.

    Done: status.version is `version`, UpdatingVersion False, AllNodesHealthy
    True (and SupportedVersionSkew True where the operator reports it).
    Stalled: no node finished for `stall_minutes`, or one node's drain was
    refused by a PodDisruptionBudget for `drain_blocked_minutes` (the snapshot
    says which node, PDB and pods). Heartbeats every poll.
    """
    snapshot = _resumed_snapshot()
    while True:
        nodepool, machine_set, objects = await _nodepool_view(request)
        done, seen = upgrade_state.nodepool_progress(
            nodepool, None if objects is None else objects["nodes"],
            machine_set.get(upgrade_state.TARGET_CONFIG_ANNOTATION), request.version,
            current_config=machine_set.get(upgrade_state.CURRENT_CONFIG_ANNOTATION),
        )
        now = datetime.now(UTC)
        snapshot = upgrade_state.advance(snapshot, seen, now)
        blocked = None
        if objects is not None and request.drain_blocked_minutes is not None and not done:
            blocked = next(filter(None, (
                upgrade_state.drain_blocker(node, objects["pods"], objects["pdbs"], now, request.drain_blocked_minutes)
                for node in objects["nodes"]
            )), None)
        activity.heartbeat(snapshot.model_dump(mode="json"))
        if blocked is not None:
            snapshot = snapshot.model_copy(update={"summary": f"{snapshot.summary}; {blocked}"})
            activity.logger.info("NodePool %s of %s: %s", request.nodepool, request.cluster, blocked)
            return WatchOutcome(result=WatchResult.STALLED, snapshot=snapshot)
        verdict = upgrade_state.watch_verdict(
            snapshot, done, now, stall_minutes=request.stall_minutes, deadline_minutes=request.deadline_minutes,
        )
        if verdict is not None:
            activity.logger.info("NodePool %s of %s → %s: %s (%s)", request.nodepool, request.cluster,
                                 request.version, verdict.value, snapshot.summary)
            return WatchOutcome(result=verdict, snapshot=snapshot)
        await asyncio.sleep(_WATCH_POLL_SECONDS)


async def _gate_checks(request: NodePoolGateRequest) -> list[CheckResult]:
    """This poll's gate checks: platform and, when asked, workload, from the hosted cluster."""
    hosted = await _hosted_cluster(request.cluster)
    objects, unreadable = None, f"HostedCluster {request.cluster} not found on the MCE"
    if hosted is not None:
        try:
            objects = await hosted_upgrade_api.read_gate_objects(
                await _kubeconfig(hosted), hosted_cluster_api.kas_service_url(_hcp_namespace(hosted)),
                request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL, request.workload_checks,
            )
        except (HostedClusterUnreachableError, HostedClusterAccessError) as err:
            activity.logger.info("hosted cluster %s unreadable this poll: %s", request.cluster, err)
            unreadable = f"the hosted API was unreadable: {err}"
    return upgrade_state.gate_checks(request.nodepool, objects, request.workload_checks, unreadable)


@activity.defn
async def watch_nodepool_gate(request: NodePoolGateRequest) -> GateOutcome:
    """Until every gate check has held for the soak window, or the deadline.

    Platform: the NodePool's nodes Ready and schedulable. Workload (when asked):
    PDBs on its nodes back to disruptionsAllowed >= 1, StatefulSets ready.
    The customer's own readiness checks are not run here: the customer runs
    them, and their `continue` is the sign-off (D19). Heartbeats every poll; a
    retry keeps the soak clock.
    """
    snapshot = _resumed_snapshot()
    while True:
        checks = await _gate_checks(request)
        now = datetime.now(UTC)
        snapshot = upgrade_state.advance(snapshot, upgrade_state.gate_snapshot(checks), now)
        activity.heartbeat(snapshot.model_dump(mode="json"))
        outcome = upgrade_state.gate_verdict(
            snapshot, checks, now, soak_minutes=request.soak_minutes, deadline_minutes=request.deadline_minutes,
        )
        if outcome is not None:
            activity.logger.info("gate after NodePool %s of %s: %s (%s)", request.nodepool, request.cluster,
                                 outcome.result.value, snapshot.summary)
            return outcome
        await asyncio.sleep(_WATCH_POLL_SECONDS)


@activity.defn
async def collect_evidence(request: EvidenceRequest) -> EvidenceBundle:
    """Read-only evidence for the agent: conditions, operators, events, Argo, data plane.

    For a NodePool also its drain: cordoned nodes, the pods left on them, the
    PDBs covering those pods. (The hosted-cluster-config-operator's drainer
    lines, DESIGN §6.3, come with the pod-log read.) Never a Secret; what cannot
    be read says so instead of failing the bundle.
    """
    hosted = await _hosted_cluster(request.cluster)
    nodepools: list[dict] = []
    operators = pods = warnings = None
    drain = None
    if hosted is not None:
        nodepools = [
            np for np in await cluster_api.list_custom_objects(
                group=_HYPERSHIFT_GROUP, version=_HYPERSHIFT_VERSION, plural="nodepools", kind="NodePool",
                namespace=(hosted.get("metadata") or {}).get("namespace", ""),
            )
            if (np.get("spec") or {}).get("clusterName") == request.cluster
            and (request.nodepool is None or (np.get("metadata") or {}).get("name") == request.nodepool)
        ]
        try:
            kubeconfig = await _kubeconfig(hosted)
            server = hosted_cluster_api.kas_service_url(_hcp_namespace(hosted))
            operators = (await hosted_upgrade_api.read_upgrade_objects(kubeconfig, server, request.cluster, []))[
                "cluster_operators"
            ]
            extra = await hosted_upgrade_api.read_evidence_objects(kubeconfig, server, request.cluster)
            pods, warnings = extra["data_plane_pods"], extra["warning_events"]
            if request.nodepool is not None:
                objects = await hosted_upgrade_api.read_nodepool_objects(
                    kubeconfig, server, request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL,
                )
                drain = upgrade_state.drain_text(objects["nodes"], objects["pods"], objects["pdbs"])
        except (HostedClusterUnreachableError, HostedClusterAccessError) as err:
            activity.logger.info("hosted cluster %s partly unreadable for evidence: %s", request.cluster, err)
    sections, truncated = upgrade_state.evidence_sections(
        hosted_cluster=hosted,
        nodepools=nodepools,
        cluster_operators=operators,
        argo=upgrade_state.argo_application_state(await _application(request.cluster)),
        data_plane_pods=pods,
        warnings=warnings,
        drain=drain,
    )
    return EvidenceBundle(sections=sections, truncated=truncated)


_ACTION_OUTCOMES = {
    "deleted": (True, "deleted"),
    "gone": (False, "not executed: already gone"),
    "replaced": (False, "not executed: replaced since the proposal (its UID differs)"),
    "created": (True, "created Job {job}"),
    "exists": (True, "created Job {job} (found on retry)"),
    "missing": (False, "not executed: no such CronJob"),
}


@activity.defn
async def execute_proposed_action(request: ProposedActionRequest) -> ActionResult:
    """Run one approved tier-2 action with the workflow's identity, through the typed catalog.

    Raises ProposedActionRefusedError (permanent) for a kind not in the catalog,
    or a target outside its rules (DESIGN §8.3). Idempotent: a retry finds the
    pod gone or its own Job, and says so.
    """
    action = action_catalog.check(request.action)
    action_id = request.action.action_id
    hosted = await _hosted_cluster(request.cluster)
    if hosted is None:
        return ActionResult(action_id=action_id, executed=False,
                            detail=f"not executed: HostedCluster {request.cluster} not found on the MCE")
    job = None
    if action.kind == action_catalog.DELETE_HCP_POD:
        outcome = await cluster_api.delete_pod(_hcp_namespace(hosted), action.name, action.uid)
    else:
        kubeconfig = await _kubeconfig(hosted)
        server = hosted_cluster_api.kas_service_url(_hcp_namespace(hosted))
        if action.kind == action_catalog.DELETE_POD:
            outcome = await hosted_upgrade_api.delete_pod(
                kubeconfig, server, request.cluster, action.namespace or "", action.name, action.uid)
        else:
            job = action_catalog.rerun_job_name(action.name, action_id)
            outcome = await hosted_upgrade_api.rerun_cronjob(
                kubeconfig, server, request.cluster, action.namespace or "", action.name, job)
    executed, detail = _ACTION_OUTCOMES[outcome]
    detail = detail.format(job=job)
    activity.logger.info("action %s (%s %s), approved by %s, on %s: %s", action_id, action.kind,
                         request.action.target, request.approved_by, request.cluster, detail)
    return ActionResult(action_id=action_id, executed=executed, detail=detail)

