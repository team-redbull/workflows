"""Cluster-upgrade activities — the per-MCE limb (DESIGN §2). Thin: each one reads, then hands to upgrade_state.

Runs inside each MCE as the workflow ServiceAccount (`cluster-upgrade-worker`),
polling cluster_upgrade_activity_queue(<mce>) (worker_init.py). The MCE's own
API is read through activities/server_lifecycle/cluster_api.py; a hosted
cluster's through its admin kubeconfig, by hosted_upgrade_api.py — the workflow
path only, never the agent's (AGENT.md Part 4).

Its writes on the MCE are two: the `operation` of the cluster's Argo CD
Application (sync_argo_application), and a tier-2 `delete_hcp_pod` a person
approved. It never writes a HostedCluster, NodePool or IDMS (DESIGN D1).
Every verb it uses is in deploy/charts/cluster-upgrade-worker/templates/rbac.yaml.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

from temporalio import activity

from activities.cluster_upgrade import action_catalog, hosted_upgrade_api, upgrade_state
from activities.server_lifecycle import cluster_api, hosted_cluster_api, hosted_resources
from shared.exceptions import (
    AmbiguousHostedClusterError,
    ArgoOperationInProgressError,
    ArgoSyncFailedError,
    ArgoSyncRefusedError,
    HostedClusterAccessError,
    HostedClusterNotFoundError,
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
from shared.settings import ClusterUpgradeActivitySettings

# Argo CD on the MCE: one Application per hosted cluster, named after its day1
# file, made by the hcAppset (CONTEXT §6.1), in ARGO_NAMESPACE; the day1
# commit is in its ARGO_VALUES_REF source when it has several.
_settings = ClusterUpgradeActivitySettings()
# Who Argo CD records as having started the sync.
_SYNC_INITIATOR = "cluster-upgrade-worker"
# Poll the operation this often, and give up this long after asking — under the
# 5-minute start_to_close the workflow gives a sync (dispatch.SYNC_TIMEOUT).
_SYNC_POLL_SECONDS = 5
_SYNC_WAIT_SECONDS = 240
# A watch looks this often, and heartbeats each time (heartbeat_timeout 2 min,
# dispatch.WATCH_HEARTBEAT_TIMEOUT).
_WATCH_POLL_SECONDS = 30

# What the limb reads and writes on the MCE, as cluster_api takes it.
_HOSTED_CLUSTER = {
    "group": hosted_resources.HOSTED_CLUSTER_GROUP, "version": hosted_resources.HOSTED_CLUSTER_VERSION,
    "plural": hosted_resources.HOSTED_CLUSTER_PLURAL, "kind": hosted_resources.HOSTED_CLUSTER_KIND,
}
_NODEPOOL = {
    "group": hosted_resources.HOSTED_CLUSTER_GROUP, "version": hosted_resources.HOSTED_CLUSTER_VERSION,
    "plural": "nodepools", "kind": "NodePool",
}
_MACHINE_SET = {"group": hosted_resources.MACHINE_GROUP, "version": "v1beta1", "plural": "machinesets", "kind": "MachineSet"}
_APPLICATION = {"group": "argoproj.io", "version": "v1alpha1", "plural": "applications", "kind": "Application"}
_IDMS = {"group": "config.openshift.io", "version": "v1", "plural": "imagedigestmirrorsets", "kind": "ImageDigestMirrorSet"}

# A hosted API this attempt cannot read: down or 5xx (clears by itself), or an
# admin kubeconfig it cannot use — none published yet, 403, TLS (a person's).
# Reads and watches report either instead of raising: the pre-flight or the
# watch shows it, rather than a read retried for ever.
_HOSTED_UNREADABLE = (HostedClusterUnreachableError, HostedClusterAccessError)
# An Argo CD operation still under way.
_OPERATION_RUNNING = ("Running", "Terminating")


def _target(request) -> tuple[str, str | None]:
    """(name, namespace) of the HostedCluster a request is about.

    The hostedclusters-setup chart names it `openshift-<clusterName>` in
    `hcp-<clusterName>`, not after the day1 file, so the run sends its real
    name (`hosted_cluster`, from day1). A request of a run planned before
    that sends None: the HostedCluster named after the file, in any namespace.
    """
    return request.hosted_cluster or request.cluster, request.hosted_cluster_namespace


async def _hosted_cluster(name: str, namespace: str | None = None) -> dict | None:
    """The HostedCluster `namespace/name` on the MCE; None when there is none.

    With no namespace it is found by listing, by name in any namespace. Two of
    one name is then AmbiguousHostedClusterError, which a person resolves —
    never a guess.
    """
    if namespace:
        return await cluster_api.read_custom_object(**_HOSTED_CLUSTER, namespace=namespace, name=name)
    found = [
        hc for hc in await cluster_api.list_cluster_custom_objects(**_HOSTED_CLUSTER)
        if (hc.get("metadata") or {}).get("name") == name
    ]
    if len(found) > 1:
        namespaces = sorted((hc.get("metadata") or {}).get("namespace", "?") for hc in found)
        raise AmbiguousHostedClusterError(
            f"{len(found)} HostedClusters are named {name} on this MCE, in {', '.join(namespaces)}"
        )
    return found[0] if found else None


def _namespace(hosted_cluster: dict) -> str:
    return (hosted_cluster.get("metadata") or {}).get("namespace", "")


def _hcp_namespace(hosted_cluster: dict) -> str:
    metadata = hosted_cluster.get("metadata") or {}
    return hosted_resources.hosted_control_plane_namespace(metadata.get("namespace", ""), metadata.get("name", ""))


async def _hosted_access(hosted_cluster: dict) -> tuple[str, str]:
    """(admin kubeconfig text, the control plane's in-cluster API URL) of a HostedCluster.

    The kubeconfig from the Secret its status names, decoded where every admin
    kubeconfig is (hosted_cluster_api.admin_kubeconfig); HostedClusterAccessError
    while there is none.
    """
    namespace = _namespace(hosted_cluster)
    label = f"{namespace}/{(hosted_cluster.get('metadata') or {}).get('name')}"
    secret = hosted_resources.admin_kubeconfig_secret(hosted_cluster)
    if not secret:
        raise HostedClusterAccessError(f"HostedCluster {label} publishes no status.kubeconfig yet")
    kubeconfig = hosted_cluster_api.admin_kubeconfig(
        await cluster_api.read_secret_data(namespace, secret), f"{namespace}/{secret}", label
    )
    return kubeconfig, hosted_cluster_api.kas_service_url(_hcp_namespace(hosted_cluster))


async def _nodepools(hosted_cluster: dict) -> list[dict]:
    """Every NodePool in the HostedCluster's namespace (upgrade_state.nodepools_of says whose)."""
    return await cluster_api.list_custom_objects(**_NODEPOOL, namespace=_namespace(hosted_cluster))


@activity.defn
async def read_mce_state(request: MceRequest) -> MceState:
    """ConfigMap `hypershift/supported-versions`, and the MCE's IDMS sources."""
    found, versions = upgrade_state.supported_versions(
        await cluster_api.read_config_map_data("hypershift", "supported-versions")
    )
    idms = await cluster_api.list_cluster_custom_objects(**_IDMS)
    return MceState(supported_versions_found=found, supported_versions=versions,
                    idms_mirrors=upgrade_state.idms_mirrors(idms))


@activity.defn
async def read_argo_application(request: HostedClusterRequest) -> ArgoApplicationState:
    """The cluster's Application in ARGO_NAMESPACE: sync, health, what is out of sync."""
    return upgrade_state.argo_application_state(await _application(request.cluster), _settings.argo_values_ref)


async def _application(cluster: str) -> dict | None:
    """The cluster's Application: named after its day1 file, in ARGO_NAMESPACE."""
    return await cluster_api.read_custom_object(**_APPLICATION, namespace=_settings.argo_namespace, name=cluster)


@activity.defn
async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
    """Sync the Application to exactly `request.revision`, and wait for the operation.

    Raises ArgoSyncRefusedError (permanent) when anything outside
    `allowed_out_of_sync` is out of sync, when a multi-source Application
    has no ARGO_VALUES_REF source to pin the day1 commit in, or when the
    Application is gone; ArgoOperationInProgressError (retryable) while
    another operation holds it — nothing is asked over one. The ask carries
    the resourceVersion read, so a change in between is a retryable 409.
    Idempotent: already synced at the revision is success, and an operation
    already asked for it, or running, is waited for, not started twice.
    """
    ref = _settings.argo_values_ref
    label = f"{_settings.argo_namespace}/{request.cluster}"
    application = await _application(request.cluster)
    if upgrade_state.synced_at(application, request.revision, ref):
        return ArgoSyncResult(revision=request.revision, phase="Succeeded")
    refusal = upgrade_state.sync_refusal(application, request.allowed_out_of_sync, ref)
    if refusal is not None:
        raise ArgoSyncRefusedError(f"Application {label}: {refusal}")
    busy = upgrade_state.other_operation(application, request.revision, ref)
    if busy is not None:
        raise ArgoOperationInProgressError(
            f"Application {label}: {busy}; nothing was asked, the next attempt reads it again"
        )
    if not upgrade_state.operation_requested(application) and (
        upgrade_state.operation_phase(application, request.revision, ref) not in _OPERATION_RUNNING
    ):
        patched = await cluster_api.patch_custom_object(
            **_APPLICATION, namespace=_settings.argo_namespace, name=request.cluster,
            body=upgrade_state.sync_operation(application, request.revision, _SYNC_INITIATOR, ref),
        )
        if not patched:
            raise ArgoSyncRefusedError(f"Application {label}: the Application does not exist")
        activity.logger.info("asked Argo CD to sync %s to %s", label, request.revision)
    deadline = time.monotonic() + _SYNC_WAIT_SECONDS
    while time.monotonic() < deadline:
        await asyncio.sleep(_SYNC_POLL_SECONDS)
        activity.heartbeat()
        application = await _application(request.cluster)
        if application is None:
            raise ArgoSyncRefusedError(f"Application {label}: the Application does not exist")
        phase = upgrade_state.operation_phase(application, request.revision, ref)
        # While `operation` is set, operationState may still be the previous one's.
        ended = not upgrade_state.operation_requested(application)
        if upgrade_state.synced_at(application, request.revision, ref) or (phase == "Succeeded" and ended):
            return ArgoSyncResult(revision=request.revision, phase="Succeeded")
        if phase in ("Failed", "Error") and ended:
            raise ArgoSyncFailedError(
                f"Argo CD's sync of {label} to {request.revision} ended {phase}: "
                f"{upgrade_state.operation_message(application)}"
            )
    raise ArgoSyncFailedError(
        f"Argo CD's sync of {label} to {request.revision} did not finish in {_SYNC_WAIT_SECONDS}s"
    )


@activity.defn
async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
    """The HostedCluster and its NodePools: versions, conditions, rollout settings."""
    hosted = await _hosted_cluster(*_target(request))
    if hosted is None:
        return upgrade_state.hosted_cluster_state(None, [])
    return upgrade_state.hosted_cluster_state(hosted, await _nodepools(hosted))


@activity.defn
async def read_hosted_health(request: HostedHealthRequest) -> HostedHealth:
    """Through the hosted cluster's admin kubeconfig (the existing workflow-path pattern), in one session.

    A hosted API that cannot be read — unreachable, or an admin kubeconfig
    that cannot be used (none published, 403, TLS) — is `reachable: False`,
    which the pre-flight shows a person, rather than a read retried for ever.
    """
    hosted = await _hosted_cluster(*_target(request))
    if hosted is None:
        return HostedHealth(reachable=False)
    try:
        health = await hosted_upgrade_api.read_health_objects(*await _hosted_access(hosted), request.cluster)
    except _HOSTED_UNREADABLE as err:
        activity.logger.warning("hosted cluster %s unreadable for its health (%s): %s",
                                request.cluster, type(err).__name__, err)
        return HostedHealth(reachable=False)
    nodepools = upgrade_state.hosted_cluster_state(hosted, await _nodepools(hosted)).nodepools
    scope = upgrade_state.nodes_in_scope(health["nodes"], request.nodepools)
    return HostedHealth(
        reachable=True,
        operators_unhealthy=upgrade_state.operators_unhealthy(health["cluster_operators"]),
        **upgrade_state.upgradeable(health["cluster_version"]),
        nodes_not_ready=upgrade_state.nodes_not_ready(health["nodes"]),
        nodepools_short=upgrade_state.nodepools_short(nodepools, request.nodepools),
        pending_csrs=upgrade_state.pending_csrs(health["csrs"]),
        blocking_pdbs=upgrade_state.blocking_pdbs(health["pdbs"], health["pods"], scope),
        dead_webhooks=upgrade_state.dead_webhooks(health["webhooks"], health["endpoints"]),
        idms_mirrors=upgrade_state.idms_mirrors(health["image_digest_mirror_sets"]),
    )


@activity.defn
async def read_admin_acks(request: AdminAckRequest) -> AdminAckState:
    """The hosted cluster's admin gates and acks, and who called `request.apis` in the last 24 hours.

    A hosted API that cannot be read is `reachable: False`, as in read_hosted_health.
    """
    hosted = await _hosted_cluster(*_target(request))
    if hosted is None:
        return AdminAckState(reachable=False)
    try:
        objects = await hosted_upgrade_api.read_admin_ack_objects(
            *await _hosted_access(hosted), request.cluster, request.apis
        )
    except _HOSTED_UNREADABLE as err:
        activity.logger.warning("hosted cluster %s unreadable for its admin acks (%s): %s",
                                request.cluster, type(err).__name__, err)
        return AdminAckState(reachable=False)
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

    Raises AdminGateNotFoundError (permanent) for a gate its admin-gates does
    not hold; HostedClusterNotFoundError (retried) when the HostedCluster is
    not on the MCE, and AdminAckNotAppliedError (retried) for an ack that did
    not read back — nothing is reported written that the cluster does not hold.
    """
    name, namespace = _target(request)
    hosted = await _hosted_cluster(name, namespace)
    if hosted is None:
        raise HostedClusterNotFoundError(
            f"no HostedCluster {name}{f' in {namespace}' if namespace else ''} on this MCE "
            f"to ack {request.gate} in; nothing was written"
        )
    changed = await hosted_upgrade_api.give_admin_ack(*await _hosted_access(hosted), request.cluster, request.gate)
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
    """The hosted ClusterOperators — the only hosted read a control-plane poll needs — or None
    when the hosted API cannot be read this time."""
    try:
        return await hosted_upgrade_api.read_cluster_operators(*await _hosted_access(hosted), cluster)
    except _HOSTED_UNREADABLE as err:
        activity.logger.info("hosted cluster %s unreadable this poll: %s", cluster, err)
        return None


@activity.defn
async def watch_control_plane(request: WatchControlPlaneRequest) -> WatchOutcome:
    """Until the HostedCluster runs `version`, nothing changes for the stall window, or the deadline.

    Done: version history[0] is `version` and Completed, ClusterVersionSucceeding
    and ValidReleaseInfo True, not Progressing, and every hosted ClusterOperator
    at `version`. Heartbeats every poll.
    """
    snapshot = _resumed_snapshot()
    while True:
        hosted = await _hosted_cluster(*_target(request))
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


async def _nodepool_view(request: WatchNodePoolRequest) -> tuple[dict | None, dict | None, dict | None]:
    """(NodePool, its CAPI MachineSet, the hosted objects or None when unreadable); None where absent."""
    hosted = await _hosted_cluster(*_target(request))
    if hosted is None:
        return None, None, None
    nodepool = await cluster_api.read_custom_object(**_NODEPOOL, namespace=_namespace(hosted), name=request.nodepool)
    machine_set = await cluster_api.read_custom_object(
        **_MACHINE_SET, namespace=_hcp_namespace(hosted), name=request.nodepool,
    )
    try:
        objects = await hosted_upgrade_api.read_nodepool_objects(
            *await _hosted_access(hosted), request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL,
        )
    except _HOSTED_UNREADABLE as err:
        activity.logger.info("hosted cluster %s unreadable this poll: %s", request.cluster, err)
        objects = None
    return nodepool, machine_set, objects


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
        target_config, current_config = upgrade_state.machine_set_configs(machine_set)
        done, seen = upgrade_state.nodepool_progress(
            nodepool, None if objects is None else objects["nodes"], target_config, request.version,
            current_config=current_config,
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
    hosted = await _hosted_cluster(*_target(request))
    objects, unreadable = None, f"HostedCluster {_target(request)[0]} not found on the MCE"
    if hosted is not None:
        try:
            objects = await hosted_upgrade_api.read_gate_objects(
                *await _hosted_access(hosted),
                request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL, request.workload_checks,
            )
        except _HOSTED_UNREADABLE as err:
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

    For a NodePool also its drain: cordoned nodes, the pods a drain still has
    to move off them with their state, and the PDBs in those pods' namespaces
    with disruptionsAllowed and healthy counts. No pod logs: the limb reads
    none (no `pods/log`), so no hosted-cluster-config-operator drainer lines
    either. Never a Secret; what cannot be read says so instead of failing
    the bundle.
    """
    name, namespace = _target(request)
    hosted = await _hosted_cluster(name, namespace)
    nodepools: list[dict] = []
    operators = pods = warnings = None
    drain = None
    if hosted is not None:
        nodepools = upgrade_state.nodepools_of(await _nodepools(hosted), name, request.nodepool)
        try:
            kubeconfig, server = await _hosted_access(hosted)
            extra = await hosted_upgrade_api.read_evidence_objects(kubeconfig, server, request.cluster)
            operators, pods, warnings = extra["cluster_operators"], extra["data_plane_pods"], extra["warning_events"]
            if request.nodepool is not None:
                objects = await hosted_upgrade_api.read_nodepool_objects(
                    kubeconfig, server, request.cluster, request.nodepool, upgrade_state.NODEPOOL_LABEL,
                )
                drain = upgrade_state.drain_text(objects["nodes"], objects["pods"], objects["pdbs"])
        except _HOSTED_UNREADABLE as err:
            activity.logger.info("hosted cluster %s partly unreadable for evidence: %s", request.cluster, err)
    sections, truncated = upgrade_state.evidence_sections(
        hosted_cluster=hosted,
        nodepools=nodepools,
        cluster_operators=operators,
        argo=upgrade_state.argo_application_state(await _application(request.cluster), _settings.argo_values_ref),
        data_plane_pods=pods,
        warnings=warnings,
        drain=drain,
    )
    return EvidenceBundle(sections=sections, truncated=truncated)


def _job_annotations(request: ProposedActionRequest) -> dict[str, str]:
    """What a Job this request creates carries: the action, the cluster, the workflow, and a
    fingerprint of the request and of this activity execution (a retry has the same)."""
    info = activity.info()
    return action_catalog.job_annotations(
        action_id=request.action.action_id,
        cluster=request.cluster,
        workflow_id=info.workflow_id,
        fingerprint=action_catalog.request_fingerprint(
            request.model_dump_json(), info.workflow_id, info.workflow_run_id, info.activity_id,
        ),
    )


@activity.defn
async def execute_proposed_action(request: ProposedActionRequest) -> ActionResult:
    """Run one approved tier-2 action with the workflow's identity, through the typed catalog.

    Raises ProposedActionRefusedError (permanent) for a kind not in the catalog,
    or a target outside its rules (DESIGN §8.3). Idempotent: a retry finds the
    pod gone or its own Job (by the annotations it carries), and says so; a Job
    of that name that is not its own is reported, never claimed.
    """
    action = action_catalog.check(request.action)
    action_id = request.action.action_id
    hosted = await _hosted_cluster(*_target(request))
    if hosted is None:
        return ActionResult(action_id=action_id, executed=False,
                            detail=f"not executed: HostedCluster {_target(request)[0]} not found on the MCE")
    job = None
    if action.kind == action_catalog.DELETE_HCP_POD:
        outcome = await cluster_api.delete_pod(_hcp_namespace(hosted), action.name, action.uid)
    else:
        kubeconfig, server = await _hosted_access(hosted)
        if action.kind == action_catalog.DELETE_POD:
            outcome = await hosted_upgrade_api.delete_pod(
                kubeconfig, server, request.cluster, action.namespace or "", action.name, action.uid)
        elif action.kind == action_catalog.DELETE_FAILED_JOB:
            outcome = await hosted_upgrade_api.delete_failed_job(
                kubeconfig, server, request.cluster, action.namespace or "", action.name, action.uid)
        else:
            # ProposedActionRequest carries no agent invocation id yet: once it
            # does, pass request.invocation_id here, so the "same" action of two
            # invocations gets two names.
            job = action_catalog.rerun_job_name(action.name, action_id, invocation_id=None)
            outcome = await hosted_upgrade_api.rerun_cronjob(
                kubeconfig, server, request.cluster, action.namespace or "", action.name, job,
                _job_annotations(request))
    executed, detail = action_catalog.outcome(outcome, job)
    activity.logger.info("action %s (%s %s), approved by %s, on %s: %s", action_id, action.kind,
                         request.action.target, request.approved_by, request.cluster, detail)
    return ActionResult(action_id=action_id, executed=executed, detail=detail)
