"""Cluster-upgrade activity signatures — the domain's PER-MCE cluster limb.

Registered by `cluster-upgrade-worker`, one deployment inside each MCE, polling
`cluster_upgrade_activity_queue(<mce>)` — the server-lifecycle routing (see
shared/consts.py): the worker authenticates to its own API server as the
workflow ServiceAccount and dials out to Temporal, so no MCE credential ever
leaves the MCE.

Its cluster WRITES are two. `sync_argo_application`, on the MCE. And
`give_admin_ack`, inside a hosted cluster: one key of its
openshift-config/admin-acks, a gate the rules file lists (DESIGN §6.2a, D16).
It never writes a HostedCluster, NodePool or IDMS: those change only through a
day1 commit that Argo CD applies (DESIGN D1). `execute_proposed_action` runs a
tier-2 action a person approved, through the typed catalog.

The watches are long: they poll inside the activity and heartbeat, so a
three-hour control-plane upgrade is a handful of history events, and a worker
restart resumes from the last heartbeat with its clocks intact.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.cluster_upgrade import (
    ActionResult,
    AdminAckGiven,
    AdminAckRequest,
    AdminAckState,
    ArgoApplicationState,
    ArgoSyncRequest,
    ArgoSyncResult,
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
    ProposedActionRequest,
    WatchControlPlaneRequest,
    WatchNodePoolRequest,
    WatchOutcome,
)


@activity.defn
async def read_mce_state(request: MceRequest) -> MceState:
    """ConfigMap `hypershift/supported-versions`, and the MCE's IDMS sources."""
    ...


@activity.defn
async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
    """The HostedCluster and its NodePools: versions, conditions, rollout settings."""
    ...


@activity.defn
async def read_argo_application(request: HostedClusterRequest) -> ArgoApplicationState:
    """The cluster's Application in openshift-gitops: sync, health, what is out of sync."""
    ...


@activity.defn
async def read_hosted_health(request: HostedHealthRequest) -> HostedHealth:
    """Through the hosted cluster's admin kubeconfig (the existing workflow-path pattern)."""
    ...


@activity.defn
async def read_admin_acks(request: AdminAckRequest) -> AdminAckState:
    """The hosted cluster's admin gates and acks, and who called `request.apis` in the last 24 hours.

    openshift-config-managed/admin-gates and openshift-config/admin-acks as
    found, the CVO's UpgradeableAdminAckRequired message, and one ApiUsage per
    (API, user, user agent) from the APIRequestCount `status.last24h`. An API
    with no APIRequestCount was not called. Through the hosted cluster's admin
    kubeconfig (the existing workflow-path pattern).
    """
    ...


@activity.defn
async def give_admin_ack(request: GiveAdminAckRequest) -> AdminAckGiven:
    """Merge `data.<gate>: "true"` into the hosted cluster's openshift-config/admin-acks.

    Only that one key, and only for a gate the hosted admin-gates holds; reads
    it back before returning. Idempotent: an ack already "true" is
    `changed: False`.
    """
    ...


@activity.defn
async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
    """Sync the Application to exactly `request.revision`, and wait for the operation.

    Raises ArgoSyncRefusedError (permanent) when anything outside
    `allowed_out_of_sync` is out of sync.
    """
    ...


@activity.defn
async def watch_control_plane(request: WatchControlPlaneRequest) -> WatchOutcome:
    """Until the HostedCluster runs `version`, nothing changes for the stall window, or the deadline.

    Done: version history[0] is `version` and Completed, ClusterVersionSucceeding
    and ValidReleaseInfo True, not Progressing, and every hosted ClusterOperator
    at `version`. Heartbeats every poll.
    """
    ...


@activity.defn
async def watch_nodepool(request: WatchNodePoolRequest) -> WatchOutcome:
    """Until the NodePool runs `version` with every node healthy, a stall, or the deadline.

    Done: status.version is `version`, UpdatingVersion False, AllNodesHealthy
    True (and SupportedVersionSkew True where the operator reports it).
    Stalled: no node finished for `stall_minutes`, or one node's drain was
    refused by a PodDisruptionBudget for `drain_blocked_minutes` (the snapshot
    says which node, PDB and pods). Heartbeats every poll.
    """
    ...


@activity.defn
async def watch_nodepool_gate(request: NodePoolGateRequest) -> GateOutcome:
    """Until every gate check has held for the soak window, or the deadline.

    Platform: the NodePool's nodes Ready and schedulable. Workload (when asked):
    PDBs on its nodes back to disruptionsAllowed >= 1, StatefulSets ready.
    Then the cluster's declared readiness checks. Heartbeats every poll.
    """
    ...


@activity.defn
async def collect_evidence(request: EvidenceRequest) -> EvidenceBundle:
    """Read-only evidence for the agent: conditions, operators, events, Argo, data plane.

    For a NodePool also its drain: cordoned nodes, the pods left on them, the
    PDBs covering those pods, and the hosted-cluster-config-operator's drainer lines.
    """
    ...


@activity.defn
async def execute_proposed_action(request: ProposedActionRequest) -> ActionResult:
    """Run one approved tier-2 action with the workflow's identity, through the typed catalog.

    Raises ProposedActionRefusedError (permanent) for a kind not in the catalog.
    """
    ...
