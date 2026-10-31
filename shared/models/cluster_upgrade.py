"""Typed state for the cluster-upgrade workflows — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model, never
an untyped dict.

None of these models carries `extra="forbid"`: they are decoded from Temporal
history on every replay — and an upgrade run lives for weeks, through deploys —
so a field removed later would wedge in-flight runs. Strictness about unknown
fields belongs on the API-edge request models in the router.

Versions are carried as bare `X.Y.Z` strings (see `shared/ocp_version.py`);
the `-x86_64` mirror tag is derived where an image reference is built, never
stored, so there is one spelling in history.

Three limbs serve the domain, one per credential set (CLAUDE.md §2), and the
models are grouped the same way below: the hub's git and registry work, the
per-MCE cluster reads and Argo CD syncs, and the per-MCE agent call.

Design: docs/upgrade/DESIGN.md in the Cluster-AutoUpgrade workspace.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

# --- the path ----------------------------------------------------------------


class LadderRung(BaseModel):
    """One verified release on the upgrade ladder (day1 `upgrade/ladder.yaml`).

    A rung is a release the team has checked upgrades cleanly into the next
    minor. `must` rungs are never skipped; `data_plane_required` makes every
    NodePool roll to this release before the control plane moves past it.
    """

    version: str
    must: bool = False
    data_plane_required: bool = False


class StepKind(str, Enum):
    """What a step moves: the HostedCluster, or NodePools."""

    CONTROL_PLANE = "control-plane"
    WORKERS = "workers"


class StepReason(str, Enum):
    """Why the resolver put a step in the plan — shown to the approver."""

    LADDER = "ladder"  # an intermediate rung of the ladder
    TARGET = "target"  # the requested target release
    SKEW = "skew"  # workers must move before the control plane passes the skew limit
    RUNG_REQUIRED = "rung-required"  # the rung has dataPlane: required
    FINAL = "final"  # workers follow the control plane to the target at the end


class UpgradeStep(BaseModel):
    """One step of a plan: move the control plane, or a set of NodePools, to `version`."""

    kind: StepKind
    version: str
    reason: StepReason
    # WORKERS only: every NodePool below `version` when the step runs, in the
    # order the cluster's profile will roll them. Empty for CONTROL_PLANE.
    nodepools: list[str] = Field(default_factory=list)


class PinnedRelease(BaseModel):
    """The digests one release tag resolved to in the mirror.

    The release payload and the hypershift control-plane-operator image are
    both pinned: the chart renders both from tags (`tag`, `cpoTag`), and either
    moving under a run would deploy something nobody approved.
    """

    release: str
    control_plane_operator: str


class UpgradePlan(BaseModel):
    """The ordered steps from a cluster's current versions to its target.

    `pinned` maps each release in the plan to its digests. Mirror validation
    fills it, and every sync re-checks it (DESIGN D2), so a tag that moves in
    the mirror after approval stops the run instead of deploying it.
    """

    control_plane: str
    nodepools: dict[str, str]
    target: str
    max_worker_skew: int
    steps: list[UpgradeStep]
    pinned: dict[str, PinnedRelease] = Field(default_factory=dict)


# --- the request and the run ---------------------------------------------------


class NodePoolMode(str, Enum):
    """Who starts each NodePool roll: a person (`continue`), or the run after a soak."""

    MANUAL = "manual"
    AUTO = "auto"


class UpgradeClusterInput(BaseModel):
    """What one upgrade-cluster run was asked to do — decoded from history.

    Exactly one of `target_version` and `target_minor` is set; the router
    enforces it. With `target_minor`, the run picks the recommended target:
    the newest release of that minor that is both mirrored and a ladder rung.
    """

    mce_cluster: str
    cluster: str
    target_version: str | None = None
    target_minor: str | None = None
    nodepool_mode: NodePoolMode = NodePoolMode.MANUAL
    # Default: the cluster file's `upgrade.nodePoolOrder`, else zone order.
    nodepool_order: list[str] | None = None
    workers_to_target: bool = True
    auto_approve: bool = False
    requested_by: str


class ClusterProfile(str, Enum):
    """How a cluster's NodePools roll (DESIGN D7): derived from zone labels."""

    GENERIC = "generic"
    ZONED = "zoned"


class ReadinessCheck(BaseModel):
    """A customer-declared check a NodePool gate waits on (cluster file `upgrade.readinessChecks`).

    Exactly one of `promql` and `http` is set; the day1 validator enforces it.
    """

    name: str
    promql: str | None = None
    expect: str | None = None  # e.g. "== 1", compared with the query's single value
    http: str | None = None
    expect_status: int | None = None


class ReleaseLayout(BaseModel):
    """How a release image reference is built (ladder.yaml `release`), templates unexpanded."""

    image_path: str
    repo: str
    tag: str
    cpo_repo: str
    cpo_tag: str


class UpgradeContext(BaseModel):
    """Everything the steps need that planning settled. It never changes after approval.

    Carried through continue-as-new, so a run that resumes weeks later rolls
    its next NodePool with the profile, order and limits it was approved with,
    whatever the rules file says by then.
    """

    plan: UpgradePlan
    profile: ClusterProfile
    nodepool_order: list[str]
    soak_minutes: int
    readiness_checks: list[ReadinessCheck] = Field(default_factory=list)
    max_concurrent_control_planes: int
    release: ReleaseLayout
    verified_against_graph: str | None = None
    day1_path: str
    tier1_enabled: bool = False


class UpgradePhase(str, Enum):
    """Where an upgrade-cluster run is (the lifecycle in DESIGN §3)."""

    PLANNING = "planning"
    PREFLIGHT = "preflight"
    AWAITING_APPROVAL = "awaiting-approval"
    WAITING_FOR_SLOT = "waiting-for-slot"
    CONTROL_PLANE_STEP = "control-plane-step"
    AWAITING_DAY2_CONFIRM = "awaiting-day2-confirm"
    AWAITING_CONTINUE = "awaiting-continue"
    NODEPOOL_ROLL = "nodepool-roll"
    NODEPOOL_GATE = "nodepool-gate"
    AGENT_INVOCATION = "agent-invocation"
    NEEDS_HUMAN = "needs-human"
    PAUSED = "paused"
    POSTFLIGHT = "postflight"
    ABORTING = "aborting"
    DONE = "done"
    CANCELLED = "cancelled"
    ABORTED = "aborted"


class HumanDecision(BaseModel):
    """The argument of approve, confirm-day2, pause, resume, abort and retry: who, and why."""

    by: str
    reason: str | None = None


class ContinueNodePool(BaseModel):
    """The argument of continue: who, and optionally which NodePool rolls next."""

    by: str
    nodepool: str | None = None


class ActionDecision(BaseModel):
    """The argument of an agent proposal's approve or reject."""

    by: str
    action_id: str


class UpgradeLogEntry(BaseModel):
    """One line of the run's audit log: a human decision, a commit, an agent verdict."""

    at: datetime
    event: str
    by: str | None = None
    detail: str = ""


class PendingAction(BaseModel):
    """An agent proposal (tier 2) waiting for a person, and what became of it."""

    action: ProposedAction
    decision: Literal["pending", "approved", "rejected"] = "pending"
    decided_by: str | None = None
    executed: bool = False
    result: str | None = None


class UpgradeClusterResumeState(BaseModel):
    """What a run hands the next one when it continues as new.

    A run continues as new after every control-plane step and every NodePool,
    so the step it resumes at is either untouched or, for a worker step, has
    `nodepools_done` rolled already.
    """

    context: UpgradeContext
    approved_by: str
    next_step: int
    nodepools_done: list[str] = Field(default_factory=list)
    # True when the step at `next_step` has already committed: a retry after
    # it must not run the pre-flight a mid-upgrade cluster cannot pass.
    step_committed: bool = False
    control_plane_version: str
    nodepool_versions: dict[str, str]
    day1_blob: str
    paused: HumanDecision | None = None
    log: list[UpgradeLogEntry] = Field(default_factory=list)


class UpgradeClusterRunArgs(BaseModel):
    """run()'s single argument: the request, plus state when continuing as new."""

    input: UpgradeClusterInput
    resume: UpgradeClusterResumeState | None = None


class UpgradeClusterProgress(BaseModel):
    """The `progress` query: what a person needs to know to act on the run."""

    phase: UpgradePhase
    waiting_for: str | None = None
    plan: UpgradePlan | None = None
    profile: ClusterProfile | None = None
    current_step: int | None = None
    current_nodepool: str | None = None
    nodepools_done: list[str] = Field(default_factory=list)
    control_plane_version: str | None = None
    nodepool_versions: dict[str, str] = Field(default_factory=dict)
    paused: bool = False
    control_plane_workflow_id: str | None = None
    pending_actions: list[PendingAction] = Field(default_factory=list)
    last_verdict: AgentVerdict | None = None
    verified_against_graph: str | None = None
    log: list[UpgradeLogEntry] = Field(default_factory=list)


class UpgradeOutcome(str, Enum):
    DONE = "done"  # every step ran; or nothing was needed
    CANCELLED = "cancelled"  # aborted before approval: nothing was committed
    ABORTED = "aborted"  # aborted after approval: pausedUntil committed and synced


class UpgradeClusterResult(BaseModel):
    outcome: UpgradeOutcome
    mce_cluster: str
    cluster: str
    plan: UpgradePlan | None = None
    steps_completed: int = 0
    control_plane_version: str | None = None
    nodepool_versions: dict[str, str] = Field(default_factory=dict)
    log: list[UpgradeLogEntry] = Field(default_factory=list)


# --- checks --------------------------------------------------------------------


class CheckFailure(str, Enum):
    """What a failed check does to the run (the "On failure" column of DESIGN §7)."""

    FAILED = "failed"  # the run fails; nothing was changed
    NEEDS_HUMAN = "needs-human"
    NOTIFY_OWNER = "notify-owner"  # needs-human, and the owner is the one to act
    WAIT = "wait"  # re-checked for a while, then needs-human


class CheckResult(BaseModel):
    name: str
    passed: bool
    expected: str = ""
    observed: str = ""
    on_failure: CheckFailure = CheckFailure.NEEDS_HUMAN


# --- the hub limb: day1 git, the mirror, day2, Brain ---------------------------


class UpgradeRulesRequest(BaseModel):
    mce_cluster: str


class UpgradeRules(BaseModel):
    """day1 `upgrade/ladder.yaml` plus `upgrade/mces/<mce>.yaml`, as read at `revision`."""

    revision: str
    ladder: list[LadderRung]
    max_worker_skew: int
    verified_against_graph: str | None = None
    release: ReleaseLayout
    mce_version: str
    supported_hosted_versions: list[str]
    max_concurrent_control_planes: int


class Day1ClusterRequest(BaseModel):
    mce_cluster: str
    cluster: str


class Day1NodePool(BaseModel):
    """One `nodepool[]` entry of a day1 cluster file, as written (not as rendered)."""

    name: str
    workertag: str | None = None  # bare version; None when the NodePool inherits mastertag
    upgrade_type: str | None = None
    zone: str | None = None  # nodeLabels topology.kubernetes.io/zone
    replicas: int | None = None
    max_unavailable: str | None = None  # "1", "33%" — as the chart passes it on


class ClusterUpgradeSettings(BaseModel):
    """The cluster file's optional `upgrade:` block. The chart never reads it."""

    profile: ClusterProfile | None = None
    nodepool_order: list[str] | None = None
    readiness_checks: list[ReadinessCheck] = Field(default_factory=list)
    soak_minutes: int | None = None
    wave: int | None = None


class Day1Cluster(BaseModel):
    """A day1 cluster file as the upgrade sees it, at `revision`, with its git blob id."""

    path: str
    revision: str
    blob: str
    mastertag: str | None = None  # bare version
    top_level_workertag: str | None = None
    image_path: str | None = None  # effective, after the site and MCE files
    image_hc: str | None = None
    nodepools: list[Day1NodePool] = Field(default_factory=list)
    upgrade: ClusterUpgradeSettings = Field(default_factory=ClusterUpgradeSettings)
    # What the parser refused, line by line: a duplicate key, a NodePool
    # named twice. Non-empty fails planning (check 3).
    problems: list[str] = Field(default_factory=list)


class ReleaseTagsRequest(BaseModel):
    release: ReleaseLayout


class ReleaseTags(BaseModel):
    """The release tags the mirror lists, either spelling, unfiltered."""

    tags: list[str]


class PinReleasesRequest(BaseModel):
    versions: list[str]
    release: ReleaseLayout


class PinnedReleases(BaseModel):
    """Digests of every release that resolved, and what did not.

    `missing` is a tag absent from the mirror; `problems` is a release whose
    payload is incomplete there (`oc adm release info` failed, or an image it
    references is missing). Neither raises: the workflow turns both into one
    "mirror these first" message with the ImageSetConfiguration to do it.
    """

    pinned: dict[str, PinnedRelease] = Field(default_factory=dict)
    missing: list[str] = Field(default_factory=list)
    problems: list[str] = Field(default_factory=list)


class Day2LayersRequest(BaseModel):
    cluster: str
    versions: list[str]


class Day2LayerGap(BaseModel):
    sig: str
    chart: str
    version: str


class Day2Layers(BaseModel):
    """Every pinned day2 chart missing an `ocp-versions/<v>/` layer for a step's version."""

    missing: list[Day2LayerGap] = Field(default_factory=list)


class Day1ChangeKind(str, Enum):
    CONTROL_PLANE = "control-plane"  # mastertag, and every NodePool's version pinned
    NODEPOOL = "nodepool"  # one nodepool[].workertag
    PAUSE = "pause"  # pausedUntil, for an abort
    FINISH = "finish"  # drop the top-level workertag the run added


class Day1ChangeRequest(BaseModel):
    """One commit to a day1 cluster file (DESIGN §6.1).

    `expected_blob` is compare-and-set: the limb refuses with
    Day1FileChangedError when the file's blob is not that one, so a person's
    edit is never committed over. None only for an abort, which must land
    whatever the file holds.
    """

    workflow_id: str
    mce_cluster: str
    cluster: str
    path: str
    expected_blob: str | None
    change: Day1ChangeKind
    version: str | None = None
    nodepool: str | None = None
    # CONTROL_PLANE: the release every NodePool stays on, written explicitly so
    # moving mastertag never moves a worker.
    nodepool_versions: dict[str, str] = Field(default_factory=dict)


class Day1Commit(BaseModel):
    """`changed` False: the file already said so; `revision` is then the current HEAD."""

    revision: str
    blob: str
    changed: bool


class IncidentRequest(BaseModel):
    workflow_id: str
    invocation_id: str
    mce_cluster: str
    cluster: str
    phase: str
    step: UpgradeStep
    nodepool: str | None = None
    verdict: AgentVerdict


class IncidentRecord(BaseModel):
    path: str  # in Brain, e.g. incidents/2026-11-02-hypershift-<slug>.md


# --- the per-MCE limb: cluster reads, Argo CD syncs, watches -------------------


class MceRequest(BaseModel):
    """Names the MCE so the limb can refuse a request routed to the wrong worker."""

    mce_cluster: str


class MceState(BaseModel):
    """What the MCE itself says it can do: its HyperShift and its mirror config."""

    supported_versions_found: bool
    supported_versions: list[str] = Field(default_factory=list)
    # The mirror repositories of the MCE's ImageDigestMirrorSets, which its
    # HyperShift operator pulls release payloads through.
    idms_mirrors: list[str] = Field(default_factory=list)


class HostedClusterRequest(BaseModel):
    mce_cluster: str
    cluster: str


class NodePoolState(BaseModel):
    name: str
    version: str | None = None  # status.version, bare
    replicas: int | None = None
    ready_replicas: int | None = None
    upgrade_type: str | None = None
    max_unavailable: str | None = None
    zone: str | None = None
    conditions: dict[str, str] = Field(default_factory=dict)  # type -> status


class HostedClusterState(BaseModel):
    """A HostedCluster and its NodePools as the MCE reports them: the truth."""

    found: bool
    namespace: str | None = None
    # status.version.history[0] when its state is Completed; None mid-upgrade.
    control_plane_version: str | None = None
    progressing: bool | None = None
    conditions: dict[str, str] = Field(default_factory=dict)
    # The mirror repositories spec.imageContentSources points at.
    image_content_mirrors: list[str] = Field(default_factory=list)
    nodepools: list[NodePoolState] = Field(default_factory=list)


class ArgoApplicationState(BaseModel):
    """The hosted cluster's Application in openshift-gitops on its MCE."""

    found: bool
    sync_status: str | None = None
    health_status: str | None = None
    revision: str | None = None
    out_of_sync: list[str] = Field(default_factory=list)  # "Kind/name"


class HostedHealthRequest(BaseModel):
    mce_cluster: str
    cluster: str
    # Only these NodePools' nodes are in scope for the PDB check; all when empty.
    nodepools: list[str] = Field(default_factory=list)


class HostedHealth(BaseModel):
    """Read through the hosted cluster's API: operators, nodes, PDBs, webhooks, IDMS."""

    reachable: bool
    operators_unhealthy: list[str] = Field(default_factory=list)
    cluster_version_upgradeable: bool | None = None
    upgradeable_message: str | None = None
    nodes_not_ready: list[str] = Field(default_factory=list)
    nodepools_short: list[str] = Field(default_factory=list)  # below desired replicas
    pending_csrs: int = 0
    blocking_pdbs: list[str] = Field(default_factory=list)  # "ns/name", disruptionsAllowed 0
    dead_webhooks: list[str] = Field(default_factory=list)  # failurePolicy Fail, no endpoints
    # The mirror repositories of the hosted cluster's ImageDigestMirrorSets (day2 chart).
    idms_mirrors: list[str] = Field(default_factory=list)


class ArgoSyncRequest(BaseModel):
    """Sync the cluster's Application to exactly `revision`.

    Refused (ArgoSyncRefusedError) when anything outside `allowed_out_of_sync`
    is out of sync: someone else's change is waiting there, and syncing would
    deploy it under this run's name. Already synced at `revision` is success.
    """

    mce_cluster: str
    cluster: str
    revision: str
    allowed_out_of_sync: list[str]


class ArgoSyncResult(BaseModel):
    revision: str
    phase: str


class WatchResult(str, Enum):
    DONE = "done"
    STALLED = "stalled"  # nothing changed for the stall window
    DEADLINE = "deadline"  # the step's whole budget is spent


class ProgressSnapshot(BaseModel):
    """The last thing a watch saw; also its heartbeat details, so a retry keeps its clocks."""

    summary: str = ""
    conditions: dict[str, str] = Field(default_factory=dict)
    updated: int | None = None
    total: int | None = None
    started_at: datetime | None = None
    last_change_at: datetime | None = None


class WatchControlPlaneRequest(BaseModel):
    mce_cluster: str
    cluster: str
    version: str
    stall_minutes: int
    deadline_minutes: int


class WatchNodePoolRequest(BaseModel):
    mce_cluster: str
    cluster: str
    nodepool: str
    version: str
    stall_minutes: int
    deadline_minutes: int


class WatchOutcome(BaseModel):
    result: WatchResult
    snapshot: ProgressSnapshot = Field(default_factory=ProgressSnapshot)


class NodePoolGateRequest(BaseModel):
    """Passes once every check has held, continuously, for `soak_minutes`."""

    mce_cluster: str
    cluster: str
    nodepool: str
    version: str
    workload_checks: bool  # PDBs and StatefulSets; zoned profile, or any declared check
    readiness_checks: list[ReadinessCheck] = Field(default_factory=list)
    soak_minutes: int
    deadline_minutes: int


class GateResult(str, Enum):
    PASSED = "passed"
    DEADLINE = "deadline"


class GateOutcome(BaseModel):
    result: GateResult
    failing_checks: list[CheckResult] = Field(default_factory=list)


class EvidenceRequest(BaseModel):
    mce_cluster: str
    cluster: str
    nodepool: str | None = None


class EvidenceBundle(BaseModel):
    """Read-only evidence for the agent, size-capped, never a Secret.

    `sections` maps a heading ("hostedcluster", "nodepools", "clusteroperators",
    "events", "argo", "data-plane") to its rendered text.
    """

    sections: dict[str, str] = Field(default_factory=dict)
    truncated: bool = False


class ProposedActionRequest(BaseModel):
    mce_cluster: str
    cluster: str
    action: ProposedAction
    approved_by: str


class ActionResult(BaseModel):
    action_id: str
    executed: bool
    detail: str = ""


# --- the agent limb ------------------------------------------------------------


class AgentBudget(BaseModel):
    wall_clock_seconds: int = 900  # judge latency counts against it (AGENT.md #24)
    max_tool_calls: int = 40
    max_tier1_actions: int = 2  # moot while tier1_enabled is False


class AgentRequest(BaseModel):
    """One invocation of the upgrade agent, at one failed gate (DESIGN §8.1).

    `invocation_id` is unique per invocation and stable across the activity's
    retries: the tool server keys its budget counters and executed tier-1 calls
    on it, so a retry can neither repeat an action nor reset the budget.
    """

    invocation_id: str
    workflow_id: str
    mce_cluster: str
    cluster: str
    profile: ClusterProfile
    phase: str
    step: UpgradeStep
    from_version: str | None = None
    nodepool: str | None = None
    gate: str
    failing_checks: list[CheckResult] = Field(default_factory=list)
    evidence: EvidenceBundle
    budget: AgentBudget = Field(default_factory=AgentBudget)
    tier1_enabled: bool = False


class ToolCallRecord(BaseModel):
    tool: str
    args: dict[str, str] = Field(default_factory=dict)
    tier: Literal[0, 1]
    judged: Literal["n/a", "approved", "rejected"] = "n/a"
    result: str = ""  # truncated
    at: datetime


class ProposedAction(BaseModel):
    """Tier 2: run by the workflow, with ITS identity, only after a person approves."""

    action_id: str
    kind: str  # from the typed tool catalog
    target: str
    args: dict[str, str] = Field(default_factory=dict)
    rationale: str = ""


class AgentVerdict(BaseModel):
    """What the agent concluded. "continue" and "skip" are deliberately not verdicts."""

    diagnosis: str
    confidence: Literal["verified", "observed", "hypothesis"]
    actions_taken: list[ToolCallRecord] = Field(default_factory=list)
    actions_proposed: list[ProposedAction] = Field(default_factory=list)
    verdict: Literal["retry_gate", "needs_human", "abort_recommended"]


# --- the control-plane step child ----------------------------------------------


class UpgradeControlPlaneInput(BaseModel):
    """One control-plane step, run on a per-MCE slot (`upgrade-control-plane-<mce>_<n>`)."""

    parent_workflow_id: str
    mce_cluster: str
    cluster: str
    step_index: int
    step: UpgradeStep
    from_version: str
    pinned: PinnedRelease
    release: ReleaseLayout
    nodepool_versions: dict[str, str]
    day1_path: str
    day1_blob: str
    profile: ClusterProfile
    tier1_enabled: bool = False


class UpgradeControlPlaneRunArgs(BaseModel):
    input: UpgradeControlPlaneInput


class ControlPlaneStepOutcome(str, Enum):
    DONE = "done"
    MIRROR_CHANGED = "mirror-changed"  # a pinned digest moved: nothing committed
    DAY1_CHANGED = "day1-changed"  # someone else edited the file: nothing committed
    DAY1_REJECTED = "day1-rejected"  # the day1 validator refused the edit
    SYNC_REFUSED = "sync-refused"  # someone else's change is pending in Argo CD
    NEEDS_HUMAN = "needs-human"  # the watch failed and the agent could not clear it
    STOPPED = "stopped"  # the parent stopped the step (an abort)


class UpgradeControlPlaneResult(BaseModel):
    outcome: ControlPlaneStepOutcome
    step_index: int | None = None  # which step of the parent's plan; a stale report is ignored
    detail: str = ""
    committed: bool = False
    revision: str | None = None
    day1_blob: str | None = None
    verdict: AgentVerdict | None = None
    incident_path: str | None = None
    failing_checks: list[CheckResult] = Field(default_factory=list)


class UpgradeControlPlaneProgress(BaseModel):
    phase: str
    parent_workflow_id: str
    cluster: str
    version: str


# The run's models above name the agent's (a proposed action, a verdict), which
# read better next to the agent limb they belong to. Resolve those references
# now, at import, rather than on first use inside a workflow task.
for _model in (PendingAction, UpgradeClusterProgress, IncidentRequest, ProposedActionRequest):
    _model.model_rebuild()
