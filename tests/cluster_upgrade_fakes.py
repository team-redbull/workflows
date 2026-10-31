"""A fake fleet behind every cluster-upgrade activity, and the harness that runs the real workflows on it.

Shared by the workflow tests and the replay tests. The fake is CONSISTENT, not a
script of canned answers: a commit edits a day1 file held here, a sync applies
that file to the HostedCluster held here, and a watch reports done only when the
cluster really runs the version — so a test proves the plan executes, not just
that activities were called.

Each mock is registered ONLY on its limb's queue, as in production: the hub's on
UPGRADE_GITOPS_ACTIVITY_QUEUE, the cluster limb's on the MCE's queue, the agent
on the MCE's agent queue. A dispatch to the wrong limb therefore hangs the run
and fails the test. `test_every_interface_is_mocked_on_its_own_queue` keeps
the mock names equal to each interface module's activities.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from shared.consts import (
    UPGRADE_CLUSTER_WORKFLOW_QUEUE,
    UPGRADE_CONTROL_PLANE_WORKFLOW_QUEUE,
    UPGRADE_GITOPS_ACTIVITY_QUEUE,
    cluster_upgrade_activity_queue,
    upgrade_agent_activity_queue,
)
from shared.exceptions import AdminGateNotFoundError, ArgoSyncRefusedError, Day1FileChangedError
from shared.models.cluster_upgrade import (
    ActionResult,
    AdminAckGiven,
    AdminAckRequest,
    AdminAckRule,
    AdminAckState,
    AgentRequest,
    AgentVerdict,
    ApiUsage,
    ArgoApplicationState,
    ArgoSyncRequest,
    ArgoSyncResult,
    ClusterUpgradeSettings,
    Day1ChangeKind,
    Day1ChangeRequest,
    Day1Cluster,
    Day1ClusterRequest,
    Day1Commit,
    Day1NodePool,
    Day2Layers,
    Day2LayersRequest,
    EvidenceBundle,
    EvidenceRequest,
    GateOutcome,
    GateResult,
    GiveAdminAckRequest,
    HostedClusterRequest,
    HostedClusterState,
    HostedHealth,
    HostedHealthRequest,
    IncidentRecord,
    IncidentRequest,
    LadderRung,
    MceRequest,
    MceState,
    NodePoolGateRequest,
    NodePoolState,
    PinnedRelease,
    PinnedReleases,
    PinReleasesRequest,
    ProposedActionRequest,
    ReleaseLayout,
    ReleaseTags,
    ReleaseTagsRequest,
    UpgradeClusterInput,
    UpgradeClusterProgress,
    UpgradeClusterRunArgs,
    UpgradePhase,
    UpgradeRules,
    UpgradeRulesRequest,
    WatchControlPlaneRequest,
    WatchNodePoolRequest,
    WatchOutcome,
    WatchResult,
)
from shared.ocp_version import minor_of
from shared.workflow_ids import upgrade_cluster_workflow_id
from workflow_domains.cluster_upgrade.gates import admin_gate_minor, release_repositories
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow
from workflow_domains.cluster_upgrade.upgrade_control_plane import UpgradeControlPlaneWorkflow

MCE = "ocp4-prod-mce-site1-a"
CLUSTER = "ocp4-prod-herzi-site1"
WORKFLOW_ID = upgrade_cluster_workflow_id(MCE, CLUSTER)
HUB_QUEUE = UPGRADE_GITOPS_ACTIVITY_QUEUE
MCE_QUEUE = cluster_upgrade_activity_queue(MCE)
AGENT_QUEUE = upgrade_agent_activity_queue(MCE)

IMAGE_PATH = "registry-internal/openshift-release"
RELEASE = ReleaseLayout(
    image_path=IMAGE_PATH,
    repo="{imagePath}/openshift/release-images",
    tag="{version}-x86_64",
    cpo_repo="{imagePath}/openshift/release",
    cpo_tag="{version}-x86_64-hypershift",
)
MIRRORS = release_repositories(RELEASE)
LADDER = [
    LadderRung(version=v, must=True)
    for v in ("4.15.41", "4.16.55", "4.17.58", "4.18.56", "4.19.53", "4.20.44", "4.21.40")
]
SUPPORTED = ["4.22", "4.21", "4.20", "4.19", "4.18"]
DAY1_PATH = f"sites/site1/mces/{MCE}/hostedClusters/{CLUSTER}.yaml"

# The admin gate a 4.19 cluster holds (CONTEXT §8.11), as the rules file lists it.
ADMIN_GATE = "ack-4.19-admissionregistration-v1beta1-api-removals-in-4.20"
ADMIN_GATE_MESSAGE = (
    "The admissionregistration.k8s.io/v1beta1 group version is deprecated in 4.19 and will be removed in 4.20."
)
GATED_APIS = [
    f"{resource}.v1beta1.admissionregistration.k8s.io"
    for resource in ("mutatingwebhookconfigurations", "validatingwebhookconfigurations",
                     "validatingadmissionpolicies", "validatingadmissionpolicybindings")
]
ADMIN_ACKS = [AdminAckRule(gate=ADMIN_GATE, apis=GATED_APIS, source="Red Hat KCS 7130599")]
IGNORED_USERS = [
    "system:serviceaccount:kube-system:generic-garbage-collector",
    "system:serviceaccount:kube-system:namespace-controller",
    "system:kube-controller-manager",
    "system:cluster-policy-controller",
]


def digest(version: str, salt: str = "") -> PinnedRelease:
    return PinnedRelease(
        release=f"sha256:release-{version}{salt}", control_plane_operator=f"sha256:cpo-{version}{salt}"
    )


def verdict(kind: str = "retry_gate", **kwargs: Any) -> AgentVerdict:
    return AgentVerdict(diagnosis=kwargs.pop("diagnosis", "a pruner pod was stuck"),
                        confidence="observed", verdict=kind, **kwargs)


class Fleet:
    """One MCE, one hosted cluster, its day1 file and its Argo CD Application — all fake, all consistent.

    The `*_script` lists are consumed one item per call; once empty, every
    call gets the default (done, passed, the version's digest, no error).
    """

    def __init__(
        self,
        *,
        control_plane: str = "4.18.56",
        nodepools: dict[str, str] | None = None,
        zones: dict[str, str] | None = None,
        upgrade: ClusterUpgradeSettings | None = None,
        mirror: list[str] | None = None,
        max_concurrent_control_planes: int = 2,
    ) -> None:
        self.nodepools = dict(nodepools or {"np-a": control_plane, "np-b": control_plane})
        self.zones = zones or {}
        self.upgrade = upgrade or ClusterUpgradeSettings()
        self.mirror = mirror if mirror is not None else [r.version for r in LADDER]
        self.max_concurrent_control_planes = max_concurrent_control_planes
        # day1, as written: production files pin every NodePool's workertag.
        self.mastertag = control_plane
        self.workertags = dict(self.nodepools)
        self.paused_until = False
        self.blob = 1
        self.revision = 1
        # the cluster, as Argo CD last applied it
        self.cluster_cp = control_plane
        self.cluster_np = dict(self.nodepools)
        # scripts
        self.pin_script: list[dict[str, PinnedRelease]] = []
        self.commit_errors: list[Exception | None] = []
        self.sync_errors: list[Exception | None] = []
        self.cp_watch_script: list[WatchResult] = []
        self.np_watch_script: list[WatchResult] = []
        self.gate_script: list[GateOutcome] = []
        self.verdicts: list[AgentVerdict] = []
        self.health_script: list[HostedHealth] = []
        # The hosted cluster's admin gates (admin-gates) and acks (admin-acks),
        # who called the gated APIs in the last 24 hours, and how many reads
        # after an ack the CVO still says AdminAckRequired.
        self.admin_gates: dict[str, str] = {ADMIN_GATE: ADMIN_GATE_MESSAGE}
        self.admin_acks: dict[str, str] = {}
        self.api_usage: list[ApiUsage] = []
        self.cvo_ack_lag = 0
        self._cvo_lagging = 0
        # The next ack finds its gate gone from admin-gates (changed since the read).
        self.ack_gate_vanishes = False
        # How many more HostedCluster reads report the control plane Progressing.
        self.settling = 0
        self.agent_down = False
        self.block_nodepool_watch = False
        self.block_control_plane_watch = False
        # A step's own re-pin (one version), not planning's: holds it in flight.
        self.block_step_pin = False
        self.block_evidence = False
        # what happened
        self.calls: list[str] = []
        self.commits: list[Day1ChangeRequest] = []
        self.commit_runs: list[str] = []
        self.syncs: list[ArgoSyncRequest] = []
        self.agent_requests: list[AgentRequest] = []
        self.gate_requests: list[NodePoolGateRequest] = []
        self.actions: list[ProposedActionRequest] = []
        self.incidents: list[IncidentRequest] = []
        self.acks_given: list[str] = []
        self.np_watch_requests: list[WatchNodePoolRequest] = []

    def admin_ack_pending(self) -> str | None:
        """What the hosted CVO says while a gate of its current minor has no ack, else None."""
        minor = minor_of(self.cluster_cp)
        missing = [
            message for gate, message in self.admin_gates.items()
            if admin_gate_minor(gate) in (minor, None) and self.admin_acks.get(gate) != "true"
        ]
        return " ".join(sorted(missing)) or None

    # --- day1 and Argo CD ----------------------------------------------------

    def _blob(self) -> str:
        return f"blob-{self.blob}"

    def _apply(self) -> None:
        """Argo CD syncing the Application: the cluster becomes what the file says."""
        self.cluster_cp = self.mastertag
        for name in self.cluster_np:
            self.cluster_np[name] = self.workertags.get(name, self.mastertag)

    def edit_by_hand(self) -> None:
        """Someone else commits to the cluster file between two of the run's commits."""
        self.blob += 1
        self.revision += 1

    def mocks(self) -> dict[str, list[Any]]:
        fleet = self

        @activity.defn(name="read_upgrade_rules")
        async def read_upgrade_rules(request: UpgradeRulesRequest) -> UpgradeRules:
            fleet.calls.append("read_upgrade_rules")
            return UpgradeRules(
                revision=f"rev-{fleet.revision}", ladder=LADDER, max_worker_skew=2,
                verified_against_graph="2026-10-31", release=RELEASE, mce_version="2.17.3",
                supported_hosted_versions=SUPPORTED,
                max_concurrent_control_planes=fleet.max_concurrent_control_planes,
                admin_acks=ADMIN_ACKS, api_usage_ignored_users=IGNORED_USERS,
            )

        @activity.defn(name="read_day1_cluster")
        async def read_day1_cluster(request: Day1ClusterRequest) -> Day1Cluster:
            fleet.calls.append("read_day1_cluster")
            return Day1Cluster(
                path=DAY1_PATH, revision=f"rev-{fleet.revision}", blob=fleet._blob(),
                mastertag=fleet.mastertag, image_path=IMAGE_PATH,
                nodepools=[
                    Day1NodePool(name=name, workertag=fleet.workertags.get(name), upgrade_type="InPlace",
                                 zone=fleet.zones.get(name), replicas=2, max_unavailable="1")
                    for name in fleet.nodepools
                ],
                upgrade=fleet.upgrade,
            )

        @activity.defn(name="list_release_tags")
        async def list_release_tags(request: ReleaseTagsRequest) -> ReleaseTags:
            fleet.calls.append("list_release_tags")
            return ReleaseTags(tags=[f"{v}-x86_64" for v in fleet.mirror] + ["latest"])

        @activity.defn(name="pin_release_digests")
        async def pin_release_digests(request: PinReleasesRequest) -> PinnedReleases:
            fleet.calls.append("pin_release_digests")
            while fleet.block_step_pin and len(request.versions) == 1:
                await asyncio.sleep(0.02)
            override = fleet.pin_script.pop(0) if fleet.pin_script else {}
            pinned, missing = {}, []
            for version in request.versions:
                if version in override:
                    pinned[version] = override[version]
                elif version in fleet.mirror:
                    pinned[version] = digest(version)
                else:
                    missing.append(version)
            return PinnedReleases(pinned=pinned, missing=missing)

        @activity.defn(name="check_day2_layers")
        async def check_day2_layers(request: Day2LayersRequest) -> Day2Layers:
            fleet.calls.append("check_day2_layers")
            return Day2Layers()

        @activity.defn(name="commit_day1_change")
        async def commit_day1_change(request: Day1ChangeRequest) -> Day1Commit:
            fleet.calls.append(f"commit:{request.change.value}:{request.nodepool or ''}:{request.version or ''}")
            error = fleet.commit_errors.pop(0) if fleet.commit_errors else None
            if error is not None:
                raise error
            if request.expected_blob is not None and request.expected_blob != fleet._blob():
                raise Day1FileChangedError(f"blob is {fleet._blob()}, expected {request.expected_blob}")
            fleet.commits.append(request)
            fleet.commit_runs.append(activity.info().workflow_run_id)
            before = (fleet.mastertag, dict(fleet.workertags), fleet.paused_until)
            if request.change is Day1ChangeKind.CONTROL_PLANE:
                fleet.mastertag = request.version or fleet.mastertag
                fleet.workertags.update(request.nodepool_versions)
            elif request.change is Day1ChangeKind.NODEPOOL:
                fleet.workertags[request.nodepool or ""] = request.version or ""
            elif request.change is Day1ChangeKind.PAUSE:
                fleet.paused_until = True
            changed = before != (fleet.mastertag, dict(fleet.workertags), fleet.paused_until)
            if changed:
                fleet.blob += 1
                fleet.revision += 1
            return Day1Commit(revision=f"rev-{fleet.revision}", blob=fleet._blob(), changed=changed)

        @activity.defn(name="write_incident")
        async def write_incident(request: IncidentRequest) -> IncidentRecord:
            fleet.calls.append("write_incident")
            fleet.incidents.append(request)
            return IncidentRecord(path=f"incidents/2026-11-01-hypershift-stall-{len(fleet.incidents)}.md")

        @activity.defn(name="read_mce_state")
        async def read_mce_state(request: MceRequest) -> MceState:
            fleet.calls.append("read_mce_state")
            return MceState(supported_versions_found=True, supported_versions=SUPPORTED, idms_mirrors=MIRRORS)

        @activity.defn(name="read_hosted_cluster")
        async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
            fleet.calls.append("read_hosted_cluster")
            settling = fleet.settling > 0
            fleet.settling = max(0, fleet.settling - 1)
            return HostedClusterState(
                found=True, namespace="clusters", control_plane_version=fleet.cluster_cp,
                progressing=settling,
                conditions={
                    "ClusterVersionProgressing": str(settling), "ClusterVersionSucceeding": "True",
                },
                image_content_mirrors=MIRRORS,
                nodepools=[
                    NodePoolState(name=name, version=version, replicas=2, ready_replicas=2,
                                  upgrade_type="InPlace", max_unavailable="1", zone=fleet.zones.get(name))
                    for name, version in fleet.cluster_np.items()
                ],
            )

        @activity.defn(name="read_argo_application")
        async def read_argo_application(request: HostedClusterRequest) -> ArgoApplicationState:
            fleet.calls.append("read_argo_application")
            return ArgoApplicationState(found=True, sync_status="Synced", health_status="Healthy",
                                        revision=f"rev-{fleet.revision}")

        @activity.defn(name="read_hosted_health")
        async def read_hosted_health(request: HostedHealthRequest) -> HostedHealth:
            fleet.calls.append("read_hosted_health")
            if fleet.health_script:
                return fleet.health_script.pop(0)
            missing_ack = fleet.admin_ack_pending()
            if missing_ack:
                return HostedHealth(
                    reachable=True, cluster_version_upgradeable=False, upgradeable_reason="AdminAckRequired",
                    upgradeable_message=missing_ack, idms_mirrors=MIRRORS,
                )
            return HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS)

        @activity.defn(name="read_admin_acks")
        async def read_admin_acks(request: AdminAckRequest) -> AdminAckState:
            fleet.calls.append("read_admin_acks")
            required = fleet.admin_ack_pending()
            if required is None and fleet._cvo_lagging > 0:
                fleet._cvo_lagging -= 1
                required = ADMIN_GATE_MESSAGE
            return AdminAckState(
                reachable=True, gates=dict(fleet.admin_gates), acks=dict(fleet.admin_acks),
                usage=[use for use in fleet.api_usage if use.api in request.apis],
                admin_ack_required=required,
            )

        @activity.defn(name="give_admin_ack")
        async def give_admin_ack(request: GiveAdminAckRequest) -> AdminAckGiven:
            fleet.calls.append(f"ack:{request.gate}")
            if fleet.ack_gate_vanishes:
                fleet.ack_gate_vanishes = False
                fleet.admin_gates.pop(request.gate, None)
                raise AdminGateNotFoundError(
                    f"hosted cluster {request.cluster} holds no admin gate {request.gate} in "
                    "openshift-config-managed/admin-gates; nothing was written"
                )
            assert request.gate in fleet.admin_gates, "acked a gate the cluster does not hold"
            changed = fleet.admin_acks.get(request.gate) != "true"
            fleet.admin_acks[request.gate] = "true"
            fleet.acks_given.append(request.gate)
            fleet._cvo_lagging = fleet.cvo_ack_lag
            return AdminAckGiven(gate=request.gate, changed=changed)

        @activity.defn(name="sync_argo_application")
        async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
            fleet.calls.append(f"sync:{request.revision}")
            error = fleet.sync_errors.pop(0) if fleet.sync_errors else None
            if error is not None:
                raise error
            fleet.syncs.append(request)
            fleet._apply()
            return ArgoSyncResult(revision=request.revision, phase="Succeeded")

        @activity.defn(name="watch_control_plane")
        async def watch_control_plane(request: WatchControlPlaneRequest) -> WatchOutcome:
            fleet.calls.append(f"watch-cp:{request.version}")
            while fleet.block_control_plane_watch:
                activity.heartbeat()
                await asyncio.sleep(0.05)
            result = fleet.cp_watch_script.pop(0) if fleet.cp_watch_script else WatchResult.DONE
            if result is WatchResult.DONE:
                assert fleet.cluster_cp == request.version, "watched a version Argo CD never applied"
            return WatchOutcome(result=result)

        @activity.defn(name="watch_nodepool")
        async def watch_nodepool(request: WatchNodePoolRequest) -> WatchOutcome:
            fleet.calls.append(f"watch-np:{request.nodepool}:{request.version}")
            fleet.np_watch_requests.append(request)
            while fleet.block_nodepool_watch:
                activity.heartbeat()
                await asyncio.sleep(0.05)
            result = fleet.np_watch_script.pop(0) if fleet.np_watch_script else WatchResult.DONE
            if result is WatchResult.DONE:
                assert fleet.cluster_np[request.nodepool] == request.version, "watched a roll never applied"
            return WatchOutcome(result=result)

        @activity.defn(name="watch_nodepool_gate")
        async def watch_nodepool_gate(request: NodePoolGateRequest) -> GateOutcome:
            fleet.calls.append(f"gate:{request.nodepool}")
            fleet.gate_requests.append(request)
            return fleet.gate_script.pop(0) if fleet.gate_script else GateOutcome(result=GateResult.PASSED)

        @activity.defn(name="collect_evidence")
        async def collect_evidence(request: EvidenceRequest) -> EvidenceBundle:
            fleet.calls.append("collect_evidence")
            while fleet.block_evidence:
                await asyncio.sleep(0.02)
            return EvidenceBundle(sections={"hostedcluster": "Progressing=True for 25m"})

        @activity.defn(name="execute_proposed_action")
        async def execute_proposed_action(request: ProposedActionRequest) -> ActionResult:
            fleet.calls.append(f"action:{request.action.action_id}")
            fleet.actions.append(request)
            return ActionResult(action_id=request.action.action_id, executed=True, detail="pod deleted")

        @activity.defn(name="invoke_upgrade_agent")
        async def invoke_upgrade_agent(request: AgentRequest) -> AgentVerdict:
            fleet.calls.append(f"agent:{request.gate}")
            fleet.agent_requests.append(request)
            if fleet.agent_down:
                raise RuntimeError("upgrade-agent: connection refused")
            return fleet.verdicts.pop(0) if fleet.verdicts else verdict("needs_human")

        return {
            HUB_QUEUE: [read_upgrade_rules, read_day1_cluster, list_release_tags, pin_release_digests,
                        check_day2_layers, commit_day1_change, write_incident],
            MCE_QUEUE: [read_mce_state, read_hosted_cluster, read_argo_application, read_hosted_health,
                        read_admin_acks, give_admin_ack, sync_argo_application, watch_control_plane,
                        watch_nodepool, watch_nodepool_gate, collect_evidence, execute_proposed_action],
            AGENT_QUEUE: [invoke_upgrade_agent],
        }

    # --- reading what happened -------------------------------------------------

    def commit_summary(self) -> list[str]:
        return [
            f"{c.change.value}:{c.nodepool or ''}:{c.version or ''}".rstrip(":")
            for c in self.commits
        ]

    def sync_refused(self, message: str = "ConfigMap/extra is out of sync") -> ArgoSyncRefusedError:
        return ArgoSyncRefusedError(message)


BLOCKER_QUEUE = "test-cp-blocker"


@workflow.defn(name="UpgradeControlPlaneBlocker")
class SlotBlocker:
    """Holds a control-plane slot id open — another cluster's upgrade on the same MCE."""

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


class Harness:
    """A time-skipping Temporal with the real workflows and the fleet's mocks on their own queues."""

    def __init__(self, fleet: Fleet) -> None:
        self.fleet = fleet
        self._first_runs: dict[str, str] = {}

    async def __aenter__(self) -> Harness:
        self.env = await WorkflowEnvironment.start_time_skipping()
        config = self.env.client.config()
        config["data_converter"] = pydantic_data_converter
        self.client = Client(**config)
        self._workers = [
            Worker(self.client, task_queue=UPGRADE_CLUSTER_WORKFLOW_QUEUE, workflows=[UpgradeClusterWorkflow]),
            Worker(self.client, task_queue=UPGRADE_CONTROL_PLANE_WORKFLOW_QUEUE,
                   workflows=[UpgradeControlPlaneWorkflow]),
            Worker(self.client, task_queue=BLOCKER_QUEUE, workflows=[SlotBlocker],
                   workflow_runner=UnsandboxedWorkflowRunner()),
            *(
                Worker(self.client, task_queue=queue, activities=activities)
                for queue, activities in self.fleet.mocks().items()
            ),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self.fleet.block_nodepool_watch = False
        self.fleet.block_control_plane_watch = False
        self.fleet.block_step_pin = False
        self.fleet.block_evidence = False
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self.env.__aexit__(*exc_info)

    # --- driving a run -----------------------------------------------------------

    async def start(self, request: UpgradeClusterInput, workflow_id: str = WORKFLOW_ID) -> WorkflowHandle:
        handle = await self.client.start_workflow(
            UpgradeClusterWorkflow.run,
            UpgradeClusterRunArgs(input=request),
            id=workflow_id,
            task_queue=UPGRADE_CLUSTER_WORKFLOW_QUEUE,
        )
        self._first_runs[workflow_id] = handle.result_run_id or ""
        return handle

    def run(self, workflow_id: str = WORKFLOW_ID) -> WorkflowHandle:
        """The LATEST run of the id: queries and Updates must follow continue-as-new."""
        return self.client.get_workflow_handle_for(UpgradeClusterWorkflow.run, workflow_id)

    async def progress(self, workflow_id: str = WORKFLOW_ID) -> UpgradeClusterProgress:
        return await self.run(workflow_id).query(UpgradeClusterWorkflow.progress)

    async def until(self, phase: UpgradePhase, workflow_id: str = WORKFLOW_ID, timeout: float = 30) -> UpgradeClusterProgress:
        """Poll the progress query until the run reaches `phase`."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last: UpgradeClusterProgress | None = None
        while loop.time() < deadline:
            try:
                last = await self.progress(workflow_id)
            except Exception:  # noqa: BLE001 — between two runs of a continue-as-new
                last = None
            if last is not None and last.phase is phase:
                return last
            await asyncio.sleep(0.02)
        raise AssertionError(f"never reached {phase.value}; last progress: {last}")

    async def called(self, name: str, times: int = 1, timeout: float = 30) -> None:
        """Wait until the fleet has seen `name` called `times` times."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self.fleet.calls.count(name) < times:
            if loop.time() > deadline:
                raise AssertionError(f"{name} called {self.fleet.calls.count(name)} times, not {times}")
            await asyncio.sleep(0.02)

    async def decide(self, update: Any, arg: Any, workflow_id: str = WORKFLOW_ID) -> None:
        await self.run(workflow_id).execute_update(update, arg)

    async def result(self, workflow_id: str = WORKFLOW_ID, timeout: float = 60) -> Any:
        return await asyncio.wait_for(self.run(workflow_id).result(), timeout=timeout)

    async def histories(self, workflow_id: str = WORKFLOW_ID) -> list[WorkflowHistory]:
        """Every run of the id, first to last, following continue-as-new."""
        run_id: str | None = self._first_runs[workflow_id]
        histories = []
        while run_id:
            history = await self.client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
            histories.append(history)
            last = history.events[-1]
            run_id = (
                last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
                if last.HasField("workflow_execution_continued_as_new_event_attributes")
                else None
            )
        return histories

    async def block_slots(self, count: int) -> list[WorkflowHandle]:
        from shared.workflow_ids import upgrade_control_plane_workflow_id

        return [
            await self.client.start_workflow(
                SlotBlocker.run, id=upgrade_control_plane_workflow_id(MCE, slot), task_queue=BLOCKER_QUEUE
            )
            for slot in range(1, count + 1)
        ]


def request(**kwargs: Any) -> UpgradeClusterInput:
    defaults: dict[str, Any] = {
        "mce_cluster": MCE, "cluster": CLUSTER, "target_version": "4.20.44", "requested_by": "ops@example",
    }
    defaults.update(kwargs)
    return UpgradeClusterInput(**defaults)


def unique_id() -> str:
    return f"{WORKFLOW_ID}-{uuid.uuid4().hex[:8]}"


SEVEN_DAYS = timedelta(days=7)
