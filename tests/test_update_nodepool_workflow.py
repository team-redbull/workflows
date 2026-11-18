"""update-nodepool and its two children: the real workflows, mock activities, time-skipping env.

One limb, as in production: the MCE's server-lifecycle worker answers every
call — the day1 read and commit, the NodePool read and Argo CD sync it
re-registers from cluster-upgrade, and the scale-down's Machines. A small fake
fleet holds day1, the MCE's NodePools and their Machines, so a commit followed
by a sync is what moves the replicas the run reads back.

The API takes ONE NodePool by name since 2026-11-18; `run_args` still
defaults to an `infra_env_kind`, because runs recorded with one replay on
this code and the by-kind tests below are what keeps that path honest.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from pydantic import BaseModel, Field
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    SCALE_DOWN_NODEPOOL_WORKFLOW_QUEUE,
    SCALE_UP_NODEPOOL_WORKFLOW_QUEUE,
    UPDATE_NODEPOOL_WORKFLOW_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import ArgoSyncRefusedError, Day1FileChangedError, Day1NotConfiguredError
from shared.models.cluster_upgrade import (
    ArgoSyncRequest,
    ArgoSyncResult,
    Day1ChangeKind,
    Day1ChangeRequest,
    Day1Cluster,
    Day1ClusterRequest,
    Day1Commit,
    Day1NodePool,
    HostedClusterRequest,
    HostedClusterState,
    NodePoolState,
)
from shared.models.server_lifecycle import (
    MachineMarkRequest,
    MachineMarks,
    MarkedMachinesOutcome,
    MarkedMachinesWatch,
    MarkedMachinesWatchRequest,
    ScaleDownCandidates,
    ScaleDownCandidatesRequest,
    ScaleDownMachine,
    ScaleDownPolicy,
    UpdateNodePoolInput,
    UpdateNodePoolRunArgs,
)
from shared.workflow_ids import scale_up_nodepool_workflow_id
from workflow_domains.server_lifecycle.scale_down_nodepool import ScaleDownNodePoolWorkflow
from workflow_domains.server_lifecycle.scale_up_nodepool import ScaleUpNodePoolWorkflow
from workflow_domains.server_lifecycle.update_nodepool import UpdateNodePoolWorkflow

MCE = "ocp4-mce-a"
CLUSTER = "ocp4-prod-alpha"
PATH = f"sites/site1/mces/{MCE}/hostedClusters/{CLUSTER}.yaml"
HC, HC_NS = "openshift-prod-alpha", "hcp-prod-alpha"
HCP = f"{HC_NS}-{HC}"
KIND = "dell-r660-128c"
ZONES = ("alpha-zone-a", "alpha-zone-b", "alpha-zone-c")
DOWN_ID = f"scale-down-nodepool-{CLUSTER}"


class _Fleet:
    """day1, the MCE's NodePools and their Machines, as the mocks see and change them."""

    def __init__(
        self,
        counts=(3, 3, 2),
        *,
        live=None,
        policies=None,
        machines=None,
        path=PATH,
        problems=(),
    ) -> None:
        self.day1 = dict(zip(ZONES, counts)) | {"alpha-gpu": 1}
        self.live = dict(zip(ZONES, live if live is not None else counts)) | {"alpha-gpu": 1}
        self.infra_envs = {name: f"{KIND}-zone-{name[-1]}" for name in ZONES} | {"alpha-gpu": "gpu-a100"}
        self.policies = dict(policies or {})
        self.machines = {m.name: m for m in (machines or [])}
        self.path = path
        self.problems = list(problems)
        self.revision = "rev-1"
        self.calls: list[tuple[str, object]] = []
        self.read_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.sync_error: Exception | None = None
        self.argo_applies = True
        self.watch_result = MarkedMachinesOutcome.DONE
        self.only_old_mce_limb = False
        # A worker from before `server_names`: its candidates carry no `server` key.
        self.candidates_without_servers = False

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]

    def first(self, name: str):
        return next(arg for call, arg in self.calls if call == name)

    def activities(self):
        fleet = self

        @activity.defn(name="read_day1_nodepools")
        async def read_day1_nodepools(request: Day1ClusterRequest) -> Day1Cluster:
            fleet.calls.append(("read_day1_nodepools", request))
            if fleet.read_error is not None:
                raise fleet.read_error
            return Day1Cluster(
                path=fleet.path, revision=fleet.revision, blob="blob-1", mastertag="4.18.56",
                hosted_cluster=HC, hosted_cluster_namespace=HC_NS, problems=fleet.problems,
                nodepools=[Day1NodePool(name=n, replicas=c, infra_env=fleet.infra_envs[n]) for n, c in fleet.day1.items()],
            )

        @activity.defn(name="commit_day1_replicas")
        async def commit_day1_replicas(request: Day1ChangeRequest) -> Day1Commit:
            fleet.calls.append(("commit_day1_replicas", request))
            if fleet.commit_error is not None:
                raise fleet.commit_error
            changed = any(fleet.day1[n] != c for n, c in request.nodepool_replicas.items())
            fleet.day1.update(request.nodepool_replicas)
            if changed:
                fleet.revision = "rev-2"
            return Day1Commit(revision=fleet.revision, blob="blob-2", changed=changed)

        @activity.defn(name="read_hosted_cluster")
        async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
            fleet.calls.append(("read_hosted_cluster", request))
            return HostedClusterState(found=True, namespace=HC_NS, nodepools=[
                NodePoolState(name=n, replicas=c, ready_replicas=c, infra_env=fleet.infra_envs[n],
                              scale_down_policy=fleet.policies.get(n))
                for n, c in fleet.live.items()
            ])

        @activity.defn(name="read_hosted_cluster")
        async def read_hosted_cluster_old(request: HostedClusterRequest) -> _OldHostedClusterState:
            fleet.calls.append(("read_hosted_cluster", request))
            return _OldHostedClusterState(found=True, nodepools=[
                _OldNodePoolState(name=n, replicas=c) for n, c in fleet.live.items()
            ])

        @activity.defn(name="sync_argo_application")
        async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
            fleet.calls.append(("sync_argo_application", request))
            if fleet.sync_error is not None:
                raise fleet.sync_error
            if fleet.argo_applies:
                fleet.live.update(fleet.day1)
            return ArgoSyncResult(revision=request.revision, phase="Succeeded")

        @activity.defn(name="read_scale_down_candidates")
        async def read_scale_down_candidates(request: ScaleDownCandidatesRequest) -> ScaleDownCandidates:
            fleet.calls.append(("read_scale_down_candidates", request))
            wanted = {np.name for np in request.nodepools}
            machines = [m for m in fleet.machines.values() if m.nodepool in wanted]
            if fleet.candidates_without_servers:
                return _OldScaleDownCandidates(hcp_namespace=HCP, machines=[
                    _OldScaleDownMachine(**m.model_dump(exclude={"server"})) for m in machines
                ])
            return ScaleDownCandidates(hcp_namespace=HCP, machines=machines)

        @activity.defn(name="mark_machines_for_deletion")
        async def mark_machines_for_deletion(request: MachineMarkRequest) -> MachineMarks:
            fleet.calls.append(("mark_machines_for_deletion", request))
            for name in request.machines:
                fleet.machines[name].delete_mark = request.mark
            return MachineMarks(changed=list(request.machines))

        @activity.defn(name="unmark_machines")
        async def unmark_machines(request: MachineMarkRequest) -> MachineMarks:
            fleet.calls.append(("unmark_machines", request))
            for name in request.machines:
                if fleet.machines[name].delete_mark == request.mark:
                    fleet.machines[name].delete_mark = None
            return MachineMarks(changed=list(request.machines))

        @activity.defn(name="watch_marked_machines")
        async def watch_marked_machines(request: MarkedMachinesWatchRequest) -> MarkedMachinesWatch:
            fleet.calls.append(("watch_marked_machines", request))
            done = fleet.watch_result is MarkedMachinesOutcome.DONE
            return MarkedMachinesWatch(
                result=fleet.watch_result,
                deleting=list(request.marked) if done else [],
                pending=[] if done else list(request.marked),
                unexpected=["alpha-zone-c-other"] if fleet.watch_result is MarkedMachinesOutcome.WRONG_MACHINE else [],
            )

        return [
            read_day1_nodepools, commit_day1_replicas,
            read_hosted_cluster_old if self.only_old_mce_limb else read_hosted_cluster, sync_argo_application,
            read_scale_down_candidates, mark_machines_for_deletion, unmark_machines, watch_marked_machines,
        ]


class _OldNodePoolState(BaseModel):
    """A NodePoolState as a worker predating update-nodepool sends it."""

    name: str
    replicas: int | None = None


class _OldHostedClusterState(BaseModel):
    found: bool
    nodepools: list[_OldNodePoolState] = Field(default_factory=list)


class _OldScaleDownMachine(BaseModel):
    """A ScaleDownMachine as a worker predating `server_names` sends it: no `server`."""

    nodepool: str
    namespace: str
    name: str
    deleting: bool = False
    delete_mark: str | None = None
    capi_healthy: bool = True
    machine_set: str | None = None
    node: str | None = None
    node_found: bool = False
    ready: bool = False


class _OldScaleDownCandidates(BaseModel):
    hcp_namespace: str
    machines: list[_OldScaleDownMachine] = Field(default_factory=list)


@workflow.defn(name="Blocker")
class _Blocker:
    """Holds an id open, as a child already running there would."""

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


class _Harness:
    """The three workflows, and every mock activity on the MCE's server-lifecycle queue."""

    def __init__(self, fleet: _Fleet, mce: str = MCE) -> None:
        self._fleet = fleet
        self._mce = mce

    async def __aenter__(self) -> Client:
        activities = self._fleet.activities()
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=UPDATE_NODEPOOL_WORKFLOW_QUEUE, workflows=[UpdateNodePoolWorkflow]),
            Worker(client, task_queue=SCALE_UP_NODEPOOL_WORKFLOW_QUEUE, workflows=[ScaleUpNodePoolWorkflow]),
            Worker(client, task_queue=SCALE_DOWN_NODEPOOL_WORKFLOW_QUEUE, workflows=[ScaleDownNodePoolWorkflow]),
            Worker(client, task_queue="blocker", workflows=[_Blocker]),
            Worker(client, task_queue=server_lifecycle_activity_queue(self._mce), activities=activities),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.shutdown()


def run_args(
    replicas: int, *, kind: str | None = KIND, mce: str = MCE, nodepool: str | None = None,
    policy: str | None = None, servers: list[str] | None = None,
) -> UpdateNodePoolRunArgs:
    """By kind (the default: a recorded run's shape), or by `nodepool` — which drops the kind unless one is given too."""
    if nodepool is not None and kind == KIND:
        kind = None
    return UpdateNodePoolRunArgs(input=UpdateNodePoolInput(
        mce_cluster=mce, cluster=CLUSTER, replicas=replicas, infra_env_kind=kind, nodepool=nodepool,
        scale_down_policy=ScaleDownPolicy(policy) if policy else None, server_names=servers,
    ))


async def _run(
    fleet: _Fleet, replicas: int, *, kind: str | None = KIND, mce: str = MCE, nodepool: str | None = None,
    policy: str | None = None, servers: list[str] | None = None, before=None,
):
    async with _Harness(fleet, mce) as client:
        if before is not None:
            await before(client)
        return await asyncio.wait_for(
            client.execute_workflow(
                UpdateNodePoolWorkflow.run,
                run_args(replicas, kind=kind, mce=mce, nodepool=nodepool, policy=policy, servers=servers),
                id=f"update-nodepool-{uuid.uuid4()}",
                task_queue=UPDATE_NODEPOOL_WORKFLOW_QUEUE,
            ),
            timeout=60,
        )


async def _failure(fleet: _Fleet, replicas: int, **kwargs) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as excinfo:
        await _run(fleet, replicas, **kwargs)
    cause = excinfo.value.cause
    assert isinstance(cause, ApplicationError)
    return cause


# --- the parent's reads and plan ----------------------------------------------------------


class TestScalingUp:
    async def test_the_new_total_is_spread_committed_synced_and_read_back(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 10)

        assert fleet.names() == [
            "read_day1_nodepools", "read_hosted_cluster", "commit_day1_replicas", "sync_argo_application",
            "read_hosted_cluster",
        ]
        # The NodePools are read as day1 names the HostedCluster, not by the file's name.
        read = fleet.first("read_hosted_cluster")
        assert (read.hosted_cluster, read.hosted_cluster_namespace) == (HC, HC_NS)
        commit = fleet.first("commit_day1_replicas")
        assert commit.change is Day1ChangeKind.REPLICAS and commit.expected_blob == "blob-1"
        assert commit.nodepool_replicas == {"alpha-zone-a": 4, "alpha-zone-c": 3}
        assert commit.workflow_id == scale_up_nodepool_workflow_id(CLUSTER)
        sync = fleet.first("sync_argo_application")
        assert sync.revision == "rev-2"
        assert sync.allowed_out_of_sync == ["NodePool/alpha-zone-a", "NodePool/alpha-zone-c"]

        assert (result.direction, result.total_before, result.total_after) == ("up", 8, 10)
        assert result.child_workflow_id == f"scale-up-nodepool-{CLUSTER}"
        assert {np.name: (np.before, np.after) for np in result.nodepools} == {
            "alpha-zone-a": (3, 4), "alpha-zone-b": (3, 3), "alpha-zone-c": (2, 3),
        }
        assert result.scale.committed and {np.name: np.replicas for np in result.scale.nodepools} == {
            "alpha-zone-a": 4, "alpha-zone-b": 3, "alpha-zone-c": 3,
        }
        # Another kind's NodePool is never part of it.
        assert fleet.day1["alpha-gpu"] == 1

    async def test_the_total_already_asked_for_writes_nothing(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 8)
        assert fleet.names() == ["read_day1_nodepools", "read_hosted_cluster"]
        assert result.direction == "none" and result.child_workflow_id is None and result.scale is None


class TestScalingDownWithoutAPolicy:
    async def test_capi_picks_and_nothing_is_marked(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 6)
        assert "read_scale_down_candidates" not in fleet.names() and "mark_machines_for_deletion" not in fleet.names()
        assert fleet.first("commit_day1_replicas").nodepool_replicas == {"alpha-zone-a": 2, "alpha-zone-b": 2}
        assert result.direction == "down" and result.child_workflow_id == DOWN_ID
        assert result.scale.chosen == [] and fleet.day1 == {
            "alpha-zone-a": 2, "alpha-zone-b": 2, "alpha-zone-c": 2, "alpha-gpu": 1,
        }

    async def test_down_to_zero(self):
        fleet = _Fleet((1, 1, 1))
        result = await _run(fleet, 0)
        assert result.total_after == 0 and all(fleet.day1[n] == 0 for n in ZONES)


class TestTheRefusals:
    async def test_live_and_day1_disagreeing_on_replicas_writes_nothing(self):
        fleet = _Fleet((3, 3, 2), live=(4, 3, 2))
        error = await _failure(fleet, 10)
        assert error.type == "NodePoolReplicasDriftError"
        assert "alpha-zone-a 4 on the MCE, 3 in day1" in error.message
        assert "commit_day1_replicas" not in fleet.names()

    async def test_a_nodepool_of_the_kind_only_on_the_mce_is_drift(self):
        fleet = _Fleet((3, 3, 2))
        fleet.live["alpha-zone-d"] = 2
        fleet.infra_envs["alpha-zone-d"] = f"{KIND}-zone-d"
        error = await _failure(fleet, 10)
        assert error.type == "NodePoolReplicasDriftError" and "alpha-zone-d is on the MCE but not in" in error.message

    async def test_a_nodepool_of_the_kind_only_in_day1_is_drift(self):
        fleet = _Fleet((3, 3, 2))
        del fleet.live["alpha-zone-c"]
        error = await _failure(fleet, 10)
        assert error.type == "NodePoolReplicasDriftError" and "alpha-zone-c is in" in error.message

    async def test_an_unbalanced_start_is_refused(self):
        fleet = _Fleet((5, 2, 2))
        error = await _failure(fleet, 12)
        assert error.type == "NodePoolsUnbalancedError" and "alpha-zone-a 5" in error.message
        assert "commit_day1_replicas" not in fleet.names()

    async def test_a_kind_no_nodepool_takes_is_refused_naming_the_infraenvs_seen(self):
        fleet = _Fleet()
        error = await _failure(fleet, 3, kind="cisco-m6")
        assert error.type == "NodePoolKindNotFoundError" and "gpu-a100" in error.message

    async def test_a_cluster_day1_keeps_under_another_mce_is_never_read_there(self):
        fleet = _Fleet(path=f"sites/site1/mces/ocp4-mce-b/hostedClusters/{CLUSTER}.yaml")
        error = await _failure(fleet, 10)
        assert error.type == "ClusterNotOnMceError"
        assert fleet.names() == ["read_day1_nodepools"]

    async def test_a_file_with_problems_is_refused(self):
        fleet = _Fleet(problems=["nodepool[1]: NodePool alpha-zone-b is named twice"])
        error = await _failure(fleet, 10)
        assert error.type == "Day1ClusterProblemsError" and "named twice" in error.message

    async def test_a_server_lifecycle_worker_predating_update_nodepool_is_named(self):
        fleet = _Fleet()
        fleet.only_old_mce_limb = True
        error = await _failure(fleet, 10)
        assert error.type == "NodePoolKindNotFoundError" and f"server-lifecycle-worker on MCE {MCE}" in error.message

    async def test_a_worker_without_day1_settings_fails_at_the_first_call_and_writes_nothing(self):
        fleet = _Fleet()
        fleet.read_error = Day1NotConfiguredError("no DAY1_REPO_URL")
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _run(fleet, 10)
        cause = excinfo.value.cause.cause
        assert isinstance(cause, ApplicationError) and cause.type == "Day1NotConfiguredError"
        assert fleet.names() == ["read_day1_nodepools"]  # non-retryable: one attempt


class TestTheChildsFailures:
    async def test_someone_elses_edit_fails_once_and_names_it(self):
        fleet = _Fleet((3, 3, 2))
        fleet.commit_error = Day1FileChangedError("someone else changed it")
        error = await _failure(fleet, 10)
        assert error.type == "ScaleNodePoolFailedError" and "Day1FileChangedError" in error.message
        assert fleet.names().count("commit_day1_replicas") == 1

    async def test_a_refused_sync_fails_once(self):
        fleet = _Fleet((3, 3, 2))
        fleet.sync_error = ArgoSyncRefusedError("out of sync beyond NodePool/alpha-zone-a: HostedCluster/x")
        error = await _failure(fleet, 10)
        assert error.type == "ScaleNodePoolFailedError" and "ArgoSyncRefusedError" in error.message
        assert fleet.names().count("sync_argo_application") == 1

    async def test_the_mce_not_showing_the_commit_is_not_reconciled(self):
        fleet = _Fleet((3, 3, 2))
        fleet.argo_applies = False
        error = await _failure(fleet, 10)
        assert error.type == "ScaleNodePoolFailedError" and "NodePoolsNotReconciledError" in error.message
        assert "alpha-zone-a: spec.replicas 3, planned 4" in error.message

    async def test_a_child_already_running_on_the_id_is_named(self):
        fleet = _Fleet((3, 3, 2))

        async def _hold(client: Client) -> None:
            await client.start_workflow(_Blocker.run, id=scale_up_nodepool_workflow_id(CLUSTER), task_queue="blocker")

        error = await _failure(fleet, 10, before=_hold)
        assert error.type == "ScaleNodePoolAlreadyRunningError"
        assert "commit_day1_replicas" not in fleet.names()


class TestARerun:
    async def test_day1_already_holding_the_plan_resumes_from_the_sync(self):
        """A child committed and never synced: the re-run hands that plan on instead of failing on drift."""
        fleet = _Fleet((4, 3, 3), live=(3, 3, 2))
        result = await _run(fleet, 10)
        assert result.direction == "up" and not result.scale.committed
        assert fleet.first("commit_day1_replicas").nodepool_replicas == {"alpha-zone-a": 4, "alpha-zone-c": 3}
        assert fleet.live["alpha-zone-a"] == 4

    async def test_day1_holding_another_total_is_still_drift(self):
        fleet = _Fleet((4, 3, 3), live=(3, 3, 2))
        error = await _failure(fleet, 12)
        assert error.type == "NodePoolReplicasDriftError"


# --- one NodePool, by name -----------------------------------------------------------------


class TestOneNodePool:
    """`nodepool` instead of `infra_env_kind`: that NodePool's replicas, set exactly (the operator, 2026-11-08)."""

    async def test_only_the_named_nodepool_moves_to_exactly_the_number_asked(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 5, nodepool="alpha-zone-c")
        assert fleet.first("commit_day1_replicas").nodepool_replicas == {"alpha-zone-c": 5}
        assert fleet.first("sync_argo_application").allowed_out_of_sync == ["NodePool/alpha-zone-c"]
        # No balance rule: 3, 3, 5 is what was asked for.
        assert fleet.day1 == {"alpha-zone-a": 3, "alpha-zone-b": 3, "alpha-zone-c": 5, "alpha-gpu": 1}
        assert (result.nodepool, result.infra_env_kind, result.direction) == ("alpha-zone-c", None, "up")
        assert [(np.name, np.before, np.after) for np in result.nodepools] == [("alpha-zone-c", 2, 5)]
        assert (result.total_before, result.total_after) == (2, 5)

    async def test_an_unbalanced_kind_is_no_reason_to_refuse(self):
        fleet = _Fleet((5, 2, 2))
        result = await _run(fleet, 3, nodepool="alpha-zone-b")
        assert result.direction == "up" and fleet.day1["alpha-zone-b"] == 3

    async def test_any_nodepool_of_the_cluster_whatever_its_kind(self):
        fleet = _Fleet((3, 3, 2))
        await _run(fleet, 2, nodepool="alpha-gpu")
        assert fleet.first("commit_day1_replicas").nodepool_replicas == {"alpha-gpu": 2}

    async def test_down_by_its_policy_marks_the_servers_first(self):
        fleet = _policy_fleet()
        result = await _run(fleet, 1, nodepool="alpha-zone-c")
        assert fleet.first("mark_machines_for_deletion").machines == ["c2", "c3"]  # 1 and 3 pods
        names = fleet.names()
        assert names.index("mark_machines_for_deletion") < names.index("commit_day1_replicas")
        assert [c.machine for c in result.scale.chosen] == ["c2", "c3"]

    async def test_to_zero(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 0, nodepool="alpha-zone-a")
        assert result.direction == "down" and fleet.day1["alpha-zone-a"] == 0

    async def test_already_the_number_writes_nothing(self):
        fleet = _Fleet((3, 3, 2))
        result = await _run(fleet, 3, nodepool="alpha-zone-a")
        assert result.direction == "none" and fleet.names() == ["read_day1_nodepools", "read_hosted_cluster"]

    async def test_a_nodepool_the_cluster_does_not_have_is_named_and_nothing_written(self):
        fleet = _Fleet((3, 3, 2))
        error = await _failure(fleet, 4, nodepool="alpha-zone-x")
        assert error.type == "NodePoolNotFoundError" and "alpha-zone-a, alpha-zone-b" in error.message
        assert "commit_day1_replicas" not in fleet.names()

    async def test_one_on_the_mce_only_is_drift(self):
        fleet = _Fleet((3, 3, 2))
        fleet.live["alpha-zone-d"] = 2
        fleet.infra_envs["alpha-zone-d"] = f"{KIND}-zone-d"
        error = await _failure(fleet, 4, nodepool="alpha-zone-d")
        assert error.type == "NodePoolReplicasDriftError" and "not in" in error.message

    async def test_its_replicas_disagreeing_is_drift(self):
        fleet = _Fleet((3, 3, 2), live=(3, 3, 4))
        error = await _failure(fleet, 6, nodepool="alpha-zone-c")
        assert error.type == "NodePoolReplicasDriftError" and "alpha-zone-c 4 on the MCE, 2 in day1" in error.message

    async def test_a_rerun_resumes_a_commit_that_never_synced(self):
        fleet = _Fleet((3, 3, 5), live=(3, 3, 2))
        result = await _run(fleet, 5, nodepool="alpha-zone-c")
        assert result.direction == "up" and not result.scale.committed and fleet.live["alpha-zone-c"] == 5

    # (run_args drops the DEFAULT kind when a NodePool is named, so "both" names another.)
    @pytest.mark.parametrize("target", [{"kind": "gpu-a100", "nodepool": "alpha-zone-c"}, {"kind": None}])
    async def test_both_targets_or_neither_is_refused_before_anything_is_read(self, target):
        fleet = _Fleet((3, 3, 2))
        error = await _failure(fleet, 4, **target)
        assert error.type == "NodePoolTargetInvalidError" and fleet.names() == []


# --- scale-down with a policy ---------------------------------------------------------------


def _machine(name: str, nodepool: str = "alpha-zone-c", **fields) -> ScaleDownMachine:
    defaults = dict(nodepool=nodepool, namespace=HCP, name=name, node=f"node-{name}", node_found=True,
                    ready=True, workload_pods=5, requested_permille=500)
    return ScaleDownMachine(**(defaults | fields))


def _policy_fleet(**machine_fields) -> _Fleet:
    """(3, 3, 3) to 8: zone-c loses one, and it names fewest-pods."""
    machines = [_machine("c1", workload_pods=5), _machine("c2", workload_pods=1), _machine("c3", workload_pods=3)]
    for name, fields in machine_fields.items():
        machines = [m.model_copy(update=fields) if m.name == name else m for m in machines]
    return _Fleet((3, 3, 3), policies={"alpha-zone-c": "fewest-pods"}, machines=machines)


class TestScalingDownByAPolicy:
    async def test_the_chosen_server_is_marked_before_the_push(self):
        fleet = _policy_fleet()
        result = await _run(fleet, 8)

        assert fleet.names() == [
            "read_day1_nodepools", "read_hosted_cluster", "read_scale_down_candidates",
            "mark_machines_for_deletion", "commit_day1_replicas", "sync_argo_application",
            "read_hosted_cluster", "watch_marked_machines",
        ]
        candidates = fleet.first("read_scale_down_candidates")
        assert (candidates.hosted_cluster_namespace, candidates.hosted_cluster) == (HC_NS, HC)
        assert [(np.name, np.policy.value) for np in candidates.nodepools] == [("alpha-zone-c", "fewest-pods")]
        mark = fleet.first("mark_machines_for_deletion")
        assert (mark.namespace, mark.machines, mark.mark) == (HCP, ["c2"], DOWN_ID)
        watch = fleet.first("watch_marked_machines")
        assert (watch.hcp_namespace, watch.hosted_cluster_namespace, watch.nodepools, watch.marked) == (
            HCP, HC_NS, ["alpha-zone-c"], ["c2"],
        )
        [choice] = result.scale.chosen
        assert (choice.machine, choice.policy, choice.reason) == ("c2", "fewest-pods", "1 workload pods")
        assert result.scale.deleting == ["c2"]

    async def test_a_refused_commit_takes_the_marks_off_again(self):
        fleet = _policy_fleet()
        fleet.commit_error = Day1FileChangedError("someone else changed it")
        error = await _failure(fleet, 8)
        assert "Day1FileChangedError" in error.message
        assert fleet.names()[-2:] == ["commit_day1_replicas", "unmark_machines"]
        assert fleet.machines["c2"].delete_mark is None

    async def test_a_failed_sync_after_the_commit_keeps_the_marks(self):
        fleet = _policy_fleet()
        fleet.sync_error = ArgoSyncRefusedError("someone else's change is pending")
        await _failure(fleet, 8)
        assert "unmark_machines" not in fleet.names() and fleet.machines["c2"].delete_mark == DOWN_ID

    async def test_capi_taking_another_machine_is_named(self):
        fleet = _policy_fleet()
        fleet.watch_result = MarkedMachinesOutcome.WRONG_MACHINE
        error = await _failure(fleet, 8)
        assert "ScaleDownWrongMachineError" in error.message and "alpha-zone-c-other" in error.message

    async def test_the_marked_machine_not_deleting_by_the_deadline(self):
        fleet = _policy_fleet()
        fleet.watch_result = MarkedMachinesOutcome.DEADLINE
        error = await _failure(fleet, 8)
        assert "NodePoolsNotReconciledError" in error.message and "c2" in error.message

    async def test_someone_elses_mark_stops_it_before_anything_is_written(self):
        fleet = _policy_fleet(c3={"delete_mark": "yes"})
        error = await _failure(fleet, 8)
        assert "ScaleDownMarkConflictError" in error.message
        assert "mark_machines_for_deletion" not in fleet.names() and "commit_day1_replicas" not in fleet.names()

    async def test_a_nodepool_mid_rollout_is_refused_before_anything_is_marked(self):
        fleet = _policy_fleet(c1={"machine_set": "alpha-zone-c-old"}, c2={"machine_set": "alpha-zone-c-new"})
        error = await _failure(fleet, 8)
        assert "ScaleDownRolloutInProgressError" in error.message
        assert "mark_machines_for_deletion" not in fleet.names() and "commit_day1_replicas" not in fleet.names()

    async def test_a_rerun_keeps_the_machine_it_marked_before(self):
        fleet = _policy_fleet(c1={"delete_mark": DOWN_ID, "workload_pods": 9})
        result = await _run(fleet, 8)
        # Its own mark goes first, whatever the policy says now: the same server.
        assert [c.machine for c in result.scale.chosen] == ["c1"] and "unmark_machines" not in fleet.names()

    async def test_an_earlier_attempts_mark_on_a_machine_not_chosen_comes_off_first(self):
        fleet = _policy_fleet(c1={"delete_mark": DOWN_ID}, c3={"delete_mark": DOWN_ID})
        result = await _run(fleet, 8)
        assert [c.machine for c in result.scale.chosen] == ["c1"]
        names = fleet.names()
        assert names.index("unmark_machines") < names.index("mark_machines_for_deletion")
        assert fleet.first("unmark_machines").machines == ["c3"] and fleet.machines["c3"].delete_mark is None

    async def test_a_policy_that_does_not_exist_reads_nothing(self):
        fleet = _Fleet((3, 3, 3), policies={"alpha-zone-c": "random"})
        error = await _failure(fleet, 8)
        assert "ScaleDownPolicyInvalidError" in error.message and "alpha-zone-c: 'random'" in error.message
        assert "read_scale_down_candidates" not in fleet.names()

    async def test_a_policy_on_a_nodepool_that_does_not_shrink_reads_nothing(self):
        fleet = _Fleet((3, 3, 3), policies={"alpha-zone-a": "fewest-pods"})
        result = await _run(fleet, 8)  # zone-c shrinks, and names no policy
        assert "read_scale_down_candidates" not in fleet.names() and result.scale.chosen == []


# --- what the request says about a scale-down (the operator, 2026-11-18) --------------------


def _named_fleet(**machine_fields) -> _Fleet:
    """(3, 3, 3): zone-c's Machines c1-c3 are servers ocp-r660-1 … 3 (1, 2, 3 pods); no policy anywhere."""
    machines = [_machine(f"c{i}", server=f"ocp-r660-{i}", workload_pods=i) for i in (1, 2, 3)]
    for name, fields in machine_fields.items():
        machines = [m.model_copy(update=fields) if m.name == name else m for m in machines]
    return _Fleet((3, 3, 3), machines=machines)


class TestTheRequestsPolicy:
    async def test_it_goes_ahead_of_the_nodepools_own(self):
        # zone-c's own fewest-pods would take c2; the request's least-requested takes c1.
        fleet = _policy_fleet(c1={"requested_permille": 100})
        result = await _run(fleet, 2, nodepool="alpha-zone-c", policy="least-requested")
        candidates = fleet.first("read_scale_down_candidates")
        assert [(np.name, np.policy) for np in candidates.nodepools] == [
            ("alpha-zone-c", ScaleDownPolicy.LEAST_REQUESTED)
        ]
        assert not candidates.with_servers
        assert fleet.first("mark_machines_for_deletion").machines == ["c1"]
        assert result.nodepools[0].scale_down_policy == "least-requested"
        assert [(c.machine, c.policy) for c in result.scale.chosen] == [("c1", "least-requested")]

    async def test_a_nodepool_naming_none_is_ranked_by_it(self):
        fleet = _named_fleet()
        await _run(fleet, 1, nodepool="alpha-zone-c", policy="fewest-pods")
        assert fleet.first("mark_machines_for_deletion").machines == ["c1", "c2"]

    async def test_it_stands_in_for_a_nodepool_value_that_is_no_policy(self):
        fleet = _named_fleet()
        fleet.policies["alpha-zone-c"] = "random"
        result = await _run(fleet, 2, nodepool="alpha-zone-c", policy="fewest-pods")
        assert [c.machine for c in result.scale.chosen] == ["c1"]


class TestNamedServers:
    async def test_exactly_the_named_servers_are_marked_then_pushed(self):
        fleet = _named_fleet()
        result = await _run(fleet, 1, nodepool="alpha-zone-c", servers=["OCP-R660-3", "ocp-r660-1"])

        assert fleet.names() == [
            "read_day1_nodepools", "read_hosted_cluster", "read_scale_down_candidates",
            "mark_machines_for_deletion", "commit_day1_replicas", "sync_argo_application",
            "read_hosted_cluster", "watch_marked_machines",
        ]
        candidates = fleet.first("read_scale_down_candidates")
        # Nothing ranks, so no policy is asked for: the hosted cluster is not read.
        assert candidates.with_servers and [(np.name, np.policy) for np in candidates.nodepools] == [
            ("alpha-zone-c", None)
        ]
        mark = fleet.first("mark_machines_for_deletion")
        assert (mark.machines, mark.mark) == (["c3", "c1"], DOWN_ID)
        assert fleet.first("commit_day1_replicas").nodepool_replicas == {"alpha-zone-c": 1}
        assert fleet.first("watch_marked_machines").marked == ["c3", "c1"]
        assert [(c.machine, c.server, c.policy) for c in result.scale.chosen] == [
            ("c3", "ocp-r660-3", "named"), ("c1", "ocp-r660-1", "named"),
        ]
        assert result.nodepools[0].scale_down_policy is None and result.ignored == []

    async def test_the_nodepools_own_policy_does_not_apply(self):
        fleet = _named_fleet()
        fleet.policies["alpha-zone-c"] = "random"  # would be ScaleDownPolicyInvalidError by policy
        result = await _run(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-2"])
        assert [c.machine for c in result.scale.chosen] == ["c2"]

    async def test_a_count_that_is_not_the_replicas_removed_starts_nothing(self):
        fleet = _named_fleet()
        error = await _failure(fleet, 1, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert error.type == "ScaleDownServersInvalidError"
        assert "alpha-zone-c goes from 3 to 1, so 2 server(s) leave" in error.message
        assert fleet.names() == ["read_day1_nodepools", "read_hosted_cluster"]

    async def test_a_server_no_machine_of_the_nodepool_is_writes_nothing(self):
        fleet = _named_fleet()
        error = await _failure(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-9"])
        assert error.type == "ScaleNodePoolFailedError" and "ScaleDownServersInvalidError" in error.message
        assert "server ocp-r660-9" in error.message and "ocp-r660-1 (c1)" in error.message
        assert "mark_machines_for_deletion" not in fleet.names() and "commit_day1_replicas" not in fleet.names()

    async def test_an_unhealthy_machine_capi_would_take_instead_is_refused(self):
        fleet = _named_fleet(c1={"capi_healthy": False})
        error = await _failure(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-2"])
        assert "ScaleDownServersInvalidError" in error.message and "ocp-r660-1 (c1) is unhealthy" in error.message
        assert "mark_machines_for_deletion" not in fleet.names() and "commit_day1_replicas" not in fleet.names()

    async def test_naming_the_unhealthy_one_is_fine(self):
        fleet = _named_fleet(c1={"capi_healthy": False, "node": None, "node_found": False})
        result = await _run(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert [c.machine for c in result.scale.chosen] == ["c1"]

    async def test_an_unhealthy_machine_capi_reaches_after_the_named_one_is_fine(self):
        fleet = _named_fleet(c3={"capi_healthy": False})
        result = await _run(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-2"])
        assert [c.machine for c in result.scale.chosen] == ["c2"]

    async def test_a_worker_that_cannot_say_which_server_is_named(self):
        fleet = _named_fleet()
        fleet.candidates_without_servers = True
        error = await _failure(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert "ScaleDownServersInvalidError" in error.message and "predates `server_names`" in error.message
        assert "mark_machines_for_deletion" not in fleet.names()

    async def test_someone_elses_mark_stops_it_before_anything_is_written(self):
        fleet = _named_fleet(c3={"delete_mark": "yes"})
        error = await _failure(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert "ScaleDownMarkConflictError" in error.message and "commit_day1_replicas" not in fleet.names()

    async def test_a_refused_commit_takes_the_marks_off_again(self):
        fleet = _named_fleet()
        fleet.commit_error = Day1FileChangedError("someone else changed it")
        await _failure(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert fleet.names()[-2:] == ["commit_day1_replicas", "unmark_machines"]
        assert fleet.machines["c1"].delete_mark is None

    async def test_a_rerun_naming_another_server_takes_the_earlier_mark_off_first(self):
        fleet = _named_fleet(c1={"delete_mark": DOWN_ID})
        result = await _run(fleet, 2, nodepool="alpha-zone-c", servers=["ocp-r660-2"])
        assert [c.machine for c in result.scale.chosen] == ["c2"]
        names = fleet.names()
        assert names.index("unmark_machines") < names.index("mark_machines_for_deletion")
        assert fleet.first("unmark_machines").machines == ["c1"] and fleet.machines["c1"].delete_mark is None

    async def test_with_a_policy_as_well_nothing_is_read(self):
        fleet = _named_fleet()
        error = await _failure(fleet, 2, nodepool="alpha-zone-c", policy="fewest-pods", servers=["ocp-r660-1"])
        assert error.type == "ScaleDownServersInvalidError" and fleet.names() == []


class TestScaleDownFieldsWhenNothingScalesDown:
    """Sent with a scale-up or no change, they are ignored and the result says so — never a failure."""

    async def test_a_scale_up_ignores_named_servers(self):
        fleet = _named_fleet()
        result = await _run(fleet, 5, nodepool="alpha-zone-c", servers=["ocp-r660-1"])
        assert result.direction == "up" and fleet.day1["alpha-zone-c"] == 5
        assert "read_scale_down_candidates" not in fleet.names() and "mark_machines_for_deletion" not in fleet.names()
        assert result.ignored == ["server_names ocp-r660-1: the NodePool scales up"]

    async def test_a_scale_up_ignores_a_policy(self):
        result = await _run(_named_fleet(), 4, nodepool="alpha-zone-c", policy="least-utilized")
        assert result.direction == "up" and result.ignored == [
            "scale_down_policy least-utilized: the NodePool scales up"
        ]
        assert result.nodepools[0].scale_down_policy is None

    async def test_no_change_ignores_them_whatever_their_count(self):
        result = await _run(_named_fleet(), 3, nodepool="alpha-zone-c", servers=["ocp-r660-1", "ocp-x"])
        assert result.direction == "none" and result.ignored == [
            "server_names ocp-r660-1, ocp-x: nothing scales down, the replicas are already alpha-zone-c 3"
        ]
