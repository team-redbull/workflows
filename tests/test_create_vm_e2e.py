"""create-vm END TO END: the real workflow, the REAL vm-lifecycle activities and their real
httpx calls and git clone, against tests/kv_agents_simulator.py — a FLEET of KubeVirt
clusters in every state the placement and the create can meet.

Nothing between the workflow and the wire is mocked: the limb reads its
settings (pointed at the fleet and at a bare day1 repo served over the
`insteadOf` trick), opens its own clients, fans out over every cluster of the
zone, ranks, PUTs, polls the root disk with its heartbeating wait and rolls
back. The simulator answers in the agents' wire shapes (pinned by
tests/test_kv_agents_contract.py); what this cannot prove is that the real
agents read a real cluster the way the simulator does — the lab on
cluster-64spr did that (docs/lab/create-vm/README.md).

What tests/test_vm_lifecycle_placement.py pins on node tables, this pins
through the whole stack over many clusters: the fullest fitting cluster
wins, ties go by name, every unusable cluster is a row with its reason, a
Halted reservation from one create tips the next, retries and permanent
failures land where the classification says, and a parallel pair of runs
sees the same table (reservations protect SEQUENTIAL creates only).
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import uvicorn
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import activities.vm_lifecycle.activities as limb
from shared.consts import CREATE_VM_WORKFLOW_QUEUE, VM_LIFECYCLE_ACTIVITY_QUEUE
from shared.exceptions import (
    KvAgentAuthError,
    NoKvClusterError,
    NoKvClusterFitsError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
    VmRootDiskNotBoundError,
)
from shared.models.vm_lifecycle import CreateVmInput, CreateVmResult, CreateVmRunArgs
from tests.kv_agents_simulator import GIB, VM_LABEL, Fleet, SimCluster, SimNode, SimVm, fleet_app
from tests.test_git_repo import HOST, SECRET, _bare_repo, serve_over_https
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow

SITE, ZONE, MCE = "lab", "a", "ocp4-lab-mce-lab-a"
SEED = f"sites/{SITE}/mces/ocp4-seed-{SITE}-{ZONE}/hostedClusters"
LIMB_ACTIVITIES = [
    limb.list_kv_clusters, limb.rank_kv_clusters, limb.create_vm_on_cluster, limb.await_vm_ready, limb.delete_vm_on_cluster,
]


def _name(n: int) -> str:
    return f"ocp4-kubevirt-{SITE}-{ZONE}-{n}"


def _cluster(n: int, nodes: list[SimNode] | None = None, **behaviour) -> SimCluster:
    """Cluster `n` of the zone: its chart clusterName is the day1 name minus `ocp4-`."""
    return SimCluster(name=f"kubevirt-{SITE}-{ZONE}-{n}", nodes=nodes if nodes is not None else [_node()], **behaviour)


def _node(cores: float = 64, gib: float = 512, used_cores: float = 0, used_gib: float = 0, schedulable: bool = True) -> SimNode:
    return SimNode("n", cores, gib, used_cores, used_gib, schedulable)


def _input(cpu: int = 8, zone: str = ZONE, namespace: str = "inventory") -> CreateVmInput:
    return CreateVmInput(
        mce_cluster=MCE, infra_env=f"x201-zone-{zone}-{cpu}", namespace=namespace, site=SITE, zone=zone,
        cpu=cpu, memory_gib=cpu * 8,
    )


# --- the fleet, served once per module ----------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Served:
    def __init__(self, app, port: int) -> None:
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> _Served:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("the fleet simulator did not start")
            time.sleep(0.02)
        return self

    def __exit__(self, *exc_info) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


@pytest.fixture(scope="module")
def served_fleet() -> Iterator[tuple[Fleet, str]]:
    fleet = Fleet()
    port = _free_port()
    with _Served(fleet_app(fleet), port):
        yield fleet, f"http://127.0.0.1:{port}"


@pytest.fixture
def fleet(served_fleet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fleet:
    """An empty fleet, the limb pointed at it and at a day1 repo in tmp_path, fast polls."""
    fleet, base = served_fleet
    fleet.reset([])
    for variable in ("NO_PROXY", "no_proxy"):  # httpx honours a proxy but not CIDRs in NO_PROXY
        existing = [v for v in (os.environ.get(variable) or "").split(",") if v]
        monkeypatch.setenv(variable, ",".join([*existing, "127.0.0.1"]))
    settings = limb._settings
    monkeypatch.setattr(settings, "kv_capacity_url_template", f"{base}/clusters/{{cluster}}")
    monkeypatch.setattr(settings, "kv_vm_api_url_template", f"{base}/clusters/{{cluster}}")
    monkeypatch.setattr(settings, "kv_capacity_token", fleet.capacity_token)
    monkeypatch.setattr(settings, "kv_vm_api_token", fleet.vm_token)
    monkeypatch.setattr(settings, "headroom_buffer_percent", 20)
    monkeypatch.setattr(settings, "vm_mac_prefix", "02:a1:b2")
    monkeypatch.setattr(settings, "day1_repo_url", HOST + "day1.git")
    monkeypatch.setattr(settings, "day1_git_token", SECRET)
    monkeypatch.setattr(limb, "READY_POLL_SECONDS", 0.02)
    monkeypatch.setattr(limb, "PROBE_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(limb, "FANOUT_TIMEOUT_SECONDS", 5.0)
    serve_over_https(tmp_path, monkeypatch)
    return fleet


def _day1(tmp_path: Path, clusters: list[SimCluster], extra: dict[str, str] | None = None) -> None:
    """A day1 main whose zone-a seed folder lists exactly these clusters (plus `extra` files)."""
    files = {"sites/configValues.yaml": "dhcp_values:\n  leaseDurationDays: 8\n"}
    for cluster in clusters:
        files[f"{SEED}/ocp4-{cluster.name}.yaml"] = "platform: agent\nnodepool: []\n"
    files.update(extra or {})
    _bare_repo(tmp_path, "day1.git", files)


# --- the run ------------------------------------------------------------------------------


class _Harness:
    def __init__(self, *, time_skipping: bool = True) -> None:
        self._time_skipping = time_skipping

    async def __aenter__(self) -> Client:
        # Two results awaited at once on the time-skipping server intermittently end one run
        # with a workflow-execution timeout (observed, cause not pinned down); a plain local
        # dev server does not, so the parallel pair runs on one.
        self._env = await (
            WorkflowEnvironment.start_time_skipping() if self._time_skipping else WorkflowEnvironment.start_local()
        )
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=CREATE_VM_WORKFLOW_QUEUE, workflows=[CreateVmWorkflow]),
            Worker(client, task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE, activities=LIMB_ACTIVITIES),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _run(client: Client, wf_input: CreateVmInput) -> CreateVmResult:
    return await asyncio.wait_for(
        client.execute_workflow(
            CreateVmWorkflow.run, CreateVmRunArgs(input=wf_input), id=f"e2e-{uuid.uuid4()}",
            task_queue=CREATE_VM_WORKFLOW_QUEUE,
        ),
        timeout=120,
    )


async def _fails(client: Client, wf_input: CreateVmInput) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as excinfo:
        await _run(client, wf_input)
    cause = excinfo.value.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


def _rows(result: CreateVmResult) -> dict[str, tuple[bool, str | None]]:
    return {row.name: (row.fits, row.skip_reason) for row in result.placement.ranked}


# --- placement over a fleet ---------------------------------------------------------------


async def test_the_fullest_fitting_cluster_of_a_mixed_fleet_takes_the_vm(fleet, tmp_path):
    """Twelve clusters in day1, every state at once; an 8-vCPU VM is a 7-core / 65 GiB pod."""
    clusters = [
        _cluster(1, [_node(used_cores=20, used_gib=100)]),           # fits: cpu after 27/64 = 42%
        _cluster(2, [_node(used_cores=40, used_gib=200)]),           # fits: cpu after 47/64 = 73%
        _cluster(3, [_node(gib=300, used_cores=10, used_gib=160)]),  # fits: MEMORY after 225/300 = 75% — the winner
        _cluster(4, [_node(used_cores=40, used_gib=200)]),           # the exact tie of 2: loses by name
        _cluster(5, [_node(used_cores=50)]),                         # fit-buffer: 57/64 = 89% > 80%
        _cluster(6, [_node(cores=8, gib=64) for _ in range(10)]),    # fit-node: 80 free cores, no node holds the pod
        _cluster(7, [_node(schedulable=False)]),                     # no-nodes
        _cluster(8, capacity="no-agent"),
        _cluster(9, capacity="refused"),
        _cluster(10, capacity="malformed"),
        _cluster(11, capacity="down"),
        _cluster(12, capacity="slow"),                               # past the 0.5 s probe: unreachable
    ]
    clusters[0].add_vm(SimVm("inventory", "foreign", "02:A1:B2:AA:BB:CC", 2, 16, labels={}))
    fleet.reset(clusters)
    _day1(tmp_path, clusters, extra={f"{SEED}/{_name(13)}.yaml": "platform: agent\n"})  # in day1, no agents at all

    async with _Harness() as client:
        result = await _run(client, _input(cpu=8))

    assert result.kv_cluster.name == _name(3)
    assert [row.name for row in result.placement.ranked[:4]] == [_name(3), _name(2), _name(4), _name(1)]
    assert _rows(result) == {
        _name(3): (True, None), _name(2): (True, None), _name(4): (True, None), _name(1): (True, None),
        _name(5): (False, "fit-buffer"), _name(6): (False, "fit-node"), _name(7): (False, "no-nodes"),
        _name(8): (False, "no-agent"), _name(9): (False, "refused-token"), _name(10): (False, "malformed"),
        _name(11): (False, "unreachable"), _name(12): (False, "unreachable"), _name(13): (False, "no-agent"),
    }
    assert (result.placement.pod_cpu_millicores, result.placement.pod_memory_bytes) == (7000, 65 * GIB)
    assert "02:a1:b2:aa:bb:cc" in result.placement.known_macs
    # ONE VM in the whole fleet, on the chosen cluster, named and addressed as the run says.
    assert [(c, vm.name) for c, vm in fleet.all_vms() if vm.name != "foreign"] == [(clusters[2].name, result.vm_name)]
    vm = clusters[2].vms[f"inventory/{result.vm_name}"]
    assert result.vm_name == f"ocp-x201-zone-a-8-{result.mac[-8:].replace(':', '')}"
    assert (vm.mac, vm.cpu, vm.memory_gib, vm.labels, vm.run_strategy) == (result.mac, 8, 64, VM_LABEL, "Halted")
    assert (result.created, result.redraws, result.root_disk_phase) == (True, 0, "Succeeded")
    assert all(c.capacity_requests == 1 for c in clusters)


async def test_a_halted_reservation_from_one_create_tips_the_next_elsewhere(fleet, tmp_path, monkeypatch):
    """Buffer 35 → ceiling 65%. First: a1 (58% after) beats a2 (42%). Then a1 carries a Halted
    VM: 69% after is past the ceiling, and the second VM goes to a2."""
    monkeypatch.setattr(limb._settings, "headroom_buffer_percent", 35)
    clusters = [_cluster(1, [_node(used_cores=30)]), _cluster(2, [_node(used_cores=20)])]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        first = await _run(client, _input(cpu=8))
        second = await _run(client, _input(cpu=8))

    assert first.kv_cluster.name == _name(1)
    assert second.kv_cluster.name == _name(2)
    a1 = next(row for row in second.placement.ranked if row.name == _name(1))
    assert (a1.fits, a1.skip_reason, a1.halted_reservations) == (False, "fit-buffer", 1)
    assert a1.cpu_fill_before == 37_000 / 64_000  # the first VM's 7 cores are counted before placing
    assert first.vm_name != second.vm_name and first.mac != second.mac
    assert sorted(c for c, _ in fleet.all_vms()) == sorted([clusters[0].name, clusters[1].name])


async def test_two_runs_at_once_read_the_same_table_and_both_land(fleet, tmp_path):
    """A `count: 2` request is two runs in parallel: both rank BEFORE either creates, so a Halted
    reservation cannot keep them apart — both take the fullest cluster, with distinct names."""
    clusters = [_cluster(1, [_node(used_cores=30)]), _cluster(2, [_node(used_cores=20)])]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness(time_skipping=False) as client:
        handles = [
            await client.start_workflow(
                CreateVmWorkflow.run, CreateVmRunArgs(input=_input(cpu=8)), id=f"e2e-{uuid.uuid4()}",
                task_queue=CREATE_VM_WORKFLOW_QUEUE,
            )
            for _ in range(2)
        ]
        one, two = await asyncio.wait_for(asyncio.gather(*(handle.result() for handle in handles)), timeout=120)

    assert {one.kv_cluster.name, two.kv_cluster.name} == {_name(1)}
    assert one.vm_name != two.vm_name and one.mac != two.mac
    assert sorted(vm.name for _, vm in fleet.all_vms()) == sorted([one.vm_name, two.vm_name])


async def test_nothing_fitting_fails_naming_every_cluster_by_its_reason(fleet, tmp_path):
    clusters = [
        _cluster(1, [_node(used_cores=50)]), _cluster(2, [_node(cores=8, gib=64)]),
        _cluster(3, capacity="no-agent"), _cluster(4, capacity="refused"),
    ]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, _input(cpu=8))

    assert error.type == NoKvClusterFitsError.__name__
    assert f"{_name(1)} would go past the headroom buffer" in error.message
    assert f"{_name(2)} have no schedulable node with the headroom" in error.message
    assert f"{_name(3)} have no kv-capacity agent" in error.message
    assert f"{_name(4)} refused the kv-capacity token" in error.message
    assert error.message.endswith("Nothing was created.")
    assert fleet.all_vms() == []


async def test_a_zone_with_no_seed_folder_fails_before_any_cluster_is_read(fleet, tmp_path):
    clusters = [_cluster(1)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, _input(cpu=8, zone="c"))

    assert error.type == NoKvClusterError.__name__
    assert "ocp4-seed-lab-c/hostedClusters/ (0 files there)" in error.message
    assert clusters[0].capacity_requests == 0


# --- the fan-out's classification, end to end --------------------------------------------


async def test_every_cluster_refusing_the_token_fails_the_run_after_one_attempt(fleet, tmp_path, monkeypatch):
    monkeypatch.setattr(limb._settings, "kv_capacity_token", "not-the-fleet-token")
    clusters = [_cluster(1), _cluster(2), _cluster(3)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, _input(cpu=8))

    assert error.type == KvAgentAuthError.__name__
    assert [c.capacity_requests for c in clusters] == [1, 1, 1]
    assert fleet.all_vms() == []


async def test_no_cluster_readable_is_retried_until_one_answers(fleet, tmp_path):
    clusters = [_cluster(1, capacity_down_for=2), _cluster(2, capacity="no-agent")]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        result = await _run(client, _input(cpu=8))

    assert result.kv_cluster.name == _name(1)
    assert clusters[0].capacity_requests == 3  # two outages, then the read that ranked


# --- the create and the wait, end to end --------------------------------------------------


async def test_a_name_born_in_between_is_redrawn_and_the_foreign_vm_left_alone(fleet, tmp_path):
    clusters = [_cluster(1, names_taken_before=1)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        result = await _run(client, _input(cpu=8))

    assert result.redraws == 1
    names = sorted(vm.name for vm in clusters[0].vms.values())
    assert len(names) == 2 and result.vm_name in names
    foreign = next(vm for vm in clusters[0].vms.values() if vm.name != result.vm_name)
    assert foreign.mac == "02:ff:ff:00:00:01"
    assert [put["name"] for put in clusters[0].put_requests] == [foreign.name, result.vm_name]
    assert clusters[0].delete_requests == []


async def test_an_answer_lost_after_the_create_is_found_by_the_retry_not_doubled(fleet, tmp_path):
    clusters = [_cluster(1, lost_answers=1)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        result = await _run(client, _input(cpu=8))

    assert result.created is False  # the retry's PUT met its own VM: 200, the same identity
    assert len(clusters[0].put_requests) == 2 and len(clusters[0].vms) == 1
    assert clusters[0].put_requests[0]["mac"] == clusters[0].put_requests[1]["mac"] == result.mac


@pytest.mark.parametrize(
    ("behaviour", "wf_input", "error_type"),
    [
        ({}, _input(cpu=8, namespace="nowhere"), VmNamespaceNotFoundError),
        ({"vm_api": "reject"}, _input(cpu=8), VmRequestRejectedError),
        ({"vm_api": "refused"}, _input(cpu=8), KvAgentAuthError),
    ],
    ids=["namespace-missing", "template-refuses", "vm-api-token-wrong"],
)
async def test_each_permanent_create_failure_fails_after_one_put_and_deletes_nothing(fleet, tmp_path, behaviour, wf_input, error_type):
    clusters = [_cluster(1, **behaviour)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, wf_input)

    assert error.type == error_type.__name__
    assert len(clusters[0].put_requests) <= 1
    assert clusters[0].delete_requests == [] and clusters[0].our_vms() == []


async def test_a_root_disk_that_never_binds_is_rolled_back_then_the_run_fails(fleet, tmp_path, monkeypatch):
    # The wait runs on real time inside the activity; a retry resumes its clock from the
    # heartbeat, so a clock that started eleven minutes ago is past the ten-minute deadline.
    monkeypatch.setattr(limb, "_resumed_clock", lambda: (datetime.now(UTC) - timedelta(minutes=11), 0))
    clusters = [_cluster(1, binds_after_polls=None)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, _input(cpu=8))

    assert error.type == VmRootDiskNotBoundError.__name__
    assert "The VM was deleted again" in error.message and "WaitForFirstConsumer" in error.message
    [put] = clusters[0].put_requests
    [delete] = clusters[0].delete_requests
    assert (delete["name"], delete["mac"]) == (put["name"], put["mac"])
    assert clusters[0].vms == {}


async def test_a_vm_replaced_behind_the_run_is_not_deleted(fleet, tmp_path, monkeypatch):
    monkeypatch.setattr(limb, "_resumed_clock", lambda: (datetime.now(UTC) - timedelta(minutes=11), 0))
    clusters = [_cluster(1, binds_after_polls=None, replace_after_put=True)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        error = await _fails(client, _input(cpu=8))

    assert error.type == VmIdentityConflictError.__name__
    assert "not the caller's VM" in error.message
    [vm] = clusters[0].vms.values()
    assert vm.mac == "02:a1:b2:de:ad:00"  # the replacement stands


async def test_the_wait_polls_until_the_disk_binds_and_reports_the_polls(fleet, tmp_path):
    clusters = [_cluster(1, binds_after_polls=4)]
    fleet.reset(clusters)
    _day1(tmp_path, clusters)

    async with _Harness() as client:
        result = await _run(client, _input(cpu=8))

    assert result.root_disk_phase == "Succeeded"
    assert len(clusters[0].get_requests) == 4
    assert clusters[0].delete_requests == []
