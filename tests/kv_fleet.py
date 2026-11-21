"""A fleet of FAKE KV clusters for create-vm — dozens of them, each behaving like a cluster's API.

There are never dozens of real KubeVirt clusters to test placement on, so the
fleet is simulated where it matters and nowhere else: each `FakeKvCluster` is
an in-memory Kubernetes API with the surface of
activities/vm_lifecycle/kubevirt_api.KubevirtApi (nodes, pods, VirtualMachines,
VirtualMachineInstances, DataVolumes, PVCs, namespaces), and `KvFleet` gives
each one its OWN Temporal worker on its OWN queue, running the REAL limb code
(activities/vm_lifecycle/vm_operations.py: the figures arithmetic, the MAC
draws, the request-id lookup, the lease check, the ready wait, the delete).
The REAL create-vm and kv-zone-lock workflows run against it. What is faked
is only the cluster behind the API, and it behaves like one where the logic
depends on it:

  * a VirtualMachine created is THERE: the next figures read counts it as a
    stopped VM (which is what replaces any memory between runs), a second one
    of its name in its namespace is AlreadyExists, a MAC any VM holds is
    refused the way KubeMacPool refuses it, a missing namespace is a 404;
  * label selectors and the unfinished-pods filter select;
  * a root disk binds after a few polls (an Immediate storage class) or never
    (WaitForFirstConsumer), and a deleted VM and its DataVolume are gone;
  * any call can be made to fail (5xx, 403, a timeout) or to hang, a set number
    of times; a cluster's worker can be absent, stopped and started again while
    a run is going — and so can the hub's own three workers (the brain's for
    create-vm and for the zone locks, the day1 reader).

The same fleet can be attached to a REAL Temporal server next to one real
cluster's worker (`python -m tests.kv_fleet`, docs/lab/create-vm-per-kv-cluster/),
which is how a lab with one KV cluster sees a zone of many.
"""

from __future__ import annotations

import asyncio
import copy
import random
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from activities.vm_lifecycle import cluster_figures, vm_operations, vm_spec
from activities.vm_lifecycle.kubevirt_api import MacRefused, NameTaken, NotFound
from activities.vm_lifecycle.vm_operations import ClusterValues
from shared.cluster_names import parse_kv_cluster, seed_cluster
from shared.consts import (
    CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
    CREATE_VM_WORKFLOW_QUEUE,
    KV_ZONE_LOCK_WORKFLOW_QUEUE,
    vm_lifecycle_activity_queue,
)
from shared.models.vm_lifecycle import (
    KvClusterEntry,
    KvClusterFigures,
    KvClusterList,
    KvClusterListRequest,
    KvFiguresRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow
from workflow_domains.vm_lifecycle.kv_zone_lock import KvZoneLockWorkflow

GIB = 1024**3
REDFISH_LABEL = ("redfish-enabled", "true")


# --- raw objects, as the API server sends them -----------------------------------------------


def node(
    name: str,
    cpu: int | str,
    memory_gib: int | str,
    *,
    ready: bool = True,
    cordoned: bool = False,
    taints: list[dict] | None = None,
    labels: dict[str, str] | None = None,
) -> dict:
    """A Node: `cpu` cores and `memory_gib` GiB allocatable (or raw quantity strings)."""
    node_object: dict[str, Any] = {
        "metadata": {"name": name, "labels": dict(labels or {})},
        "spec": {},
        "status": {
            "allocatable": {
                "cpu": str(cpu),
                "memory": f"{memory_gib}Gi" if isinstance(memory_gib, int) else memory_gib,
            },
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }
    if cordoned:
        node_object["spec"]["unschedulable"] = True
    if taints:
        node_object["spec"]["taints"] = taints
    return node_object


def pod(
    name: str,
    node_name: str | None,
    cpu: str | None,
    memory: str | None,
    *,
    namespace: str = "default",
    phase: str = "Running",
    init: list[dict] | None = None,
    overhead: dict | None = None,
    containers: list[tuple[str | None, str | None]] | None = None,
) -> dict:
    """A Pod with one container requesting (cpu, memory) — or `containers`, several."""
    requested = containers if containers is not None else [(cpu, memory)]
    spec: dict[str, Any] = {
        "containers": [
            {"name": f"c{index}", "resources": {"requests": {k: v for k, v in (("cpu", c), ("memory", m)) if v is not None}}}
            for index, (c, m) in enumerate(requested)
        ]
    }
    if node_name:
        spec["nodeName"] = node_name
    if init:
        spec["initContainers"] = init
    if overhead:
        spec["overhead"] = overhead
    return {"metadata": {"name": name, "namespace": namespace}, "spec": spec, "status": {"phase": phase}}


def stopped_vm(
    name: str, cpu: int, memory_gib: int, *, namespace: str = "inventory", mac: str | None = None,
    labels: dict[str, str] | None = None, run_strategy: str = "Halted",
) -> dict:
    """A VirtualMachine as a person or another tool left it (not one of this fleet's creates)."""
    interface: dict[str, Any] = {"name": "default", "bridge": {}}
    if mac:
        interface["macAddress"] = mac
    return {
        "metadata": {
            "name": name, "namespace": namespace, "uid": str(uuid.uuid4()),
            "labels": {REDFISH_LABEL[0]: REDFISH_LABEL[1]} if labels is None else dict(labels),
        },
        "spec": {
            "runStrategy": run_strategy,
            "template": {"spec": {"domain": {
                "cpu": {"cores": cpu}, "memory": {"guest": f"{memory_gib}Gi"},
                "devices": {"interfaces": [interface]},
            }}},
        },
        "status": {"printableStatus": "Stopped"},
    }


def _matches(obj: dict, selector: str | None) -> bool:
    if not selector:
        return True
    labels = (obj.get("metadata") or {}).get("labels") or {}
    for part in selector.split(","):
        key, _, value = part.partition("=")
        if labels.get(key.strip()) != value.strip():
            return False
    return True


# --- one fake KV cluster -----------------------------------------------------------------------


class FakeKvCluster:
    """A KV cluster's API in memory: kubevirt_api.KubevirtApi's async surface, plus knobs.

    `storage` is how the cluster's VM storage class behaves: "immediate" (the
    root disk is Succeeded and Bound after `bind_after_polls` reads of its
    DataVolume) or "wffc" (it never binds while the VM is Halted).
    """

    def __init__(
        self,
        name: str,
        *,
        nodes: list[dict],
        mac_prefix: str = "0e:00:01",
        namespaces: tuple[str, ...] = ("inventory",),
        storage: str = "immediate",
        bind_after_polls: int = 1,
        kubemacpool: bool = True,
        seed: int | None = None,
        node_selector: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.values = ClusterValues(
            kv_cluster=name, site=parse_kv_cluster(name).site, mac_prefix=mac_prefix, storage_class_name="cephfs",
            redfish_label=REDFISH_LABEL, network_name="openshift-cnv/vm-external-network",
            node_selector=dict(node_selector or {}), root_disk_size="120Gi",
        )
        self.nodes: list[dict] = list(nodes)
        self.pods: list[dict] = []
        self.vms: dict[tuple[str, str], dict] = {}
        self.vmis: dict[tuple[str, str], dict] = {}
        self.datavolumes: dict[tuple[str, str], dict] = {}
        self.namespaces: set[str] = set(namespaces)
        self.storage = storage
        self.bind_after_polls = bind_after_polls
        self.kubemacpool = kubemacpool
        # What each next call of a method raises (popped front first), and how long it takes.
        self.failures: dict[str, list[BaseException]] = {}
        self.delays: dict[str, float] = {}
        self.calls: list[str] = []
        self.created: list[str] = []  # VM names, in creation order
        self.deleted: list[str] = []
        self.draws: list[bytes] = []  # MAC bytes the next draws return, before the random ones
        self._random = random.Random(seed if seed is not None else hash(name) & 0xFFFF)
        self._dv_polls: dict[tuple[str, str], int] = {}
        self._dv_storage: dict[tuple[str, str], str] = {}  # how each disk's class behaved when it was made

    # --- knobs ---------------------------------------------------------------------------

    def fail(self, method: str, *errors: BaseException) -> None:
        """The next calls of `method` raise these, one each, in order."""
        self.failures.setdefault(method, []).extend(errors)

    def random_bytes(self, count: int) -> bytes:
        if self.draws:
            return self.draws.pop(0)
        return self._random.randbytes(count)

    def add_pod(self, node_name: str, cpu_millicores: int, memory_bytes: int, *, name: str | None = None) -> dict:
        """A running pod on a node, requesting exactly this much."""
        created = pod(name or f"load-{len(self.pods)}", node_name, f"{cpu_millicores}m", str(memory_bytes))
        self.pods.append(created)
        return created

    def add_vm(self, vm: dict) -> dict:
        """A VirtualMachine that is simply there (someone else's, or left from before)."""
        self.vms[(vm_spec.namespace_of(vm), vm_spec.name_of(vm))] = vm
        return vm

    def start(self, namespace: str, name: str, node_name: str, *, phase: str = "Running") -> None:
        """Start a VM as kubevirt-redfish would: a VMI, and its launcher pod on a node,
        requesting what the VM's template asks plus KubeVirt's real overhead (well under 1 GiB)."""
        vm = self.vms[(namespace, name)]
        cpu, memory = cluster_figures.stopped_vm_request(vm)
        self.vmis[(namespace, name)] = {"metadata": {"name": name, "namespace": namespace}, "status": {"phase": phase}}
        if phase in {"Scheduled", "Running", "Unknown"}:
            self.pods.append(pod(f"virt-launcher-{name}", node_name, f"{cpu}m", str(memory - GIB + 300 * 1024**2),
                                 namespace=namespace))

    @property
    def vm_names(self) -> list[str]:
        return sorted(name for _, name in self.vms)

    def macs(self) -> list[str]:
        return sorted(mac for vm in self.vms.values() for mac in vm_spec.macs(vm))

    # --- the API surface -------------------------------------------------------------------

    async def _enter(self, method: str) -> None:
        self.calls.append(method)
        delay = self.delays.get(method)
        if delay:
            await asyncio.sleep(delay)
        queued = self.failures.get(method)
        if queued:
            raise queued.pop(0)

    async def list_nodes(self) -> list[dict]:
        await self._enter("list_nodes")
        return copy.deepcopy(self.nodes)

    async def list_unfinished_pods(self, keep: Callable[[dict], Any]) -> list[Any]:
        await self._enter("list_unfinished_pods")
        kept = [keep(p) for p in self.pods if (p.get("status") or {}).get("phase") not in {"Succeeded", "Failed"}]
        return [item for item in kept if item is not None]

    async def list_vms(self, label_selector: str | None = None) -> list[dict]:
        await self._enter("list_vms")
        return [copy.deepcopy(vm) for vm in self.vms.values() if _matches(vm, label_selector)]

    async def list_vmis(self) -> list[dict]:
        await self._enter("list_vmis")
        return copy.deepcopy(list(self.vmis.values()))

    async def namespace_exists(self, namespace: str) -> bool:
        await self._enter("namespace_exists")
        return namespace in self.namespaces

    async def get_vm(self, namespace: str, name: str) -> dict | None:
        await self._enter("get_vm")
        vm = self.vms.get((namespace, name))
        return copy.deepcopy(vm) if vm else None

    async def create_vm(self, namespace: str, body: dict) -> dict:
        await self._enter("create_vm")
        name = vm_spec.name_of(body)
        if namespace not in self.namespaces:
            raise NotFound(f'namespaces "{namespace}" not found')
        if (namespace, name) in self.vms:
            raise NameTaken(f'virtualmachines.kubevirt.io "{name}" already exists')
        if self.kubemacpool:
            held = {mac for vm in self.vms.values() for mac in vm_spec.macs(vm)}
            clash = [mac for mac in vm_spec.macs(body) if mac in held]
            if clash:
                raise MacRefused(f"admission webhook denied the request: Failed to allocate mac to the vm object: failed to allocate requested mac address {clash[0]}")
        stored = copy.deepcopy(body)
        stored["metadata"]["uid"] = str(uuid.uuid4())
        stored["metadata"]["creationTimestamp"] = datetime.now(UTC).isoformat()
        stored["status"] = {"printableStatus": "Provisioning" if self.storage == "immediate" and self.bind_after_polls > 0 else "Stopped"}
        self.vms[(namespace, name)] = stored
        root = vm_spec.root_disk_name(stored)
        if root:
            self.datavolumes[(namespace, root)] = {
                "metadata": {"name": root, "namespace": namespace},
                "status": {"phase": "WaitForFirstConsumer" if self.storage == "wffc" else "Pending"},
            }
            self._dv_polls[(namespace, root)] = 0
            self._dv_storage[(namespace, root)] = self.storage
        self.created.append(name)
        return copy.deepcopy(stored)

    async def delete_vm(self, namespace: str, name: str, uid: str | None = None) -> bool:
        await self._enter("delete_vm")
        vm = self.vms.get((namespace, name))
        if vm is None or (uid is not None and vm_spec.uid_of(vm) != uid):
            return False
        del self.vms[(namespace, name)]
        self.vmis.pop((namespace, name), None)
        self.deleted.append(name)
        return True

    async def get_datavolume(self, namespace: str, name: str) -> dict | None:
        await self._enter("get_datavolume")
        datavolume = self.datavolumes.get((namespace, name))
        if datavolume is None:
            return None
        self._dv_polls[(namespace, name)] = self._dv_polls.get((namespace, name), 0) + 1
        if self._dv_storage.get((namespace, name), self.storage) == "immediate" and self._dv_polls[(namespace, name)] >= self.bind_after_polls:
            datavolume["status"]["phase"] = "Succeeded"
            owner = name.removesuffix("-root")
            if (namespace, owner) in self.vms:
                self.vms[(namespace, owner)]["status"]["printableStatus"] = "Stopped"
        return copy.deepcopy(datavolume)

    async def delete_datavolume(self, namespace: str, name: str) -> bool:
        await self._enter("delete_datavolume")
        return self.datavolumes.pop((namespace, name), None) is not None

    async def pvc_phase(self, namespace: str, name: str) -> str | None:
        await self._enter("pvc_phase")
        datavolume = self.datavolumes.get((namespace, name))
        if datavolume is None:
            return None
        return "Bound" if datavolume["status"]["phase"] == "Succeeded" else "Pending"

    # --- conveniences for assertions -----------------------------------------------------------

    async def figures(self) -> KvClusterFigures:
        """This cluster's figures, through the real read (recorded calls and failures aside)."""
        saved_calls, saved_failures = list(self.calls), self.failures
        self.failures = {}
        try:
            return await vm_operations.read_figures(self, self.values, KvFiguresRequest(kv_cluster=self.name))
        finally:
            self.calls, self.failures = saved_calls, saved_failures

    async def requested_cpu_millicores(self) -> int:
        figures = await self.figures()
        return figures.pods_cpu_millicores + figures.stopped_vms_cpu_millicores


# --- the fleet ---------------------------------------------------------------------------------


class KvFleet:
    """Sites and zones of fake KV clusters, the day1 list that names them, and their workers."""

    def __init__(self, *, buffer_percent: int = 15, revision: str = "0123abc") -> None:
        self.buffer_percent = buffer_percent
        self.revision = revision
        self.clusters: dict[str, FakeKvCluster] = {}
        # What day1 lists per (site, zone), in the order added; sorted by id when answered.
        self.day1: dict[tuple[str, str], list[KvClusterEntry]] = {}
        self.listed: list[KvClusterListRequest] = []
        self.list_failures: list[BaseException] = []
        # Seconds the fleet's clusters' clocks run ahead of this process's (a lease-expiry knob).
        self.skew = timedelta(0)
        self.poll_seconds = 0.0  # how long the ready wait really sleeps between polls
        self.graceful_shutdown_seconds = 1.0
        # False: the brain's workers keep no run in memory, so EVERY workflow task replays the
        # run from its history — what a restarted brain does once — and a run is never "sticky"
        # to a worker that is gone (the time-skipping test server never lets go of one).
        self.cache_workflows = True
        self.waited: dict[tuple[str, str, str], float] = {}  # seconds each VM's ready wait has run, across attempts
        self._workers: dict[str, Worker] = {}
        self._client: Client | None = None
        self._runner = None

    def now(self) -> datetime:
        return datetime.now(UTC) + self.skew

    def add(
        self,
        name: str,
        zone: str,
        *,
        nodes: list[dict] | None = None,
        in_day1: bool = True,
        problems: list[str] | None = None,
        **cluster_kwargs: Any,
    ) -> FakeKvCluster:
        """A KV cluster of `zone`: its fake API, and (unless `in_day1` is False) its day1 entry."""
        parsed = parse_kv_cluster(name)
        prefix = cluster_kwargs.pop("mac_prefix", f"0e:{len(self.clusters) // 256:02x}:{len(self.clusters) % 256:02x}")
        cluster = FakeKvCluster(
            name, nodes=nodes if nodes is not None else [node(f"{name}-n{i}", 100, 1024) for i in range(1)],
            mac_prefix=prefix, **cluster_kwargs,
        )
        self.clusters[name] = cluster
        if in_day1:
            self.list_in_day1(name, parsed.site, zone, problems=problems)
        return cluster

    def list_in_day1(self, name: str, site: str, zone: str, *, problems: list[str] | None = None,
                     confirmed_zone: str | None = "same", identifier: str | None = "from-name") -> KvClusterEntry:
        """A day1 entry in the seed folder of (site, zone), with or without a cluster behind it."""
        folder = f"sites/{site}/mces/{seed_cluster(site, zone)}/hostedClusters"
        try:
            parsed_id: str | None = parse_kv_cluster(name).id
        except ValueError:
            parsed_id = None
        entry = KvClusterEntry(
            name=name, cluster_name=name.removeprefix("ocp4-"), hosted_cluster=f"openshift-{name.removeprefix('ocp4-')}",
            path=f"{folder}/{name}.yaml", zone=zone if confirmed_zone == "same" else confirmed_zone,
            id=parsed_id if identifier == "from-name" else identifier, problems=list(problems or []),
        )
        self.day1.setdefault((site, zone), []).append(entry)
        return entry

    def entries(self, site: str, zone: str) -> list[KvClusterEntry]:
        """What day1 answers for the zone: oldest first (by id), entries without an id last."""
        listed = self.day1.get((site, zone), [])
        return sorted(listed, key=lambda entry: (entry.id is None, entry.id or "", entry.name))

    # --- the activities ----------------------------------------------------------------------

    def list_activity(self):  # noqa: ANN201 — an @activity.defn function
        fleet = self

        @activity.defn(name="list_kv_clusters")
        async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
            fleet.listed.append(request)
            if fleet.list_failures:
                raise fleet.list_failures.pop(0)
            folder = f"sites/{request.site}/mces/{seed_cluster(request.site, request.zone)}/hostedClusters/"
            return KvClusterList(
                clusters=fleet.entries(request.site, request.zone), buffer_percent=fleet.buffer_percent,
                revision=fleet.revision, folder=folder,
            )

        return list_kv_clusters

    def cluster_activities(self, cluster: FakeKvCluster) -> list:
        """The four vm-lifecycle activities, bound to ONE fake cluster — the bodies are the
        real limb's (vm_operations), exactly as activities/vm_lifecycle/activities.py binds them."""
        fleet = self

        @activity.defn(name="read_kv_cluster_figures")
        async def read_kv_cluster_figures(request: KvFiguresRequest) -> KvClusterFigures:
            return await vm_operations.read_figures(cluster, cluster.values, request)

        @activity.defn(name="create_vm_on_cluster")
        async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
            return await vm_operations.create_vm(
                cluster, cluster.values, request, now=fleet.now, random_bytes=cluster.random_bytes
            )

        @activity.defn(name="await_vm_ready")
        async def await_vm_ready(request: VmReadyRequest) -> VmReadyOutcome:
            details = activity.info().heartbeat_details
            started_at, polls = datetime.now(UTC), 0
            if details and isinstance(details[0], dict):
                started_at, polls = datetime.fromisoformat(details[0]["started_at"]), int(details[0].get("polls", 0))
            # The wait's own clock: every poll's sleep moves it on at once, so a
            # ten-minute deadline takes seventy-five polls, not ten minutes. It is the
            # CLUSTER's clock, not the attempt's: a wait resumed on a restarted worker
            # finds the time that has already passed, as it would on a wall clock.
            key = (cluster.name, request.namespace, request.name)
            fleet.waited.setdefault(key, 0.0)

            async def sleep(seconds: float) -> None:
                fleet.waited[key] += seconds
                await asyncio.sleep(fleet.poll_seconds)

            return await vm_operations.wait_ready(
                cluster, cluster.values, request, started_at=started_at, polls=polls,
                now=lambda: datetime.now(UTC) + timedelta(seconds=fleet.waited[key]),
                heartbeat=lambda started, count: activity.heartbeat({"started_at": started.isoformat(), "polls": count}),
                sleep=sleep,
            )

        @activity.defn(name="delete_vm_on_cluster")
        async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
            return await vm_operations.delete_vm(cluster, cluster.values, request)

        return [read_kv_cluster_figures, create_vm_on_cluster, await_vm_ready, delete_vm_on_cluster]

    # --- the workers ---------------------------------------------------------------------------

    # The hub's three workers, by the name tests take them away under.
    CREATE_VM = "create-vm"      # the brain's worker for the create-vm workflow
    ZONE_LOCK = "kv-zone-lock"   # the brain's worker for the zone locks
    DAY1 = "day1"                # the cluster-lifecycle worker (list_kv_clusters)

    @asynccontextmanager
    async def running(
        self, client: Client, *, down: set[str] | None = None, brain: bool = True, workflow_runner: Any = None,
        list_worker: bool = True, lock_worker: bool = True,
    ) -> AsyncIterator[KvFleet]:
        """The brain's two workflow workers, the cluster-lifecycle queue's `list_kv_clusters`,
        and one worker per cluster (none for those in `down`), all in this process. Any of
        them can be stopped and started again while runs are going (stop_worker / start_worker)."""
        self._client, self._runner = client, workflow_runner
        try:
            if brain:
                await self.start_worker(self.CREATE_VM)
                if lock_worker:
                    await self.start_worker(self.ZONE_LOCK)
            if list_worker:
                await self.start_worker(self.DAY1)
            for name in self.clusters:
                if name not in (down or set()):
                    await self.start_worker(name)
            yield self
        finally:
            for name in list(self._workers):
                await self.stop_worker(name)

    def _worker(self, name: str) -> Worker:
        assert self._client is not None
        runner: dict[str, Any] = {"workflow_runner": self._runner} if self._runner is not None else {}
        if not self.cache_workflows:
            runner["max_cached_workflows"] = 0
        if name == self.CREATE_VM:
            return Worker(self._client, task_queue=CREATE_VM_WORKFLOW_QUEUE, workflows=[CreateVmWorkflow], **runner)
        if name == self.ZONE_LOCK:
            return Worker(self._client, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE, workflows=[KvZoneLockWorkflow], **runner)
        if name == self.DAY1:
            return Worker(self._client, task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE, activities=[self.list_activity()])
        return Worker(
            self._client, task_queue=vm_lifecycle_activity_queue(name),
            activities=self.cluster_activities(self.clusters[name]),
            graceful_shutdown_timeout=timedelta(seconds=self.graceful_shutdown_seconds),
        )

    async def start_worker(self, name: str) -> None:
        """Bring a worker up: a cluster's (by the cluster's name), or one of the hub's three."""
        if name in self._workers or self._client is None:
            return
        worker = self._worker(name)
        await worker.__aenter__()
        self._workers[name] = worker

    async def stop_worker(self, name: str) -> None:
        """Take a worker away: its queue is then polled by nobody. What it was running is cut
        off after `graceful_shutdown_seconds`, as a pod is at the end of its grace period."""
        worker = self._workers.pop(name, None)
        if worker is not None:
            await worker.__aexit__(None, None, None)

    async def restart_worker(self, name: str) -> None:
        await self.stop_worker(name)
        await self.start_worker(name)

    async def start_lock_worker(self) -> None:
        await self.start_worker(self.ZONE_LOCK)


# --- attach the fleet to a real Temporal server (the lab) ---------------------------------------


async def _main() -> None:  # pragma: no cover — a lab tool
    import argparse
    import logging

    from temporalio.contrib.pydantic import pydantic_data_converter

    parser = argparse.ArgumentParser(description="Poll the queues of fake KV clusters on a real Temporal server.")
    parser.add_argument("--temporal", default="localhost:7233")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--cluster", action="append", default=[], metavar="NAME:CORES:MEMORY_GIB[:NODES]",
                        help="a fake KV cluster: its day1 name, each node's cores and GiB, how many nodes (default 1)")
    parser.add_argument("--load", action="append", default=[], metavar="NAME:CORES:MEMORY_GIB",
                        help="requests already on a cluster's first node")
    parser.add_argument("--wffc", action="append", default=[], metavar="NAME", help="a cluster whose root disks never bind")
    parser.add_argument("--no-namespace", action="append", default=[], metavar="NAME", help="a cluster without the VM namespace")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    fleet = KvFleet()
    for spec in args.cluster:
        name, cores, memory, *rest = spec.split(":")
        count = int(rest[0]) if rest else 1
        fleet.add(
            name, "az1", in_day1=False, nodes=[node(f"{name}-n{i}", int(cores), int(memory)) for i in range(count)],
            storage="wffc" if name in args.wffc else "immediate",
            namespaces=() if name in args.no_namespace else ("inventory",),
        )
    for spec in args.load:
        name, cores, memory = spec.split(":")
        fleet.clusters[name].add_pod(fleet.clusters[name].nodes[0]["metadata"]["name"], int(cores) * 1000, int(memory) * GIB)
    client = await Client.connect(args.temporal, namespace=args.namespace, data_converter=pydantic_data_converter)
    async with fleet.running(client, brain=False, list_worker=False):
        logging.info("fake KV clusters polling: %s", ", ".join(vm_lifecycle_activity_queue(n) for n in fleet.clusters))
        while True:
            await asyncio.sleep(30)
            for name, cluster in fleet.clusters.items():
                figures = await cluster.figures()
                logging.info("%s: %d VMs, requested %d m of %d m", name, len(cluster.vms),
                             figures.pods_cpu_millicores + figures.stopped_vms_cpu_millicores,
                             figures.allocatable_cpu_millicores)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_main())
