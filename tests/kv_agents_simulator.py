"""A FLEET of simulated KubeVirt clusters, each with the two agents' surfaces, in one process.

tests/test_create_vm_e2e.py drives the REAL CreateVmWorkflow and the REAL
vm-lifecycle limb (its httpx clients, its classification, its heartbeating
wait) against this, so what it proves is that the workflow, the limb and the
agents' wire contract agree — over as many clusters, in as many states, as
one test wants. The agents' own FastAPI apps are NOT imported: each is a
module singleton configured by its environment (one token, one cluster), so
one process cannot host twelve of them; the wire contract they share is
pinned by tests/test_kv_agents_contract.py, and this module answers in the
shapes of shared/models/vm_lifecycle.py's contract section only.

Every cluster is a SimCluster: a node table, the VMs it holds (ours and
foreign), and HOW ITS AGENTS BEHAVE — `capacity` and `vm_api` modes (ok,
no-agent, refused, malformed, down, slow), a root disk that binds after N
polls or never, names already taken, an answer lost after the create, a VM
replaced behind the run's back. The fleet is addressed as
`http://127.0.0.1:<port>/clusters/{cluster}/v1/...`, so the limb's URL
templates render with `{cluster}` as in production; a cluster day1 lists
but the fleet does not hold answers 404, which the limb reads as
`no-agent` — the same as the router's 503 for a Route with no endpoints.

Semantics mirror the agents (kv-agents: kv_capacity/capacity.py,
kv_vm_api/app.py): a Halted, labelled VM with no launcher is a reservation
sized as its template requests it (CPU `max(1, cpu × 15 // 16)` cores,
memory the GUEST's — the launcher pad is placement's, not the agent's); a
PUT is idempotent by name AND identity (200 for the same namespace, MAC,
size and labels, 409 for another); a DELETE wants the caller's MAC.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

GIB = 1024**3
VM_LABEL = {"redfish-enabled": "true"}
CAPACITY_PATH = "/clusters/{cluster}/v1/capacity"
VMS_PATH = "/clusters/{cluster}/v1/vms/{name}"

CapacityMode = str  # ok | no-agent | refused | malformed | down | slow
VmApiMode = str  # ok | refused | down | reject


@dataclass
class SimNode:
    name: str
    cores: float
    gib: float
    used_cores: float = 0
    used_gib: float = 0
    schedulable: bool = True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "schedulable": self.schedulable,
            "allocatable_cpu_millicores": int(self.cores * 1000),
            "allocatable_memory_bytes": int(self.gib * GIB),
            "requested_cpu_millicores": int(self.used_cores * 1000),
            "requested_memory_bytes": int(self.used_gib * GIB),
        }


@dataclass
class SimVm:
    namespace: str
    name: str
    mac: str | None
    cpu: int
    memory_gib: int
    labels: dict[str, str] = field(default_factory=dict)
    run_strategy: str = "Halted"
    has_launcher: bool = False
    polls: int = 0  # GETs the agent answered for it: the root disk binds after N of them

    def reservation(self) -> dict:
        """What vm.yaml.j2 makes the launcher request: CPU floored at one core, memory the guest's."""
        return {
            "namespace": self.namespace,
            "name": self.name,
            "cpu_millicores": max(1, self.cpu * 15 // 16) * 1000,
            "memory_bytes": self.memory_gib * GIB,
        }

    def identity(self) -> dict:
        return {"namespace": self.namespace, "name": self.name, "mac": self.mac}


@dataclass
class SimCluster:
    """One KubeVirt cluster: its facts, and how its two agents answer."""

    name: str
    nodes: list[SimNode] = field(default_factory=list)
    vms: dict[str, SimVm] = field(default_factory=dict)  # "<namespace>/<name>"
    namespaces: set[str] = field(default_factory=lambda: {"inventory"})
    capacity: CapacityMode = "ok"
    vm_api: VmApiMode = "ok"
    # Capacity answers 500 for this many requests, then as `capacity` says.
    capacity_down_for: int = 0
    slow_seconds: float = 2.0
    # The root disk is Succeeded + bound from this GET on; None = never.
    binds_after_polls: int | None = 1
    # Before this many creates of NEW names, the name is found held by a foreign VM (409).
    names_taken_before: int = 0
    # This many creates land and then answer 500: a lost answer the retry must find.
    lost_answers: int = 0
    # After the create, the VM is replaced by one with another MAC (never ours again).
    replace_after_put: bool = False
    # What happened, for the assertions.
    capacity_requests: int = 0
    put_requests: list[dict] = field(default_factory=list)
    get_requests: list[str] = field(default_factory=list)
    delete_requests: list[dict] = field(default_factory=list)

    def add_vm(self, vm: SimVm) -> SimVm:
        self.vms[f"{vm.namespace}/{vm.name}"] = vm
        return vm

    def our_vms(self) -> list[SimVm]:
        return [vm for vm in self.vms.values() if vm.labels.get("redfish-enabled") == "true" and vm.mac and vm.mac.startswith("02:a1:b2")]

    def report(self) -> dict:
        halted = [
            vm.reservation()
            for vm in self.vms.values()
            if vm.run_strategy == "Halted" and not vm.has_launcher
            and all(vm.labels.get(k) == v for k, v in VM_LABEL.items())
        ]
        return {
            "cluster": self.name,
            "observed_at": "2026-11-05T12:00:00Z",
            "nodes": [node.to_dict() for node in self.nodes],
            "halted_reservations": halted,
            "vms": [vm.identity() for vm in self.vms.values()],
        }


@dataclass
class Fleet:
    clusters: dict[str, SimCluster] = field(default_factory=dict)
    capacity_token: str = "fleet-capacity-token"
    vm_token: str = "fleet-vm-token"

    def reset(self, clusters: list[SimCluster]) -> None:
        self.clusters = {cluster.name: cluster for cluster in clusters}

    def all_vms(self) -> list[tuple[str, SimVm]]:
        return [(cluster.name, vm) for cluster in self.clusters.values() for vm in cluster.vms.values()]


def _error(status: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail})


def _bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" else ""


def _state(vm: SimVm, cluster: SimCluster) -> dict:
    bound = cluster.binds_after_polls is not None and vm.polls >= cluster.binds_after_polls
    return {
        "name": vm.name, "namespace": vm.namespace, "mac": vm.mac, "cpu": vm.cpu, "memory_gib": vm.memory_gib,
        "run_strategy": vm.run_strategy, "admitted": True,
        "root_disk": {"name": f"{vm.name}-root", "phase": "Succeeded" if bound else "Pending", "bound": bound},
    }


def _conflict(reason: str, existing: SimVm | None, cluster: SimCluster) -> JSONResponse:
    return JSONResponse(
        status_code=409, content={"reason": reason, "existing": _state(existing, cluster) if existing else None}
    )


def _same_identity(vm: SimVm, request: dict) -> str | None:
    if vm.namespace != request["namespace"]:
        return f"VM {vm.name} is in namespace {vm.namespace}, not {request['namespace']}"
    if vm.mac != request["mac"]:
        return f"VM {vm.name} has mac {vm.mac}, not {request['mac']}"
    if vm.cpu != request["cpu"]:
        return f"VM {vm.name} has {vm.cpu} vCPU, not {request['cpu']}"
    if vm.memory_gib != request["memory_gib"]:
        return f"VM {vm.name} has {vm.memory_gib} GiB, not {request['memory_gib']}"
    for key, value in {**VM_LABEL, **request.get("labels", {})}.items():
        if vm.labels.get(key) != value:
            return f"VM {vm.name} has label {key}={vm.labels.get(key)!r}, not {value!r}"
    return None


def fleet_app(fleet: Fleet) -> FastAPI:
    app = FastAPI(title="kv-agents fleet simulator")

    @app.get(CAPACITY_PATH)
    async def capacity(cluster: str, request: Request) -> Response:
        sim = fleet.clusters.get(cluster)
        if sim is None:
            return _error(404, "not-found", f"no agents on {cluster}")
        sim.capacity_requests += 1
        if sim.capacity == "no-agent":
            return Response(status_code=503, content="<html>Application is not available</html>")
        if sim.capacity == "refused" or _bearer(request) != fleet.capacity_token:
            return _error(401, "unauthorized", "a valid bearer token is required")
        if sim.capacity_down_for > 0:
            sim.capacity_down_for -= 1
            return _error(500, "internal", "the apiserver is not answering")
        if sim.capacity == "down":
            return _error(500, "internal", "the apiserver is not answering")
        if sim.capacity == "malformed":
            return JSONResponse({"hello": "this is not a capacity report"})
        if sim.capacity == "slow":
            await asyncio.sleep(sim.slow_seconds)
        return JSONResponse(sim.report())

    def _vm_api_gate(sim: SimCluster | None, request: Request) -> Response | None:
        if sim is None:
            return _error(404, "not-found", "no agents here")
        if sim.vm_api == "refused" or _bearer(request) != fleet.vm_token:
            return _error(401, "unauthorized", "a valid bearer token is required")
        if sim.vm_api == "down":
            return _error(500, "internal", "the apiserver is not answering")
        return None

    @app.put(VMS_PATH)
    async def put_vm(cluster: str, name: str, request: Request) -> Response:
        sim = fleet.clusters.get(cluster)
        gate = _vm_api_gate(sim, request)
        if gate is not None:
            return gate
        assert sim is not None
        body = await request.json()
        body["mac"] = body["mac"].lower()
        sim.put_requests.append({"name": name, **body})
        if sim.vm_api == "reject":
            return _error(422, "invalid", "vm.yaml.j2 cannot render this request: memory must be a multiple of 1Gi")
        key = f"{body['namespace']}/{name}"
        existing = sim.vms.get(key)
        if existing is None and body["namespace"] not in sim.namespaces:
            return _error(404, "namespace-not-found", f"namespace {body['namespace']} does not exist on this cluster")
        if existing is None and sim.names_taken_before > 0:
            # Born in between: a foreign VM holds this name by the time the create runs.
            sim.names_taken_before -= 1
            existing = sim.add_vm(SimVm(body["namespace"], name, "02:ff:ff:00:00:01", body["cpu"], body["memory_gib"], labels={}))
        if existing is None:
            vm = sim.add_vm(SimVm(body["namespace"], name, body["mac"], body["cpu"], body["memory_gib"],
                                  labels={**VM_LABEL, **body.get("labels", {})}))
            if sim.lost_answers > 0:
                sim.lost_answers -= 1
                return _error(500, "internal", "the answer was lost after the create landed")
            if sim.replace_after_put:
                vm.mac = "02:a1:b2:de:ad:00"  # someone replaced it with another identity
                sim.replace_after_put = False
            return JSONResponse(status_code=201, content={
                "name": name, "namespace": body["namespace"], "mac": body["mac"], "cpu": body["cpu"],
                "memory_gib": body["memory_gib"], "created": True,
            })
        why = _same_identity(existing, body)
        if why is not None:
            return _conflict(why, existing, sim)
        return JSONResponse(status_code=200, content={
            "name": name, "namespace": body["namespace"], "mac": body["mac"], "cpu": body["cpu"],
            "memory_gib": body["memory_gib"], "created": False,
        })

    @app.get(VMS_PATH)
    async def get_vm(cluster: str, name: str, namespace: str, request: Request) -> Response:
        sim = fleet.clusters.get(cluster)
        gate = _vm_api_gate(sim, request)
        if gate is not None:
            return gate
        assert sim is not None
        sim.get_requests.append(f"{namespace}/{name}")
        vm = sim.vms.get(f"{namespace}/{name}")
        if vm is None:
            return _error(404, "not-found", f"no VM {namespace}/{name}")
        vm.polls += 1
        return JSONResponse(_state(vm, sim))

    @app.delete(VMS_PATH)
    async def delete_vm(cluster: str, name: str, namespace: str, mac: str, request: Request) -> Response:
        sim = fleet.clusters.get(cluster)
        gate = _vm_api_gate(sim, request)
        if gate is not None:
            return gate
        assert sim is not None
        sim.delete_requests.append({"name": name, "namespace": namespace, "mac": mac})
        vm = sim.vms.get(f"{namespace}/{name}")
        if vm is None:
            return _error(404, "not-found", f"no VM {namespace}/{name}")
        if vm.mac != mac.lower():
            return _conflict(f"VM {namespace}/{name} has mac {vm.mac}, not {mac.lower()}: not the caller's VM", vm, sim)
        del sim.vms[f"{namespace}/{name}"]
        return Response(status_code=204)

    return app
