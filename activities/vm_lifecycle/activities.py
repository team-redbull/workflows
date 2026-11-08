"""VM-lifecycle activities — the domain's one hub limb (`vm-lifecycle-worker`). Thin.

Polls VM_LIFECYCLE_ACTIVITY_QUEUE on the hub. Holds no Kubernetes credential:
each KubeVirt cluster is reached through its two agents behind Routes
(kv_capacity_api.py, kv_vm_api.py), one fleet bearer token each, and the
zone's cluster list comes from the day1 repo (day1_repo.py, on the monorepo's
shared reader activities/day1_repo.py) with the day1 git token. Each activity reads its settings and
hands plain parameters to one module per dependency; placement.py decides
where a VM goes.

Budgets, all under the workflow's timeouts (workflow_domains/vm_lifecycle/create_vm.py):
  * one HTTP call to kv-vm-api: 30 s, under the 90 s start_to_close;
  * the capacity fan-out: 10 s per cluster, 8 at a time, 45 s for the whole,
    under the 60 s start_to_close — a cluster the budget cuts off is
    `unreachable` in the table, never an exception;
  * the ready wait: one heartbeating activity polling every 8 s (the
    operator, 2026-11-05), its clock in the heartbeat details so a retry on
    another worker resumes it, `deadline` on its own clock, under the
    workflow's 12 min start_to_close and 2 min heartbeat timeout.
"""

from __future__ import annotations

import asyncio
import ssl
from datetime import UTC, datetime

from temporalio import activity

from activities.vm_lifecycle import day1_repo, kv_capacity_api, kv_vm_api, placement
from activities.vm_lifecycle.kv_capacity_api import KvProbeFailure
from shared.exceptions import KvAgentAuthError, KvCapacityUnavailableError, VmDisappearedError
from shared.models.vm_lifecycle import (
    KvCapacityReport,
    KvClusterList,
    KvClusterListRequest,
    KvClusterRef,
    KvPlacement,
    KvPlacementRequest,
    KvVmCreateRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)
from shared.settings import VmLifecycleActivitySettings

_settings = VmLifecycleActivitySettings()

HTTP_TIMEOUT_SECONDS = 30.0
PROBE_TIMEOUT_SECONDS = 10.0
FANOUT_TIMEOUT_SECONDS = 45.0
FANOUT_PARALLEL = 8
READY_POLL_SECONDS = 8.0
# The root disk is `done` in this DataVolume phase with its PVC bound.
_ROOT_DISK_READY_PHASE = "Succeeded"


def tls_verify(ca_path: str) -> ssl.SSLContext | bool:
    """What httpx verifies the agents' certificates against: the CA file, or NOTHING when
    none is configured — the operator's decision (shared/settings.py, kv_agent_ca)."""
    return ssl.create_default_context(cafile=ca_path) if ca_path else False


_VERIFY = tls_verify(_settings.kv_agent_ca)


def mac_for(suffix_hex: str) -> str:
    """`VM_MAC_PREFIX:xx:xx:xx` from the run's six hex digits — the one place a MAC is composed."""
    return f"{_settings.vm_mac_prefix}:{suffix_hex[0:2]}:{suffix_hex[2:4]}:{suffix_hex[4:6]}"


def _host(url: str) -> str:
    return url.split("://", 1)[-1].split("/", 1)[0]


# --- list_kv_clusters ----------------------------------------------------------------


@activity.defn
async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
    clusters = await day1_repo.list_kv_clusters(
        repo_url=_settings.day1_repo_url, token=_settings.day1_git_token, site=request.site, zone=request.zone
    )
    activity.logger.info(
        "day1 %s lists %d KubeVirt clusters for site %s zone %s: %s",
        clusters.revision, len(clusters.clusters), request.site, request.zone,
        ", ".join(entry.name for entry in clusters.clusters) or "none",
    )
    return clusters


# --- rank_kv_clusters ----------------------------------------------------------------


async def _probe(ref: KvClusterRef, gate: asyncio.Semaphore) -> KvCapacityReport | KvProbeFailure:
    async with gate:
        return await kv_capacity_api.read_capacity(
            _settings.kv_capacity_url(ref), _settings.kv_capacity_token, verify=_VERIFY, timeout=PROBE_TIMEOUT_SECONDS
        )


async def probe_all(candidates: list[KvClusterRef]) -> list[KvCapacityReport | KvProbeFailure]:
    """Every candidate's report or failure, in the candidates' order, within FANOUT_TIMEOUT_SECONDS."""
    gate = asyncio.Semaphore(FANOUT_PARALLEL)
    tasks = [asyncio.create_task(_probe(ref, gate)) for ref in candidates]
    try:
        await asyncio.wait(tasks, timeout=FANOUT_TIMEOUT_SECONDS)
    finally:
        for task in tasks:
            task.cancel()
    outcomes: list[KvCapacityReport | KvProbeFailure] = []
    for task in tasks:
        if task.done() and not task.cancelled():
            outcomes.append(task.result())
        else:
            outcomes.append(KvProbeFailure("unreachable", f"not answered within the fan-out's {FANOUT_TIMEOUT_SECONDS:g}s"))
    return outcomes


@activity.defn
async def rank_kv_clusters(request: KvPlacementRequest) -> KvPlacement:
    candidates = [
        ref.model_copy(update={"agent_host": _host(_settings.kv_capacity_url(ref))}) for ref in request.candidates
    ]
    outcomes = await probe_all(candidates)
    for ref, outcome in zip(candidates, outcomes, strict=True):
        if isinstance(outcome, KvProbeFailure):
            activity.logger.info("kv-capacity %s (%s): %s — %s", ref.name, ref.agent_host, outcome.reason, outcome.detail)
        else:
            activity.logger.info(
                "kv-capacity %s (%s): %d nodes, %d halted reservations, %d VMs",
                ref.name, ref.agent_host, len(outcome.nodes), len(outcome.halted_reservations), len(outcome.vms),
            )
    failures = [outcome for outcome in outcomes if isinstance(outcome, KvProbeFailure)]
    if candidates and len(failures) == len(outcomes):
        reasons = {failure.reason for failure in failures}
        said = "; ".join(f"{ref.name}: {failure.detail}" for ref, failure in zip(candidates, failures, strict=True))
        if reasons == {"refused-token"}:
            raise KvAgentAuthError(f"every KubeVirt cluster of the zone refused the kv-capacity token: {said}")
        if reasons <= {"unreachable", "no-agent"}:
            raise KvCapacityUnavailableError(f"no KubeVirt cluster of the zone could be read: {said}")
    table = placement.rank(
        candidates, outcomes, cpu=request.cpu, memory_gib=request.memory_gib,
        buffer_percent=_settings.headroom_buffer_percent,
    )
    activity.logger.info(
        "placement for %d vCPU / %d GiB (pod %d m / %d GiB, buffer %d%%): chosen %s; %s",
        request.cpu, request.memory_gib, table.pod_cpu_millicores, table.pod_memory_bytes // placement.GIB,
        table.buffer_percent, table.chosen or "none",
        ", ".join(f"{row.name}={'fits' if row.fits else row.skip_reason}" for row in table.ranked),
    )
    return table


# --- create_vm_on_cluster --------------------------------------------------------------


@activity.defn
async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
    mac = mac_for(request.suffix_hex)
    created = await kv_vm_api.put_vm(
        _settings.kv_vm_api_url(request.kv_cluster), _settings.kv_vm_api_token, name=request.name,
        request=KvVmCreateRequest(
            namespace=request.namespace, cpu=request.cpu, memory_gib=request.memory_gib, mac=mac, labels=request.labels
        ),
        verify=_VERIFY, timeout=HTTP_TIMEOUT_SECONDS,
    )
    activity.logger.info(
        "VM %s/%s on %s (%s): %s, mac %s, %d vCPU / %d GiB",
        created.namespace, created.name, request.kv_cluster.name, _host(_settings.kv_vm_api_url(request.kv_cluster)),
        "created" if created.created else "already there with this identity", created.mac, created.cpu, created.memory_gib,
    )
    return VmCreated(
        name=created.name, namespace=created.namespace, mac=created.mac, cpu=created.cpu,
        memory_gib=created.memory_gib, created=created.created, kv_cluster=request.kv_cluster.name,
    )


# --- await_vm_ready --------------------------------------------------------------------


def _resumed_clock() -> tuple[datetime, int] | None:
    """(started_at, polls) from the last heartbeat, when this attempt retries the wait."""
    details = activity.info().heartbeat_details
    if not details or not isinstance(details[0], dict):
        return None
    try:
        return datetime.fromisoformat(details[0]["started_at"]), int(details[0].get("polls", 0))
    except (KeyError, TypeError, ValueError):  # an unreadable detail starts the clock afresh
        return None


@activity.defn
async def await_vm_ready(request: VmReadyRequest) -> VmReadyOutcome:
    """Until the root disk is bound and the VM admitted, or `deadline_minutes` on this activity's clock."""
    resumed = _resumed_clock()
    started_at, polls = resumed if resumed is not None else (datetime.now(UTC), 0)
    url = _settings.kv_vm_api_url(request.kv_cluster)
    while True:
        state = await kv_vm_api.get_vm(
            url, _settings.kv_vm_api_token, name=request.name, namespace=request.namespace,
            verify=_VERIFY, timeout=HTTP_TIMEOUT_SECONDS,
        )
        polls += 1
        activity.heartbeat({"started_at": started_at.isoformat(), "polls": polls})
        if state is None:
            raise VmDisappearedError(
                f"VM {request.namespace}/{request.name} is gone from {request.kv_cluster.name} before its root disk bound"
            )
        disk = state.root_disk
        bound = disk is not None and disk.bound and disk.phase == _ROOT_DISK_READY_PHASE
        detail = (
            f"root disk {disk.name if disk else '(none)'} phase {disk.phase if disk else None}, "
            f"bound {disk.bound if disk else False}, admitted {state.admitted}"
        )
        if bound and state.admitted:
            activity.logger.info("VM %s/%s ready on %s after %d polls: %s", request.namespace, request.name,
                                 request.kv_cluster.name, polls, detail)
            return VmReadyOutcome(
                result="done", root_disk_phase=disk.phase if disk else None, root_disk_bound=True,
                admitted=True, mac=state.mac, detail=detail, polls=polls,
            )
        elapsed = datetime.now(UTC) - started_at
        if elapsed.total_seconds() >= request.deadline_minutes * 60:
            activity.logger.info("VM %s/%s not ready on %s after %d polls (%s): %s", request.namespace, request.name,
                                 request.kv_cluster.name, polls, elapsed, detail)
            return VmReadyOutcome(
                result="deadline", root_disk_phase=disk.phase if disk else None,
                root_disk_bound=bool(disk and disk.bound), admitted=state.admitted, mac=state.mac,
                detail=detail, polls=polls,
            )
        await asyncio.sleep(READY_POLL_SECONDS)


# --- delete_vm_on_cluster ---------------------------------------------------------------


@activity.defn
async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
    deleted = await kv_vm_api.delete_vm(
        _settings.kv_vm_api_url(request.kv_cluster), _settings.kv_vm_api_token, name=request.name,
        namespace=request.namespace, mac=request.mac, verify=_VERIFY, timeout=HTTP_TIMEOUT_SECONDS,
    )
    activity.logger.info("VM %s/%s on %s (mac %s): %s", request.namespace, request.name, request.kv_cluster.name,
                         request.mac, "deleted" if deleted else "already gone")
    return VmDeleted(deleted=deleted)
