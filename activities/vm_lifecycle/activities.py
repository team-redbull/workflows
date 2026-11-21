"""VM-lifecycle activities — the worker INSIDE one KV cluster (`vm-lifecycle-worker`). Thin.

Polls `vm_lifecycle_activity_queue(KV_CLUSTER)` from inside the KubeVirt (KV)
cluster it serves, acts on that cluster's own API server as its own
ServiceAccount (kubevirt_api.py) and dials OUT to the hub's Temporal. It holds
no Secret and exposes nothing. Each activity reads the cluster's own values
from its settings and hands them, with the API, to vm_operations.py, where
its whole body lives (so the fleet simulator runs the same code for dozens of
fake clusters); cluster_figures.py and vm_spec.py are the arithmetic and the
VirtualMachine it builds.

Budgets, all under the workflow's timeouts (workflow_domains/vm_lifecycle/create_vm.py):
  * one call to the cluster's API: 5 s to connect, 20 s to answer (kubevirt_api.REQUEST_TIMEOUT);
  * the figures read: its four list calls together cut at 20 s, under the
    60 s start_to_close — an unreadable cluster is `readable: false`, never
    an exception, because the caller holds the zone's lock;
  * the create: a list, a namespace read, at most three creates and a
    read-back; an attempt starts no call after 55 s
    (vm_operations.CREATE_BUDGET_SECONDS), so with one call's own bound it is
    over inside the 90 s start_to_close and two attempts never overlap; and
    nothing is created after the request's `create_before`;
  * the ready wait: one heartbeating activity polling every 8 s (the
    operator, 2026-11-05), its clock in the heartbeat details so a retry on a
    restarted worker resumes it, `deadline` on its own clock, under the
    workflow's 12 min start_to_close and 2 min heartbeat timeout.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime

from temporalio import activity

from activities.vm_lifecycle import vm_operations
from activities.vm_lifecycle.kubevirt_api import KubevirtApi
from activities.vm_lifecycle.vm_operations import ClusterValues
from shared.models.vm_lifecycle import (
    KvClusterFigures,
    KvFiguresRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)
from shared.settings.vm_lifecycle import VmLifecycleActivitySettings

_settings = VmLifecycleActivitySettings()
_cluster = ClusterValues(
    kv_cluster=_settings.kv_cluster,
    site=_settings.site,
    mac_prefix=_settings.vm_mac_prefix,
    storage_class_name=_settings.vm_storage_class_name,
    redfish_label=_settings.redfish_label,
    network_name=_settings.vm_network_name,
    node_selector=_settings.vm_node_selector,
    root_disk_size=_settings.vm_root_disk_size,
)
# The client is built from the pod's ServiceAccount on its first call, not here:
# importing this module must not need a cluster.
_api = KubevirtApi()


def _now() -> datetime:
    return datetime.now(UTC)


@activity.defn
async def read_kv_cluster_figures(request: KvFiguresRequest) -> KvClusterFigures:
    figures = await vm_operations.read_figures(_api, _cluster, request)
    if figures.readable:
        activity.logger.info(
            "figures of %s: %d usable of %d nodes, allocatable %d m / %d bytes, pods %d m / %d bytes, "
            "%d stopped VMs (%d m / %d bytes)",
            figures.kv_cluster, figures.usable_nodes, figures.nodes, figures.allocatable_cpu_millicores,
            figures.allocatable_memory_bytes, figures.pods_cpu_millicores, figures.pods_memory_bytes,
            figures.stopped_vms, figures.stopped_vms_cpu_millicores, figures.stopped_vms_memory_bytes,
        )
    else:
        activity.logger.warning("figures of %s are not readable: %s", request.kv_cluster, figures.detail)
    return figures


@activity.defn
async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
    created = await vm_operations.create_vm(
        _api, _cluster, request, now=_now, random_bytes=secrets.token_bytes, log=activity.logger.info
    )
    activity.logger.info(
        "VM %s/%s on %s: %s, mac %s, %d vCPU / %d GiB, request %s (%d draws)",
        created.namespace, created.name, created.kv_cluster,
        "created" if created.created else "already there for this request", created.mac, created.cpu,
        created.memory_gib, request.request_id, created.draws,
    )
    return created


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
    started_at, polls = resumed if resumed is not None else (_now(), 0)
    return await vm_operations.wait_ready(
        _api, _cluster, request, started_at=started_at, polls=polls, now=_now,
        heartbeat=lambda started, count: activity.heartbeat({"started_at": started.isoformat(), "polls": count}),
        log=activity.logger.info,
    )


@activity.defn
async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
    return await vm_operations.delete_vm(_api, _cluster, request, log=activity.logger.info)
