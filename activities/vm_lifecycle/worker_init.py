"""VM-lifecycle activity worker — the `vm-lifecycle-worker` deployment.

ONE DEPLOYMENT PER KV CLUSTER, running INSIDE that cluster. Its queue is
scoped to the cluster it serves (`vm-lifecycle-activity-<KV_CLUSTER>`), so
the brain on the hub picks the target cluster purely by which queue it
dispatches to — the per-MCE `server-lifecycle-worker` pattern. This worker
talks to its OWN API server as its own ServiceAccount and reaches Temporal by
dialling OUT (gRPC, TLS through the hub's Route): the KV cluster exposes
nothing, and nothing on the hub holds a credential for it.

Its own deployment because of where it runs and what it may do (CLAUDE.md §2):
read nodes, pods, namespaces, VirtualMachineInstances and PersistentVolumeClaims,
create/read/delete VirtualMachines, read/delete DataVolumes — on its own
cluster only. No Secret at all.

`KV_CLUSTER` is EXACTLY the cluster's day1 file name: the hub builds the same
queue name from the list it reads in day1, so a worker deployed under any
other spelling polls a queue no run sends to.

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.vm_lifecycle.activities import (
    await_vm_ready,
    create_vm_on_cluster,
    delete_vm_on_cluster,
    read_kv_cluster_figures,
)
from shared.consts import vm_lifecycle_activity_queue
from shared.logging_config import configure_logging
from shared.settings.temporal import TemporalSettings
from shared.settings.vm_lifecycle import VmLifecycleActivitySettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_activity_settings = VmLifecycleActivitySettings()
_TASK_QUEUE = vm_lifecycle_activity_queue(_activity_settings.kv_cluster)


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[read_kv_cluster_figures, create_vm_on_cluster, await_vm_ready, delete_vm_on_cluster],
        # The ready wait runs for minutes; on shutdown it is cancelled after
        # this and Temporal's next attempt resumes its clock from the
        # heartbeat. A create cut off mid-call is found again by its request
        # id. Keep it below the pod's grace period.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info(
        "Activity worker polling queue=%s on %s (KV cluster %s, site %s)",
        _TASK_QUEUE, _settings.temporal_host, _activity_settings.kv_cluster, _activity_settings.site,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
