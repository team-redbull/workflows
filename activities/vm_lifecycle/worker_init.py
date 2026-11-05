"""VM-lifecycle activity worker — the `vm-lifecycle-worker` deployment.

ONE DEPLOYMENT ON THE HUB, polling VM_LIFECYCLE_ACTIVITY_QUEUE for every zone.
Its own deployment because its dependency and credential set is its own
(CLAUDE.md §2): the KubeVirt clusters' agents behind Routes with their two
fleet tokens, and the day1 git token — no Kubernetes credential at all, so
nothing to run per MCE or per KubeVirt cluster.

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
    list_kv_clusters,
    rank_kv_clusters,
)
from shared.consts import VM_LIFECYCLE_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_TASK_QUEUE = VM_LIFECYCLE_ACTIVITY_QUEUE


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[list_kv_clusters, rank_kv_clusters, create_vm_on_cluster, await_vm_ready, delete_vm_on_cluster],
        # The ready wait runs for minutes; on shutdown it is cancelled after
        # this and Temporal's next attempt resumes its clock from the
        # heartbeat. Keep it below the pod's grace period.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info("Activity worker polling queue=%s on %s", _TASK_QUEUE, _settings.temporal_host)
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
