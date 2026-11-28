"""UPI-server activity worker — the `upi-server-worker` deployment, ONE PER UPI CLUSTER, inside it.

Its queue is `upi-server-activity-<UPI_CLUSTER>`, so the brain on the hub picks the
target cluster purely by which queue it dispatches to (the per-MCE
server-lifecycle pattern). It acts on its OWN cluster's API as its own
ServiceAccount, dials OUT to the hub's Temporal (gRPC, TLS through the hub's
Route), drives BMCs over Redfish with its own credentials, and builds the ISOs an
nginx sidecar of the same pod serves to them. Nothing on the hub holds a
credential for this cluster (CLAUDE.md §2: credentials never cross clusters).

`UPI_CLUSTER` is EXACTLY the name create-upi-cluster was given.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.upi_server.activities import (
    allocate_upi_server,
    await_node_joined,
    boot_from_virtual_media,
    build_node_iso,
    extend_upi_server_lock,
    finish_upi_node,
    inspect_upi_node,
    release_upi_server,
)
from shared.consts import upi_server_activity_queue
from shared.logging_config import configure_logging
from shared.settings.temporal import TemporalSettings
from shared.settings.upi_server import UpiServerActivitySettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_activity_settings = UpiServerActivitySettings()
_TASK_QUEUE = upi_server_activity_queue(_activity_settings.upi_cluster)


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[
            allocate_upi_server, extend_upi_server_lock, release_upi_server, inspect_upi_node,
            build_node_iso, boot_from_virtual_media, await_node_joined, finish_upi_node,
        ],
        # A node-image build or a join wait cut off by shutdown is retried: the build
        # finds its finished ISO by hash or starts again, the wait resumes its clock
        # from the heartbeat. Keep it below the pod's grace period.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info("Activity worker polling queue=%s on %s (UPI cluster %s)", _TASK_QUEUE, _settings.temporal_host,
                _activity_settings.upi_cluster)
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
