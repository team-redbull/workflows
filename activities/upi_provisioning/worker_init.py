"""UPI-provisioning activity worker — the `upi-provisioning-worker` deployment (ONE, on the hub).

Runs bootstrap-installer as Jobs in its own namespace, reads each installed
cluster's admin kubeconfig from the Secret its prepare Job stored, and writes
Argo CD's cluster Secret in ARGOCD_NAMESPACE — as its own ServiceAccount. Its own
deployment because no other limb holds that credential set or runs Jobs (CLAUDE.md §2).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.upi_provisioning.activities import (
    await_upi_cluster_ready,
    register_upi_cluster_in_argocd,
    run_installer_step,
)
from shared.consts import UPI_PROVISIONING_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings.temporal import TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=UPI_PROVISIONING_ACTIVITY_QUEUE,
        activities=[run_installer_step, await_upi_cluster_ready, register_upi_cluster_in_argocd],
        # The two waits (a Job, a cluster coming up) are cancelled after this on
        # shutdown; the next attempt attaches to the same Job, or resumes the wait
        # from its heartbeat. Keep it below the pod's grace period.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info("Activity worker polling queue=%s on %s", UPI_PROVISIONING_ACTIVITY_QUEUE, _settings.temporal_host)
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
