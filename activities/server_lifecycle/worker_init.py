"""Server-lifecycle activity worker — the `server-lifecycle-worker` deployment.

Registers the server-lifecycle activities and polls the server-lifecycle
activity queue. A separate deployment from the segment-lifecycle limb because
the driver for one is the dependency + credential set, and this one's is
entirely different: a Kubernetes client with RBAC to create BareMetalHosts,
Secrets and NMStateConfigs in the target namespace, plus per-vendor BMC
credentials and a server-scan token.

install-server's VLAN lookup is deliberately NOT registered here: it reads the
Segments Manager, whose credential lives on the segment-lifecycle limb, so the
workflow routes that one activity to that queue instead of this deployment
holding a second copy of the token.

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from activities.server_lifecycle.activities import (
    acquire_servers,
    create_baremetal_host,
    create_bmc_secret,
    create_nmstate_config,
    get_baremetal_host,
)
from shared.consts import SERVER_LIFECYCLE_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from shared.shutdown import install_shutdown_handler

_settings = TemporalSettings()


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )
    worker = Worker(
        client,
        task_queue=SERVER_LIFECYCLE_ACTIVITY_QUEUE,
        activities=[
            # install-server
            acquire_servers,
            create_bmc_secret,
            create_baremetal_host,
            create_nmstate_config,
            get_baremetal_host,
        ],
        # In-flight activities get this long to finish after shutdown starts
        # before being cancelled — keep it below the pod's
        # terminationGracePeriodSeconds (K8s default 30s).
        graceful_shutdown_timeout=timedelta(seconds=20),
    )

    # Graceful rollout: on SIGTERM/SIGINT stop polling, let in-flight
    # activities finish, then exit — instead of dying mid-activity.
    stop = install_shutdown_handler()

    logger.info(
        "Activity worker polling queue=%s on %s",
        SERVER_LIFECYCLE_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
