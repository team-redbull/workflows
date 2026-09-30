"""Server-provisioning activity worker — the `server-provisioning-worker` deployment.

ONE deployment for the whole estate, on the hub, polling
SERVER_PROVISIONING_ACTIVITY_QUEUE. A separate deployment from both other limbs
because its dependency + credential set is its own: the OME service account,
the iDRAC root passwords, the naming service and a server-scan token — and it
needs no Kubernetes RBAC at all.

It needs network reach to every iDRAC it may provision (HTTPS/443), to the OME
appliance, to the naming service and to server-scan.

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

from activities.server_provisioning.activities import (
    apply_staged_idrac_jobs,
    check_idrac_login,
    clear_idrac_os_hostname,
    deploy_ome_template,
    find_in_server_scan,
    find_ome_device,
    get_idrac_jobs,
    get_ome_job,
    get_ome_profile,
    probe_idrac_credentials,
    read_idrac_identity,
    read_storage_layout,
    request_server_name,
    set_idrac_root_password,
    stage_storage_config,
    start_ome_discovery,
)
from shared.consts import SERVER_PROVISIONING_ACTIVITY_QUEUE
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
        task_queue=SERVER_PROVISIONING_ACTIVITY_QUEUE,
        activities=[
            # provision-dell-server
            probe_idrac_credentials,
            check_idrac_login,
            read_idrac_identity,
            set_idrac_root_password,
            clear_idrac_os_hostname,
            read_storage_layout,
            stage_storage_config,
            apply_staged_idrac_jobs,
            get_idrac_jobs,
            find_ome_device,
            start_ome_discovery,
            get_ome_job,
            deploy_ome_template,
            get_ome_profile,
            request_server_name,
            find_in_server_scan,
        ],
        # Below the pod's terminationGracePeriodSeconds (K8s default 30s).
        graceful_shutdown_timeout=timedelta(seconds=20),
    )

    stop = install_shutdown_handler()
    logger.info(
        "Activity worker polling queue=%s on %s",
        SERVER_PROVISIONING_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
