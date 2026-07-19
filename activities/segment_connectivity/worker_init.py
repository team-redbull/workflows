"""Segment-connectivity activity worker — the execution "limb" deployment.

Registers the segment-connectivity activities and polls the segment-connectivity activity
queue. Workflows are NOT registered here: the brain runs in its own deployment
on a separate queue (see workflows/main_worker_init.py).

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

from activities.segment_connectivity.activities import (
    check_segment_connectivity_requests,
    get_next_checking_request_interval,
    get_segment_site,
    list_mce_segments,
    publish_segment_connectivity_failure,
    publish_request_ids,
    submit_open_rules,
    unlock_segment,
)
from shared.consts import SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE
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
        task_queue=SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
        activities=[
            get_segment_site,
            list_mce_segments,
            submit_open_rules,
            publish_request_ids,
            check_segment_connectivity_requests,
            get_next_checking_request_interval,
            unlock_segment,
            publish_segment_connectivity_failure,
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
        SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
