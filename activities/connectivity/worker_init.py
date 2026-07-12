"""Connectivity activity worker — the execution "limb" deployment.

Registers the connectivity activities and polls the connectivity activity
queue. Workflows are NOT registered here: the brain runs in its own deployment
on a separate queue (see workflows/main_worker_init.py).

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from activities.connectivity.activities import (
    check_connectivity_requests,
    list_mce_segments,
    publish_request_ids,
    submit_open_rules,
    unlock_segment,
    validate_segment_exists,
)
from shared.consts import CONNECTIVITY_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings

_settings = TemporalSettings()


async def main() -> None:
    configure_logging()
    client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )
    worker = Worker(
        client,
        task_queue=CONNECTIVITY_ACTIVITY_QUEUE,
        activities=[
            validate_segment_exists,
            list_mce_segments,
            submit_open_rules,
            publish_request_ids,
            check_connectivity_requests,
            unlock_segment,
        ],
    )
    logging.getLogger(__name__).info(
        "Activity worker polling queue=%s on %s",
        CONNECTIVITY_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
