from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from activities.segment_allocation.activities import *
from shared.consts import SEGMENT_ALLOCATION_ACTIVITY_QUEUE
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
        task_queue=SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
        activities=[
            get_available_segment,
            request_segment,
            register_segment,
            allocate_segment,
        ],
    )
    logging.getLogger(__name__).info(
        "Activity worker polling queue=%s on %s",
        SEGMENT_ALLOCATION_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
