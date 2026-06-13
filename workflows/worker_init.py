"""Workflow worker — the lightweight "brain" deployment.

Registers SegmentAllocationWorkflow and polls the workflow task queue. Activities
are NOT registered here: they run in their own deployment on a separate queue.

Connects with the Pydantic data converter so that Pydantic models (AllocationInput,
AllocationResult, ...) serialize correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from shared.consts import SEGMENT_ALLOCATION_WORKFLOW_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from workflows.segment_allocation import SegmentAllocationWorkflow

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
        task_queue=SEGMENT_ALLOCATION_WORKFLOW_QUEUE,
        workflows=[SegmentAllocationWorkflow],
    )
    logging.getLogger(__name__).info(
        "Workflow worker polling queue=%s on %s",
        SEGMENT_ALLOCATION_WORKFLOW_QUEUE,
        _settings.temporal_host,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
