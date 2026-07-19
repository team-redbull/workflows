"""Workflow worker — the lightweight "brain" deployment.

Registers SegmentConnectivityWorkflow and polls the workflow task queue. 
Activities are NOT registered here: they run in their own deployment on a separate queue
(see activities/segment_connectivity/worker_init.py).

Connects with the Pydantic data converter so that Pydantic models
(SegmentConnectivityInput, SegmentConnectivityResult, ...) serialize correctly across the
workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from shared.consts import SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from shared.shutdown import install_shutdown_handler
from workflows.segment_connectivity import SegmentConnectivityWorkflow

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
        task_queue=SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
        workflows=[SegmentConnectivityWorkflow],
    )

    # Graceful rollout: on SIGTERM/SIGINT finish the workflow tasks in hand,
    # then exit — instead of dying mid-task.
    stop = install_shutdown_handler()

    logger.info(
        "Workflow worker polling queue=%s on %s",
        SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
        _settings.temporal_host,
    )

    async with worker:
        await stop.wait()
    logger.info("Workflow worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
