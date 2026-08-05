"""Workflow worker — the lightweight "brain" deployment.

ONE deployment, ONE process, but one Worker PER WORKFLOW: each workflow has its
own task queue (see shared/consts.py), and a Temporal Worker polls exactly one
queue. Every workflow of every domain registers below and is polled
concurrently from this same process — the queue split buys per-workflow
concurrency limits, drain/pause and backlog metrics, not extra deployments.

Activities are NOT registered here: they run in their own per-domain deployment
on a separate queue (see activities/segment_lifecycle/worker_init.py).

Connects with the Pydantic data converter so that Pydantic models
(OpenSegmentRulesInput, OpenSegmentRulesResult, ...) serialize correctly across the
workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from shared.consts import OPEN_SEGMENT_RULES_WORKFLOW_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from shared.shutdown import install_shutdown_handler
from workflow_domains.segment_lifecycle.open_segment_rules import (
    OpenSegmentRulesWorkflow,
)

_settings = TemporalSettings()

# The brain's registry: (task queue, workflows polled on it). Adding a workflow
# — in this domain or a brand-new one — is one entry here plus its queue name in
# shared/consts.py; no new deployment, image or chart.
_WORKER_SPECS: list[tuple[str, list[type]]] = [
    (OPEN_SEGMENT_RULES_WORKFLOW_QUEUE, [OpenSegmentRulesWorkflow]),
]


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )

    # Graceful rollout: on SIGTERM/SIGINT finish the workflow tasks in hand,
    # then exit — instead of dying mid-task.
    stop = install_shutdown_handler()

    # Every Worker runs for as long as the stack is open; leaving it shuts them
    # all down (in reverse order), so one signal drains the whole brain.
    async with AsyncExitStack() as stack:
        for task_queue, workflows in _WORKER_SPECS:
            await stack.enter_async_context(
                Worker(client, task_queue=task_queue, workflows=workflows)
            )

        logger.info(
            "Workflow worker polling queues=%s on %s",
            ", ".join(task_queue for task_queue, _ in _WORKER_SPECS),
            _settings.temporal_host,
        )

        await stop.wait()

    logger.info("Workflow worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
