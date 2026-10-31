"""Upgrade-agent activity worker — the `upgrade-agent-worker` deployment.

ONE DEPLOYMENT PER MCE, running INSIDE that MCE beside the upgrade agent
service it calls, polling `upgrade-agent-activity-<MCE_CLUSTER>`. Its own
deployment because its dependency and credential set is its own (CLAUDE.md
§2): an HTTP endpoint and, optionally, that endpoint's token — no Kubernetes
credential at all. The agent's identities live in the agent service.

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.upgrade_agent.activities import invoke_upgrade_agent
from shared.consts import upgrade_agent_activity_queue
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings, UpgradeAgentActivitySettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_activity_settings = UpgradeAgentActivitySettings()
_TASK_QUEUE = upgrade_agent_activity_queue(_activity_settings.mce_cluster)


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[invoke_upgrade_agent],
        # An agent call runs for minutes; on shutdown it is cancelled after
        # this, and Temporal's second attempt (same invocation id) picks the
        # service's run back up. Keep it below the pod's grace period.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info("Activity worker polling queue=%s on %s", _TASK_QUEUE, _settings.temporal_host)
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
