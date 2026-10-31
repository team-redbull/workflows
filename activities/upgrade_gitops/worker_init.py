"""Upgrade-gitops activity worker — the `upgrade-gitops-worker` deployment, the cluster-upgrade HUB limb.

ONE deployment on the hub, polling UPGRADE_GITOPS_ACTIVITY_QUEUE: the day1
values repo (rules, cluster files, the upgrade's commits), the release
registry, the day2 repo and Brain. Its credentials are a git token per repo
and a registry auth file; it needs no Kubernetes RBAC at all, only egress to
the git hosts and the registries.

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.upgrade_gitops.activities import (
    check_day2_layers,
    commit_day1_change,
    list_release_tags,
    pin_release_digests,
    read_day1_cluster,
    read_upgrade_rules,
    write_incident,
)
from shared.consts import UPGRADE_GITOPS_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings import TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=UPGRADE_GITOPS_ACTIVITY_QUEUE,
        activities=[
            # upgrade-cluster and upgrade-targets: planning
            read_upgrade_rules,
            read_day1_cluster,
            list_release_tags,
            pin_release_digests,
            check_day2_layers,
            # upgrade-cluster and upgrade-control-plane: the steps
            commit_day1_change,
            write_incident,
        ],
        # A push in flight gets this long to land after SIGTERM (an interrupted
        # one is retried from a fresh clone, so this is only courtesy). Below
        # the chart's terminationGracePeriodSeconds (90s).
        graceful_shutdown_timeout=timedelta(seconds=60),
    )

    stop = install_shutdown_handler()
    logger.info(
        "Activity worker polling queue=%s on %s",
        UPGRADE_GITOPS_ACTIVITY_QUEUE,
        _settings.temporal_host,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
