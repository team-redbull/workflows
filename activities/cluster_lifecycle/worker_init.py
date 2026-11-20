"""Cluster-lifecycle activity worker — the `cluster-lifecycle-worker` deployment.

ONE DEPLOYMENT ON THE HUB, polling CLUSTER_LIFECYCLE_ACTIVITY_QUEUE. Its own
deployment because its dependency and credential set is its own (CLAUDE.md
§2): the day1 git token used for a push AND for the repo's git provider API
(the merge request), plus the day2 sig repos' push token and the shortcuts service's bearer
token — no Kubernetes
credential at all, so nothing to run per MCE. Because it holds the day1 token,
it also answers other domains' questions about day1 (list_day1_clusters) and gives create-vm
a zone's KV clusters with the placement buffer (list_kv_clusters).

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.cluster_lifecycle.activities import (
    ensure_cluster_shortcut,
    ensure_merge_request,
    get_merge_request_state,
    inspect_cluster_target,
    list_day1_clusters,
    list_kv_clusters,
    publish_cluster_file,
    register_cluster_in_day2_sig,
)
from shared.consts import CLUSTER_LIFECYCLE_ACTIVITY_QUEUE
from shared.logging_config import configure_logging
from shared.settings.temporal import TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_TASK_QUEUE = CLUSTER_LIFECYCLE_ACTIVITY_QUEUE


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[
            inspect_cluster_target,
            publish_cluster_file,
            ensure_merge_request,
            get_merge_request_state,
            register_cluster_in_day2_sig,
            ensure_cluster_shortcut,
            # Shared with other domains: day1's clusters, read with this limb's token.
            list_day1_clusters,
            list_kv_clusters,
        ],
        # A clone or a push in flight gets this long to finish after shutdown
        # starts; a cut push is re-done from a fresh clone by Temporal's retry.
        # Keep it below the pod's terminationGracePeriodSeconds.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )
    stop = install_shutdown_handler()
    logger.info("Activity worker polling queue=%s on %s", _TASK_QUEUE, _settings.temporal_host)
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
