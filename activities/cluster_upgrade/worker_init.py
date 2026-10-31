"""Cluster-upgrade activity worker — the `cluster-upgrade-worker` deployment, the domain's per-MCE limb.

ONE DEPLOYMENT PER MCE, running INSIDE that MCE, polling
`cluster-upgrade-activity-<MCE_CLUSTER>` (shared/consts.py) — the
server-lifecycle routing: the brain on the hub picks the MCE purely by which
queue it dispatches to, this worker talks to its OWN API server as its own
ServiceAccount (the workflow identity, DESIGN §2), and it reaches Temporal by
dialling OUT. No credential of the MCE ever leaves it.

It reads HostedClusters, NodePools, MachineSets, ConfigMap
hypershift/supported-versions and the MCE's IDMS; it syncs the cluster's Argo
CD Application (its one routine write on the MCE); it reads each hosted cluster
through that HostedCluster's admin kubeconfig, and writes there only an admin
ack (D16) and the tier-2 actions a person approved. It never writes a
HostedCluster or NodePool: Argo CD applies the day1 commit (DESIGN D1).

A separate deployment from the domain's two other limbs because its credential
set is different (CLAUDE.md §2): `upgrade-gitops-worker` on the hub holds the git
and registry tokens, `upgrade-agent-worker` holds no Kubernetes credential at
all. Connects with the Pydantic data converter, like every worker.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.cluster_upgrade.activities import (
    collect_evidence,
    execute_proposed_action,
    give_admin_ack,
    read_admin_acks,
    read_argo_application,
    read_hosted_cluster,
    read_hosted_health,
    read_mce_state,
    sync_argo_application,
    watch_control_plane,
    watch_nodepool,
    watch_nodepool_gate,
)
from shared.consts import cluster_upgrade_activity_queue
from shared.logging_config import configure_logging
from shared.settings import ClusterUpgradeActivitySettings, TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_activity_settings = ClusterUpgradeActivitySettings()
_TASK_QUEUE = cluster_upgrade_activity_queue(_activity_settings.mce_cluster)


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[
            # planning and the pre-flights
            read_mce_state,
            read_hosted_cluster,
            read_argo_application,
            read_hosted_health,
            # a control-plane minor step's admin acks (D16)
            read_admin_acks,
            give_admin_ack,
            # every commit's sync
            sync_argo_application,
            # the long watches: they poll and heartbeat inside the activity
            watch_control_plane,
            watch_nodepool,
            watch_nodepool_gate,
            # a failed gate: the agent's evidence, and an action a person approved
            collect_evidence,
            execute_proposed_action,
        ],
        # In-flight activities get this long to finish after shutdown starts
        # before being cancelled — keep it below the pod's
        # terminationGracePeriodSeconds. A watch cancelled here is retried on
        # another pod and resumes from its last heartbeat's snapshot.
        graceful_shutdown_timeout=timedelta(seconds=20),
    )

    # Graceful rollout: on SIGTERM/SIGINT stop polling, let in-flight
    # activities finish, then exit — instead of dying mid-activity.
    stop = install_shutdown_handler()

    logger.info(
        "Activity worker polling queue=%s on %s (Argo CD Applications in %s)",
        _TASK_QUEUE,
        _settings.temporal_host,
        _activity_settings.argo_namespace,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
