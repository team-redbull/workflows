"""Server-lifecycle activity worker — the `server-lifecycle-worker` deployment.

ONE DEPLOYMENT PER MCE CLUSTER, running INSIDE that MCE. Its queue is scoped to
the MCE it serves (`server-lifecycle-activity-<MCE_CLUSTER>`), so the brain on
the hub picks the target cluster purely by which queue it dispatches to.

That is what lets a hub-side workflow create resources on a dozen other API
servers without holding a single cross-cluster credential: this worker talks to
its OWN API server as its own ServiceAccount, and reaches Temporal by dialling
OUT. The hub never needs inbound access to an MCE.

A separate deployment from the segment-lifecycle limb because the driver for
one is the dependency + credential set, and this one's is entirely different: a
Kubernetes client with RBAC to create BareMetalHosts, Secrets and
NMStateConfigs (and read InfraEnvs) in the target namespace, plus per-vendor BMC
credentials, a server-scan token and, for IPMI servers, the site PXE map's.

install-server's VLAN lookup is deliberately NOT registered here: it reads the
Segments Manager, whose credential lives on the segment-lifecycle limb, so the
workflow routes that one activity to that queue instead of this deployment
holding a second copy of the token.

Connects with the Pydantic data converter so that Pydantic models serialize
correctly across the workflow boundary.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from temporalio.worker import Worker

from activities.server_lifecycle.activities import (
    acquire_servers,
    check_infra_env,
    create_baremetal_host,
    delete_agents,
    inspect_inventory_host,
    set_host_online,
    create_bmc_secret,
    create_nmstate_config,
    find_agent_for_host,
    get_baremetal_host,
    publish_infra_env_boot,
    register_pxe_boot,
    release_server,
    remove_host_pxe_entries,
    reserve_server,
    teardown_bmh_resources,
    unregister_pxe_boot,
)
from shared.consts import server_lifecycle_activity_queue
from shared.logging_config import configure_logging
from shared.settings import ServerLifecycleActivitySettings, TemporalSettings
from shared.shutdown import install_shutdown_handler
from shared.temporal_client import connect_temporal

_settings = TemporalSettings()
_activity_settings = ServerLifecycleActivitySettings()
_TASK_QUEUE = server_lifecycle_activity_queue(_activity_settings.mce_cluster)


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    client = await connect_temporal(_settings)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        activities=[
            # install-server
            check_infra_env,
            acquire_servers,
            register_pxe_boot,
            publish_infra_env_boot,
            unregister_pxe_boot,
            create_bmc_secret,
            create_baremetal_host,
            create_nmstate_config,
            find_agent_for_host,
            get_baremetal_host,
            reserve_server,
            release_server,
            teardown_bmh_resources,
            # remove-server
            inspect_inventory_host,
            set_host_online,
            delete_agents,
            remove_host_pxe_entries,
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
        _TASK_QUEUE,
        _settings.temporal_host,
    )
    async with worker:
        await stop.wait()
    logger.info("Activity worker shut down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
