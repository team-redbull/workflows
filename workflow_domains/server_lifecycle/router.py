"""Server-lifecycle HTTP surface — the domain's workflows, one path each.

ASYNC trigger: POST returns 202 with the workflow id immediately and the caller
polls GET /workflows/runs/{workflow_id} (workflow_domains/routers/runs.py) for
progress/result. Async even though the run is short, because the id IS the
dedup key — handing it back at once is what lets a caller re-poll, or recognise
a duplicate, rather than holding one HTTP connection open through a bounded
registration wait that can take minutes.

PATHS ARE `/workflows/<domain>/<workflow>`. The domain prefix lives here and
every workflow in the domain adds its own path under it — a domain holds many
workflows, so it can never be the endpoint of one of them. Adding
uninstall-server later is a new route, not a redesign.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.cluster_names import canonical_mce_cluster
from shared.consts import (
    INSTALL_SERVER_WORKFLOW_QUEUE,
    PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
    REMOVE_SERVER_WORKFLOW_QUEUE,
)
from shared.models.server_lifecycle import (
    InstallServerInput,
    InstallServerRunArgs,
    PublishInfraEnvBootInput,
    PublishInfraEnvBootRunArgs,
    RemoveServerInput,
    RemoveServerRunArgs,
)
from shared.workflow_ids import (
    install_server_slot_workflow_id,
    install_server_workflow_id,
    publish_infraenv_boot_workflow_id,
    remove_server_workflow_id,
)
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.server_lifecycle.install_server import InstallServerWorkflow
from workflow_domains.server_lifecycle.publish_infraenv_boot import (
    PublishInfraEnvBootWorkflow,
)
from workflow_domains.server_lifecycle.remove_server import RemoveServerWorkflow

router = APIRouter(prefix="/workflows/server-lifecycle", tags=["server-lifecycle"])

_INSTALL_SERVER_PATH = "/install-server"
_REMOVE_SERVER_PATH = "/remove-server"
_PUBLISH_INFRAENV_BOOT_PATH = "/publish-infraenv-boot"

# Where the inventory InfraEnvs live on an MCE unless the caller says otherwise.
_DEFAULT_NAMESPACE = "inventory"

_MCE_CLUSTER_DESCRIPTION = (
    "The target MCE, with or without its `ocp4-` prefix: `prod-mce-bat-yam` "
    "and `ocp4-prod-mce-bat-yam` name the same cluster, and the run always "
    "uses the prefixed name."
)


# Deterministic ids come from shared/workflow_ids.py — the ONE definition per
# scheme, so the id this route builds and the id anything else builds for the
# same target can never drift apart.
def _workflow_id(install_input: InstallServerInput) -> str:
    return install_server_workflow_id(
        install_input.infra_env, install_input.server_name
    )


class InstallServerRequest(InstallServerInput):
    """The install-server request body: InstallServerInput, refusing unknown fields.

    Chiefly a `vlan_id`: the VLAN belongs to the target MCE's inventory segment
    and is looked up in the Segments Manager, so a caller supplying one expects
    it to be used. Ignoring it would tag the host with a different VLAN than
    was asked for; refusing says so.

    The strictness lives HERE, at the edge, not on InstallServerInput — that
    model is decoded from Temporal history on every replay, and a forbidding
    model would fail to decode a payload recorded before a field was removed
    and wedge the run.
    """

    model_config = ConfigDict(extra="forbid")

    # Defaulted HERE, at the edge: the run's input always carries the
    # namespace it was started with, so a later change of this default never
    # alters what a recorded run targets on replay.
    namespace: str = Field(
        default=_DEFAULT_NAMESPACE,
        min_length=1,
        description=(
            "Namespace of the InfraEnv on the MCE, where the BareMetalHost, its "
            f"Secret and NMStateConfig are created. Defaults to `{_DEFAULT_NAMESPACE}`."
        ),
    )
    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        """Canonical before the run starts: the name finds the VLAN in the
        Segments Manager and builds the queue only that MCE's worker polls."""
        return canonical_mce_cluster(value)


@router.post(_INSTALL_SERVER_PATH, response_model=StartWorkflowResponse, status_code=202)
async def start_install_server(
    install_input: InstallServerRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Install one server into an InfraEnv — returns immediately (202).

    The body names the INFRAENV to fill and the MCE cluster it belongs to. The
    InfraEnv's name states which hardware it is for
    (`cisco-m6-bat-yam-64c-512gb` — vendor, model, site, cores, memory), so it
    also selects the server; pass `server_name` to install one specific machine
    instead. The VLAN is not an input: it is read from the MCE's own inventory
    segment in the Segments Manager.

    Poll GET /workflows/runs/{workflow_id} for progress/result. No healthy
    server matching the InfraEnv, no candidate with two link-up NICs on
    distinct physical ports, or a BareMetalHost that never registers all
    surface there as a FAILED run — not as a 4xx here.

    Installs from one InfraEnv are SERIAL: the workflow id keys on the
    candidate pool, so a second trigger while one is in flight gets a 409 here
    — including one aimed at a different MCE, because both draw from the same
    pool. That is deliberate: server-scan hands out candidates without
    reserving them, so two concurrent runs could otherwise draw the same
    machine.
    """
    try:
        handle = await client.start_workflow(
            InstallServerWorkflow.run,
            InstallServerRunArgs(input=install_input),
            id=_workflow_id(install_input),
            task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "An install-server run is already in flight for "
                + (
                    f"server {install_input.server_name}"
                    if install_input.server_name
                    else f"InfraEnv {install_input.infra_env}"
                )
            ),
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


# The most servers one bulk request installs at once — the candidate_count
# ceiling, so every slot can still be given a spare candidate or more.
_MAX_BULK_COUNT = 20
# Slots a bulk request looks through for free ones: `_1` … `_100`. Far more
# than can run at once on one pool; it only bounds the scan.
_MAX_SLOTS = 100


class BulkInstallServerRequest(BaseModel):
    """install-server/bulk: N servers from ONE InfraEnv's pool, in parallel.

    No `server_name`: naming machines already parallelizes (one id per name);
    this is for pool draws. Strict like InstallServerRequest — a `vlan_id`
    is refused for the same reason.
    """

    model_config = ConfigDict(extra="forbid")

    infra_env: str = Field(min_length=1)
    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)
    namespace: str = Field(
        default=_DEFAULT_NAMESPACE,
        min_length=1,
        description=f"Namespace of the InfraEnv on the MCE. Defaults to `{_DEFAULT_NAMESPACE}`.",
    )
    count: int = Field(
        ge=1,
        le=_MAX_BULK_COUNT,
        description=(
            "How many NEW installs to start. Slots whose run is still going are "
            "skipped, so a second request while the first batch runs adds more."
        ),
    )
    candidate_count: int | None = Field(
        default=None,
        ge=1,
        le=20,
        description=(
            "Candidates EACH run draws. Defaults to (slots still running) + "
            "count + 2, at most 20: every run on the pool - earlier ones "
            "included - locks one machine from the same top of the list, and "
            "each machine locked first is a skip, so each needs that many spares."
        ),
    )
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)


class BulkInstallServerItem(BaseModel):
    """Per-slot outcome of the FAN-OUT only; the run itself has barely begun."""

    slot: int
    workflow_id: str
    status: Literal["started", "already_running", "failed"]
    run_id: str | None = None
    error: str | None = None


class BulkStartInstallServerResponse(BaseModel):
    """`started` new runs, after skipping `already_running` busy slots.

    `results` lists every slot looked at, in order: the busy ones it stepped
    over (`already_running`), the ones it started, and a `failed` start, if
    any. `started` < `count` means not enough free slots were found, or a start
    failed (the scan stops there rather than failing slot after slot).
    """

    started: int
    already_running: int
    failed: int
    results: list[BulkInstallServerItem]


@router.post(
    f"{_INSTALL_SERVER_PATH}/bulk",
    response_model=BulkStartInstallServerResponse,
    status_code=202,
)
async def start_install_server_bulk(
    bulk: BulkInstallServerRequest,
    client: Client = Depends(get_temporal_client),
) -> BulkStartInstallServerResponse:
    """Start `count` NEW installs from one InfraEnv's pool, in parallel — one run per slot (202).

    Runs are `install-server-<infraEnv>_<slot>`. The route walks the slots from
    `_1` up, steps over every slot whose run is still going, and starts a run
    in each free one until `count` have started — so a second request while
    the first batch is still installing ADDS installs rather than colliding
    with it. A slot whose run has finished is free again (Temporal reuses the
    id of a closed run). Two requests at once cannot start one slot twice:
    Temporal allows one running workflow per id, and the loser moves on.

    The runs cannot take the same machine: each takes server-scan's install
    lock (an atomic compare-and-set there) before writing anything, and a
    machine another run locked is a skip to its next candidate. Poll
    GET /workflows/runs/{workflow_id} per slot. A run that finds every
    candidate locked by its siblings fails with ServerReservedError — raise
    `candidate_count`, or the pool has fewer free servers than are installing.
    """
    results: list[BulkInstallServerItem] = []
    started = busy = 0
    for slot in range(1, _MAX_SLOTS + 1):
        if started == bulk.count:
            break
        workflow_id = install_server_slot_workflow_id(bulk.infra_env, slot)
        # Every run on the pool - the busy slots below this one and the ones
        # this request has started - takes one machine from the same top of
        # the list, so each new run needs that many spares, plus its own.
        candidate_count = bulk.candidate_count or min(20, busy + bulk.count + 2)
        run_input = InstallServerInput(
            infra_env=bulk.infra_env,
            mce_cluster=bulk.mce_cluster,
            namespace=bulk.namespace,
            candidate_count=candidate_count,
            labels=bulk.labels,
        )
        try:
            handle = await client.start_workflow(
                InstallServerWorkflow.run,
                InstallServerRunArgs(input=run_input),
                id=workflow_id,
                task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
            )
        except WorkflowAlreadyStartedError:
            busy += 1
            results.append(
                BulkInstallServerItem(slot=slot, workflow_id=workflow_id, status="already_running")
            )
            continue
        except Exception as exc:  # noqa: BLE001 — reported, not raised
            # Not a busy slot: Temporal itself refused. Every further slot would
            # fail the same way, so stop and report what did start.
            results.append(
                BulkInstallServerItem(
                    slot=slot, workflow_id=workflow_id, status="failed", error=str(exc)
                )
            )
            break
        started += 1
        results.append(
            BulkInstallServerItem(
                slot=slot,
                workflow_id=handle.id,
                status="started",
                run_id=handle.result_run_id or "",
            )
        )

    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return BulkStartInstallServerResponse(
        started=counts["started"],
        already_running=counts["already_running"],
        failed=counts["failed"],
        results=results,
    )


class RemoveServerRequest(RemoveServerInput):
    """The remove-server request body: strict at the edge, namespace defaulted here."""

    model_config = ConfigDict(extra="forbid")

    namespace: str = Field(
        default=_DEFAULT_NAMESPACE,
        min_length=1,
        description=(
            "Namespace of the server's BareMetalHost, NMStateConfig and Agent on "
            f"the MCE. Defaults to `{_DEFAULT_NAMESPACE}`."
        ),
    )
    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        """Canonical before the run starts: the name builds the queue only
        that MCE's worker polls."""
        return canonical_mce_cluster(value)


@router.post(_REMOVE_SERVER_PATH, response_model=StartWorkflowResponse, status_code=202)
async def start_remove_server(
    remove_input: RemoveServerRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Take one server out of an MCE's inventory — returns immediately (202).

    Deletes the server's Agent, its BareMetalHost (`<server>`) and its
    NMStateConfig (`nmstate-config-<server>`) after powering the host off.
    Only for a server that is just inventory: if its Agent is bound to a
    cluster the run FAILS having changed nothing (`AgentBoundToClusterError`).
    A server with nothing on the MCE completes as a no-op (`found: false`).

    Poll GET /workflows/runs/{workflow_id} for progress/result. One removal
    per server at a time: a second trigger while one runs gets a 409.
    """
    try:
        handle = await client.start_workflow(
            RemoveServerWorkflow.run,
            RemoveServerRunArgs(input=remove_input),
            id=remove_server_workflow_id(remove_input.server_name),
            task_queue=REMOVE_SERVER_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"A remove-server run is already in flight for {remove_input.server_name}",
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


class PublishInfraEnvBootRequest(PublishInfraEnvBootInput):
    """The publish-infraenv-boot request body: strict at the edge, namespace defaulted here."""

    model_config = ConfigDict(extra="forbid")

    namespace: str = Field(
        default=_DEFAULT_NAMESPACE,
        min_length=1,
        description=f"Namespace of the InfraEnv on the MCE. Defaults to `{_DEFAULT_NAMESPACE}`.",
    )
    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        """Canonical before the run starts: the name finds the site in the
        Segments Manager, builds the MCE's queue, and is the boot entry's
        directory on the PXE VM — so `prod-mce-x` and `ocp4-prod-mce-x` must
        land on the same file."""
        return canonical_mce_cluster(value)


@router.post(_PUBLISH_INFRAENV_BOOT_PATH, response_model=StartWorkflowResponse, status_code=202)
async def start_publish_infraenv_boot(
    publish_input: PublishInfraEnvBootRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Put an InfraEnv's current iPXE script on its site's PXE VM — returns immediately (202).

    Writes `/var/www/pxe-map/<mce_cluster>/<infra_env>.ipxe` on the PXE VM of
    the MCE's inventory-segment site: the boot entry every IPMI host mapped to
    the InfraEnv chains to. Waits (bounded, 10 min) for the InfraEnv to have a
    ready discovery image; a VM already holding a newer image keeps it, and
    the run completes as `superseded`.

    Poll GET /workflows/runs/{workflow_id} for progress/result. One publish per
    (MCE, InfraEnv) at a time: a second trigger while one runs gets a 409.
    """
    try:
        handle = await client.start_workflow(
            PublishInfraEnvBootWorkflow.run,
            PublishInfraEnvBootRunArgs(input=publish_input),
            id=publish_infraenv_boot_workflow_id(
                publish_input.mce_cluster, publish_input.infra_env
            ),
            task_queue=PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "A publish-infraenv-boot run is already in flight for "
                f"{publish_input.mce_cluster}/{publish_input.infra_env}"
            ),
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")
