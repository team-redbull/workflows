"""Server-provisioning HTTP surface — what a DC technician calls from Swagger.

ASYNC trigger, like every domain: POST returns 202 with the workflow id at once
and the caller polls GET /workflows/runs/{workflow_id}. A provisioning run can
take hours — a template deployment and a RAID reboot are both real waits — so
holding a connection open was never an option.

Technicians rack servers in batches, so the `/bulk` variant takes a list of
iDRAC IPs and starts ONE WORKFLOW PER MACHINE — each with its own dedup id, its
own status and its own failure — answering 202 with a per-item report.

The region is resolved HERE, from the iDRAC's address prefix (regions.py), and
travels into the run as input. An address no prefix matches is refused now —
a 422 on the single route, a `rejected` item in bulk — rather than failing the
run later. An explicit `region` overrides the lookup.
"""

from __future__ import annotations

import asyncio
import ipaddress
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import PROVISION_DELL_SERVER_WORKFLOW_QUEUE
from shared.models.server_provisioning import (
    ProvisionDellServerInput,
    ProvisionDellServerRunArgs,
)
from shared.workflow_ids import provision_dell_server_workflow_id
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.server_provisioning.provision_dell_server import (
    ProvisionDellServerWorkflow,
)
from workflow_domains.server_provisioning.regions import resolve_region

router = APIRouter(prefix="/workflows/server-provisioning", tags=["server-provisioning"])

_PROVISION_DELL_SERVER_PATH = "/provision-dell-server"


class ProvisionDellServerRequest(ProvisionDellServerInput):
    """The provision-dell-server request body: one iDRAC IP, unknown fields refused.

    The IP is normalized here (`010.001.002.003` is refused, not reinterpreted)
    so the workflow id — the dedup key — is the same however it was typed.
    """

    model_config = ConfigDict(extra="forbid")

    @field_validator("idrac_ip")
    @classmethod
    def _idrac_ip_is_an_address(cls, value: str) -> str:
        return str(ipaddress.ip_address(value.strip()))


class BulkProvisionDellServerRequest(BaseModel):
    servers: list[ProvisionDellServerRequest] = Field(min_length=1)


class BulkProvisionDellServerItem(BaseModel):
    """Per-machine outcome of the FAN-OUT only; the run itself has barely begun."""

    idrac_ip: str
    workflow_id: str
    status: Literal["started", "already_running", "rejected", "failed"]
    region: str | None = None
    run_id: str | None = None
    error: str | None = None


class BulkStartProvisionDellServerResponse(BaseModel):
    started: int
    already_running: int
    rejected: int
    failed: int
    results: list[BulkProvisionDellServerItem]


def _with_region(request: ProvisionDellServerRequest) -> ProvisionDellServerInput:
    """The run's input, region resolved; ValueError when no prefix matches."""
    region = request.region or resolve_region(request.idrac_ip)
    if not region:
        raise ValueError(
            f"No region for iDRAC {request.idrac_ip}: its address matches no prefix in "
            "workflow_domains/server_provisioning/regions.py — add the prefix, or pass `region`"
        )
    return ProvisionDellServerInput(idrac_ip=request.idrac_ip, region=region)


async def _start(client: Client, run_input: ProvisionDellServerInput):
    return await client.start_workflow(
        ProvisionDellServerWorkflow.run,
        ProvisionDellServerRunArgs(input=run_input),
        id=provision_dell_server_workflow_id(run_input.idrac_ip),
        task_queue=PROVISION_DELL_SERVER_WORKFLOW_QUEUE,
    )


@router.post(_PROVISION_DELL_SERVER_PATH, response_model=StartWorkflowResponse, status_code=202)
async def start_provision_dell_server(
    request: ProvisionDellServerRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Provision one Dell server from its iDRAC IP — returns immediately (202).

    The run discovers the server in OpenManage, deploys the template for its
    model and iDRAC firmware (which sets root's password), builds the RAID 1 on
    the BOSS and sets every PERC drive Non-RAID, has the naming service rename
    its profile, and completes once the machine is configured: root on the
    enforced password, the template applied, the storage layout verified and
    the OME profile carrying the right name. It does NOT wait for server-scan
    to list the machine — server-scan's own collector finds it on its next
    pass (every 6 h), which is well after this run has finished.

    Poll GET /workflows/runs/{workflow_id}; its `progress.phase` and
    `progress.waiting_on` say where the run is. A second POST for the same IP
    while a run is going gets a 409.
    """
    try:
        run_input = _with_region(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    try:
        handle = await _start(client, run_input)
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"A provision-dell-server run is already in flight for iDRAC {request.idrac_ip}",
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


@router.post(
    f"{_PROVISION_DELL_SERVER_PATH}/bulk",
    response_model=BulkStartProvisionDellServerResponse,
    status_code=202,
)
async def start_provision_dell_server_bulk(
    bulk: BulkProvisionDellServerRequest,
    client: Client = Depends(get_temporal_client),
) -> BulkStartProvisionDellServerResponse:
    """Provision many Dell servers — one workflow per iDRAC IP (202).

    Always 202 with a per-item report: some start, some are already running,
    some are rejected (no region for their prefix) — one bad row never holds
    the others back. A repeated IP in one request reports `already_running`.
    """

    async def _start_one(request: ProvisionDellServerRequest) -> BulkProvisionDellServerItem:
        workflow_id = provision_dell_server_workflow_id(request.idrac_ip)
        try:
            run_input = _with_region(request)
        except ValueError as exc:
            return BulkProvisionDellServerItem(
                idrac_ip=request.idrac_ip, workflow_id=workflow_id, status="rejected", error=str(exc)
            )
        try:
            handle = await _start(client, run_input)
        except WorkflowAlreadyStartedError:
            return BulkProvisionDellServerItem(
                idrac_ip=request.idrac_ip,
                workflow_id=workflow_id,
                status="already_running",
                region=run_input.region,
            )
        except Exception as exc:  # noqa: BLE001 — one bad row must not sink the batch
            return BulkProvisionDellServerItem(
                idrac_ip=request.idrac_ip,
                workflow_id=workflow_id,
                status="failed",
                region=run_input.region,
                error=str(exc),
            )
        return BulkProvisionDellServerItem(
            idrac_ip=request.idrac_ip,
            workflow_id=handle.id,
            status="started",
            region=run_input.region,
            run_id=handle.result_run_id or "",
        )

    results = list(await asyncio.gather(*(_start_one(s) for s in bulk.servers)))
    counts = {status: 0 for status in ("started", "already_running", "rejected", "failed")}
    for item in results:
        counts[item.status] += 1
    return BulkStartProvisionDellServerResponse(
        started=counts["started"],
        already_running=counts["already_running"],
        rejected=counts["rejected"],
        failed=counts["failed"],
        results=results,
    )
