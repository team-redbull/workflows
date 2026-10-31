"""Cluster-upgrade HTTP surface — start an upgrade-cluster run, and every decision a person makes on it.

ASYNC start: POST returns 202 with the workflow id; GET /workflows/runs/{id}
(workflow_domains/routers/runs.py) shows the `progress` query — the phase, the
plan, and `waiting_for`, which says what a person has to do next.

There is no dry-run route: a started run plans, pre-flights and then WAITS for
`approve` with nothing committed, so starting it is the dry run. Reading the
plan and then aborting ends the run CANCELLED, having changed nothing.

DECISIONS ARE TEMPORAL UPDATES, answered at once: 200 when the run took it,
409 with the reason when it is not waiting for it (a `continue` while no
NodePool waits, an `approve` after approval), 404 for no such running run.
The run's validator is the only judge of that — this router never reads the
run's state to second-guess it, so the two can never disagree.

PATHS ARE `/workflows/cluster-upgrade/upgrade-cluster[/{workflow_id}/<decision>]`.
The workflow id is the one the start answered with, `upgrade-cluster-<mce>_<cluster>`;
a decision route refuses any other id, so it can only ever reach an upgrade run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from temporalio.client import Client, WorkflowUpdateFailedError, WorkflowUpdateRPCTimeoutOrCancelledError
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.cluster_names import canonical_mce_cluster
from shared.consts import UPGRADE_CLUSTER_WORKFLOW_QUEUE
from shared.models.cluster_upgrade import (
    ActionDecision,
    ContinueNodePool,
    HumanDecision,
    UpgradeClusterInput,
    UpgradeClusterRunArgs,
)
from shared.ocp_version import parse_minor, release_version
from shared.workflow_ids import upgrade_cluster_workflow_id
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse

router = APIRouter(prefix="/workflows/cluster-upgrade", tags=["cluster-upgrade"])

_UPGRADE_CLUSTER_PATH = "/upgrade-cluster"
# `upgrade-cluster-`: every id upgrade_cluster_workflow_id builds starts with it.
_RUN_ID_PREFIX = upgrade_cluster_workflow_id("", "").removesuffix("_")
# Updates are answered by the run's validator and a handler that only records
# the decision: seconds at most. Longer means no brain worker is polling.
_UPDATE_TIMEOUT = timedelta(seconds=15)

_MCE_CLUSTER_DESCRIPTION = (
    "The hosting MCE, with or without its `ocp4-` prefix; the run always uses "
    "the prefixed name, which is also its day1 folder `sites/*/mces/<mce>/`."
)


class UpgradeClusterRequest(UpgradeClusterInput):
    """The upgrade-cluster request body: strict at the edge, normalised before the run sees it."""

    model_config = ConfigDict(extra="forbid")

    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)
    cluster: str = Field(min_length=1, description="The HostedCluster: its day1 file's name.")
    target_version: str | None = Field(
        default=None,
        description="The release to end on, `X.Y.Z` (or `X.Y.Z-x86_64`). Must be in the mirror.",
    )
    target_minor: str | None = Field(
        default=None,
        description=(
            "Instead of `target_version`: a minor, `4.21`. The run picks the newest "
            "release of it that is both mirrored and a ladder rung."
        ),
    )
    nodepool_order: list[str] | None = Field(
        default=None,
        min_length=1,
        description=(
            "The order NodePools roll in; each exactly once. Default: the cluster "
            "file's `upgrade.nodePoolOrder`, else by zone, else file order."
        ),
    )
    requested_by: str = Field(min_length=1, description="Who asked; recorded in the run's log.")

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        """Canonical before the run starts: the name builds the queues only that MCE's workers poll."""
        return canonical_mce_cluster(value)

    @field_validator("target_version")
    @classmethod
    def _bare_version(cls, value: str | None) -> str | None:
        return None if value is None else release_version(value)

    @field_validator("target_minor")
    @classmethod
    def _minor(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parse_minor(value)
        return value.strip()

    @field_validator("nodepool_order")
    @classmethod
    def _each_nodepool_once(cls, value: list[str] | None) -> list[str] | None:
        if value is not None and len(set(value)) != len(value):
            raise ValueError("`nodepool_order` names a NodePool more than once")
        return value

    @model_validator(mode="after")
    def _one_target(self) -> UpgradeClusterRequest:
        if (self.target_version is None) == (self.target_minor is None):
            raise ValueError("send exactly one of `target_version` and `target_minor`")
        return self

    def run_input(self) -> UpgradeClusterInput:
        """The run's own input: the history model, never this edge subclass."""
        return UpgradeClusterInput.model_validate(self.model_dump())


class DecisionRequest(HumanDecision):
    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1, description="Who decides; recorded in the run's log.")


class AbortRequest(DecisionRequest):
    reason: str = Field(min_length=1, description="Why; an abort is never anonymous or unexplained.")


class AdminAckApprovalRequest(DecisionRequest):
    reason: str = Field(
        min_length=1,
        description=(
            "Why the clients the run listed may lose the API this ack lets the upgrade remove "
            "(they moved off it, or they may break). Recorded in the run's log."
        ),
    )


class ContinueRequest(ContinueNodePool):
    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1)
    nodepool: str | None = Field(
        default=None, description="Which NodePool rolls next. Default: the next in the run's order."
    )


class ActionDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1)


class DecisionResponse(BaseModel):
    """200 to a decision: the run took it. What it does next shows in its progress."""

    workflow_id: str
    decision: str


@router.post(_UPGRADE_CLUSTER_PATH, response_model=StartWorkflowResponse, status_code=202)
async def start_upgrade_cluster(
    request: UpgradeClusterRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Start upgrading one HostedCluster — returns immediately (202).

    The run plans the path, pins every release to a digest, runs the
    pre-flight, and waits for `approve` with nothing committed. Poll
    GET /workflows/runs/{workflow_id}: its `progress.waiting_for` says what is
    needed next. One run per cluster: a second start while one runs is a 409.
    """
    workflow_id = upgrade_cluster_workflow_id(request.mce_cluster, request.cluster)
    try:
        handle = await client.start_workflow(
            UpgradeClusterWorkflow.run,
            UpgradeClusterRunArgs(input=request.run_input()),
            id=workflow_id,
            task_queue=UPGRADE_CLUSTER_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"{request.cluster} on {request.mce_cluster} already has an upgrade run: {workflow_id}",
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


async def _decide(client: Client, workflow_id: str, update: Any, decision: BaseModel, name: str) -> DecisionResponse:
    """Send one Update and translate the run's answer into HTTP."""
    if not workflow_id.startswith(_RUN_ID_PREFIX):
        raise HTTPException(status_code=404, detail=f"{workflow_id} is not an upgrade-cluster run")
    handle = client.get_workflow_handle(workflow_id)
    try:
        await handle.execute_update(update, decision, rpc_timeout=_UPDATE_TIMEOUT)
    except WorkflowUpdateFailedError as exc:
        # The validator refused it (UpgradeUpdateRefusedError): the reason is
        # the root cause's message, which names what the run IS waiting for.
        root: BaseException = exc
        while getattr(root, "cause", None) is not None:
            root = root.cause  # type: ignore[assignment]
        raise HTTPException(
            status_code=409, detail=root.message if isinstance(root, ApplicationError) else str(root)
        )
    except WorkflowUpdateRPCTimeoutOrCancelledError:
        # What the SDK raises when `rpc_timeout` passes — not an RPCError.
        raise HTTPException(
            status_code=504,
            detail=f"{workflow_id} did not answer in {_UPDATE_TIMEOUT.seconds}s: is the brain worker up?",
        )
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"no running upgrade-cluster run {workflow_id}")
        raise
    return DecisionResponse(workflow_id=workflow_id, decision=name)


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/approve", response_model=DecisionResponse)
async def approve(
    workflow_id: str, decision: DecisionRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """The one approval, before anything is committed. Only while the run waits for it."""
    return await _decide(client, workflow_id, UpgradeClusterWorkflow.approve, HumanDecision(**decision.model_dump()), "approve")


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/approve-admin-ack", response_model=DecisionResponse)
async def approve_admin_ack(
    workflow_id: str, decision: AdminAckApprovalRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """Give the admin acks a control-plane minor step needs although clients still call the APIs
    they remove (listed in the run's `waiting_for`). Only while the run waits for it; `retry`
    instead checks the callers again."""
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.approve_admin_ack,
        HumanDecision(**decision.model_dump()), "approve-admin-ack",
    )


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/continue", response_model=DecisionResponse)
async def continue_nodepool(
    workflow_id: str, decision: ContinueRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """Roll the next NodePool (manual mode) — optionally naming which. Only while one waits."""
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.continue_nodepool,
        ContinueNodePool(**decision.model_dump()), "continue",
    )


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/confirm-day2", response_model=DecisionResponse)
async def confirm_day2(
    workflow_id: str, decision: DecisionRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """The day2 layer for the control plane's new version is synced: carry on."""
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.confirm_day2, HumanDecision(**decision.model_dump()), "confirm-day2"
    )


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/pause", response_model=DecisionResponse)
async def pause(
    workflow_id: str, decision: DecisionRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """Hold the run before its next commit. What is already rolling finishes."""
    return await _decide(client, workflow_id, UpgradeClusterWorkflow.pause, HumanDecision(**decision.model_dump()), "pause")


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/resume", response_model=DecisionResponse)
async def resume(
    workflow_id: str, decision: DecisionRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    return await _decide(client, workflow_id, UpgradeClusterWorkflow.resume, HumanDecision(**decision.model_dump()), "resume")


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/abort", response_model=DecisionResponse)
async def abort(
    workflow_id: str, decision: AbortRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """Stop. Before approval the run is CANCELLED, nothing committed. After it, the run
    commits `pausedUntil` for the HostedCluster and every NodePool and syncs: ABORTED.
    Nothing is ever rolled back — there is no downgrade."""
    return await _decide(client, workflow_id, UpgradeClusterWorkflow.abort, HumanDecision(**decision.model_dump()), "abort")


@router.post(f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/retry", response_model=DecisionResponse)
async def retry(
    workflow_id: str, decision: DecisionRequest, client: Client = Depends(get_temporal_client)
) -> DecisionResponse:
    """Re-run what failed. Only while the run waits for a person (needs-human), or for an
    admin-ack approval, where it checks the API callers again."""
    return await _decide(client, workflow_id, UpgradeClusterWorkflow.retry, HumanDecision(**decision.model_dump()), "retry")


@router.post(
    f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/actions/{{action_id}}/approve", response_model=DecisionResponse
)
async def approve_action(
    workflow_id: str, action_id: str, decision: ActionDecisionRequest,
    client: Client = Depends(get_temporal_client),
) -> DecisionResponse:
    """Run one action the agent proposed, with the workflow's identity. Then `retry`."""
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.approve_action,
        ActionDecision(by=decision.by, action_id=action_id), "approve-action",
    )


@router.post(
    f"{_UPGRADE_CLUSTER_PATH}/{{workflow_id}}/actions/{{action_id}}/reject", response_model=DecisionResponse
)
async def reject_action(
    workflow_id: str, action_id: str, decision: ActionDecisionRequest,
    client: Client = Depends(get_temporal_client),
) -> DecisionResponse:
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.reject_action,
        ActionDecision(by=decision.by, action_id=action_id), "reject-action",
    )
