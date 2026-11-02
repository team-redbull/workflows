"""Cluster-upgrade HTTP surface — start an upgrade-cluster run, and every decision a person makes on it.

ASYNC start: POST returns 202 with the workflow id; GET /workflows/runs/{id}
(workflow_domains/routers/runs.py) shows the `progress` query — the phase, the
plan, and `waiting_for`, which says what a person has to do next.

There is no dry-run route: a started run plans, pre-flights and then WAITS for
`approve` with nothing committed, so starting it is the dry run. Reading the
plan and then aborting ends the run CANCELLED, having changed nothing.

DECISIONS ARE TEMPORAL UPDATES, answered at once: 200 when the run took it,
409 with the reason when its validator refused it (UpgradeUpdateRefusedError:
a `continue` while no NodePool waits, an `approve` after approval), 404 for no
such running run, 500 for an update that failed any other way. The run's
validator is the only judge of a refusal — this router never reads the run's
state to second-guess it, so the two can never disagree.

ONE ROUTE IS SYNCHRONOUS, ON PURPOSE (DESIGN D22): GET upgrade-targets, the
"New upgrade" form's choices. It starts a short upgrade-targets run (two day1
git reads on the hub limb) and WAITS for its answer — `execute_workflow`, up to
60 s — because the form needs the list to draw itself, and a read that changes
nothing has no run worth handing back to poll. Its id is per cluster with
USE_EXISTING, so two asks at once share one read. 404 for no such cluster in
day1, 409 for a file or rules it cannot use, 504 when no limb answered.

PATHS ARE `/workflows/cluster-upgrade/upgrade-cluster[/{workflow_id}/<decision>]`
and `/workflows/cluster-upgrade/upgrade-targets`. The workflow id is the one
the start answered with, `upgrade-cluster-<cluster>`; a decision route refuses
any other id, so it can only ever reach an upgrade run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError, TimeoutError as TemporalTimeoutError, WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.consts import UPGRADE_CLUSTER_WORKFLOW_QUEUE, UPGRADE_TARGETS_WORKFLOW_QUEUE
from shared.exceptions import ClusterFileNotFoundError, UpgradeUpdateRefusedError
from shared.models.cluster_upgrade import (
    ActionDecision,
    ContinueNodePool,
    HumanDecision,
    NodePoolMode,
    UpgradeClusterInput,
    UpgradeClusterRunArgs,
    UpgradeTargets,
    UpgradeTargetsInput,
    UpgradeTargetsRunArgs,
)
from shared.ocp_version import parse_minor, release_version
from shared.workflow_ids import upgrade_cluster_workflow_id, upgrade_targets_workflow_id
from workflow_domains.cluster_upgrade import gates
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow
from workflow_domains.cluster_upgrade.upgrade_targets import UpgradeTargetsWorkflow
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse

router = APIRouter(prefix="/workflows/cluster-upgrade", tags=["cluster-upgrade"])

_UPGRADE_CLUSTER_PATH = "/upgrade-cluster"
_UPGRADE_TARGETS_PATH = "/upgrade-targets"
# `upgrade-cluster-`: every id upgrade_cluster_workflow_id builds starts with it.
_RUN_ID_PREFIX = upgrade_cluster_workflow_id("")
# Updates are answered by the run's validator and a handler that only records
# the decision: seconds at most. Longer means no brain worker is polling.
_UPDATE_TIMEOUT = timedelta(seconds=15)
# The targets read is two git reads on the hub limb; longer means no worker answered.
_TARGETS_TIMEOUT = timedelta(seconds=60)
# A day1 cluster file's name, which is also its Argo CD Application's: a DNS
# subdomain. Checked on every route that takes one, before anything runs.
_CLUSTER_NAME = r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$"
_CLUSTER_DESCRIPTION = (
    "The day1 cluster file's name (also its Argo CD Application), without `.yaml`. "
    "Not the HostedCluster's name, which the chart derives from the file."
)


class UpgradeClusterRequest(UpgradeClusterInput):
    """The upgrade-cluster request body: strict at the edge, normalised before the run sees it."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(
        pattern=_CLUSTER_NAME,
        description=(
            f"{_CLUSTER_DESCRIPTION} The run finds the hosting MCE from where that file is, "
            "`sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml`."
        ),
    )
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
    max_unavailable: dict[str, int | str] | None = Field(
        default=None,
        description=(
            "Per NodePool, how many of its nodes an InPlace roll takes down at once: a whole number "
            "(1 or more) or a percentage (`\"33%\"`, 1% to 100%), e.g. `{\"zone-a\": 2}`. The run "
            "writes it into the cluster file's `nodepool[].inPlace.maxUnavailable` in the same commit "
            "as that NodePool's workertag, and it stays there after the upgrade. A NodePool not named "
            "keeps the file's own. Each name must be a NodePool this plan rolls, else planning fails."
        ),
    )
    # The three modes (docs: upgrade-cluster.html#modes). The two automatic ones are
    # refused at planning unless the cluster file says upgrade.wave 0 or 1 (gates.py).
    nodepool_mode: NodePoolMode = Field(
        default=NodePoolMode.MANUAL,
        description=(
            "Who starts each NodePool roll. `manual` (default): the run waits for `continue` before "
            "every NodePool, the customer's sign-off after their own readiness checks. `auto`: the "
            "next NodePool rolls once the previous one's gate has held for the soak, and nobody is "
            "asked. `auto` only for a cluster file with `upgrade.wave` 0 or 1 (lab, canary), else "
            "planning fails."
        ),
    )
    workers_to_target: bool = Field(
        default=True,
        description=(
            "`true` (default): the plan ends with every NodePool at the target. `false`: control "
            "plane only; a NodePool moves only where the path forces it (more than `maxWorkerSkew` "
            "minors behind, or a `dataPlane: required` rung)."
        ),
    )
    auto_approve: bool = Field(
        default=False,
        description=(
            "`true`: the run approves its own plan after pre-flight instead of waiting for "
            "`approve`. Only for a cluster file with `upgrade.wave` 0 or 1 (lab, canary), else "
            "planning fails. `confirm-day2`, an admin ack a client still needs, every needs-human "
            "stop and every agent proposal still wait for a person."
        ),
    )
    requested_by: str = Field(min_length=1, description="Who asked; recorded in the run's log.")

    @model_validator(mode="before")
    @classmethod
    def _no_mce_cluster(cls, data: Any) -> Any:
        """A caller still sending the MCE learns why it is not taken, not just "extra field"."""
        if isinstance(data, dict) and "mce_cluster" in data:
            raise ValueError(
                "mce_cluster is not taken: the run finds the hosting MCE from where day1 keeps "
                "the cluster file, sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml"
            )
        return data

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

    @field_validator("max_unavailable", mode="before")
    @classmethod
    def _no_true_or_false(cls, value: Any) -> Any:
        """Before pydantic reads `true` as the int 1."""
        if isinstance(value, dict):
            for name, raw in value.items():
                if isinstance(raw, bool):
                    raise ValueError(f"NodePool {name}: maxUnavailable {raw!r} is not a number of nodes or a percentage")
        return value

    @field_validator("max_unavailable")
    @classmethod
    def _max_unavailable(cls, value: dict[str, int | str] | None) -> dict[str, str] | None:
        """Each value as the text the cluster file gets, or a 422 naming the NodePool."""
        if value is None:
            return None
        if not value:
            raise ValueError("`max_unavailable` is empty: leave it out to keep every NodePool's own")
        checked: dict[str, str] = {}
        for name, raw in value.items():
            try:
                checked[name] = gates.max_unavailable_value(raw)
            except ValueError as err:
                raise ValueError(f"NodePool {name}: {err}") from None
        return checked

    @model_validator(mode="after")
    def _one_target(self) -> UpgradeClusterRequest:
        if (self.target_version is None) == (self.target_minor is None):
            raise ValueError("send exactly one of `target_version` and `target_minor`")
        return self

    def run_input(self) -> UpgradeClusterInput:
        """The run's own input: the history model, never this edge subclass."""
        return UpgradeClusterInput.model_validate(self.model_dump())


class DecisionRequest(HumanDecision):
    """The body of approve, confirm-day2, pause, resume and retry: who decides, and optionally why."""

    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1, description="Who decides; recorded in the run's log.")
    reason: str | None = Field(default=None, description="Why, optionally; recorded in the run's log.")


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
    """The body of continue: the customer's sign-off that their readiness checks passed."""

    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1, description="Who signs off; recorded in the run's log.")
    nodepool: str | None = Field(
        default=None, description="Which NodePool rolls next. Default: the next in the run's order."
    )


class ActionDecisionRequest(BaseModel):
    """The body of an agent proposal's approve or reject; the action is named in the path."""

    model_config = ConfigDict(extra="forbid")

    by: str = Field(min_length=1, description="Who decides on the action; recorded in the run's log.")


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
    workflow_id = upgrade_cluster_workflow_id(request.cluster)
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
            detail=f"{request.cluster} already has an upgrade run: {workflow_id}",
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


def _root_cause(exc: BaseException) -> BaseException:
    """The end of a Temporal error's `cause` chain: what the run, or its validator, raised."""
    root = exc
    while getattr(root, "cause", None) is not None:
        root = root.cause  # type: ignore[assignment]
    return root


@router.get(_UPGRADE_TARGETS_PATH, response_model=UpgradeTargets)
async def upgrade_targets(
    cluster: str = Query(pattern=_CLUSTER_NAME, description=_CLUSTER_DESCRIPTION),
    client: Client = Depends(get_temporal_client),
) -> UpgradeTargets:
    """What one cluster can be upgraded to: the "New upgrade" form's choices (D22).

    Every ladder rung newer than the cluster's mastertag, of a minor its MCE
    supports, to which a path resolves, newest first. SYNCHRONOUS, unlike every
    other route here: read from day1 git by a short upgrade-targets run, waited
    for up to 60 s. 404 when day1 has no such cluster, 409 when its file or
    rules cannot be used, 504 when nothing answered.
    """
    try:
        return await client.execute_workflow(
            UpgradeTargetsWorkflow.run,
            UpgradeTargetsRunArgs(input=UpgradeTargetsInput(cluster=cluster)),
            id=upgrade_targets_workflow_id(cluster),
            task_queue=UPGRADE_TARGETS_WORKFLOW_QUEUE,
            execution_timeout=_TARGETS_TIMEOUT,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    except WorkflowFailureError as exc:
        root = _root_cause(exc)
        if isinstance(root, TemporalTimeoutError):
            raise HTTPException(
                status_code=504,
                detail=f"no answer in {_TARGETS_TIMEOUT.seconds}s: is the upgrade-gitops limb running?",
            )
        message = root.message if isinstance(root, ApplicationError) else str(root)
        kind = root.type if isinstance(root, ApplicationError) else None
        raise HTTPException(status_code=404 if kind == ClusterFileNotFoundError.__name__ else 409, detail=message)


async def _decide(client: Client, workflow_id: str, update: Any, decision: BaseModel, name: str) -> DecisionResponse:
    """Send one Update and translate the run's answer into HTTP."""
    if not workflow_id.startswith(_RUN_ID_PREFIX):
        raise HTTPException(status_code=404, detail=f"{workflow_id} is not an upgrade-cluster run")
    handle = client.get_workflow_handle(workflow_id)
    try:
        await handle.execute_update(update, decision, rpc_timeout=_UPDATE_TIMEOUT)
    except WorkflowUpdateFailedError as exc:
        root = _root_cause(exc)
        message = root.message if isinstance(root, ApplicationError) else str(root)
        if isinstance(root, ApplicationError) and root.type == UpgradeUpdateRefusedError.__name__:
            # The validator refused it: its message names what the run IS waiting for.
            raise HTTPException(status_code=409, detail=message)
        # Anything else is not "the run is not waiting for that": a handler or
        # a decode that failed is a fault, never a refusal to retry later.
        kind = f"{root.type}: " if isinstance(root, ApplicationError) and root.type else ""
        raise HTTPException(status_code=500, detail=f"{workflow_id}: the {name} update failed: {kind}{message}")
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
    """Carry on from where a `pause` held the run. Only while it is paused."""
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
    """Refuse one action the agent proposed: it is recorded and never run. Then `retry` or `abort`."""
    return await _decide(
        client, workflow_id, UpgradeClusterWorkflow.reject_action,
        ActionDecision(by=decision.by, action_id=action_id), "reject-action",
    )
