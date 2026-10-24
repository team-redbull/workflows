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

import asyncio
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
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
    install_server_name_workflow_id,
    install_server_slot_workflow_id,
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


# The most runs one request starts: `count` and a `server_names` list alike.
_MAX_INSTALLS_PER_REQUEST = 20
# InstallServerInput.candidate_count's ceiling, which caps the spares a
# pool run gets by default.
_MAX_CANDIDATES = 20
# Slots a pool request looks through for free ones: `_1` … `_100`. Far more
# than can run at once on one pool; it only bounds the scan.
_MAX_SLOTS = 100

_StartStatus = Literal["started", "already_running", "failed"]

_ServerName = Annotated[str, Field(min_length=1)]
_ServerNameList = Annotated[
    list[_ServerName], Field(min_length=1, max_length=_MAX_INSTALLS_PER_REQUEST)
]


# Deterministic ids come from shared/workflow_ids.py — the ONE definition per
# scheme, so the id this route builds and the id anything else builds for the
# same target can never drift apart: a pool run is on a slot of its InfraEnv
# (install_server_slot_workflow_id), a named run on its machine
# (install_server_name_workflow_id).


async def _start(client: Client, run_input: InstallServerInput, workflow_id: str):
    """Start one install-server run. The ways of asking differ only in which id
    each run gets and how the outcome is reported, never in how a run starts."""
    return await client.start_workflow(
        InstallServerWorkflow.run,
        InstallServerRunArgs(input=run_input),
        id=workflow_id,
        task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
    )


def _count_statuses(results: list[Any]) -> dict[str, int]:
    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return counts


class InstallServerRequest(BaseModel):
    """The install-server request body: one shape for every way of asking.

    Which servers: with none named, the InfraEnv's pool picks `count` healthy
    ones (1 unless sent); `server_names` names the machines instead, one or a
    list. Either way every server is a run of its own (`run_input`).

    Deliberately NOT InstallServerInput, nor a subclass of it: a request is no
    longer one run. That model is decoded from Temporal history on every
    replay, so it keeps its shape (`server_name` singular, no `count`) and the
    strictness lives here, at the edge, instead — chiefly a `vlan_id` is
    refused: the VLAN belongs to the MCE's inventory segment, so a caller
    supplying one expects it to be used, and ignoring it would tag the host
    with a different VLAN than was asked for.
    """

    model_config = ConfigDict(extra="forbid")

    infra_env: str = Field(
        min_length=1,
        description=(
            "The InfraEnv to fill. Its name states the hardware "
            "(`cisco-m6-bat-yam-64c-512gb`), so it is also the pool: the healthy, "
            "unclaimed servers server-scan lists as `ocp-<infra_env>…`."
        ),
    )
    mce_cluster: str = Field(min_length=1, description=_MCE_CLUSTER_DESCRIPTION)
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
    server_names: _ServerName | _ServerNameList | None = Field(
        default=None,
        description=(
            "Install exactly these machines (server-scan names) instead of letting "
            f"the pool pick: one name, or a list of up to {_MAX_INSTALLS_PER_REQUEST}, "
            "each once. Each is its own run. Not with `count` or `candidate_count`."
        ),
    )
    # None, not 1: it must stay unset next to `server_names` (a 422 there),
    # and sending it is what asks for the per-run report.
    count: int | None = Field(
        default=None,
        ge=1,
        le=_MAX_INSTALLS_PER_REQUEST,
        description=(
            "Let the pool pick this many healthy servers and install them IN "
            "PARALLEL, one run each. Omitted, the pool picks one. Runs already "
            "going on the pool are stepped over, so asking again — with or "
            "without `count` — adds installs rather than colliding."
        ),
    )
    candidate_count: int | None = Field(
        default=None,
        ge=1,
        le=_MAX_CANDIDATES,
        description=(
            "How many healthy servers each pool run asks server-scan for, so it "
            "has others to try when one is unusable or another run takes it. "
            "Defaults to one per run on the pool plus 2 "
            f"(at most {_MAX_CANDIDATES}): 3 for a lone install."
        ),
    )
    labels: dict[str, str] = Field(
        default_factory=dict, description="Extra labels for each BareMetalHost."
    )

    @model_validator(mode="before")
    @classmethod
    def _server_name_became_server_names(cls, data: Any) -> Any:
        # The field was `server_name`, one machine. A caller still sending it
        # is told what replaced it, not just "extra field".
        if isinstance(data, dict) and "server_name" in data:
            raise ValueError("`server_name` is now `server_names`, which takes one name or a list")
        return data

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        """Canonical before the run starts: the name finds the VLAN in the
        Segments Manager and builds the queue only that MCE's worker polls."""
        return canonical_mce_cluster(value)

    @field_validator("server_names", mode="before")
    @classmethod
    def _one_name_or_a_list(cls, value: Any) -> Any:
        # Checked BEFORE the str-or-list union, so a bad value gets ONE error
        # saying what is wrong, not one per shape it failed to be ("Input
        # should be a valid string" next to the list's real complaint).
        if value is None:
            return value
        names = [value] if isinstance(value, str) else value
        if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise ValueError("`server_names` takes one server name or a list of them")
        if not names:
            raise ValueError("`server_names` lists no server; omit it to let the pool pick")
        if len(names) > _MAX_INSTALLS_PER_REQUEST:
            raise ValueError(
                f"`server_names` lists {len(names)} servers; at most "
                f"{_MAX_INSTALLS_PER_REQUEST} per request"
            )
        if not all(names):
            raise ValueError("a server name cannot be empty")
        return value

    @field_validator("server_names")
    @classmethod
    def _each_server_once(cls, names: str | list[str] | None) -> str | list[str] | None:
        # Case-insensitive, because the run id lowercases the name: `ocp-X` and
        # `ocp-x` are one run, and the second would only report already_running.
        if isinstance(names, list):
            seen: set[str] = set()
            for name in names:
                if name.lower() in seen:
                    raise ValueError(f"server {name} is listed more than once")
                seen.add(name.lower())
        return names

    @model_validator(mode="after")
    def _pool_options_need_the_pool(self) -> InstallServerRequest:
        # Both size a draw from the pool. A named server is drawn alone, so
        # either one next to `server_names` could only be ignored.
        if self.server_names is not None:
            sent = [
                f"`{field}`"
                for field in ("count", "candidate_count")
                if getattr(self, field) is not None
            ]
            if sent:
                verb = "apply" if len(sent) > 1 else "applies"
                raise ValueError(
                    f"{' and '.join(sent)} only {verb} when the pool picks the servers, "
                    "and `server_names` installs exactly the machines named; send one "
                    "or the other"
                )
        return self

    def run_input(
        self, *, server_name: str | None = None, candidate_count: int | None = None
    ) -> InstallServerInput:
        """The input ONE run is started with: one named server, or one pool draw."""
        fields: dict[str, Any] = {
            "infra_env": self.infra_env,
            "mce_cluster": self.mce_cluster,
            "namespace": self.namespace,
            "server_name": server_name,
            "labels": self.labels,
        }
        chosen = candidate_count if candidate_count is not None else self.candidate_count
        if chosen is not None:  # unset, the run's own default applies
            fields["candidate_count"] = chosen
        return InstallServerInput(**fields)


class InstallServerItem(BaseModel):
    """One run of a multi-server request: its START only — the run has barely begun.

    `server_name` for a named server; `slot` for a pool run, whose machine the
    run itself picks (its progress and result name it).
    """

    workflow_id: str
    status: _StartStatus
    server_name: str | None = None
    slot: int | None = None
    run_id: str | None = None
    error: str | None = None


class InstallServerBatchResponse(BaseModel):
    """The answer to `count` or a `server_names` list: one item per run.

    Always 202 with a per-run report, never one pass/fail code: starting
    several runs is partial by nature (some start, some are already running,
    Temporal may refuse one), and one status code would hide which got a run.

    For `count`, `results` lists every slot looked at, in order: the busy ones
    stepped over, the ones started, and a `failed` start if any. `started` <
    `count` means not enough free slots were found, or a start failed (the
    scan stops there rather than failing slot after slot). For a list of
    names, one item per name, in order; a refused name does not stop the rest.
    """

    started: int
    already_running: int
    failed: int
    results: list[InstallServerItem]


@router.post(
    _INSTALL_SERVER_PATH,
    response_model=StartWorkflowResponse | InstallServerBatchResponse,
    status_code=202,
)
async def start_install_server(
    install_request: InstallServerRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse | InstallServerBatchResponse:
    """Install servers into an InfraEnv — returns immediately (202).

    The body names the INFRAENV to fill and the MCE cluster it belongs to. The
    InfraEnv's name states which hardware it is for
    (`cisco-m6-bat-yam-64c-512gb` — vendor, model, site, cores, memory), so it
    also selects the servers: the healthy, unclaimed machines server-scan lists
    under that name. The VLAN is not an input: it is read from the MCE's own
    inventory segment in the Segments Manager.

    EVERY SERVER IS ITS OWN WORKFLOW RUN, with its own id, status and failure;
    there is never one run for a batch. The body decides how many:

    - **Neither `count` nor `server_names`**: the pool picks ONE server, in
      the first free slot (`install-server-<infraEnv>_<slot>`), answered with
      `{workflow_id, run_id}`. Slots still running are stepped over, so the
      same request sent again while it installs starts a SECOND install — it
      is never a 409 (only when all 100 slots are busy).
    - **`count: N`**: the pool picks N, installed IN PARALLEL, one per free
      slot, answered with a per-run report. A second request adds N more.
    - **`server_names: "<name>"`**: that one machine
      (`install-server-name-<server>`), answered with `{workflow_id, run_id}`;
      a 409 while it is already installing.
    - **`server_names: [...]`**: each named machine in parallel, answered with
      a per-run report — always, even for a list of one — in which a machine
      already installing is `already_running`.

    Runs cannot take the same machine, whichever way they were asked for: each
    takes server-scan's install lock (an atomic compare-and-set there) before
    writing anything, and server-scan stops listing a locked machine. A pool
    run that loses the race skips to its next candidate, and one left with
    none fails with ServerReservedError — raise `candidate_count`, or the pool
    has fewer free servers than are being installed. A named machine another
    run took first fails its own run.

    Poll GET /workflows/runs/{workflow_id} (one per run) for progress/result.
    No healthy server matching, no candidate with two link-up NICs on distinct
    physical ports, or a BareMetalHost that never registers all surface there
    as a FAILED run — not as a 4xx here.
    """
    if isinstance(install_request.server_names, list):
        return await _start_named_installs(client, install_request, install_request.server_names)
    if install_request.server_names is not None:
        return await _start_named_install(client, install_request, install_request.server_names)

    results = await _start_pool_installs(client, install_request, install_request.count or 1)
    if install_request.count is not None:
        return InstallServerBatchResponse(**_count_statuses(results), results=results)
    # A lone pool install answers like any single run.
    last = results[-1]
    if last.status == "started":
        return StartWorkflowResponse(workflow_id=last.workflow_id, run_id=last.run_id or "")
    if last.status == "failed":
        raise HTTPException(status_code=502, detail=_temporal_refused(last.workflow_id, last.error))
    raise HTTPException(
        status_code=409,
        detail=(
            f"Every slot of InfraEnv {install_request.infra_env} (`_1` to "
            f"`_{_MAX_SLOTS}`) has an install-server run in flight"
        ),
    )


def _temporal_refused(workflow_id: str, error: str | None) -> str:
    return f"Temporal did not start {workflow_id}: {error}"


async def _start_named_install(
    client: Client, request: InstallServerRequest, server_name: str
) -> StartWorkflowResponse:
    """One named machine, answered as one run: a 409 while it is installing."""
    run_input = request.run_input(server_name=server_name)
    workflow_id = install_server_name_workflow_id(server_name)
    try:
        handle = await _start(client, run_input, workflow_id)
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"An install-server run is already in flight for server {server_name}",
        )
    except Exception as exc:  # noqa: BLE001 — answered with its reason, not a bare 500
        raise HTTPException(status_code=502, detail=_temporal_refused(workflow_id, str(exc)))
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


async def _start_pool_installs(
    client: Client, request: InstallServerRequest, count: int
) -> list[InstallServerItem]:
    """`count` NEW runs from the InfraEnv's pool, each in the first free slot.

    Walks the slots from `_1` up and steps over every one whose run is still
    going. A slot whose run has finished is free again (Temporal reuses the id
    of a closed run), and two requests at once cannot start one slot twice:
    Temporal allows one running workflow per id, and the loser moves on.
    """
    results: list[InstallServerItem] = []
    started = busy = 0
    for slot in range(1, _MAX_SLOTS + 1):
        if started == count:
            break
        workflow_id = install_server_slot_workflow_id(request.infra_env, slot)
        # Every run on the pool - the busy slots below this one and the ones
        # this request has started - takes one machine from the same top of
        # the list, so each new run needs that many spares, plus its own.
        run_input = request.run_input(
            candidate_count=(
                request.candidate_count
                if request.candidate_count is not None
                else min(_MAX_CANDIDATES, busy + count + 2)
            )
        )
        try:
            handle = await _start(client, run_input, workflow_id)
        except WorkflowAlreadyStartedError:
            busy += 1
            results.append(
                InstallServerItem(slot=slot, workflow_id=workflow_id, status="already_running")
            )
            continue
        except Exception as exc:  # noqa: BLE001 — reported, not raised
            # Not a busy slot: Temporal itself refused. Every further slot would
            # fail the same way, so stop and report what did start.
            results.append(
                InstallServerItem(
                    slot=slot, workflow_id=workflow_id, status="failed", error=str(exc)
                )
            )
            break
        started += 1
        results.append(
            InstallServerItem(
                slot=slot,
                workflow_id=handle.id,
                status="started",
                run_id=handle.result_run_id or "",
            )
        )

    return results


async def _start_named_installs(
    client: Client, request: InstallServerRequest, server_names: list[str]
) -> InstallServerBatchResponse:
    """One run per named server, all started at once, on the same id a single
    named install uses — so one machine is one run however it was asked for."""

    async def _start_one(server_name: str) -> InstallServerItem:
        run_input = request.run_input(server_name=server_name)
        workflow_id = install_server_name_workflow_id(server_name)
        try:
            handle = await _start(client, run_input, workflow_id)
        except WorkflowAlreadyStartedError:
            return InstallServerItem(
                server_name=server_name, workflow_id=workflow_id, status="already_running"
            )
        except Exception as exc:  # noqa: BLE001 — one bad name must not sink the rest
            return InstallServerItem(
                server_name=server_name, workflow_id=workflow_id, status="failed", error=str(exc)
            )
        return InstallServerItem(
            server_name=server_name,
            workflow_id=handle.id,
            status="started",
            run_id=handle.result_run_id or "",
        )

    results = list(await asyncio.gather(*(_start_one(name) for name in server_names)))
    return InstallServerBatchResponse(**_count_statuses(results), results=results)


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
