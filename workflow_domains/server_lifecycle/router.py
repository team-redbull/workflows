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

from fastapi import APIRouter, Depends, HTTPException
from pydantic import ConfigDict
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import INSTALL_SERVER_WORKFLOW_QUEUE
from shared.models.server_lifecycle import InstallServerInput, InstallServerRunArgs
from shared.workflow_ids import install_server_workflow_id
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.server_lifecycle.install_server import InstallServerWorkflow

router = APIRouter(prefix="/workflows/server-lifecycle", tags=["server-lifecycle"])

_INSTALL_SERVER_PATH = "/install-server"


# Deterministic ids come from shared/workflow_ids.py — the ONE definition per
# scheme, so the id this route builds and the id anything else builds for the
# same target can never drift apart.
def _workflow_id(install_input: InstallServerInput) -> str:
    return install_server_workflow_id(install_input.infra_env, install_input.mce_cluster)


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

    Installs into one (InfraEnv, MCE) are SERIAL: the workflow id keys on that
    pair, so a second trigger while one is in flight gets a 409 here. That is
    deliberate — server-scan hands out candidates without reserving them, so
    two concurrent runs could otherwise draw the same machine.
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
                "An install-server run is already in flight for InfraEnv "
                f"{install_input.infra_env} on MCE {install_input.mce_cluster}"
            ),
        )
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")
