"""VM-lifecycle HTTP surface — ONE path today, `POST /workflows/vm-lifecycle/create-vm`.

ASYNC trigger: 202 with the workflow id at once; the caller polls
GET /workflows/runs/{workflow_id} (workflow_domains/routers/runs.py) for
progress and the result. PATHS ARE `/workflows/<domain>/<workflow>`: the
domain prefix lives here, and every workflow of the domain adds its own path.

The request is the smallest thing that names the work — the MCE and the
InfraEnv — and the edge derives what the run carries: the site from the MCE's
name, the zone and the VM's size from the InfraEnv's (CLAUDE.md §4: derived
ONCE here, never on replay). A VM's name is drawn inside the run, so a run is
a SLOT of its InfraEnv on its MCE (shared/workflow_ids.py), filled by
workflow_domains/routers/slots.py exactly as add-server-to-mce's pool draws.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from temporalio.client import Client

from shared.cluster_names import canonical_mce_cluster, site_of_mce_cluster
from shared.consts import CREATE_VM_WORKFLOW_QUEUE
from shared.infraenv_names import InfraEnvKind, parse_infraenv, vm_name
from shared.models.vm_lifecycle import CreateVmInput, CreateVmRunArgs
from shared.workflow_ids import create_vm_slot_workflow_id
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import SlotStart, StartWorkflowResponse
from workflow_domains.routers.slots import count_statuses, fill_free_slots
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow

router = APIRouter(prefix="/workflows/vm-lifecycle", tags=["vm-lifecycle"])

_CREATE_VM_PATH = "/create-vm"
# Where the inventory InfraEnvs live on an MCE, and so the VM's namespace on
# the KubeVirt cluster (a kubevirt-redfish chassis namespace), unless the
# caller says otherwise — add-server-to-mce's default, verbatim.
_DEFAULT_NAMESPACE = "inventory"
# The most runs one request starts, and the slots it looks through for free
# ones (`_1` … `_100`) — add-server-to-mce's numbers.
_MAX_CREATES_PER_REQUEST = 20
_MAX_SLOTS = 100


class CreateVmRequest(BaseModel):
    """The create-vm request body: ONE shape, one or `count` runs.

    Its own model, not a CreateVmInput subclass: one request is many runs,
    and the run's input carries what the edge derives (`run_input`).
    """

    model_config = ConfigDict(extra="forbid")

    mce_cluster: str = Field(
        min_length=1,
        description=(
            "The MCE whose InfraEnv the VM is for, with or without its `ocp4-` prefix — "
            "`prod-mce-bat-yam-a` and `ocp4-prod-mce-bat-yam-a` name the same cluster. Must be "
            "named `ocp4-<env>-mce-<site>-<letter>`: the site is read from it."
        ),
    )
    infra_env: str = Field(
        min_length=1,
        description=(
            "A KubeVirt InfraEnv, `<conf>[-<letter>]-zone-<zone>-<cpu>` (`x201-zone-a-8`): the zone "
            "picks the KubeVirt clusters, the number the VM's vCPUs (memory is cpu × 8 GiB). A "
            "`-metal` InfraEnv has servers, not VMs, and is refused."
        ),
    )
    namespace: str = Field(
        default=_DEFAULT_NAMESPACE,
        min_length=1,
        description=(
            f"The VM's namespace on the KubeVirt cluster — the InfraEnv's namespace on the MCE, "
            f"verbatim; must exist there as a kubevirt-redfish chassis namespace. Defaults to `{_DEFAULT_NAMESPACE}`."
        ),
    )
    count: int | None = Field(
        default=None,
        ge=1,
        le=_MAX_CREATES_PER_REQUEST,
        description=(
            f"Create this many VMs, one run per free slot, answered with a per-run report; "
            f"at most {_MAX_CREATES_PER_REQUEST}. Without it: one run, `{{workflow_id, run_id}}`."
        ),
    )

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_named_mce(cls, value: str) -> str:
        name = canonical_mce_cluster(value)
        site_of_mce_cluster(name)  # MceNameError is a ValueError: a 422 naming the shape
        return name

    @field_validator("infra_env")
    @classmethod
    def _kubevirt_infraenv(cls, value: str) -> str:
        parsed = parse_infraenv(value)  # InfraEnvNameError is a ValueError: a 422
        if parsed.kind is not InfraEnvKind.KUBEVIRT:
            raise ValueError(
                f"InfraEnv {value!r} is bare metal (`-metal`): it takes servers from server-scan "
                "through add-server-to-mce, not VMs"
            )
        # The VM name the run will build must be a Kubernetes name: refused HERE,
        # because inside the run it would be a plain ValueError and hang the task (§5).
        vm_name(value, "000000")
        return value

    def run_input(self) -> CreateVmInput:
        """What ONE run carries: the request plus the site, zone and size derived here."""
        parsed = parse_infraenv(self.infra_env)
        assert parsed.cpu is not None and parsed.memory_gib is not None  # validated above
        return CreateVmInput(
            mce_cluster=self.mce_cluster, infra_env=self.infra_env, namespace=self.namespace,
            site=site_of_mce_cluster(self.mce_cluster), zone=parsed.zone,
            cpu=parsed.cpu, memory_gib=parsed.memory_gib,
        )


class CreateVmBatchResponse(BaseModel):
    """The answer to `count`: one item per slot looked at, in order — the busy ones
    stepped over, the ones started, a `failed` start if any (the scan stops there).
    Always 202 with the report, never one pass/fail code."""

    started: int
    already_running: int
    failed: int
    results: list[SlotStart]


@router.post(_CREATE_VM_PATH, response_model=StartWorkflowResponse | CreateVmBatchResponse, status_code=202)
async def start_create_vm(
    create_request: CreateVmRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse | CreateVmBatchResponse:
    """Create VirtualMachines for a KubeVirt InfraEnv — returns immediately (202).

    EVERY VM IS ITS OWN RUN on a slot of the InfraEnv on its MCE
    (`create-vm-<mce>_<infraEnv>_<slot>`), because the VM's name is drawn
    inside the run. Without `count`: one run, in the first free slot, answered
    with `{workflow_id, run_id}` — asking again while it runs starts a SECOND
    VM, never a 409 (only when all 100 slots are busy). With `count: N`: N runs
    in parallel, one per free slot, answered with a per-run report.

    Each run lists the zone's KubeVirt clusters from the day1 repo, reads
    every one's capacity, picks the best-fitting cluster under the headroom
    buffer, creates ONE Halted VM with a fixed MAC there and waits (bounded,
    10 min) for its root disk to bind. Nothing fitting, or a disk that never
    binds (the VM is deleted again), is a FAILED run — not a 4xx here.

    Poll GET /workflows/runs/{workflow_id} (one per run) for progress/result;
    the result carries the VM's name and MAC, the cluster and the ranking table.
    """
    run_args = CreateVmRunArgs(input=create_request.run_input())

    async def start(workflow_id: str):
        return await client.start_workflow(
            CreateVmWorkflow.run, run_args, id=workflow_id, task_queue=CREATE_VM_WORKFLOW_QUEUE,
        )

    results = await fill_free_slots(
        create_request.count or 1, _MAX_SLOTS,
        lambda slot: create_vm_slot_workflow_id(create_request.mce_cluster, create_request.infra_env, slot),
        start,
    )
    if create_request.count is not None:
        return CreateVmBatchResponse(**count_statuses(results), results=results)
    last = results[-1]
    if last.status == "started":
        return StartWorkflowResponse(workflow_id=last.workflow_id, run_id=last.run_id or "")
    if last.status == "failed":
        raise HTTPException(status_code=502, detail=f"Temporal did not start {last.workflow_id}: {last.error}")
    raise HTTPException(
        status_code=409,
        detail=(
            f"Every slot of InfraEnv {create_request.infra_env} on {create_request.mce_cluster} "
            f"(`_1` to `_{_MAX_SLOTS}`) has a create-vm run in flight"
        ),
    )
