"""UPI-lifecycle HTTP surface — `POST /workflows/upi-lifecycle/create-upi-cluster` and `/add-server-to-upi`.

ASYNC triggers: 202 with the workflow id at once; the caller polls
GET /workflows/runs/{workflow_id} (workflow_domains/routers/runs.py) for progress
and the result. The request models are STRICT (`extra="forbid"`, every name held
to its shape here) — the history-decoded inputs are not (CLAUDE.md §5).
"""

from __future__ import annotations

import re
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.cluster_names import name_token, upi_cluster_name
from shared.consts import ADD_SERVER_TO_UPI_WORKFLOW_QUEUE, CREATE_UPI_CLUSTER_WORKFLOW_QUEUE
from shared.models.upi_lifecycle import (
    AddServerToUpiInput,
    AddServerToUpiRunArgs,
    CreateUpiClusterInput,
    CreateUpiClusterRunArgs,
    UpiClusterKind,
    run_owned_install_keys,
)
from shared.workflow_ids import add_server_to_upi_workflow_id, create_upi_cluster_workflow_id
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.upi_lifecycle.add_server_to_upi import AddServerToUpiWorkflow
from workflow_domains.upi_lifecycle.create_upi_cluster import CreateUpiClusterWorkflow

router = APIRouter(prefix="/workflows/upi-lifecycle", tags=["upi-lifecycle"])

_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_LABEL_KEY = re.compile(r"^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
MAX_MCE_VALUES_CHARS = 64 * 1024


class CreateUpiClusterRequest(BaseModel):
    """The create-upi-cluster body. Strict: a misspelt field is a 422, not ignored."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(
        min_length=1, description="The cluster's name (≤27 lowercase letters, digits, inner hyphens): its Segments "
        "Manager cluster, Argo CD cluster and — for an MCE — day1 folder. Follow the naming convention: an MCE is "
        "`ocp4-<lifecycle>-mce-<site>-<id>`.", examples=["ocp4-prod-mce-tlv-a"],
    )
    site: str = Field(min_length=1, description="The site: a Segments Manager site, a bootstrap-installer site "
                      "profile and (for an MCE) a day1 `sites/<site>/` folder.", examples=["tlv"])
    kind: UpiClusterKind = Field(default=UpiClusterKind.MCE, description="`mce` (MCE + INVENTORY segments, the "
                                 "day1 MCE folder) or `standalone` (a UPI segment, no day1 folder).")
    vm_network: str | None = Field(default=None, description="The vSphere port group; default VM_NETWORK_TEMPLATE "
                                   "of the upi-provisioning worker filled from the allocated segment.")
    install_config: dict[str, Any] = Field(default_factory=dict, description="Extra bootstrap-installer keys on "
                                           "top of its defaults and the site profile: sizing, `compute_ip_offsets`, "
                                           "`image_mirrors`… Never the run's own keys (segment, names, secrets, argocd_*).")
    mce_values: str | None = Field(default=None, max_length=MAX_MCE_VALUES_CHARS, description="(kind mce) The "
                                   "text of the MCE's day1 `values.yaml`; empty = the site's and global layers alone.")
    requested_by: str | None = Field(default=None, max_length=128)

    @field_validator("cluster")
    @classmethod
    def _cluster(cls, value: str) -> str:
        return upi_cluster_name(value.strip())  # UpiClusterNameError is a ValueError: a 422

    @field_validator("site")
    @classmethod
    def _site(cls, value: str) -> str:
        return name_token(value.strip(), "site")  # MceNameError is a ValueError: a 422

    @field_validator("install_config")
    @classmethod
    def _install_config(cls, value: dict[str, Any]) -> dict[str, Any]:
        owned = run_owned_install_keys(value)
        if owned:
            raise ValueError(f"install_config sets keys the run owns: {', '.join(owned)}")
        return value

    @model_validator(mode="after")
    def _mce_values_only_for_mce(self) -> CreateUpiClusterRequest:
        if self.mce_values is not None and self.kind != UpiClusterKind.MCE:
            raise ValueError("mce_values is the day1 MCE folder's values.yaml: only for kind `mce`")
        return self

    def run_input(self) -> CreateUpiClusterInput:
        return CreateUpiClusterInput(**self.model_dump())


class AddServerToUpiRequest(BaseModel):
    """The add-server-to-upi body. Strict."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1, description="The UPI cluster, exactly as create-upi-cluster was given it: "
                         "its upi-server worker polls `upi-server-activity-<cluster>`.", examples=["ocp4-prod-mce-tlv-a"])
    server_name: str = Field(min_length=1, max_length=253, description="The server's name in server-scan.")
    hostname: str | None = Field(default=None, description="The Node's name; default the server's name, lowercased.")
    root_device: str | None = Field(default=None, description="The install disk (`/dev/...`); default the worker's "
                                    "DEFAULT_ROOT_DEVICE.")
    bond_mode: Literal["802.3ad", "active-backup"] = Field(default="802.3ad", description="bond0's mode; the "
                                                           "switch ports are configured for 802.3ad.")
    node_labels: dict[str, str] = Field(default_factory=dict, description="Labels set on the Node once Ready.")
    join_timeout_minutes: int = Field(default=90, ge=10, le=480)
    requested_by: str | None = Field(default=None, max_length=128)

    @field_validator("cluster")
    @classmethod
    def _cluster(cls, value: str) -> str:
        return upi_cluster_name(value.strip())

    @field_validator("hostname")
    @classmethod
    def _hostname(cls, value: str | None) -> str | None:
        if value is not None and not _DNS_LABEL.fullmatch(value.lower()):
            raise ValueError(f"hostname {value!r} must be a DNS label (it is the Node's name)")
        return value.lower() if value else value

    @field_validator("root_device")
    @classmethod
    def _root_device(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"/dev/[A-Za-z0-9/_.:-]+", value):
            raise ValueError(f"root_device {value!r} must be a /dev/ path")
        return value

    @field_validator("node_labels")
    @classmethod
    def _labels(cls, value: dict[str, str]) -> dict[str, str]:
        bad = [k for k in value if not _LABEL_KEY.fullmatch(k)]
        if bad:
            raise ValueError(f"node_labels keys that are not Kubernetes label keys: {', '.join(sorted(bad))}")
        return value

    @model_validator(mode="after")
    def _server_name_as_hostname(self) -> AddServerToUpiRequest:
        if self.hostname is None and not _DNS_LABEL.fullmatch(self.server_name.lower()):
            raise ValueError(f"server_name {self.server_name!r} is not a DNS label; send `hostname` for the Node")
        return self

    def run_input(self) -> AddServerToUpiInput:
        return AddServerToUpiInput(**self.model_dump())


def _conflict(workflow: str, workflow_id: str) -> HTTPException:
    return HTTPException(status_code=409, detail=f"{workflow} is already running: {workflow_id}")


@router.post("/create-upi-cluster", status_code=202, response_model=StartWorkflowResponse)
async def start_create_upi_cluster(
    body: CreateUpiClusterRequest, client: Client = Depends(get_temporal_client)
) -> StartWorkflowResponse:
    """Build an OpenShift UPI cluster on vSphere — returns immediately (202).

    The run allocates the segments (allocate-segment children), runs
    bootstrap-installer as two Jobs on the hub (prepare, install), waits until
    the cluster serves, registers it in the hub's Argo CD and — for an MCE —
    opens the merge request adding it to day1's MCEs and WAITS for a person to
    merge it. Every failure is a FAILED run on GET /workflows/runs/{workflow_id};
    a second request for the same cluster while its run goes is a 409.

    This API has no auth of its own (CLAUDE.md §9): this route creates VMs.
    """
    workflow_id = create_upi_cluster_workflow_id(body.cluster)
    try:
        handle = await client.start_workflow(
            CreateUpiClusterWorkflow.run,
            CreateUpiClusterRunArgs(input=body.run_input()),
            id=workflow_id,
            task_queue=CREATE_UPI_CLUSTER_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise _conflict("create-upi-cluster", workflow_id) from None
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


@router.post("/add-server-to-upi", status_code=202, response_model=StartWorkflowResponse)
async def start_add_server_to_upi(
    body: AddServerToUpiRequest, client: Client = Depends(get_temporal_client)
) -> StartWorkflowResponse:
    """Join ONE named server to a UPI cluster as a Node — returns immediately (202).

    The run finds the cluster's node VLAN, takes the server from server-scan
    (locked), picks two link-UP NICs for bond0, builds the node-image ISO on the
    cluster's upi-server worker, boots it through the BMC's virtual media,
    approves the host's CSRs until its Node is Ready, labels it and completes.
    A second request for the same server and cluster while its run goes is a 409.
    """
    workflow_id = add_server_to_upi_workflow_id(body.cluster, body.server_name)
    try:
        handle = await client.start_workflow(
            AddServerToUpiWorkflow.run,
            AddServerToUpiRunArgs(input=body.run_input()),
            id=workflow_id,
            task_queue=ADD_SERVER_TO_UPI_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise _conflict("add-server-to-upi", workflow_id) from None
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")

