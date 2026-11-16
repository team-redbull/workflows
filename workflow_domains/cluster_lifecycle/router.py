"""Cluster-lifecycle HTTP surface — ONE path today, `POST /workflows/cluster-lifecycle/provision-cluster`.

ASYNC trigger: 202 with the workflow id at once; the caller polls
GET /workflows/runs/{workflow_id} (workflow_domains/routers/runs.py) for
progress and the result. PATHS ARE `/workflows/<domain>/<workflow>`: the
domain prefix lives here, and every workflow of the domain adds its own path.

THE BODY IS THE HOSTED-CLUSTER FILE, not JSON (the operator, 2026-11-10): a
person sends the YAML they would have committed to day1, and Swagger's "Try it
out" opens a text area holding EXAMPLE_CLUSTER_FILE for them to edit in place
(FastAPI's `openapi_extra` request body, `application/yaml`). The cluster's
name and its MCE folder are query parameters — they are not keys of the file:
the file's NAME is the cluster and its FOLDER the MCE. The text reaches the
run VERBATIM; the edge only refuses what could not be a cluster file at all
(empty, too large, not UTF-8, not one YAML mapping) — everything semantic is
judged in the run against the day1 value layers, where a rejected spec is a
FAILED run naming every reason (CLAUDE.md §4).
"""

from __future__ import annotations

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.cluster_names import HostedClusterNameError, MceNameError, hosted_cluster_stem, name_token
from shared.consts import PROVISION_CLUSTER_WORKFLOW_QUEUE
from shared.models.cluster_lifecycle import ProvisionClusterInput, ProvisionClusterRunArgs
from shared.workflow_ids import provision_cluster_workflow_id
from workflow_domains.cluster_lifecycle.provision_cluster import ProvisionClusterWorkflow
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse

router = APIRouter(prefix="/workflows/cluster-lifecycle", tags=["cluster-lifecycle"])

_PROVISION_CLUSTER_PATH = "/provision-cluster"
# A cluster file is a few KiB; this is a guard against a mistaken upload, not a quota.
MAX_BODY_BYTES = 256 * 1024
YAML_CONTENT_TYPE = "application/yaml"

# The shape every hosted-cluster file has before the day1 CI chain works on it,
# and what Swagger shows pre-filled. Also the README's and the docs page's
# example, from this one constant (tests/test_cluster_lifecycle_api.py pins it),
# and a file the run's validator accepts, so the default shown is always valid.
EXAMPLE_CLUSTER_FILE = """\
owner:
  merkaz: "unit-a"
  anaf: "unit-b"
  mador: "unit-c"

mastertag: 4.22.8

nodepool:
- infraEnvName: dell-r660-site4-128c-1024gb-5tb
  replicas: 2
  labels:
    allowDeletion: false
  configs:
  - name: nm-conf
  - name: worker-kubeletconfig-reserved
  - name: nic-ring-buffer
  - name: workers-chrony-configuration
"""

EXAMPLE_CLUSTER = "prep-e2e-site2-a"
EXAMPLE_MCE = "ocp4-prep-mce-site2-a"


def _422(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def parse_cluster_file(body: bytes) -> str:
    """The body as the text the run carries, or a 422: non-empty, bounded, UTF-8, ONE YAML mapping."""
    if not body or not body.strip():
        raise _422("the request body must be the hosted-cluster YAML file (it is empty)")
    if len(body) > MAX_BODY_BYTES:
        raise _422(f"the hosted-cluster file is {len(body)} bytes; at most {MAX_BODY_BYTES} (a cluster file is a few KiB)")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as err:
        raise _422(f"the hosted-cluster file must be UTF-8 text ({err.reason} at byte {err.start})") from None
    try:
        documents = list(yaml.safe_load_all(text))
    except yaml.YAMLError as err:
        raise _422(f"the hosted-cluster file is not valid YAML: {str(err).splitlines()[0]}") from None
    if len(documents) != 1:
        raise _422(f"the hosted-cluster file must be ONE YAML document (got {len(documents)})")
    if not isinstance(documents[0], dict) or not documents[0]:
        raise _422("the hosted-cluster file must be a YAML mapping of values (owner, mastertag, nodepool, …)")
    return text


@router.post(
    _PROVISION_CLUSTER_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "description": (
                "The hosted-cluster YAML file, as a person would commit it to day1 — WITHOUT vlanId, "
                "dhcp_values or mcConfigMap: the run appends the allocation, the day1 CI chain the "
                "MachineConfigs. Sent verbatim; comments and layout reach day1 unchanged."
            ),
            "content": {
                YAML_CONTENT_TYPE: {
                    "schema": {"type": "string", "format": "yaml"},
                    "example": EXAMPLE_CLUSTER_FILE,
                }
            },
        }
    },
)
async def start_provision_cluster(
    request: Request,
    cluster: str = Query(
        ...,
        min_length=1,
        description=(
            "The hosted cluster's name, with or without its `ocp4-` prefix — `prep-e2e-site2-a` and "
            "`ocp4-prep-e2e-site2-a` name the same cluster. It becomes the day1 file `ocp4-<name>.yaml`, "
            "the Argo CD Application and the Segments Manager's cluster. A lowercase DNS-1123 label of "
            "at most 24 characters."
        ),
        examples=[EXAMPLE_CLUSTER],
    ),
    mce: str = Query(
        ...,
        min_length=1,
        description=(
            "The MCE FOLDER the cluster goes in, exactly as named under day1's `sites/<site>/mces/` "
            "(`ocp4-prep-mce-site2-a`, `ocp4-mce-site2`). The site is read from where the folder is."
        ),
        examples=[EXAMPLE_MCE],
    ),
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Provision a hosted cluster from its YAML — returns immediately (202).

    The BODY is the cluster file (`Content-Type: application/yaml`; "Try it
    out" shows the slim shape to edit). `cluster` and `mce` are query
    parameters. The run reads day1 main for the MCE folder, judges the file
    against the value layers, allocates a segment (allocate-segment, as a
    child), pushes the file with the allocation block to
    `provision-cluster/<cluster>`, opens the merge request and WAITS for a
    person to merge it — no deadline — then registers the cluster in the day2
    sig repos the value layers list, and completes. A rejected file, an
    unknown MCE folder, a cluster already in day1 or a closed merge request
    is a FAILED run on GET /workflows/runs/{workflow_id}, not a 4xx here; a
    second request for the same cluster while its run waits is a 409.

    This API has no auth of its own (CLAUDE.md §9): this route pushes a
    branch and opens a merge request on the day1 repo.
    """
    try:
        stem = hosted_cluster_stem(cluster)
    except HostedClusterNameError as err:
        raise _422(str(err)) from None
    try:
        mce_folder = name_token(mce.strip(), "mce")
    except MceNameError as err:
        raise _422(str(err)) from None
    text = parse_cluster_file(await request.body())
    workflow_id = provision_cluster_workflow_id(stem)
    try:
        handle = await client.start_workflow(
            ProvisionClusterWorkflow.run,
            ProvisionClusterRunArgs(input=ProvisionClusterInput(cluster=stem, mce=mce_folder, values=text)),
            id=workflow_id,
            task_queue=PROVISION_CLUSTER_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"provision-cluster is already running for {stem}: {workflow_id} (it may be waiting for its merge request)",
        ) from None
    return StartWorkflowResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")
