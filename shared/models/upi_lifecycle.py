"""UPI-lifecycle contract — the models crossing the boundary for the `upi-lifecycle` domain.

Two workflows (2026-11-27, with the operator):

  * create-upi-cluster — an OpenShift UPI `platform: none` cluster on vSphere,
    built by the team's bootstrap-installer run as Kubernetes Jobs on the hub, then
    verified, registered in the hub's Argo CD and — for an MCE — added to day1's
    list of MCEs through a merge request a person merges.
  * add-server-to-upi — ONE bare-metal server joined to a running UPI cluster as a
    Node: an `oc adm node-image create` ISO with bond0 over two link-UP NICs and a
    DHCP VLAN on it, mounted through the BMC's Redfish virtual media.

Two limbs, so two interface modules: `upi_provisioning` on the hub (the Jobs, the
installed cluster's admin kubeconfig, Argo CD's cluster Secret) and `upi_server`
INSIDE each UPI cluster (node-image, CSRs, Nodes, the BMCs and server-scan).

Every history-decoded model follows CLAUDE.md §5: new fields default to None, no
`extra="forbid"` (strictness is on the router's request models).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from shared.models.cluster_lifecycle import MergeRequestRef
from shared.models.segment_lifecycle import AllocateSegmentResult, SegmentType
from shared.models.server_lifecycle import AcquiredServer, BondMember


class UpiClusterKind(str, Enum):
    """What the UPI cluster is FOR — it decides its segment types and its GitOps registration.

    `mce`: an MCE cluster (3 masters + 3 infra). Its node network is an MCE
    segment, it also gets the INVENTORY segment add-server-to-mce boots
    BareMetalHosts on, and it is added to day1's `sites/<site>/mces/<cluster>/`
    so the hub's Argo CD (the `<site>-mces` ApplicationSet) starts managing it.
    `standalone`: any other UPI cluster. A UPI segment, no day1 folder.
    """

    MCE = "mce"
    STANDALONE = "standalone"


def node_segment_type(kind: UpiClusterKind) -> SegmentType:
    """The segment type of a UPI cluster's node network, by its kind."""
    return SegmentType.MCE if kind == UpiClusterKind.MCE else SegmentType.UPI


class InstallerStep(str, Enum):
    """One bootstrap-installer invocation, run as one Kubernetes Job on the hub.

    `prepare` = `bootstrap.py --skip-terraform --skip-argocd`: the IPs from the
    segment, the rendered install-config / tfvars / v4InternalSubnet, the
    ignition configs and the `*.apps` wildcard DNS record; the Job then stores
    the cluster's admin kubeconfig and cluster-context.yaml in a Secret.
    `install` = `bootstrap.py --skip-ignition --skip-dns --skip-argocd`:
    `terraform apply` (the VMs and their DNS records) and bootstrap-installer's
    own CSR approval loop. Argo CD registration is the limb's, never the Job's.
    """

    PREPARE = "prepare"
    INSTALL = "install"


# --- create-upi-cluster --------------------------------------------------------------------


# Keys of bootstrap-installer's cluster YAML that the RUN owns: `install_config`
# may set anything else of its three layers (sizing, IP offsets, image_mirrors…).
# Refused by the router (a 422) and again by the limb (InstallerConfigRejectedError).
INSTALL_CONFIG_RUN_OWNED_KEYS = frozenset({
    "cluster_name", "site", "segment", "vlan_id", "vm_network",
    "pull_secret_path", "additional_trust_bundle_path", "ssh_public_key_path",
    "terraform_dir",
})
_ARGOCD_PREFIX = "argocd_"


def run_owned_install_keys(install_config: dict[str, Any]) -> list[str]:
    """The keys of `install_config` the run owns, sorted (every `argocd_*` key included)."""
    return sorted(
        str(k) for k in install_config
        if k in INSTALL_CONFIG_RUN_OWNED_KEYS or str(k).startswith(_ARGOCD_PREFIX)
    )


class CreateUpiClusterInput(BaseModel):
    """What to build. Decoded from history on every replay: new fields default to None.

    `install_config` is merged on top of bootstrap-installer's three layers
    (defaults → site profile → this), as the cluster YAML it reads: node sizing,
    `compute_ip_offsets`, `image_mirrors`… The keys the run owns (`cluster_name`,
    `site`, `segment`, `vlan_id`, the secret paths, every `argocd_*` key) are
    refused at the edge and again by the limb.
    `mce_values` is the text of the MCE's day1 `values.yaml` (kind `mce` only;
    None writes a values file holding only a header comment — the site's and the
    global layers then apply alone). The MCE's `version.yaml` is written from the
    installed cluster's own version, never from input.
    """

    cluster: str = Field(min_length=1)
    site: str = Field(min_length=1)
    kind: UpiClusterKind = UpiClusterKind.MCE
    vm_network: str | None = None
    install_config: dict[str, Any] = Field(default_factory=dict)
    mce_values: str | None = None
    requested_by: str | None = None


class SegmentAllocations(BaseModel):
    """The cluster's segments: its node network, and an MCE's inventory network."""

    node: AllocateSegmentResult
    inventory: AllocateSegmentResult | None = None


class InstallerStepRequest(BaseModel):
    """Run one bootstrap-installer step as a Job and wait for it (heartbeating)."""

    cluster: str = Field(min_length=1)
    site: str = Field(min_length=1)
    step: InstallerStep
    # Fixed for the whole run (stored in its state): every attempt of an activity
    # finds the SAME Job by name, and a new run of the same cluster makes new Jobs.
    run_token: str = Field(min_length=4, max_length=8)
    segment: str = Field(min_length=1)
    vlan_id: int = Field(ge=1, le=4094)
    epg_name: str
    vm_network: str | None = None
    install_config: dict[str, Any] = Field(default_factory=dict)


class InstallerStepResult(BaseModel):
    """A Job that completed. A failed Job is InstallerJobFailedError, never a result."""

    step: InstallerStep
    job_name: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    log_tail: str = ""


class ClusterReadyRequest(BaseModel):
    """Wait until the installed cluster serves: every expected node Ready, ClusterVersion Available."""

    cluster: str = Field(min_length=1)
    timeout_seconds: int = Field(ge=60, le=4 * 3600)


class UpiNode(BaseModel):
    name: str
    internal_ip: str | None = None
    ready: bool
    roles: list[str] = Field(default_factory=list)


class UpiClusterStatus(BaseModel):
    """The installed cluster as its own API reports it."""

    api_url: str
    version: str | None = None
    nodes: list[UpiNode] = Field(default_factory=list)
    expected_ips: list[str] = Field(default_factory=list)
    csrs_approved: list[str] = Field(default_factory=list)


class ArgoRegistrationRequest(BaseModel):
    cluster: str = Field(min_length=1)
    kind: UpiClusterKind


class ArgoRegistration(BaseModel):
    """The hub Argo CD cluster Secret, read back: `name` is the destination name the
    hub's ApplicationSets target — for an MCE, its day1 folder name."""

    secret_name: str
    namespace: str
    name: str
    server: str
    changed: bool


class MceFolderPublishRequest(BaseModel):
    """The MCE's day1 folder (`sites/<site>/mces/<mce>/`: values.yaml + version.yaml) on its branch."""

    mce: str = Field(min_length=1)
    site: str = Field(min_length=1)
    branch: str = Field(min_length=1)
    values: str | None = None
    mastertag: str = Field(min_length=1)


class MceFolderRef(BaseModel):
    """Where the MCE folder is. `on_main`: day1 main already has it, with this
    run's content (a re-run past the merge) — nothing pushed, no merge request."""

    folder: str
    files: list[str] = Field(default_factory=list)
    branch: str
    commit_sha: str | None = None
    changed: bool
    on_main: bool = False


class CreateUpiClusterState(BaseModel):
    """What the run has done, carried across continue-as-new (the merge wait)."""

    run_token: str | None = None
    segments: SegmentAllocations | None = None
    prepared: InstallerStepResult | None = None
    installed: InstallerStepResult | None = None
    status: UpiClusterStatus | None = None
    argo: ArgoRegistration | None = None
    mce_folder: MceFolderRef | None = None
    merge_request: MergeRequestRef | None = None
    merged_sha: str | None = None
    waited_since: datetime | None = None
    polls: int = 0


class CreateUpiClusterRunArgs(BaseModel):
    input: CreateUpiClusterInput
    state: CreateUpiClusterState | None = None


class CreateUpiClusterProgress(BaseModel):
    phase: str
    segment: str | None = None
    vlan_id: int | None = None
    job_name: str | None = None
    api_url: str | None = None
    nodes_ready: int | None = None
    merge_request_url: str | None = None
    merge_request_state: str | None = None
    waited_since: datetime | None = None


class CreateUpiClusterResult(BaseModel):
    cluster: str
    site: str
    kind: UpiClusterKind
    segment: str
    vlan_id: int
    epg_name: str
    inventory_segment: str | None = None
    inventory_vlan_id: int | None = None
    api_url: str
    version: str | None = None
    nodes: list[UpiNode] = Field(default_factory=list)
    argo_secret: str
    mce_folder: str | None = None
    merge_request_url: str | None = None
    merged_sha: str | None = None


# --- add-server-to-upi ---------------------------------------------------------------------


class AddServerToUpiInput(BaseModel):
    """One server into one UPI cluster as a Node. Decoded from history: new fields default to None.

    `cluster` is the UPI cluster's name — the suffix of the per-cluster queue its
    upi-server worker polls. `hostname` defaults to the server's name lowercased
    and becomes the Node's name. `node_labels` are set on the Node once it is
    Ready (its role comes from the worker ignition: `node-role.kubernetes.io/worker`).
    """

    cluster: str = Field(min_length=1)
    server_name: str = Field(min_length=1)
    hostname: str | None = None
    root_device: str | None = None
    bond_mode: str = "802.3ad"
    node_labels: dict[str, str] = Field(default_factory=dict)
    join_timeout_minutes: int = Field(default=90, ge=10, le=480)
    requested_by: str | None = None


class AddServerToUpiRunArgs(BaseModel):
    input: AddServerToUpiInput


class AllocateUpiServerRequest(BaseModel):
    """server-scan's `POST /servers/available` for a NAMED server, locked for the run."""

    server_name: str = Field(min_length=1)
    cluster: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    ttl_seconds: int = Field(ge=300, le=86_400)


class UpiServerLockRequest(BaseModel):
    """Extend (after the join) or give back (a server the run cannot use) its server-scan lock."""

    server: AcquiredServer
    cluster: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    ttl_seconds: int = Field(ge=300, le=86_400)


class UpiNodeInspectRequest(BaseModel):
    hostname: str = Field(min_length=1)


class UpiNodeState(BaseModel):
    """The Node of that name, if the cluster has one."""

    exists: bool
    ready: bool = False
    macs: list[str] = Field(default_factory=list)


class NodeIsoRequest(BaseModel):
    """Build the node-image ISO for one host: its nodes-config.yaml, rendered by the limb."""

    hostname: str = Field(min_length=1)
    root_device: str | None = None
    members: list[BondMember] = Field(min_length=2, max_length=2)
    bond_mode: str = Field(min_length=1)
    vlan_id: int = Field(ge=1, le=4094)


class NodeIso(BaseModel):
    """The ISO the BMC fetches: `url` under the worker's ISO_BASE_URL."""

    file_name: str
    url: str
    config_sha256: str
    size_bytes: int
    built: bool


class VirtualMediaBootRequest(BaseModel):
    """Insert the ISO through the server's Redfish virtual media and boot it once."""

    server: AcquiredServer
    iso_url: str = Field(min_length=1)


class VirtualMediaBoot(BaseModel):
    system_path: str
    media_path: str
    reset_type: str


class NodeJoinRequest(BaseModel):
    """Approve this host's CSRs (and only this host's) until its Node is Ready."""

    hostname: str = Field(min_length=1)
    timeout_seconds: int = Field(ge=300, le=8 * 3600)


class NodeJoined(BaseModel):
    node: UpiNode
    csrs_approved: list[str] = Field(default_factory=list)
    waited_seconds: int = 0


class FinishUpiNodeRequest(BaseModel):
    """After the join: the Node's labels, the media ejected, the ISO removed."""

    server: AcquiredServer
    hostname: str = Field(min_length=1)
    iso_file_name: str = Field(min_length=1)
    node_labels: dict[str, str] = Field(default_factory=dict)


class FinishedUpiNode(BaseModel):
    labelled: bool
    media_ejected: bool
    iso_removed: bool
    notes: list[str] = Field(default_factory=list)


class AddServerToUpiProgress(BaseModel):
    phase: str
    cluster: str | None = None
    server_name: str | None = None
    hostname: str | None = None
    vlan_id: int | None = None
    bond_macs: list[str] = Field(default_factory=list)
    iso_url: str | None = None
    waited_seconds: int | None = None


class AddServerToUpiResult(BaseModel):
    cluster: str
    server_name: str
    server_id: str
    hostname: str
    vlan_id: int
    segment: str
    bond_macs: list[str]
    bmc_vendor: str
    iso_url: str | None = None
    node: UpiNode
    already_joined: bool = False
    csrs_approved: list[str] = Field(default_factory=list)
    finished: FinishedUpiNode | None = None
