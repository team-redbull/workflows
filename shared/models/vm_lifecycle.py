"""Typed state for the vm-lifecycle domain: create-vm's input, progress and result, the
activity requests and answers of its one hub limb, and the KubeVirt agents' wire contract.

No `extra="forbid"` on any model here: these cross the workflow/activity
boundary and are decoded from history (CLAUDE.md §5). The API edge's
`CreateVmRequest` (workflow_domains/vm_lifecycle/router.py) is the strict one.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from shared.cluster_names import name_token

# --- the run ----------------------------------------------------------------------------


class CreateVmInput(BaseModel):
    """Input to CreateVmWorkflow: which InfraEnv needs a VM, and everything derived from it.

    `site`, `zone`, `cpu` and `memory_gib` are derived ONCE at the API edge
    (from the MCE's and the InfraEnv's names, shared/cluster_names.py and
    shared/infraenv_names.py) and carried, never re-derived on replay — the
    provision-dell-server region precedent. `namespace` is the VM's namespace
    on the chosen KubeVirt cluster, verbatim: a kubevirt-redfish chassis
    namespace the platform created, never this run.
    """

    mce_cluster: str = Field(min_length=1)
    infra_env: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    site: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)


class CreateVmRunArgs(BaseModel):
    """The workflow's single argument (see AddServerToMceRunArgs for why a wrapper)."""

    input: CreateVmInput


class KvClusterRef(BaseModel):
    """One KubeVirt cluster of the zone, as the workflow and the limb name it.

    `name` is the day1 file's stem (`ocp4-kubevirt-bat-yam-a1`), `cluster_name`
    the chart's clusterName (the file's own, else the name minus the prefix),
    `hosted_cluster` the HostedCluster the chart renders (`openshift-<clusterName>`).
    The limb renders the agents' URLs from these and the site/zone
    (`KV_*_URL_TEMPLATE`); `agent_host` is what it rendered, for the table.
    """

    name: str = Field(min_length=1)
    cluster_name: str = Field(min_length=1)
    hosted_cluster: str = Field(min_length=1)
    site: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    agent_host: str | None = None


class KvClusterRank(BaseModel):
    """One row of the ranking table: what the cluster's agent reported and what was decided.

    Fills are ratios of the cluster's schedulable allocatable (0..1, over 1 when
    overcommitted); `*_after` includes this VM. `halted_reservations` is how
    many Halted, launcher-less VMs of ours the fill counts. `skip_reason` is a
    key of placement.SKIP_REASONS (mirrored by the workflow's `_SKIP_SUMMARY`).
    """

    name: str
    cluster_name: str
    reachable: bool
    fits: bool
    skip_reason: str | None = None
    skip_detail: str | None = None
    cpu_fill_before: float | None = None
    cpu_fill_after: float | None = None
    memory_fill_before: float | None = None
    memory_fill_after: float | None = None
    max_node_cpu_headroom_millicores: int | None = None
    max_node_memory_headroom_bytes: int | None = None
    halted_reservations: int = 0
    agent_host: str | None = None


class KvPlacement(BaseModel):
    """The ranked table, chosen first, then the fitting, then the skipped.

    `chosen` is the chosen cluster's `name`, None when nothing fits.
    `known_macs` is every VM MAC the reachable clusters reported, lowercased,
    so the workflow draws a suffix not in use anywhere in the zone (the bridge
    network spans every KV cluster of it).
    """

    ranked: list[KvClusterRank]
    chosen: str | None = None
    buffer_percent: int
    pod_cpu_millicores: int
    pod_memory_bytes: int
    known_macs: list[str] = Field(default_factory=list)


class CreateVmProgress(BaseModel):
    """Returned by the `progress` query."""

    phase: str
    kv_candidates: list[str] = Field(default_factory=list)
    placement: KvPlacement | None = None
    kv_cluster: str | None = None
    vm_name: str | None = None
    mac: str | None = None
    redraws: int = 0


class CreateVmResult(BaseModel):
    """The VM this run created, Halted, on the cluster it chose.

    `created` False means ONLY that an activity retry inside this run found its
    own earlier PUT had landed (kv-vm-api answered 200 for the same identity);
    a re-run of the slot is a new VM with a new suffix. `redraws` counts the
    suffixes given up: drawn MACs already in the zone, and 409s from the agent.
    """

    vm_name: str
    mac: str
    namespace: str
    cpu: int
    memory_gib: int
    kv_cluster: KvClusterRef
    placement: KvPlacement
    redraws: int = 0
    root_disk_phase: str | None = None
    created: bool = True
    infra_env: str
    mce_cluster: str
    site: str
    zone: str


# --- the day1 list ----------------------------------------------------------------------


class KvClusterListRequest(BaseModel):
    """Which zone's seed folder to list; the tokens are validated so the path they
    build holds no `/`, `..` or glob character."""

    site: str
    zone: str

    @field_validator("site")
    @classmethod
    def _site(cls, value: str) -> str:
        return name_token(value, "site")

    @field_validator("zone")
    @classmethod
    def _zone(cls, value: str) -> str:
        return name_token(value, "zone")


class KvClusterEntry(BaseModel):
    """One file of the seed folder. `problems` is what the layer reader could not use
    (a key written twice, a layer that is not a mapping) — reported, never fatal."""

    name: str
    cluster_name: str
    hosted_cluster: str
    path: str
    problems: list[str] = Field(default_factory=list)


class KvClusterList(BaseModel):
    clusters: list[KvClusterEntry] = Field(default_factory=list)
    revision: str


# --- placement ---------------------------------------------------------------------------


class KvPlacementRequest(BaseModel):
    candidates: list[KvClusterRef]
    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)


# --- the VM --------------------------------------------------------------------------------


class VmCreateRequest(BaseModel):
    """One PUT of the VM `name` on `kv_cluster`. The limb composes the MAC from
    `suffix_hex` (`VM_MAC_PREFIX:xx:xx:xx`); `labels` are extra VM labels (none today)."""

    kv_cluster: KvClusterRef
    namespace: str = Field(min_length=1)
    name: str = Field(min_length=1)
    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)
    suffix_hex: str = Field(pattern=r"^[0-9a-f]{6}$")
    labels: dict[str, str] = Field(default_factory=dict)


class VmCreated(BaseModel):
    """What kv-vm-api holds after the PUT. `created` False: it already held this identity."""

    name: str
    namespace: str
    mac: str
    cpu: int
    memory_gib: int
    created: bool
    kv_cluster: str


class VmReadyRequest(BaseModel):
    kv_cluster: KvClusterRef
    namespace: str = Field(min_length=1)
    name: str = Field(min_length=1)
    deadline_minutes: int = Field(default=10, ge=1)


class VmReadyOutcome(BaseModel):
    """`done`: the root disk is bound and the VM admitted. `deadline`: not within
    `deadline_minutes`, with the last state seen in the fields below."""

    result: Literal["done", "deadline"]
    root_disk_phase: str | None = None
    root_disk_bound: bool = False
    admitted: bool = False
    mac: str | None = None
    detail: str | None = None
    polls: int = 0


class VmDeleteRequest(BaseModel):
    """DELETE the run's own VM: by name AND by the MAC the run gave it."""

    kv_cluster: KvClusterRef
    namespace: str = Field(min_length=1)
    name: str = Field(min_length=1)
    mac: str = Field(min_length=1)


class VmDeleted(BaseModel):
    """`deleted` False: the VM was already gone (404) — success all the same."""

    deleted: bool


# --- the agents' wire contract (vendored; tests/test_kv_agents_contract.py keeps the
# --- agents' copy equal) --------------------------------------------------------------
#
# Per KubeVirt cluster, two services behind Routes (their own repo, kv-agents):
#   kv-capacity  GET  /v1/capacity                      -> KvCapacityReport
#   kv-vm-api    PUT  /v1/vms/{name}   KvVmCreateRequest -> 201/200 KvVmCreateResponse,
#                                                          409 KvVmConflict, 404/422/401 KvApiError
#                GET  /v1/vms/{name}?namespace=         -> KvVmState | 404
#                DELETE /v1/vms/{name}?namespace=&mac=  -> 204 | 404 | 409 KvVmConflict
# Integers only on the wire: millicores and bytes, never Kubernetes quantities.


class KvNodeCapacity(BaseModel):
    """One node: allocatable, and the requests of its Pending|Running pods (the
    kube-scheduler's own view). `schedulable` = not cordoned AND Ready."""

    name: str
    schedulable: bool
    allocatable_cpu_millicores: int
    allocatable_memory_bytes: int
    requested_cpu_millicores: int
    requested_memory_bytes: int


class KvHaltedReservation(BaseModel):
    """A Halted VM of ours (labelled for kubevirt-redfish) with NO launcher pod: it
    consumes nothing on the scheduler yet, so the agent reports what its
    launcher WILL request and placement counts it toward the cluster's fill."""

    namespace: str
    name: str
    cpu_millicores: int
    memory_bytes: int


class KvVmIdentity(BaseModel):
    """Every VM the agent sees, running or not, by its MAC — the zone-wide set a
    new suffix must stay out of. `mac` is None for a VM with no fixed MAC."""

    namespace: str
    name: str
    mac: str | None = None


class KvCapacityReport(BaseModel):
    cluster: str
    observed_at: str
    nodes: list[KvNodeCapacity] = Field(default_factory=list)
    halted_reservations: list[KvHaltedReservation] = Field(default_factory=list)
    vms: list[KvVmIdentity] = Field(default_factory=list)


class KvVmCreateRequest(BaseModel):
    """The body of PUT /v1/vms/{name}: the agent renders the platform's vm.yaml
    template with these and its per-cluster values (storage class, network,
    node selector); the VM is created Halted and labelled `redfish-enabled=true`."""

    namespace: str
    cpu: int
    memory_gib: int
    mac: str
    labels: dict[str, str] = Field(default_factory=dict)


class KvVmCreateResponse(BaseModel):
    name: str
    namespace: str
    mac: str
    cpu: int
    memory_gib: int
    created: bool


class KvVmConflict(BaseModel):
    """409: a VM of this name exists with ANOTHER identity (namespace, MAC, size or
    our labels differ; runStrategy is never compared). `existing` describes it."""

    reason: str
    existing: KvVmState | None = None


class KvRootDisk(BaseModel):
    name: str
    phase: str | None = None
    bound: bool = False


class KvVmState(BaseModel):
    """GET /v1/vms/{name}: the VM as the agent reads it. `admitted`: KubeVirt accepted
    the spec (no `FailedCreate`-style condition); `root_disk` is its DataVolume's
    phase and whether its PVC is Bound."""

    name: str
    namespace: str
    mac: str | None = None
    cpu: int
    memory_gib: int
    run_strategy: str | None = None
    admitted: bool = False
    root_disk: KvRootDisk | None = None


class KvApiError(BaseModel):
    """Every non-2xx body: a short machine-readable `error` and a sentence for a person."""

    error: str
    detail: str = ""
