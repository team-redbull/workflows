"""Typed state for the vm-lifecycle domain: create-vm's input, progress and result, the
zone lock's state and signals, and the activity requests and answers of the worker that
runs inside each KV cluster (plus the zone's cluster list, read by the cluster-lifecycle limb).

No `extra="forbid"` on any model here: these cross the workflow/activity
boundary and are decoded from history (CLAUDE.md §5). The API edge's
`CreateVmRequest` (workflow_domains/vm_lifecycle/router.py) is the strict one.

Every capacity number is an INTEGER, in millicores and bytes: nothing on the
hub parses a Kubernetes quantity string (docs/design/create-vm-placement.md §6.3.1).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from shared.cluster_names import name_token, zone_number

# --- the run ----------------------------------------------------------------------------


class CreateVmInput(BaseModel):
    """Input to CreateVmWorkflow: which InfraEnv needs a VM, and everything derived from it.

    `flavor`, `zone`, `cpu` and `memory_gib` are derived ONCE at the API edge
    from the InfraEnv's name (shared/infraenv_names.py), `site` and `lifecycle`
    from the MCE's (the InfraEnv's name carries no site), and carried, never
    re-derived on replay — the provision-dell-server region precedent.
    `lifecycle` (2026-11-22) picks the seeds whose KV clusters may hold the VM;
    None only in a run recorded before it, which the day1 list then refuses.
    `namespace` is the VM's namespace on the chosen KV cluster, verbatim: a
    kubevirt-redfish chassis namespace the platform created, never this run.
    """

    mce_cluster: str = Field(min_length=1)
    infra_env: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    flavor: str = Field(min_length=1)
    site: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)
    lifecycle: str | None = None


class CreateVmRunArgs(BaseModel):
    """The workflow's single argument (see AddServerToMceRunArgs for why a wrapper)."""

    input: CreateVmInput


# --- the zone's KV clusters, from day1 (cluster-lifecycle limb) ---------------------------


class KvClusterListRequest(BaseModel):
    """Which zone's KV clusters to list, and of which lifecycle's seeds (the MCE's); the
    tokens are validated so the day1 path they build holds no `/`, `..` or glob character.
    `lifecycle` is None only from a run recorded before it, and the limb refuses that."""

    site: str
    zone: str
    lifecycle: str | None = None

    @field_validator("site")
    @classmethod
    def _site(cls, value: str) -> str:
        return name_token(value, "site")

    @field_validator("zone")
    @classmethod
    def _zone(cls, value: str) -> str:
        zone_number(value)
        return value

    @field_validator("lifecycle")
    @classmethod
    def _lifecycle(cls, value: str | None) -> str | None:
        return None if value is None else name_token(value, "lifecycle")


class KvClusterEntry(BaseModel):
    """One file of a seed folder of the zone.

    `name` is the day1 file's stem, and so the cluster's name everywhere: the
    queue its worker polls and the `KV_CLUSTER` that worker is deployed with.
    `id` is the sequence letter its name ends with (creation order), None when
    the name is not `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`. `problems` is why the
    file cannot be used as it is (its NodePools name another zone, or none; a
    name outside the convention; a key written twice): an entry with any is
    listed and NEVER chosen.
    """

    name: str
    cluster_name: str
    hosted_cluster: str
    path: str
    zone: str | None = None
    id: str | None = None
    problems: list[str] = Field(default_factory=list)


class KvClusterList(BaseModel):
    """The zone's clusters, OLDEST FIRST (by `id`), the commit they were read at, and the
    placement buffer the run must use: ONE value for every KV cluster, the
    cluster-lifecycle worker's `HEADROOM_BUFFER_PERCENT`, handed over here so each run
    records the buffer it decided with."""

    clusters: list[KvClusterEntry] = Field(default_factory=list)
    buffer_percent: int = Field(ge=0, le=90)
    revision: str
    folder: str | None = None


# --- one cluster's figures (the worker inside it) -----------------------------------------


class KvFiguresRequest(BaseModel):
    """The queue already picks the cluster; the name is there so a worker on the wrong
    queue says so instead of reporting another cluster's numbers."""

    kv_cluster: str = Field(min_length=1)


class KvNodeHeadroom(BaseModel):
    """One usable node: its allocatable minus the requests of the pods on it, CPU and memory
    as a PAIR — the largest free CPU and the largest free memory can be on two different
    nodes, and then neither holds the VM. Negative when a node is overcommitted."""

    name: str
    cpu_millicores: int
    memory_bytes: int


class KvClusterFigures(BaseModel):
    """What a KV cluster's worker measured on its own cluster, at the moment it was asked.

    `readable` False: the worker answered but could not read its cluster's API
    in time, or was asked for another cluster than its own (`detail` says
    which, bounded). It is a VALUE, never an exception, so the run neither
    retries it nor holds the zone lock while it does. "Requested" is pods plus
    stopped VMs, added by whoever reads this.
    """

    kv_cluster: str
    readable: bool
    detail: str | None = None
    nodes: int = 0
    usable_nodes: int = 0
    allocatable_cpu_millicores: int = 0
    allocatable_memory_bytes: int = 0
    pods_cpu_millicores: int = 0
    pods_memory_bytes: int = 0
    stopped_vms: int = 0
    stopped_vms_cpu_millicores: int = 0
    stopped_vms_memory_bytes: int = 0
    node_headroom: list[KvNodeHeadroom] = Field(default_factory=list)


class KvClusterRow(BaseModel):
    """One row of the run's per-cluster table: a cluster the run judged, and what it decided.

    `skip_reason` is a key of placement.SKIP_REASONS, None for the cluster
    that fits. `figures` is what its worker reported (None for a cluster that
    was never read: no worker, or a day1 problem). The clusters AFTER the
    chosen one are not read and have no row (`not_read` on the progress and
    the result names them).
    """

    name: str
    fits: bool = False
    skip_reason: str | None = None
    skip_detail: str | None = None
    figures: KvClusterFigures | None = None
    cpu_fill_after: float | None = None
    memory_fill_after: float | None = None


# --- the VM (the worker inside the chosen cluster) ----------------------------------------


class VmCreateRequest(BaseModel):
    """What the VM IS; the chosen cluster's worker draws the MAC, derives the name, builds
    the VirtualMachine and creates it Halted.

    `request_id` is the RUN's id (a slot's workflow id is reused by later
    runs): the worker labels the VM with it, so a retried attempt whose answer
    was lost finds its own VM instead of drawing a second one. `workflow_id`
    goes on the VM as an annotation, for a person. `create_before` is the end
    of the zone lease the create is sent under: the worker creates nothing
    after it (VmLeaseExpiredError), though a retry still returns the VM it
    already created.
    """

    kv_cluster: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    flavor: str = Field(min_length=1)
    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)
    site: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    mce: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    workflow_id: str | None = None
    create_before: datetime


class VmCreated(BaseModel):
    """The VM the worker holds for this request. `created` False: an earlier attempt of
    the same activity had already created it (found by its request-id label). `draws` is
    how many MACs this attempt drew (0 when it found its own VM)."""

    name: str
    namespace: str
    mac: str
    cpu: int
    memory_gib: int
    created: bool
    kv_cluster: str
    draws: int = 0


class VmReadyRequest(BaseModel):
    kv_cluster: str = Field(min_length=1)
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
    """Delete the RUN's own VM: the one labelled with `request_id`.

    With `name` and `mac` (the run knows them once the create answered), the
    VM of that name must carry that MAC and that label, or it is someone
    else's and is left alone (VmIdentityConflictError). Without them (a cancel
    that cut the create's answer off), every VM in the namespace labelled with
    the request id goes — at most one.
    """

    kv_cluster: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    name: str | None = None
    mac: str | None = None


class VmDeleted(BaseModel):
    """`deleted` False: nothing of this run was there (already gone, or never created) —
    success all the same. `names` is what was deleted."""

    deleted: bool
    names: list[str] = Field(default_factory=list)


# --- progress and result ------------------------------------------------------------------


class DeleteStrayVmInput(BaseModel):
    """Delete whatever VM of one create-vm RUN is on one KV cluster (its `redbull.io/created-by`
    label is `request_id`): a cluster that failed the run's create and could not then be asked
    whether the VM landed. `workflow_id` (the create-vm run's) and `why` are for people."""

    kv_cluster: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    workflow_id: str | None = None
    why: str | None = None


class DeleteStrayVmRunArgs(BaseModel):
    input: DeleteStrayVmInput


class DeleteStrayVmProgress(BaseModel):
    phase: str
    kv_cluster: str
    request_id: str


class DeleteStrayVmResult(BaseModel):
    """`deleted` False: the cluster held no VM of the run — the create had never landed."""

    kv_cluster: str
    request_id: str
    deleted: bool
    names: list[str] = Field(default_factory=list)


class CreateVmProgress(BaseModel):
    """Returned by the `progress` query."""

    phase: str
    kv_candidates: list[str] = Field(default_factory=list)
    buffer_percent: int | None = None
    day1_revision: str | None = None
    table: list[KvClusterRow] = Field(default_factory=list)
    not_read: list[str] = Field(default_factory=list)
    zone_lock: str | None = None
    lease_ends_at: datetime | None = None
    leases: int = 0
    kv_cluster: str | None = None
    vm_name: str | None = None
    mac: str | None = None
    cancel_rollback: str | None = None
    # The KV clusters that failed this run's create and could not be checked afterwards,
    # each with its delete-stray-vm workflow, which removes the run's VM there if one landed.
    stray_cleanups: list[str] = Field(default_factory=list)


class CreateVmResult(BaseModel):
    """The VM this run created, Halted, on the cluster it chose.

    `table` is every cluster the run judged, oldest first, the chosen one
    last; `not_read` the clusters after it, never asked. `created` False means
    ONLY that an activity retry inside this run found its own earlier create
    had landed. `draws` counts the MACs the worker drew for it, `leases` the
    zone leases the run needed (1 unless clusters without a worker outlasted one).
    """

    vm_name: str
    mac: str
    namespace: str
    flavor: str
    cpu: int
    memory_gib: int
    kv_cluster: str
    table: list[KvClusterRow] = Field(default_factory=list)
    not_read: list[str] = Field(default_factory=list)
    buffer_percent: int
    day1_revision: str
    root_disk_phase: str | None = None
    created: bool = True
    draws: int = 0
    leases: int = 1
    infra_env: str
    mce_cluster: str
    site: str
    zone: str
    # delete-stray-vm workflow ids, one per cluster the run moved on from without knowing
    # whether its VM had landed there (each removes it once its cluster answers).
    stray_cleanups: list[str] = Field(default_factory=list)


# --- the zone lock -------------------------------------------------------------------------
#
# One KvZoneLockWorkflow per (site, zone) hands the zone to one create-vm run at a
# time. A run is known by its workflow id AND its run id: slot ids are reused, so a
# grant is signalled to exactly the run that asked, and a later run on the same id
# never receives a grant it did not ask for.


class KvZoneLockRequest(BaseModel):
    """A run asking for the zone's lock (signal `request_lock`), or taking itself out of
    the line (signal `withdraw`)."""

    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)


class KvZoneLockGrant(BaseModel):
    """Signal `lock_granted`, lock -> run: the zone is yours until `lease_ends_at`."""

    lease_id: str
    lease_ends_at: datetime


class KvZoneLockRelease(BaseModel):
    """Signal `release`, run -> lock. A release carrying another lease's id (an expired
    one) is ignored."""

    lease_id: str


class KvZoneLease(BaseModel):
    """The lease in force: who holds the zone and until when."""

    lease_id: str
    holder: KvZoneLockRequest
    ends_at: datetime


class KvZoneLockRunArgs(BaseModel):
    """The lock workflow's single argument: its zone, and the state a continued run carries
    on from (the line, the lease in force, how many grants it has made in all)."""

    site: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    line: list[KvZoneLockRequest] = Field(default_factory=list)
    lease: KvZoneLease | None = None
    grants: int = 0
    skipped: int = 0
    expired: int = 0


class KvZoneLockProgress(BaseModel):
    """Returned by the lock's `progress` query: the line, the holder and its lease's end.
    `skipped` counts waiters found gone when their turn came, `expired` leases that ran out."""

    phase: str
    site: str
    zone: str
    waiting: int = 0
    line: list[KvZoneLockRequest] = Field(default_factory=list)
    holder: KvZoneLockRequest | None = None
    lease_id: str | None = None
    lease_ends_at: datetime | None = None
    grants: int = 0
    skipped: int = 0
    expired: int = 0
