"""Server-provisioning activity signatures — the typed contract, no implementations.

The real implementations live in activities/server_provisioning/activities.py
and are registered against these names on SERVER_PROVISIONING_ACTIVITY_QUEUE.
Workflows import THESE for type-checked activity references.

Every activity here is ONE bounded exchange with one system. Anything that
takes minutes — a discovery job, a reboot applying RAID, server-scan's next
collector run — is waited on by the WORKFLOW with durable timers, never by an
activity sleeping, so each wait has a deadline in history and a phase in the
`progress` query.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.server_provisioning import (
    IdracIdentity,
    IdracJobsRef,
    IdracJobsState,
    IdracProbeResult,
    IdracRef,
    OmeDevice,
    OmeDeviceRef,
    OmeDiscoveryRequest,
    OmeJobRef,
    OmeJobState,
    OmeProfile,
    OmeProfileRef,
    OmeTemplateRef,
    RebootResult,
    ServerNameRequest,
    ServerScanLookup,
    ServerScanState,
    StorageConfigRequest,
    StorageLayout,
    TemplateContents,
    TemplateDeployRequest,
    TemplateDeployResult,
)

# --- iDRAC (Redfish) ---------------------------------------------------------


@activity.defn
async def probe_idrac_credentials(idrac_ip: str) -> IdracProbeResult:
    """Find which configured root password this iDRAC accepts.

    Tries the TARGET password first, then each factory password, ONE request
    each, and stops at the first that authenticates. Answers rather than
    raises: `reachable=False` when nothing answered at all, `credential=None`
    with `rejected=N` when every candidate got a 401. The workflow decides
    what to wait for.

    Never retried blindly: a retry replays every rejected login, and iDRAC9
    blocks an address after 3 failures in its fail window. So transport
    errors are reported as unreachable rather than raised.
    """
    ...


@activity.defn
async def check_idrac_login(ref: IdracRef) -> bool:
    """Whether root currently accepts the credential `ref` names. One request."""
    ...


@activity.defn
async def read_idrac_identity(ref: IdracRef) -> IdracIdentity:
    """Service tag (`SKU`), model, manufacturer, BIOS and iDRAC firmware versions."""
    ...


@activity.defn
async def set_idrac_root_password(ref: IdracRef) -> bool:
    """Set root's password to the TARGET one, using the credential `ref` names.

    True when this call changed it, False when root already had it. Both
    passwords are resolved on the limb — neither crosses this boundary.

    Runs BEFORE OME discovers the machine, so OME is only ever handed the
    password root keeps. Touches iDRAC user 2 and refuses if slot 2 turns out
    not to be root.
    """
    ...


@activity.defn
async def clear_idrac_os_hostname(ref: IdracRef) -> bool:
    """Blank the machine's OS hostname; True when it had one. Idempotent.

    Servers arrive carrying a factory OS hostname (`Miniwinpc`), and while one
    is set OME displays it instead of the machine's address next to the
    profile. Cleared unconditionally — a machine being provisioned has no OS,
    so whatever is there is stale.
    """
    ...


@activity.defn
async def read_storage_layout(ref: IdracRef) -> StorageLayout:
    """Every storage controller with its drives (and their RAID status) and volumes."""
    ...


@activity.defn
async def stage_storage_config(request: StorageConfigRequest) -> list[str]:
    """Stage the RAID 1 on the BOSS and Non-RAID on the named drives; return job ids.

    Both are staged to apply on the next reset, so ONE reboot applies both.
    Idempotent: a controller that already has a pending (not yet finished)
    configuration job gets no second one — that job's id is returned instead,
    because the iDRAC refuses a second pending job on a controller anyway.
    """
    ...


@activity.defn
async def apply_staged_idrac_jobs(ref: IdracJobsRef) -> RebootResult:
    """Power-cycle the machine so the Lifecycle Controller runs the staged jobs.

    Only resets while at least one job is still `Scheduled`: once the
    Lifecycle Controller has picked them up, a second reset would interrupt
    the apply. Refuses (retryable IdracError) while any job is still RUNNING —
    a PERC's real-time Non-RAID conversion must finish first. A powered-off
    machine is powered on instead.
    """
    ...


@activity.defn
async def get_idrac_jobs(ref: IdracJobsRef) -> IdracJobsState:
    """Current state of each named Lifecycle Controller job."""
    ...


# --- OpenManage Enterprise ---------------------------------------------------


@activity.defn
async def find_ome_device(ref: OmeDeviceRef) -> OmeDevice:
    """The OME device with this service tag, or found=False."""
    ...


@activity.defn
async def start_ome_discovery(request: OmeDiscoveryRequest) -> OmeJobRef:
    """Create (or find, by its deterministic name) a discovery of one iDRAC; return its job.

    The discovery authenticates as root with the credential `request.idrac`
    names — the password itself is resolved on the limb.
    """
    ...


@activity.defn
async def get_ome_job(ref: OmeJobRef) -> OmeJobState:
    """One OME job's last-run status, already interpreted as finished/succeeded."""
    ...


@activity.defn
async def deploy_ome_template(request: TemplateDeployRequest) -> TemplateDeployResult:
    """Deploy the configured template for (model, iDRAC firmware) to the device.

    The template is chosen from DELL_TEMPLATES and resolved by name in OME.
    Idempotent: a device already carrying a profile (ProfileState > 0) from
    that template gets no second deployment — the profile's own
    DeploymentTaskId is returned to wait on. Raises TemplateNotConfiguredError,
    TemplateNotFoundError or ProfileConflictError — all deterministic.
    """
    ...


@activity.defn
async def read_ome_template(ref: OmeTemplateRef) -> TemplateContents:
    """Every attribute the configured template would deploy, flattened.

    Read BEFORE anything touches the machine, so a template carrying iDRAC
    network settings, storage or user accounts stops the run without a single
    write. `template_policy.py` decides what is unsafe; this only reports.
    """
    ...


@activity.defn
async def get_ome_profile(ref: OmeProfileRef) -> OmeProfile:
    """The server profile assigned to the device, or found=False."""
    ...


# --- the naming service ------------------------------------------------------


@activity.defn
async def request_server_name(request: ServerNameRequest) -> None:
    """Ask the naming service to rename this device's OME profile.

    The service computes the name from OME's own inventory. Whether it
    worked is not this activity's claim — the workflow reads the profile
    name back from OME and checks it against the convention.
    """
    ...


# --- server-scan -------------------------------------------------------------


@activity.defn
async def find_in_server_scan(lookup: ServerScanLookup) -> ServerScanState:
    """Whether a cluster is already using the machine with this service tag.

    A plain Mongo-backed read (GET /servers?search=), never
    /servers/available: that endpoint live-rechecks against the vendor manager,
    which is far more than a yes/no guard needs.
    """
    ...
