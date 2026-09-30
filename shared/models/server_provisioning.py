"""Typed state for the server-provisioning workflows — the contract between brain and limb.

provision-dell-server takes a Dell server that has an iDRAC IP and nothing
else, and leaves it named, templated, RAID-configured and visible in server-scan.
Everything crossing the workflow/activity boundary is a model from this file.

NO PASSWORD EVER CROSSES THIS BOUNDARY. Activity inputs and results are recorded
verbatim in Temporal history and shown in the UI, so an iDRAC credential is
referred to by its POSITION in the limb's configured candidate list
(`IdracRef.credential`), and the limb looks the password up itself.

None of these models carries `extra="forbid"`: they are decoded from Temporal
history on every replay. Strictness about unknown fields belongs on the API-edge
subclass in the router (ProvisionDellServerRequest).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# Position of the TARGET root password in the limb's candidate list. Every other
# position is one of the factory passwords a server may arrive with.
TARGET_CREDENTIAL = 0

# Lifecycle Controller job states, HERE because both sides read them and must
# read them the same way: the limb decides from these whether it may reset the
# machine, and the workflow decides from them whether storage has converged. Two
# copies could drift apart on a firmware change and the run would then reset a
# machine mid-apply, or wait forever for a state the limb no longer reports.
IDRAC_JOB_SUCCESS = "Completed"
# After any of these a job never changes again.
IDRAC_TERMINAL_JOB_STATES = frozenset({IDRAC_JOB_SUCCESS, "Failed", "CompletedWithErrors", "RebootFailed"})
# A staged (OnReset) job waiting for the reset that runs it. Anything neither
# this nor terminal is ACTIVE — e.g. a PERC's Non-RAID conversion, which Dell
# runs at once as a RealTimeNoRebootConfiguration job — and no reset may happen
# until it is done.
IDRAC_AWAITING_RESET_STATE = "Scheduled"


class ProvisionDellServerInput(BaseModel):
    """One machine to provision, as the technician names it: its iDRAC address.

    `region` is resolved from the iDRAC's address prefix at the API edge, so a
    run started through the router always carries one; the workflow refuses a
    run without it rather than guessing.
    """

    idrac_ip: str
    region: str | None = None


class ProvisionDellServerRunArgs(BaseModel):
    """The ONE argument `run()` takes (CLAUDE.md §5, single-model workflow argument)."""

    input: ProvisionDellServerInput


class IdracRef(BaseModel):
    """Which iDRAC to talk to and which configured credential to talk to it with.

    `credential` is an index into the limb's candidate list —
    TARGET_CREDENTIAL (0) is the password the template enforces, the rest are
    the factory passwords in their configured order.
    """

    idrac_ip: str
    credential: int = TARGET_CREDENTIAL


class IdracProbeResult(BaseModel):
    """Outcome of trying the candidate root passwords against one iDRAC.

    Answered as DATA rather than raised, because the workflow owns what happens
    next: an unreachable iDRAC is waited on (a technician may still be cabling
    it), and a full round of rejections is waited out past the iDRAC's
    IP-blocking penalty before the next round — neither is an activity retry.
    """

    reachable: bool
    credential: int | None = None
    rejected: int = 0
    detail: str | None = None


class IdracIdentity(BaseModel):
    """What the iDRAC says the machine is. The service tag is Redfish `SKU`."""

    service_tag: str
    model: str
    manufacturer: str | None = None
    idrac_firmware: str
    bios_version: str | None = None
    power_state: str | None = None
    # The OS hostname the iDRAC reports (Redfish `HostName`, racadm
    # `System.ServerOS.HostName`). Servers arrive carrying a factory value —
    # `Miniwinpc` is the one the DC team keeps seeing — and while it is set, OME
    # shows it instead of the machine's address next to the profile. A server
    # being provisioned has no OS, so any value here is stale by definition.
    os_hostname: str | None = None


class IdracDrive(BaseModel):
    """One physical drive behind a storage controller.

    `raid_status` is Dell's `Oem.Dell.DellPhysicalDisk.RaidStatus`: `Ready`
    (unconfigured), `NonRAID`, `Online` (member of a volume), `Foreign`, ...
    """

    odata_id: str
    raid_status: str | None = None


class IdracVolume(BaseModel):
    """One existing volume on a controller, with the drives it spans."""

    odata_id: str
    raid_type: str | None = None
    drives: list[str] = Field(default_factory=list)


class IdracController(BaseModel):
    """One storage controller as Redfish reports it (a Storage resource)."""

    odata_id: str
    id: str
    name: str
    drives: list[IdracDrive] = Field(default_factory=list)
    volumes: list[IdracVolume] = Field(default_factory=list)


class StorageLayout(BaseModel):
    """Every storage controller on the machine, unclassified.

    Deciding which controller is the BOSS and which drives to touch is POLICY,
    and lives in workflow_domains/server_provisioning/storage_plan.py — the
    limb only reports what is there.
    """

    controllers: list[IdracController] = Field(default_factory=list)


class StorageConfigRequest(BaseModel):
    """What to stage on the iDRAC before the one reboot that applies it all."""

    idrac: IdracRef
    boss_controller: str | None = None
    boss_drives: list[str] = Field(default_factory=list)
    non_raid_drives: list[str] = Field(default_factory=list)


class IdracJobsRef(BaseModel):
    """A set of iDRAC (Lifecycle Controller) job ids on one machine."""

    idrac: IdracRef
    job_ids: list[str]


class IdracJobState(BaseModel):
    """One Lifecycle Controller job, as `Managers/iDRAC.Embedded.1/Jobs/<id>` reports it."""

    job_id: str
    state: str
    # e.g. RAIDConfiguration (staged, runs on reset) or
    # RealTimeNoRebootConfiguration (a PERC applying at once).
    job_type: str | None = None
    message: str | None = None
    percent_complete: int | None = None


class IdracJobsState(BaseModel):
    jobs: list[IdracJobState]


class RebootResult(BaseModel):
    """Whether this call power-cycled the machine to run the staged jobs.

    `rebooted=False` is the idempotent answer on a retry: once the Lifecycle
    Controller has picked the jobs up they are no longer `Scheduled`, and a
    second reset then would interrupt them mid-apply.
    """

    rebooted: bool
    reset_type: str | None = None


class BiosComparison(BaseModel):
    """One BIOS attribute the template meant to set, against what the machine has.

    `attribute_name` is None when the machine's BIOS attribute registry has no
    entry with that display name — reported rather than dropped, because
    silence would read as "verified" when it means "not checked". That happens
    when a template built for one firmware names an attribute another firmware
    does not have, which is the exact case Dell warns about.
    """

    display_name: str
    attribute_name: str | None = None
    intended: str | None = None
    actual: str | None = None
    read_only: bool = False

    @property
    def verifiable(self) -> bool:
        return bool(self.attribute_name) and not self.read_only and self.intended is not None

    @property
    def drifted(self) -> bool:
        return self.verifiable and self.actual != self.intended


class BiosVerifyRequest(BaseModel):
    """The BIOS attributes to check, keyed by the DISPLAY name OME reports."""

    idrac: IdracRef
    intended: dict[str, str | None] = Field(default_factory=dict)


class BiosVerification(BaseModel):
    compared: list[BiosComparison] = Field(default_factory=list)


class BiosStageRequest(BaseModel):
    """Attribute NAME -> value, as Redfish accepts them in a PATCH."""

    idrac: IdracRef
    attributes: dict[str, str] = Field(default_factory=dict)


class TemplateAttribute(BaseModel):
    """One attribute an OME template would deploy.

    As `TemplateService/Templates({id})/AttributeDetails` reports it: OME gives
    the GUI's display names rather than the SCP attribute names, so `group` is
    the joined path of the nested AttributeGroups ("iDRAC,IPv4 Information")
    and `name` the leaf DisplayName ("Address"). `template_policy.py` matches
    on both — there is no stable id to key on.
    """

    attribute_id: int | None = None
    name: str
    group: str = ""
    value: str | None = None
    # OME's own "capture it but do not deploy it" flag. An ignored attribute is
    # not applied, so it is never a hazard.
    is_ignored: bool = False

    def describe(self) -> str:
        return f"{self.group},{self.name}" if self.group else self.name


class TemplateContents(BaseModel):
    """What a template would deploy, for the audit that runs before it does."""

    template_id: int
    template_name: str
    attributes: list[TemplateAttribute] = Field(default_factory=list)


class OmeTemplateRef(BaseModel):
    """Which template to read, named the way DELL_TEMPLATES names it."""

    model: str
    idrac_firmware: str


class OmeDeviceRef(BaseModel):
    service_tag: str


class OmeDevice(BaseModel):
    """A device in OpenManage Enterprise, looked up by service tag."""

    found: bool
    device_id: int | None = None
    device_name: str | None = None


class OmeDiscoveryRequest(BaseModel):
    """Discover one iDRAC into OME with one of the configured root credentials.

    `group_name` is deterministic per run and per purpose, so a retried
    activity finds the discovery it already created instead of stacking a
    second one.
    """

    idrac: IdracRef
    group_name: str


class OmeJobRef(BaseModel):
    job_id: int


class OmeJobState(BaseModel):
    """An OME job's `LastRunStatus`. `finished` and `succeeded` are the limb's
    reading of the status id, so the workflow never interprets OME codes."""

    job_id: int
    status_id: int
    status: str
    finished: bool
    succeeded: bool


class TemplateDeployRequest(BaseModel):
    """Deploy the template for this machine's (model, iDRAC firmware) to it."""

    device_id: int
    service_tag: str
    model: str
    idrac_firmware: str


class TemplateDeployResult(BaseModel):
    """The job that deploys (or already deployed) the template.

    When the device already carries a profile from this template, `job_id` is
    that profile's own `DeploymentTaskId`, so a retry or re-run waits on the
    real deployment; None only when OME recorded no task for it.
    """

    template_name: str
    template_id: int
    job_id: int | None = None


class OmeProfileRef(BaseModel):
    device_id: int


class OmeProfile(BaseModel):
    """The server profile OME holds for a device — whose NAME server-scan reads."""

    found: bool
    profile_id: int | None = None
    profile_name: str | None = None
    template_name: str | None = None
    template_id: int | None = None
    # 0 unassigned, 1 assigned for auto-deploy, 4 deployed.
    profile_state: int | None = None
    deployment_task_id: int | None = None


class ServerNameRequest(BaseModel):
    """Everything the naming service could need to name one server.

    The service computes the name itself from what it reads in OME (cores,
    memory and disks, rounded its own way), so nothing here is a name — only
    which device, and the region the convention needs.
    """

    service_tag: str
    ome_device_id: int
    region: str
    idrac_ip: str


class ServerScanLookup(BaseModel):
    """Ask server-scan what it holds for one service tag."""

    service_tag: str


class ServerScanState(BaseModel):
    """What server-scan holds for one service tag.

    Read for ONE reason: `claimed_by` is set when any document with this serial
    is in use by a cluster (`INSTALLED ocp4-x`, `INSTALLED_TO_INVENTORY mce-y`),
    which is the one thing that makes rebooting and re-templating the machine
    unsafe. `name` only names the machine in that refusal.

    The run does not read this again at the end: being LISTED by server-scan is
    not this workflow's definition of done (see provision_dell_server.py).
    """

    name: str | None = None
    claimed_by: str | None = None


class ProvisionDellServerProgress(BaseModel):
    """The `progress` query. `waiting_on` names what a long phase is waiting for."""

    phase: str
    idrac_ip: str | None = None
    service_tag: str | None = None
    model: str | None = None
    ome_device_id: int | None = None
    template_name: str | None = None
    profile_name: str | None = None
    os_hostname_cleared: bool | None = None
    bios_attributes_checked: int | None = None
    bios_attributes_drifted: int | None = None
    waiting_on: str | None = None


class ProvisionDellServerResult(BaseModel):
    idrac_ip: str
    region: str
    service_tag: str
    model: str
    idrac_firmware: str
    # Which credential first opened the iDRAC: "target" means it had already
    # been provisioned (or shipped) with the enforced password.
    initial_credential: str
    ome_device_id: int
    template_name: str
    profile_name: str
    boss_raid1_created: bool
    non_raid_drives_converted: int
    # True when the machine arrived with a factory OS hostname (`Miniwinpc`)
    # that this run blanked, so OME shows its address beside the profile.
    os_hostname_cleared: bool = False
    # How much of the template this run could actually verify, and how much of
    # it the deployment silently failed to apply. `bios_attributes_unverified`
    # is the honest coverage gap: attributes the machine's BIOS registry does
    # not know, which is what a template/firmware mismatch looks like.
    bios_attributes_checked: int = 0
    bios_attributes_remediated: int = 0
    bios_attributes_unverified: int = 0
