"""Server-provisioning activity implementations — the execution limb.

These run in the `server-provisioning-worker` deployment, ONE for the whole
estate (it writes to no cluster, so unlike server-lifecycle it is not per-MCE).
This module is the @activity.defn surface and nothing else; the work lives in
the modules beside it, one technology each, taking plain parameters:

  * idrac.py         — Redfish against each machine's iDRAC.
  * ome.py           — OpenManage Enterprise: discovery, templates, profiles.
  * server_namer.py  — the naming service (its request is the one to fill in).
  * server_scan.py   — the inventory lookups.

What only this module does: hold the settings (read at import, so a missing key
crash-loops the worker instead of failing a run), turn a run's credential INDEX
into the password it names, and log through `activity.logger`. No password is
ever logged, returned or put in an error message.
"""

from __future__ import annotations

from temporalio import activity

from activities.server_provisioning import idrac, ome, server_namer, server_scan
from shared.exceptions import IdracCredentialsMissingError, TemplateNotConfiguredError
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
    RebootResult,
    ServerNameRequest,
    ServerScanLookup,
    ServerScanState,
    StorageConfigRequest,
    StorageLayout,
    TemplateDeployRequest,
    TemplateDeployResult,
)
from shared.settings import ServerProvisioningActivitySettings

_settings = ServerProvisioningActivitySettings()


def _password(ref: IdracRef) -> str:
    """The root password a run's credential index names."""
    passwords = _settings.idrac_root_passwords
    if not 0 <= ref.credential < len(passwords):
        raise IdracCredentialsMissingError(
            f"No iDRAC root password is configured at position {ref.credential} "
            f"({len(passwords)} configured) — IDRAC_FACTORY_PASSWORDS changed under this run"
        )
    return passwords[ref.credential]


def _ome_session():
    return ome.session(_settings.ome_url, _settings.ome_username, _settings.ome_password)


# --- iDRAC -------------------------------------------------------------------


@activity.defn
async def probe_idrac_credentials(idrac_ip: str) -> IdracProbeResult:
    """Find which configured root password this iDRAC accepts."""
    result = await idrac.probe_credentials(
        idrac_ip, _settings.idrac_username, _settings.idrac_root_passwords
    )
    activity.logger.info(
        "iDRAC %s probe: reachable=%s credential=%s rejected=%d%s",
        idrac_ip,
        result.reachable,
        result.credential,
        result.rejected,
        f" ({result.detail})" if result.detail else "",
    )
    return result


@activity.defn
async def check_idrac_login(ref: IdracRef) -> bool:
    """Whether root currently accepts the credential `ref` names."""
    return await idrac.check_login(ref.idrac_ip, _settings.idrac_username, _password(ref))


@activity.defn
async def read_idrac_identity(ref: IdracRef) -> IdracIdentity:
    """Service tag, model, manufacturer, BIOS and iDRAC firmware versions."""
    identity = await idrac.read_identity(ref.idrac_ip, _settings.idrac_username, _password(ref))
    activity.logger.info(
        "iDRAC %s is %s %s, iDRAC firmware %s",
        ref.idrac_ip,
        identity.model,
        identity.service_tag,
        identity.idrac_firmware,
    )
    return identity


@activity.defn
async def clear_idrac_os_hostname(ref: IdracRef) -> bool:
    """Blank the machine's OS hostname so OME shows its address, not `Miniwinpc`."""
    cleared = await idrac.clear_os_hostname(ref.idrac_ip, _settings.idrac_username, _password(ref))
    if cleared:
        activity.logger.info("iDRAC %s: OS hostname cleared", ref.idrac_ip)
    return cleared


@activity.defn
async def read_storage_layout(ref: IdracRef) -> StorageLayout:
    """Every storage controller with its drives and volumes."""
    return await idrac.read_storage(ref.idrac_ip, _settings.idrac_username, _password(ref))


@activity.defn
async def stage_storage_config(request: StorageConfigRequest) -> list[str]:
    """Stage the BOSS RAID 1 and the Non-RAID conversions; return their job ids."""
    ref = request.idrac
    job_ids = await idrac.stage_storage(
        ref.idrac_ip,
        _settings.idrac_username,
        _password(ref),
        request.boss_controller,
        request.boss_drives,
        request.non_raid_drives,
    )
    activity.logger.info("iDRAC %s: storage staged as %s", ref.idrac_ip, job_ids)
    return job_ids


@activity.defn
async def apply_staged_idrac_jobs(ref: IdracJobsRef) -> RebootResult:
    """Power-cycle the machine so the Lifecycle Controller runs the staged jobs."""
    result = await idrac.apply_staged(
        ref.idrac.idrac_ip, _settings.idrac_username, _password(ref.idrac), ref.job_ids
    )
    activity.logger.info(
        "iDRAC %s: %s",
        ref.idrac.idrac_ip,
        f"reset ({result.reset_type})" if result.rebooted else "jobs already running, no reset",
    )
    return result


@activity.defn
async def get_idrac_jobs(ref: IdracJobsRef) -> IdracJobsState:
    """Current state of each named Lifecycle Controller job."""
    jobs = await idrac.get_jobs(
        ref.idrac.idrac_ip, _settings.idrac_username, _password(ref.idrac), ref.job_ids
    )
    return IdracJobsState(jobs=jobs)


# --- OpenManage Enterprise ---------------------------------------------------


@activity.defn
async def find_ome_device(ref: OmeDeviceRef) -> OmeDevice:
    """The OME device with this service tag, or found=False."""
    async with _ome_session() as client:
        return await ome.find_device(client, ref.service_tag)


@activity.defn
async def start_ome_discovery(request: OmeDiscoveryRequest) -> OmeJobRef:
    """Create (or find, by name) a discovery of one iDRAC; return its job."""
    async with _ome_session() as client:
        job_id = await ome.start_discovery(
            client,
            request.group_name,
            request.idrac.idrac_ip,
            _settings.idrac_username,
            _password(request.idrac),
        )
    activity.logger.info(
        "OME discovery %s of %s is job %d", request.group_name, request.idrac.idrac_ip, job_id
    )
    return OmeJobRef(job_id=job_id)


@activity.defn
async def get_ome_job(ref: OmeJobRef) -> OmeJobState:
    """One OME job's last-run status."""
    async with _ome_session() as client:
        return await ome.get_job(client, ref.job_id)


@activity.defn
async def deploy_ome_template(request: TemplateDeployRequest) -> TemplateDeployResult:
    """Deploy the configured template for (model, iDRAC firmware) to the device."""
    by_firmware = _settings.dell_templates.get(request.model) or {}
    template_name = by_firmware.get(request.idrac_firmware)
    if not template_name:
        raise TemplateNotConfiguredError(
            f"DELL_TEMPLATES has no template for {request.model!r} on iDRAC firmware "
            f"{request.idrac_firmware!r} (configured for this model: "
            f"{sorted(by_firmware) or 'none'})"
        )
    async with _ome_session() as client:
        template_id = await ome.find_template_id(client, template_name)
        job_id = await ome.deploy_template(client, template_id, template_name, request.device_id)
    activity.logger.info(
        "Template %s -> device %d (%s): %s",
        template_name,
        request.device_id,
        request.service_tag,
        f"job {job_id}" if job_id is not None else "already deployed",
    )
    return TemplateDeployResult(template_name=template_name, template_id=template_id, job_id=job_id)


@activity.defn
async def get_ome_profile(ref: OmeProfileRef) -> OmeProfile:
    """The server profile assigned to the device, or found=False."""
    async with _ome_session() as client:
        return await ome.device_profile(client, ref.device_id)


# --- the naming service ------------------------------------------------------


@activity.defn
async def request_server_name(request: ServerNameRequest) -> None:
    """Ask the naming service to rename this device's OME profile."""
    await server_namer.request_name(
        _settings.server_namer_url, _settings.server_namer_api_token, request
    )
    activity.logger.info("Naming service accepted %s (region %s)", request.service_tag, request.region)


# --- server-scan -------------------------------------------------------------


@activity.defn
async def find_in_server_scan(lookup: ServerScanLookup) -> ServerScanState:
    """Whether a cluster is already using the machine with this service tag."""
    return await server_scan.lookup(
        _settings.server_scan_url, _settings.server_scan_api_token, lookup
    )
