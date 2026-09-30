"""Every call to a Dell iDRAC — Redfish over httpx, plain parameters, no settings.

Same split as activities/server_lifecycle/server_scan.py: this module owns ONE
technology and takes the address and credential as PARAMETERS, so the offline
tests drive it with respx and no environment. activities.py resolves which
password a run means and owns the logging.

Dell's Redfish layout, as used here (iDRAC9, firmware 5.x-7.x):

  /redfish/v1/Systems/System.Embedded.1           SKU = service tag, Model, PowerState
  /redfish/v1/Managers/iDRAC.Embedded.1           FirmwareVersion
  .../Systems/System.Embedded.1/Storage           one member per controller
  .../Storage/<ctrl>/Volumes                      POST RAIDType RAID1 creates a volume
  .../Oem/Dell/DellRaidService/Actions/
        DellRaidService.ConvertToNonRAID          {"PDArray": [<drive ids>]}
  /redfish/v1/Managers/iDRAC.Embedded.1/Jobs      Lifecycle Controller jobs (JID_...)

Configuration is STAGED (`@Redfish.OperationApplyTime: OnReset`), so the
iDRAC answers 202 with the job in its `Location` header and the change applies
on the next reset — which is how one reboot applies the RAID 1 and every
Non-RAID conversion together.

AUTHENTICATION IS HTTP BASIC, one request per credential. Every rejected
request counts toward iDRAC9's IP block (3 failures in its fail window), so the
probe sends exactly one request per candidate and nothing here ever retries a
401 by itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from activities.server_provisioning.http_client import TIMEOUT, VERIFY_TLS, extended_info
from shared.exceptions import IdracAuthError, IdracError, IdracRequestRejectedError
from shared.models.server_provisioning import (
    IDRAC_AWAITING_RESET_STATE,
    IDRAC_TERMINAL_JOB_STATES,
    IdracController,
    IdracDrive,
    IdracIdentity,
    IdracJobState,
    IdracProbeResult,
    IdracVolume,
    RebootResult,
    StorageLayout,
)

SYSTEM = "/redfish/v1/Systems/System.Embedded.1"
MANAGER = "/redfish/v1/Managers/iDRAC.Embedded.1"
JOBS = f"{MANAGER}/Jobs"
_CONVERT_TO_NON_RAID = f"{SYSTEM}/Oem/Dell/DellRaidService/Actions/DellRaidService.ConvertToNonRAID"
_RESET = f"{SYSTEM}/Actions/ComputerSystem.Reset"

_REJECTED_STATUSES = {400, 405, 409, 422}


def _base_url(idrac_ip: str) -> str:
    host = f"[{idrac_ip}]" if ":" in idrac_ip else idrac_ip
    return f"https://{host}"


@asynccontextmanager
async def _client(idrac_ip: str, username: str, password: str) -> AsyncIterator[httpx.AsyncClient]:
    """One client per activity invocation, so a credential never outlives it."""
    async with httpx.AsyncClient(
        base_url=_base_url(idrac_ip),
        auth=(username, password),
        timeout=TIMEOUT,
        verify=VERIFY_TLS,
        headers={"Accept": "application/json"},
    ) as client:
        yield client


def _classify(resp: httpx.Response, what: str) -> None:
    """Raise the named error for a non-2xx answer; return on success."""
    if resp.is_success:
        return
    if resp.status_code in (401, 403):
        raise IdracAuthError(f"iDRAC rejected root's credential on {what} ({resp.status_code})")
    if resp.status_code in _REJECTED_STATUSES:
        raise IdracRequestRejectedError(f"iDRAC refused {what} ({resp.status_code}): {extended_info(resp)}")
    raise IdracError(f"iDRAC answered {resp.status_code} to {what}: {extended_info(resp)}")


async def _send(client: httpx.AsyncClient, method: str, path: str, **kwargs: Any) -> httpx.Response:
    """The iDRAC's answer whatever its status — transport failures named, nothing classified.

    Separate from `_request` for the one caller that must see a 404 itself: a
    controller with no RAID capability serves no Volumes collection, and that
    is a fact about the machine rather than a failure.
    """
    try:
        return await client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        raise IdracError(f"iDRAC unreachable on {method} {path}: {type(exc).__name__}: {exc}") from exc


async def _request(client: httpx.AsyncClient, method: str, path: str, **kwargs: Any) -> httpx.Response:
    resp = await _send(client, method, path, **kwargs)
    _classify(resp, f"{method} {path}")
    return resp


async def _get(client: httpx.AsyncClient, path: str) -> dict[str, Any]:
    body = (await _request(client, "GET", path)).json()
    return body if isinstance(body, dict) else {}


def _members(body: dict[str, Any]) -> list[str]:
    return [m["@odata.id"] for m in body.get("Members", []) if isinstance(m, dict) and "@odata.id" in m]


def _job_id(resp: httpx.Response, what: str) -> str:
    """The Lifecycle Controller job a staged change was queued as."""
    location = resp.headers.get("Location", "")
    job_id = location.rstrip("/").rsplit("/", 1)[-1]
    if not job_id.startswith("JID_"):
        raise IdracRequestRejectedError(
            f"iDRAC accepted {what} but returned no job to track it (Location={location!r})"
        )
    return job_id


async def probe_credentials(idrac_ip: str, username: str, passwords: list[str]) -> IdracProbeResult:
    """Which of `passwords` root accepts — one GET each, in order, first success wins.

    Answers rather than raises. `reachable=False` covers both "nothing
    answered" and "an answer that was neither success nor 401/403" (a 5xx
    from an iDRAC still booting): either way this round proved nothing about
    the credentials, and continuing would only add failed logins.
    """
    rejected = 0
    for index, password in enumerate(passwords):
        try:
            async with _client(idrac_ip, username, password) as client:
                resp = await client.get(SYSTEM)
        except httpx.HTTPError as exc:
            return IdracProbeResult(
                reachable=False, rejected=rejected, detail=f"{type(exc).__name__}: {exc}"
            )
        if resp.is_success:
            return IdracProbeResult(reachable=True, credential=index, rejected=rejected)
        if resp.status_code in (401, 403):
            rejected += 1
            continue
        return IdracProbeResult(
            reachable=False, rejected=rejected, detail=f"HTTP {resp.status_code}: {extended_info(resp)}"
        )
    return IdracProbeResult(reachable=True, credential=None, rejected=rejected)


async def check_login(idrac_ip: str, username: str, password: str) -> bool:
    """Whether root accepts this password right now. An iDRAC that does not
    answer (restarting after the template) is False, not an error — the
    workflow's deadline decides how long that may last."""
    try:
        async with _client(idrac_ip, username, password) as client:
            resp = await client.get(SYSTEM)
    except httpx.HTTPError:
        return False
    return resp.is_success


async def read_identity(idrac_ip: str, username: str, password: str) -> IdracIdentity:
    """Service tag, model and firmware versions."""
    async with _client(idrac_ip, username, password) as client:
        system = await _get(client, SYSTEM)
        manager = await _get(client, MANAGER)
    service_tag = str(system.get("SKU") or "").strip()
    model = str(system.get("Model") or "").strip()
    firmware = str(manager.get("FirmwareVersion") or "").strip()
    if not service_tag or not model or not firmware:
        raise IdracRequestRejectedError(
            f"iDRAC {idrac_ip} did not report a service tag (SKU={service_tag!r}), model "
            f"({model!r}) and iDRAC firmware ({firmware!r})"
        )
    return IdracIdentity(
        service_tag=service_tag,
        model=model,
        manufacturer=system.get("Manufacturer"),
        idrac_firmware=firmware,
        bios_version=system.get("BiosVersion"),
        power_state=system.get("PowerState"),
        os_hostname=system.get("HostName"),
    )


async def clear_os_hostname(idrac_ip: str, username: str, password: str) -> bool:
    """Blank the OS hostname the iDRAC reports; True when it changed.

    The Redfish equivalent of `racadm set System.ServerOS.HostName ""`: the
    standard ComputerSystem `HostName` property, on the same resource
    `read_identity` already reads.

    Servers arrive with a factory OS hostname (`Miniwinpc`), and while one is
    set OME shows it in place of the machine's address beside the profile. A
    machine being provisioned has no OS, so this is cleared unconditionally
    rather than matched against a list of known-bad names — a list would need
    extending every time a factory image changes, and there is nothing here
    worth keeping in the first place.

    Reads before writing, so a machine that is already blank is untouched and a
    retry is a no-op.
    """
    async with _client(idrac_ip, username, password) as client:
        system = await _get(client, SYSTEM)
        if not str(system.get("HostName") or "").strip():
            return False
        await _request(client, "PATCH", SYSTEM, json={"HostName": ""})
    return True


def _raid_status(drive: dict[str, Any]) -> str | None:
    """Dell's RaidStatus: SAS/SATA drives report it under `DellPhysicalDisk`,
    NVMe drives under `DellPCIeSSD` (as Dell's own storage module reads it)."""
    dell = (drive.get("Oem") or {}).get("Dell") or {}
    for key in ("DellPhysicalDisk", "DellPCIeSSD"):
        status = (dell.get(key) or {}).get("RaidStatus")
        if status:
            return str(status)
    return None


async def _read_controller(client: httpx.AsyncClient, path: str) -> IdracController:
    storage = await _get(client, path)
    controllers = storage.get("StorageControllers") or []
    first = controllers[0] if controllers and isinstance(controllers[0], dict) else {}
    names = [str(n) for n in (storage.get("Name"), first.get("Name"), first.get("Model")) if n]

    drives: list[IdracDrive] = []
    for link in storage.get("Drives", []):
        drive_path = link.get("@odata.id") if isinstance(link, dict) else None
        if not drive_path:
            continue
        drive = await _get(client, drive_path)
        drives.append(IdracDrive(odata_id=drive_path, raid_status=_raid_status(drive)))

    volumes: list[IdracVolume] = []
    volumes_link = (storage.get("Volumes") or {}).get("@odata.id")
    if volumes_link:
        resp = await _send(client, "GET", volumes_link)
        # A controller with no RAID capability (CPU-attached NVMe) may not
        # serve a Volumes collection at all.
        if resp.status_code != 404:
            _classify(resp, f"GET {volumes_link}")
            for volume_path in _members(resp.json()):
                volume = await _get(client, volume_path)
                links = (volume.get("Links") or {}).get("Drives") or []
                volumes.append(
                    IdracVolume(
                        odata_id=volume_path,
                        raid_type=volume.get("RAIDType") or volume.get("VolumeType"),
                        drives=sorted(d["@odata.id"] for d in links if isinstance(d, dict) and "@odata.id" in d),
                    )
                )

    return IdracController(
        odata_id=path,
        id=str(storage.get("Id") or path.rsplit("/", 1)[-1]),
        name=" ".join(dict.fromkeys(names)),
        drives=drives,
        volumes=volumes,
    )


async def read_storage(idrac_ip: str, username: str, password: str) -> StorageLayout:
    """Every controller with its drives and volumes, unclassified."""
    async with _client(idrac_ip, username, password) as client:
        paths = _members(await _get(client, f"{SYSTEM}/Storage"))
        controllers = [await _read_controller(client, path) for path in paths]
    return StorageLayout(controllers=controllers)


async def _pending_jobs(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Every Lifecycle Controller job that has not finished yet."""
    body = await _get(client, f"{JOBS}?$expand=*($levels=1)")
    return [
        job
        for job in body.get("Members", [])
        if isinstance(job, dict) and job.get("JobState") not in IDRAC_TERMINAL_JOB_STATES
    ]


def _pending_for(jobs: list[dict[str, Any]], controller: str) -> str | None:
    """A pending configuration job on this controller.

    Dell names every storage configuration job `Configure: <controller FQDD>`
    (dellemc.openmanage's idrac_redfish_storage_controller sample:
    `"Name": "Configure: RAID.Integrated.1-1"`), whatever its JobType.
    """
    for job in jobs:
        if str(job.get("Name", "")) == f"Configure: {controller}" and str(job.get("Id", "")).startswith("JID_"):
            return str(job["Id"])
    return None


async def _require_raid1(client: httpx.AsyncClient, controller: str) -> None:
    """Refuse up front when the controller does not offer RAID 1 at all.

    Dell's redfish_storage_volume checks `StorageControllers[0].SupportedRAIDTypes`
    the same way; without it the refusal would come back as the iDRAC's own
    message from the POST, which says less.
    """
    storage = await _get(client, f"{SYSTEM}/Storage/{controller}")
    controllers = storage.get("StorageControllers") or [{}]
    supported = controllers[0].get("SupportedRAIDTypes") if isinstance(controllers[0], dict) else None
    if supported is not None and "RAID1" not in supported:
        raise IdracRequestRejectedError(
            f"{controller} does not support RAID1 (SupportedRAIDTypes: {supported})"
        )


def _controller_of(drive_path: str) -> str:
    """`.../Storage/RAID.SL.3-1/Drives/Disk.Bay.0:...` -> `RAID.SL.3-1`."""
    parts = drive_path.rstrip("/").split("/")
    return parts[parts.index("Drives") - 1] if "Drives" in parts else ""


async def stage_storage(
    idrac_ip: str,
    username: str,
    password: str,
    boss_controller: str | None,
    boss_drives: list[str],
    non_raid_drives: list[str],
) -> list[str]:
    """Stage the BOSS RAID 1 and the Non-RAID conversions; return their job ids.

    A controller that already has a pending job gets none added — its job is
    returned instead. That is what makes a retry after a lost response safe,
    and the iDRAC would refuse a second pending job on one controller anyway.
    """
    job_ids: list[str] = []
    async with _client(idrac_ip, username, password) as client:
        pending = await _pending_jobs(client)

        if boss_controller:
            existing = _pending_for(pending, boss_controller)
            if existing:
                job_ids.append(existing)
            else:
                await _require_raid1(client, boss_controller)
                what = f"a RAID 1 on {boss_controller}"
                # BOSS controllers apply volume changes OnReset only (Dell's
                # redfish_storage_volume: "BOSS-S1 and BOSS-N1 ... OnReset").
                resp = await _request(
                    client,
                    "POST",
                    f"{SYSTEM}/Storage/{boss_controller}/Volumes",
                    json={
                        "RAIDType": "RAID1",
                        "Name": "OS",
                        "Drives": [{"@odata.id": d} for d in boss_drives],
                        "@Redfish.OperationApplyTime": "OnReset",
                    },
                )
                job_ids.append(_job_id(resp, what))

        by_controller: dict[str, list[str]] = {}
        for drive in non_raid_drives:
            by_controller.setdefault(_controller_of(drive), []).append(drive)
        for controller, drives in sorted(by_controller.items()):
            existing = _pending_for(pending, controller)
            if existing:
                job_ids.append(existing)
                continue
            what = f"Non-RAID on {len(drives)} drive(s) of {controller}"
            resp = await _request(
                client,
                "POST",
                _CONVERT_TO_NON_RAID,
                json={"PDArray": [d.rstrip("/").rsplit("/", 1)[-1] for d in drives]},
            )
            job_ids.append(_job_id(resp, what))
    return job_ids


async def get_jobs(idrac_ip: str, username: str, password: str, job_ids: list[str]) -> list[IdracJobState]:
    """The current state of each named job."""
    states: list[IdracJobState] = []
    async with _client(idrac_ip, username, password) as client:
        for job_id in job_ids:
            job = await _get(client, f"{JOBS}/{job_id}")
            states.append(
                IdracJobState(
                    job_id=job_id,
                    state=str(job.get("JobState") or "Unknown"),
                    job_type=job.get("JobType"),
                    message=job.get("Message"),
                    percent_complete=job.get("PercentComplete"),
                )
            )
    return states


async def apply_staged(idrac_ip: str, username: str, password: str, job_ids: list[str]) -> RebootResult:
    """Reset the machine if (and only if) a job is waiting for a reset — and none is running.

    A PERC applies its Non-RAID conversion at once as a real-time job, so it
    can still be RUNNING when the BOSS's volume is waiting for the reset.
    Resetting then would cut the conversion off mid-apply; that is a
    retryable IdracError here, and the workflow waits for it before calling.
    """
    states = await get_jobs(idrac_ip, username, password, job_ids)
    active = [
        s
        for s in states
        if s.state not in IDRAC_TERMINAL_JOB_STATES and s.state != IDRAC_AWAITING_RESET_STATE
    ]
    if active:
        raise IdracError(
            "not resetting while job(s) still run: "
            + ", ".join(f"{s.job_id} {s.state}" for s in active)
        )
    if not any(state.state == IDRAC_AWAITING_RESET_STATE for state in states):
        return RebootResult(rebooted=False)
    async with _client(idrac_ip, username, password) as client:
        system = await _get(client, SYSTEM)
        # ForceRestart, not Graceful: a new machine has no OS to honour an ACPI
        # request, and the in-use guard already ran before this point.
        reset_type = "On" if system.get("PowerState") == "Off" else "ForceRestart"
        await _request(client, "POST", _RESET, json={"ResetType": reset_type})
    return RebootResult(rebooted=True, reset_type=reset_type)
