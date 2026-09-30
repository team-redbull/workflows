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
  .../Systems/System.Embedded.1/Bios              current BIOS attribute values
  .../Bios/BiosRegistry                           AttributeName <-> DisplayName
  .../Bios/Settings                               PATCH stages values + the job
  /redfish/v1/Managers/iDRAC.Embedded.1/Jobs      Lifecycle Controller jobs (JID_...)
  .../Managers/iDRAC.Embedded.1/Accounts/2        root (iDRAC9; see ROOT_ACCOUNT)
  .../Oem/Dell/DellAttributes/System.Embedded.1   racadm's System.* attributes

TWO DIFFERENT APPLY-TIME KEYS, and they are not interchangeable. A volume POST
carries `@Redfish.OperationApplyTime: OnReset`; a BIOS settings PATCH carries
`@Redfish.SettingsApplyTime: {ApplyTime: OnReset}`. Either way the iDRAC answers
202 with the job in its `Location` header and the change applies on the next
reset — which is how ONE reboot applies the RAID 1, every Non-RAID conversion
and any BIOS drift together.

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
    BiosComparison,
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
# Slot 2 is root on every iDRAC9. The OME account lives in another slot and is
# NEVER touched — changing it would cut OME off from the machine (CLAUDE.md §4).
#
# TWO paths, because Dell moved the accounts collection between generations.
# `ChangeIdracUserPasswordREDFISH.py` branches on the system model: 12G/13G ->
# iDRAC8 and 14G/15G/16G -> iDRAC9 both use the MANAGER collection, and only
# iDRAC10 (17G and later) uses the standard `AccountService` one. An R660 is
# 16G, so the whole current fleet is on the first — but the second is tried when
# the first is absent rather than pinning this code to one generation.
ROOT_ACCOUNT = f"{MANAGER}/Accounts/2"
ROOT_ACCOUNT_IDRAC10 = "/redfish/v1/AccountService/Accounts/2"
# racadm `System.ServerOS.HostName` — the command the DC team runs by hand —
# is a SYSTEM attribute, not the standard ComputerSystem `HostName` property
# (which Dell populates from the OS through iSM). Dell's own
# SetIdracLcSystemAttributesREDFISH.py writes System attributes here.
SYSTEM_ATTRIBUTES = f"{MANAGER}/Oem/Dell/DellAttributes/System.Embedded.1"
OS_HOSTNAME_ATTRIBUTE = "ServerOS.1.HostName"
BIOS = f"{SYSTEM}/Bios"
# AttributeName <-> DisplayName, plus ReadOnly and the value-name mapping. This
# is the ONLY bridge between what OME reports about a template (the GUI's
# display names) and what Redfish accepts in a PATCH (attribute names).
BIOS_REGISTRY = f"{BIOS}/BiosRegistry"
# Staged BIOS changes: written here, applied by the next reset — which is the
# same reset the storage jobs already need, so one reboot does both.
BIOS_SETTINGS = f"{BIOS}/Settings"
# The FQDD a BIOS configuration job is named for: `Configure: BIOS.Setup.1-1`.
_BIOS_FQDD = "BIOS.Setup.1-1"
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


def _registry_by_display(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """DisplayName -> registry entry, for the attributes a template can name."""
    entries = (registry.get("RegistryEntries") or {}).get("Attributes") or []
    by_display: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        display = str(entry.get("DisplayName") or "").strip()
        if display and display not in by_display:
            by_display[display] = entry
    return by_display


def _value_name(entry: dict[str, Any], intended: str | None) -> str | None:
    """The value as Redfish names it.

    A BIOS attribute's values have both a name and a display name
    (`PerfOptimized` / "Performance Optimized"), and OME may report either. The
    registry carries the mapping, so an intended value that matches a display
    name is translated; anything else is passed through unchanged.
    """
    if intended is None:
        return None
    for value in entry.get("Value") or []:
        if isinstance(value, dict) and str(value.get("ValueDisplayName") or "") == intended:
            return str(value.get("ValueName"))
    return intended


async def compare_bios(
    idrac_ip: str, username: str, password: str, intended: dict[str, str | None]
) -> list[BiosComparison]:
    """What the template meant each BIOS attribute to be, against what it is.

    `intended` is keyed by OME's DISPLAY name, which is all OME reports. The
    BIOS attribute registry is the only bridge to the name Redfish accepts, so
    it is read here rather than shipped to the workflow: an R660's registry runs
    to hundreds of entries and would sit in Temporal history on every poll.

    An attribute the registry does not know is reported with
    `attribute_name=None` rather than dropped — silence would read as "verified"
    when it means "not checked".

    A LIMIT worth knowing: the registry is flat, so two attributes sharing a
    display name cannot be told apart and the first wins. Dell's own client
    disambiguates by the GROUP PATH when it reads a template
    (`recurse_subattr_list` joins the group DisplayNames), but the BIOS registry
    carries no such path, so there is nothing here to join on. Dell's BIOS
    display names are unique in practice; if that ever stops being true, the
    symptom is one attribute verified in place of another.
    """
    async with _client(idrac_ip, username, password) as client:
        registry = await _get(client, BIOS_REGISTRY)
        current = (await _get(client, BIOS)).get("Attributes") or {}
    by_display = _registry_by_display(registry)

    compared: list[BiosComparison] = []
    for display, value in intended.items():
        entry = by_display.get(display)
        if entry is None:
            compared.append(BiosComparison(display_name=display, intended=value))
            continue
        name = str(entry.get("AttributeName") or "")
        actual = current.get(name)
        compared.append(
            BiosComparison(
                display_name=display,
                attribute_name=name or None,
                intended=_value_name(entry, value),
                actual=None if actual is None else str(actual),
                read_only=bool(entry.get("ReadOnly")),
            )
        )
    return compared


async def stage_bios_attributes(
    idrac_ip: str, username: str, password: str, attributes: dict[str, str]
) -> str:
    """Stage BIOS values and queue the job that applies them on the next reset.

    Two calls, as Dell's own CreateBiosConfigJob scripts do: PATCH the pending
    values onto `Bios/Settings`, then POST a job whose `TargetSettingsURI` is
    that resource. The job sits `Scheduled` until a reset — the SAME reset the
    storage jobs need, so one reboot applies BIOS drift and RAID together.

    Idempotent the same way `stage_storage` is: a controller may hold only one
    pending configuration job, so an existing `Configure: BIOS.Setup.1-1` is
    returned rather than a second one queued.
    """
    async with _client(idrac_ip, username, password) as client:
        pending = _pending_for(await _pending_jobs(client), _BIOS_FQDD)
        if pending:
            return pending
        # ONE call, as Dell's GetSetBiosAttributesREDFISH.py does it: the
        # ApplyTime is what makes the iDRAC create the config job, and it comes
        # back in the Location header. Patching the attributes alone leaves
        # pending values with nothing scheduled to apply them.
        resp = await _request(
            client,
            "PATCH",
            BIOS_SETTINGS,
            json={"@Redfish.SettingsApplyTime": {"ApplyTime": "OnReset"}, "Attributes": attributes},
        )
    return _job_id(resp, f"staging {len(attributes)} BIOS attribute(s)")


async def set_root_password(
    idrac_ip: str, username: str, current_password: str, new_password: str
) -> bool:
    """Set root's password, authenticating with the one it has now.

    True when this call changed it, False when root already had the new one.

    Applied BEFORE OME discovers the machine, so OME is only ever given the
    password root will keep. The alternative — letting the template set it and
    re-pointing OME afterwards — cannot work here: without an OME Advanced
    licence there is no `OME_<guid>` service account, so OME talks to the iDRAC
    with the discovery credential and a later change strands it.

    RETRY SAFETY. A retry after a successful PATCH would authenticate with a
    password the machine no longer has. Rather than probing the new password
    first — which would spend a failed login on every run, and iDRAC9 blocks an
    address after three — this lets the 401 happen and only then asks whether
    the new password works. The happy path costs no failed login at all; only a
    retry-after-success costs one.
    """
    async with _client(idrac_ip, username, current_password) as client:
        account = await _send(client, "GET", ROOT_ACCOUNT)
        path = ROOT_ACCOUNT
        if account.status_code == 404:
            # An iDRAC10 machine: the accounts moved to the standard collection.
            path = ROOT_ACCOUNT_IDRAC10
            account = await _send(client, "GET", path)
        if account.status_code not in (401, 403):
            _classify(account, f"GET {path}")
            body = account.json()
            owner = str((body or {}).get("UserName") or "") if isinstance(body, dict) else ""
            # Slot 2 is root by Dell convention, but a machine configured by
            # hand could have it renamed, and changing the WRONG account is how
            # OME loses a server for good.
            if owner and owner != username:
                raise IdracRequestRejectedError(
                    f"iDRAC {idrac_ip} account slot 2 belongs to {owner!r}, not {username!r} — "
                    "refusing to change it; this workflow only ever touches root"
                )
            await _request(client, "PATCH", path, json={"Password": new_password})
            return True

    if await check_login(idrac_ip, username, new_password):
        return False
    raise IdracAuthError(
        f"iDRAC {idrac_ip} refused root's current credential, and does not accept the target "
        "password either — root's password changed under this run"
    )


async def clear_os_hostname(idrac_ip: str, username: str, password: str) -> bool:
    """Blank the OS hostname the iDRAC reports; True when it changed.

    The Redfish equivalent of `racadm set System.ServerOS.HostName ""`, which is
    the command the DC team runs by hand. That is a SYSTEM ATTRIBUTE
    (`ServerOS.1.HostName`), not the standard ComputerSystem `HostName`
    property — Dell populates that one from the OS through iSM. Dell's own
    SetIdracLcSystemAttributesREDFISH.py writes System attributes to the
    DellAttributes resource used here.

    The current value is still READ from the ComputerSystem resource, because
    `read_identity` fetches it anyway and the two report the same thing.

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
        await _request(
            client, "PATCH", SYSTEM_ATTRIBUTES, json={"Attributes": {OS_HOSTNAME_ATTRIBUTE: ""}}
        )
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
