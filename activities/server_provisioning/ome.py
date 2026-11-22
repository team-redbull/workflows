"""Every call to OpenManage Enterprise — REST over httpx, plain parameters, no settings.

One OME session per activity invocation: `session()` logs in with
`POST /api/SessionService/Sessions` (`SessionType: API`; the `X-Auth-Token`
header carries it) and deletes `SessionService/Sessions('<Id>')` on the way out,
so no token outlives one activity.

The OME surface used here (OME 3.x/4.x REST API):

  GET  /api/DeviceService/Devices?$filter=DeviceServiceTag eq '<tag>'
  GET  /api/DiscoveryConfigService/ProtocolToDeviceType       SERVER's DeviceTypeId
  GET  /api/DiscoveryConfigService/DiscoveryConfigGroups?$top=9999   (find by name)
  POST /api/DiscoveryConfigService/DiscoveryConfigGroups      (RunNow discovery)
  GET  /api/JobService/Jobs(<id>)                             LastRunStatus
  GET  /api/TemplateService/Templates?$filter=Name eq '<name>'
  POST /api/TemplateService/Actions/TemplateService.Deploy    -> the job id, bare
  GET  /api/ProfileService/Profiles?$filter=TargetId eq <id>

EVERY SHAPE HERE WAS CHECKED AGAINST DELL'S OWN CLIENT CODE — the
`dellemc.openmanage` Ansible collection 9.12.3 (`plugins/module_utils/ome.py`,
`modules/ome_discovery.py`, `ome_template.py`, `ome_profile.py`), which is what
Dell ships to drive OME. Where this module departs from what it used to do, the
reason is in that code:

  * The discovery's ConnectionProfile mirrors `get_connection_profile`:
    `profileId: 0`, the WS-Man credential with `certificateDetail`/`isHttp`/
    `keepAlive`, and the SAME credential duplicated as `REDFISH` ("as in GUI").
  * The server device-type id is looked up in `ProtocolToDeviceType` rather than
    assumed to be 1000, as `get_protocol_device_map` does.
  * A device counts as templated only while its profile has `ProfileState > 0`
    (0 unassigned, 1 assigned for auto-deploy, 4 deployed), compared on
    `TemplateId` — `ome_template.py`'s deploy guard.
  * A profile carries its own `DeploymentTaskId`, which is how a retried deploy
    finds the job it already started instead of skipping the wait.
  * Job 2101 (Paused) is terminal failure, alongside Failed/Warning/Aborted/
    Stopped/Canceled — `RestOME.get_job_info`.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from activities.server_provisioning.http_client import TIMEOUT, VERIFY_TLS, extended_info
from shared.exceptions import (
    OmeAuthError,
    OmeError,
    OmeRequestRejectedError,
    ProfileConflictError,
    TemplateNotFoundError,
)
from shared.models.server_provisioning import OmeDevice, OmeJobState, OmeProfile

# OME job LastRunStatus ids (RestOME.get_job_info's job_status_map).
_JOB_COMPLETED = 2060
_JOB_FINISHED = {
    2060,  # Completed
    2070,  # Failed
    2090,  # Warning — completed with errors
    2100,  # Aborted
    2101,  # Paused
    2102,  # Stopped
    2103,  # Canceled
}
_SERVER_DEVICE_TYPE_NAME = "SERVER"
_REJECTED_STATUSES = {400, 404, 409, 422}


def _odata_literal(value: str) -> str:
    """Quote a value for an OData `eq` filter (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def _classify(resp: httpx.Response, what: str) -> None:
    if resp.is_success:
        return
    if resp.status_code in _REJECTED_STATUSES:
        raise OmeRequestRejectedError(f"OME refused {what} ({resp.status_code}): {extended_info(resp)}")
    # A 401 after a successful login is an expired session, not a bad account.
    raise OmeError(f"OME answered {resp.status_code} to {what}: {extended_info(resp)}")


@asynccontextmanager
async def session(base_url: str, username: str, password: str) -> AsyncIterator[httpx.AsyncClient]:
    """An authenticated OME client for one activity invocation."""
    async with httpx.AsyncClient(
        base_url=f"{base_url}/api", timeout=TIMEOUT, verify=VERIFY_TLS
    ) as client:
        try:
            resp = await client.post(
                "/SessionService/Sessions",
                json={"UserName": username, "Password": password, "SessionType": "API"},
            )
        except httpx.HTTPError as exc:
            raise OmeError(f"OME at {base_url} is unreachable: {type(exc).__name__}: {exc}") from exc
        if resp.status_code in (401, 403):
            raise OmeAuthError(f"OME rejected OME_USERNAME/OME_PASSWORD ({resp.status_code})")
        _classify(resp, "login")
        token = resp.headers.get("X-Auth-Token")
        if not token:
            raise OmeError("OME accepted the login but returned no X-Auth-Token")
        session_id = (resp.json() or {}).get("Id")
        client.headers["X-Auth-Token"] = token
        try:
            yield client
        finally:
            if session_id is not None:
                try:
                    await client.delete(f"/SessionService/Sessions('{session_id}')")
                except httpx.HTTPError:
                    pass


async def _request(client: httpx.AsyncClient, method: str, path: str, **kwargs: Any) -> httpx.Response:
    try:
        resp = await client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        raise OmeError(f"OME unreachable on {method} {path}: {type(exc).__name__}: {exc}") from exc
    _classify(resp, f"{method} {path}")
    return resp


async def _get_all(client: httpx.AsyncClient, path: str) -> list[dict[str, Any]]:
    """Every item of a paged collection, following `@odata.nextLink`."""
    items: list[dict[str, Any]] = []
    next_path: str | None = path
    while next_path:
        body = (await _request(client, "GET", next_path)).json()
        items.extend(i for i in body.get("value", []) if isinstance(i, dict))
        link = body.get("@odata.nextLink")
        next_path = link.split("/api", 1)[-1] if isinstance(link, str) and link else None
    return items


async def find_device(client: httpx.AsyncClient, service_tag: str) -> OmeDevice:
    """The device with this service tag, compared exactly (the filter narrows only)."""
    devices = await _get_all(
        client, f"/DeviceService/Devices?$filter=DeviceServiceTag eq {_odata_literal(service_tag)}"
    )
    for device in devices:
        if str(device.get("DeviceServiceTag", "")).upper() == service_tag.upper():
            return OmeDevice(found=True, device_id=int(device["Id"]), device_name=device.get("DeviceName"))
    return OmeDevice(found=False)


def _task_id(group: dict[str, Any]) -> int | None:
    for param in group.get("DiscoveryConfigTaskParam") or []:
        if isinstance(param, dict) and param.get("TaskId") is not None:
            return int(param["TaskId"])
    return None


async def _server_device_type(client: httpx.AsyncClient) -> int:
    """SERVER's DeviceTypeId, as this appliance numbers it."""
    for item in await _get_all(client, "/DiscoveryConfigService/ProtocolToDeviceType"):
        if item.get("DeviceTypeName") == _SERVER_DEVICE_TYPE_NAME:
            return int(item["DeviceTypeId"])
    raise OmeRequestRejectedError("OME's ProtocolToDeviceType lists no SERVER device type")


def _connection_profile(username: str, password: str) -> dict[str, Any]:
    """The discovery credential, shaped as Dell's `get_connection_profile` shapes it."""
    wsman = {
        "type": "WSMAN",
        "authType": "Basic",
        "modified": False,
        "credentials": {
            "username": username,
            "password": password,
            "port": 443,
            "retries": 3,
            "timeout": 60,
            "cnCheck": False,
            "caCheck": False,
            "certificateDetail": None,
            "isHttp": False,
            "keepAlive": True,
        },
    }
    return {
        "profileId": 0,
        "profileName": "",
        "profileDescription": "",
        "type": "DISCOVERY",
        "credentials": [wsman, {**wsman, "type": "REDFISH"}],
    }


async def start_discovery(
    client: httpx.AsyncClient, group_name: str, idrac_ip: str, username: str, password: str
) -> int:
    """Discover one iDRAC now; return the discovery job id.

    A group of this name that already exists is the one an earlier attempt of
    this same activity created — its job is returned instead of a second group.
    """
    groups = await _get_all(client, "/DiscoveryConfigService/DiscoveryConfigGroups?$top=9999")
    for group in groups:
        if group.get("DiscoveryConfigGroupName") == group_name:
            task_id = _task_id(group)
            if task_id is not None:
                return task_id

    body = {
        "DiscoveryConfigGroupName": group_name,
        "DiscoveryConfigModels": [
            {
                "DiscoveryConfigTargets": [{"NetworkAddressDetail": idrac_ip}],
                "ConnectionProfile": json.dumps(_connection_profile(username, password)),
                "DeviceType": [await _server_device_type(client)],
            }
        ],
        "Schedule": {"RunNow": True, "RunLater": False, "Cron": "startnow"},
        "TrapDestination": False,
        "CommunityString": False,
    }
    resp = await _request(client, "POST", "/DiscoveryConfigService/DiscoveryConfigGroups", json=body)
    task_id = _task_id(resp.json())
    if task_id is None:
        raise OmeError(f"OME created discovery {group_name!r} but returned no job id")
    return task_id


async def get_job(client: httpx.AsyncClient, job_id: int) -> OmeJobState:
    job = (await _request(client, "GET", f"/JobService/Jobs({job_id})")).json()
    status = job.get("LastRunStatus") or {}
    status_id = int(status.get("Id") or 0)
    return OmeJobState(
        job_id=job_id,
        status_id=status_id,
        status=str(status.get("Name") or status_id),
        finished=status_id in _JOB_FINISHED,
        succeeded=status_id == _JOB_COMPLETED,
    )


async def find_template_id(client: httpx.AsyncClient, template_name: str) -> int:
    templates = await _get_all(
        client, f"/TemplateService/Templates?$filter=Name eq {_odata_literal(template_name)}"
    )
    matches = [t for t in templates if t.get("Name") == template_name]
    if len(matches) != 1:
        raise TemplateNotFoundError(
            f"OME holds {len(matches)} template(s) named {template_name!r}; DELL_TEMPLATES must "
            "name exactly one"
        )
    return int(matches[0]["Id"])


def _profile(raw: dict[str, Any]) -> OmeProfile:
    return OmeProfile(
        found=True,
        profile_id=int(raw["Id"]),
        profile_name=raw.get("ProfileName"),
        template_name=raw.get("TemplateName"),
        template_id=raw.get("TemplateId"),
        profile_state=raw.get("ProfileState"),
        deployment_task_id=raw.get("DeploymentTaskId") or None,
    )


async def device_profile(client: httpx.AsyncClient, device_id: int) -> OmeProfile:
    """The profile ASSIGNED to this device (ProfileState > 0), compared exactly on TargetId."""
    profiles = await _get_all(client, f"/ProfileService/Profiles?$filter=TargetId eq {device_id}")
    assigned = [p for p in profiles if p.get("TargetId") == device_id and (p.get("ProfileState") or 0) > 0]
    return _profile(assigned[0]) if assigned else OmeProfile(found=False)


async def deploy_template(
    client: httpx.AsyncClient, template_id: int, template_name: str, device_id: int
) -> int | None:
    """Deploy the template to the device and return the deployment job id.

    A device already carrying a profile from THIS template answers with that
    profile's own `DeploymentTaskId`, so a retry (or a re-run) waits on the job
    that really deployed it; None only when OME recorded no task. A profile
    from any other template is ProfileConflictError — never redeployed over.
    """
    profile = await device_profile(client, device_id)
    if profile.found:
        if profile.template_id == template_id:
            return profile.deployment_task_id
        raise ProfileConflictError(
            f"OME device {device_id} already carries profile {profile.profile_name!r} from "
            f"template {profile.template_name!r}, not {template_name!r} — unassign it by hand to "
            "reprovision"
        )
    resp = await _request(
        client,
        "POST",
        "/TemplateService/Actions/TemplateService.Deploy",
        json={
            "Id": template_id,
            "TargetIds": [device_id],
            "Options": {
                "ShutdownType": 0,
                "TimeToWaitBeforeShutdown": 300,
                "EndHostPowerState": 1,
                "StrictCheckingVlan": True,
            },
        },
    )
    # OME answers the deploy action with the bare job id as the JSON body.
    body = resp.json()
    job_id = body.get("JobId") if isinstance(body, dict) else body
    if not isinstance(job_id, int):
        raise OmeError(f"OME accepted the deployment of {template_name!r} but returned no job id: {body!r}")
    return job_id
