"""Every call to OpenManage Enterprise — REST over httpx, plain parameters, no settings.

One OME session per activity invocation: `session()` logs in with
`POST /api/SessionService/Sessions` (the `X-Auth-Token` header carries it) and
deletes the session on the way out, so no token outlives one activity.

The OME surface used here (OME 3.x/4.x REST API):

  GET  /api/DeviceService/Devices?$filter=DeviceServiceTag eq '<tag>'
  GET  /api/DiscoveryConfigService/DiscoveryConfigGroups      (find by name)
  POST /api/DiscoveryConfigService/DiscoveryConfigGroups      (RunNow discovery)
  GET  /api/JobService/Jobs(<id>)                             LastRunStatus
  GET  /api/TemplateService/Templates?$filter=Name eq '<name>'
  POST /api/TemplateService/Actions/TemplateService.Deploy    -> job id
  GET  /api/ProfileService/Profiles?$filter=TargetId eq <id>

The discovery body follows Dell's own OME scripting samples: a server
(DeviceType 1000) discovered over WS-Man on 443, with the credential inside a
JSON-encoded ConnectionProfile string.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from shared.exceptions import (
    OmeAuthError,
    OmeError,
    OmeRequestRejectedError,
    ProfileConflictError,
    TemplateNotFoundError,
)
from shared.models.server_provisioning import OmeDevice, OmeJobState, OmeProfile

_HTTP_TIMEOUT = httpx.Timeout(60.0)
# The appliance ships a self-signed certificate (as server-scan's OME client notes).
_TLS_VERIFY = False

# OME job LastRunStatus ids.
_JOB_COMPLETED = 2060
_JOB_FINISHED = {
    2060,  # Completed
    2070,  # Failed
    2090,  # Warning — completed with errors
    2100,  # Aborted
    2102,  # Stopped
    2103,  # Canceled
}
_SERVER_DEVICE_TYPE = 1000
_REJECTED_STATUSES = {400, 404, 409, 422}


def _odata_literal(value: str) -> str:
    """Quote a value for an OData `eq` filter (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def _message(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return resp.text[:500]
    error = body.get("error", {}) if isinstance(body, dict) else {}
    infos = error.get("@Message.ExtendedInfo") or []
    messages = [str(i.get("Message")) for i in infos if isinstance(i, dict) and i.get("Message")]
    return "; ".join(messages) or str(error.get("message") or body)[:500]


def _classify(resp: httpx.Response, what: str) -> None:
    if resp.is_success:
        return
    if resp.status_code in _REJECTED_STATUSES:
        raise OmeRequestRejectedError(f"OME refused {what} ({resp.status_code}): {_message(resp)}")
    # A 401 after a successful login is an expired session, not a bad account.
    raise OmeError(f"OME answered {resp.status_code} to {what}: {_message(resp)}")


@asynccontextmanager
async def session(base_url: str, username: str, password: str) -> AsyncIterator[httpx.AsyncClient]:
    """An authenticated OME client for one activity invocation."""
    async with httpx.AsyncClient(
        base_url=f"{base_url}/api", timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
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


async def start_discovery(
    client: httpx.AsyncClient, group_name: str, idrac_ip: str, username: str, password: str
) -> int:
    """Discover one iDRAC now; return the discovery job id.

    A group of this name that already exists is the one an earlier attempt of
    this same activity created — its job is returned instead of a second group.
    """
    for group in await _get_all(client, "/DiscoveryConfigService/DiscoveryConfigGroups"):
        if group.get("DiscoveryConfigGroupName") == group_name:
            task_id = _task_id(group)
            if task_id is not None:
                return task_id

    connection_profile = {
        "profileName": "",
        "profileDescription": "",
        "type": "DISCOVERY",
        "credentials": [
            {
                "type": "WSMAN",
                "authType": "Basic",
                "modified": False,
                "credentials": {
                    "username": username,
                    "password": password,
                    "caCheck": False,
                    "cnCheck": False,
                    "port": 443,
                    "retries": 3,
                    "timeout": 60,
                },
            }
        ],
    }
    body = {
        "DiscoveryConfigGroupName": group_name,
        "DiscoveryConfigGroupDescription": "provision-dell-server (Temporal)",
        "DiscoveryConfigModels": [
            {
                "DiscoveryConfigTargets": [{"NetworkAddressDetail": idrac_ip}],
                "ConnectionProfile": json.dumps(connection_profile),
                "DeviceType": [_SERVER_DEVICE_TYPE],
            }
        ],
        "Schedule": {"RunNow": True, "RunLater": False, "Cron": "startnow", "StartTime": "", "EndTime": ""},
        "CreateGroup": True,
        "TrapDestination": False,
        "CommunityString": False,
        "UseAllProfiles": False,
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


async def device_profile(client: httpx.AsyncClient, device_id: int) -> OmeProfile:
    """The profile assigned to this device, compared exactly on TargetId."""
    profiles = await _get_all(client, f"/ProfileService/Profiles?$filter=TargetId eq {device_id}")
    assigned = [p for p in profiles if p.get("TargetId") == device_id]
    if not assigned:
        return OmeProfile(found=False)
    profile = assigned[0]
    return OmeProfile(
        found=True,
        profile_id=int(profile["Id"]),
        profile_name=profile.get("ProfileName"),
        template_name=profile.get("TemplateName"),
    )


async def deploy_template(
    client: httpx.AsyncClient, template_id: int, template_name: str, device_id: int
) -> int | None:
    """Deploy the template to the device; the job id, or None if already deployed.

    A device already carrying a profile from THIS template is done. One from
    any other template is ProfileConflictError — never redeployed over.
    """
    profile = await device_profile(client, device_id)
    if profile.found:
        if profile.template_name == template_name:
            return None
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
            "Schedule": {"RunNow": True, "RunLater": False},
            "Attributes": [],
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
    return int(job_id)
