"""A server's Redfish BMC: mount an ISO as virtual media and boot it once — async httpx.

Plain parameters, no settings, no Temporal (CLAUDE.md §1). The sequence is
cluster-expender's (src/boot/redfish.py: find the CD slot, eject, insert, one-time
boot to CD, power on or restart), made vendor-neutral by DISCOVERY instead of fixed
paths: the system is server-scan's collected BMC path when it is a Systems path,
else the first member of /redfish/v1/Systems; the virtual media is the manager's
(`Links.ManagedBy`) — or the system's, as newer iDRACs also expose — slot whose
MediaTypes name a CD or DVD; every action target is read from its `Actions`.

TLS is not verified: BMCs serve self-signed certificates (server-scan's and the
iDRAC client's own rule in this repo). One AsyncClient per call (AC001).

CLASSIFICATION: 401/403 -> RedfishAuthError (permanent — an iDRAC locks an address
out after repeated failures); no CD slot, no InsertMedia action, or an insert
refused as sent (400/405/422) -> VirtualMediaUnavailableError (permanent);
anything else — 5xx, a timeout, a busy BMC, an image that did not read back —
RedfishError (retried).
"""

from __future__ import annotations

from typing import Any

import httpx

from shared.exceptions import RedfishAuthError, RedfishError, VirtualMediaUnavailableError

VERIFY_TLS = False
TIMEOUT = httpx.Timeout(30.0, connect=10.0)
MAX_DETAIL_CHARS = 300
_SYSTEMS = "/redfish/v1/Systems"
_MANAGERS = "/redfish/v1/Managers"


def _detail(resp: httpx.Response) -> str:
    return resp.text[:MAX_DETAIL_CHARS].replace("\n", " ")


def _check(resp: httpx.Response, what: str, *, insert: bool = False) -> None:
    if resp.status_code < 300:
        return
    if resp.status_code in (401, 403):
        raise RedfishAuthError(f"the BMC refused the worker's credentials ({resp.status_code}) for {what}")
    if insert and resp.status_code in (400, 405, 422):
        raise VirtualMediaUnavailableError(f"the BMC refused {what} ({resp.status_code}): {_detail(resp)}")
    raise RedfishError(f"the BMC answered {resp.status_code} to {what}: {_detail(resp)}")


class Bmc:
    """One BMC session's worth of calls (a fresh AsyncClient per public function)."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._c = client

    async def get(self, path: str) -> dict[str, Any]:
        try:
            resp = await self._c.get(path)
        except httpx.HTTPError as err:
            raise RedfishError(f"the BMC could not be reached for GET {path}: {type(err).__name__}") from None
        _check(resp, f"GET {path}")
        body = resp.json()
        return body if isinstance(body, dict) else {}

    async def post(self, path: str, body: dict[str, Any], what: str, *, insert: bool = False) -> httpx.Response:
        try:
            resp = await self._c.post(path, json=body)
        except httpx.HTTPError as err:
            raise RedfishError(f"the BMC could not be reached for {what}: {type(err).__name__}") from None
        _check(resp, what, insert=insert)
        return resp

    async def patch(self, path: str, body: dict[str, Any], what: str) -> None:
        try:
            resp = await self._c.patch(path, json=body)
        except httpx.HTTPError as err:
            raise RedfishError(f"the BMC could not be reached for {what}: {type(err).__name__}") from None
        _check(resp, what)

    async def system_path(self, hint: str | None) -> str:
        if hint and hint.rstrip("/").startswith(_SYSTEMS + "/"):
            return hint.rstrip("/")
        members = (await self.get(_SYSTEMS)).get("Members") or []
        if not members:
            raise VirtualMediaUnavailableError("the BMC lists no system under /redfish/v1/Systems")
        return str(members[0]["@odata.id"]).rstrip("/")

    async def cd_media(self, system: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """(path, body) of the CD/DVD virtual media slot."""
        collections: list[str] = []
        managers = (system.get("Links") or {}).get("ManagedBy") or []
        if managers:
            manager_paths = [str(m["@odata.id"]) for m in managers]
        else:
            manager_paths = [str(m["@odata.id"]) for m in (await self.get(_MANAGERS)).get("Members") or []]
        for manager_path in manager_paths:
            ref = (await self.get(manager_path)).get("VirtualMedia") or {}
            if ref.get("@odata.id"):
                collections.append(str(ref["@odata.id"]))
        if (system.get("VirtualMedia") or {}).get("@odata.id"):
            collections.append(str(system["VirtualMedia"]["@odata.id"]))
        for collection in collections:
            for member in (await self.get(collection)).get("Members") or []:
                path = str(member["@odata.id"])
                media = await self.get(path)
                types = {str(t).upper() for t in media.get("MediaTypes") or []}
                if types & {"CD", "DVD"}:
                    return path, media
        raise VirtualMediaUnavailableError("the BMC exposes no CD/DVD virtual media slot")


def _action(resource: dict[str, Any], name: str) -> str | None:
    action = (resource.get("Actions") or {}).get(f"#{name}") or {}
    return action.get("target")


def base_url(scheme: str | None, host: str, port: int | None) -> str:
    bare = host.strip("[]")
    authority = f"[{bare}]" if ":" in bare else bare
    if port:
        authority = f"{authority}:{port}"
    return f"{scheme or 'https'}://{authority}"


async def insert_and_boot(
    *, url: str, username: str, password: str, system_hint: str | None, iso_url: str
) -> tuple[str, str, str]:
    """Mount `iso_url`, read it back, boot it once. (system path, media path, reset type)."""
    async with httpx.AsyncClient(base_url=url, auth=(username, password), verify=VERIFY_TLS, timeout=TIMEOUT) as client:
        bmc = Bmc(client)
        system_path = await bmc.system_path(system_hint)
        system = await bmc.get(system_path)
        media_path, media = await bmc.cd_media(system)

        if media.get("Inserted") and media.get("Image") != iso_url:
            eject = _action(media, "VirtualMedia.EjectMedia")
            if eject:
                await bmc.post(eject, {}, f"the eject of {media.get('Image')}")
        if not (media.get("Inserted") and media.get("Image") == iso_url):
            insert = _action(media, "VirtualMedia.InsertMedia")
            if not insert:
                raise VirtualMediaUnavailableError(f"virtual media {media_path} has no InsertMedia action")
            try:
                await bmc.post(insert, {"Image": iso_url, "Inserted": True, "WriteProtected": True},
                               "the insert of the node ISO", insert=True)
            except VirtualMediaUnavailableError:
                # iLO takes only the Image: the same insert, minimally.
                await bmc.post(insert, {"Image": iso_url}, "the insert of the node ISO", insert=True)

        read_back = await bmc.get(media_path)
        if not (read_back.get("Inserted") and read_back.get("Image") == iso_url):
            raise RedfishError(
                f"virtual media {media_path} reads back Inserted={read_back.get('Inserted')} "
                f"Image={str(read_back.get('Image'))[:200]}, not the node ISO; retrying"
            )

        await bmc.patch(system_path, {"Boot": {"BootSourceOverrideTarget": "Cd", "BootSourceOverrideEnabled": "Once"}},
                        "the one-time boot override to CD")
        reset = _action(system, "ComputerSystem.Reset") or f"{system_path}/Actions/ComputerSystem.Reset"
        reset_type = "ForceRestart" if str(system.get("PowerState")) == "On" else "On"
        await bmc.post(reset, {"ResetType": reset_type}, f"the {reset_type} reset")
        return system_path, media_path, reset_type


async def eject(*, url: str, username: str, password: str, system_hint: str | None) -> bool:
    """Eject whatever the CD slot holds; True when something was ejected."""
    async with httpx.AsyncClient(base_url=url, auth=(username, password), verify=VERIFY_TLS, timeout=TIMEOUT) as client:
        bmc = Bmc(client)
        system = await bmc.get(await bmc.system_path(system_hint))
        media_path, media = await bmc.cd_media(system)
        if not media.get("Inserted"):
            return False
        target = _action(media, "VirtualMedia.EjectMedia")
        if not target:
            raise VirtualMediaUnavailableError(f"virtual media {media_path} has no EjectMedia action")
        await bmc.post(target, {}, "the eject of the node ISO")
        return True
