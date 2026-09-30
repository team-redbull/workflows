"""server-scan lookups for provisioning — by service tag, Mongo-backed reads only.

ONE question: is a document with this serial in use by a cluster? That is the
guard the run makes before anything reboots the machine. It used to answer a
second one — whether the machine had been listed under its new name — but being
listed is no longer the run's definition of done (see provision_dell_server.py),
so the lookup at the end of the run went with it.

`GET /servers?search=<tag>` narrows by token prefix — the serial is one of
server-scan's search tokens — and `GET /servers/{id}` then confirms each hit's
serial EXACTLY, because the list projection carries no serial and a prefix
match alone could name a different machine. Never `/servers/available`: that
endpoint live-rechecks against the vendor manager on every call.
"""

from __future__ import annotations

from typing import Any

import httpx

from activities.server_provisioning.http_client import TIMEOUT, VERIFY_TLS
from shared.exceptions import ServerScanAuthError, ServerScanError
from shared.models.server_provisioning import ServerScanLookup, ServerScanState

_PAGE_SIZE = 50


def _classify(resp: httpx.Response, what: str) -> None:
    if resp.is_success:
        return
    if resp.status_code in (401, 403):
        raise ServerScanAuthError(
            f"server-scan rejected our credentials ({resp.status_code}): check SERVER_SCAN_API_TOKEN"
        )
    raise ServerScanError(f"server-scan {what} failed: {resp.status_code} {resp.text[:500]}")


async def _get(client: httpx.AsyncClient, path: str, **params: Any) -> dict[str, Any]:
    try:
        resp = await client.get(path, params=params or None)
    except httpx.HTTPError as exc:
        raise ServerScanError(f"server-scan unreachable: {type(exc).__name__}: {exc}") from exc
    _classify(resp, f"GET {path}")
    return resp.json()


async def lookup(base_url: str, token: str, request: ServerScanLookup) -> ServerScanState:
    """What server-scan holds for this service tag."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    tag = request.service_tag.upper()
    async with httpx.AsyncClient(
        base_url=base_url, timeout=TIMEOUT, verify=VERIFY_TLS, headers=headers
    ) as client:
        page = await _get(client, "/servers", search=request.service_tag, page_size=_PAGE_SIZE)
        details = [await _get(client, f"/servers/{item['id']}") for item in page.get("items", [])]

    matches = [
        d
        for d in details
        if str((d.get("identity") or {}).get("serial") or "").upper() == tag
    ]
    claimed_by: str | None = None
    for detail in matches:
        openshift = detail.get("openshift") or {}
        state = openshift.get("lifecycle_state") or "AVAILABLE"
        if state != "AVAILABLE":
            claimed_by = f"{state} {openshift.get('cluster_name') or openshift.get('mce_name') or ''}".strip()
            break

    return ServerScanState(
        name=matches[0].get("name") if matches else None,
        claimed_by=claimed_by,
    )
