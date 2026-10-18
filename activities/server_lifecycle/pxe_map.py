"""The site PXE VM's MAC -> iPXE map — the network-boot path for IPMI servers.

Split out of activities.py for the reason server_scan.py is: it holds ONE
technology (httpx against the PXE map service) and takes its endpoint and token
as PARAMETERS, so the tests drive it with respx and no environment. The
@activity.defn wrappers in activities.py own the settings, pick the site's URL
and read the iPXE script from the InfraEnv.

Two calls are the whole surface, one per MAC entry:
  * `PUT /pxe-map/<mac>` with `{"ipxe_url": ...}` — an upsert, so a Temporal
    retry, a re-run and a later install of the same machine into another
    InfraEnv all simply overwrite the entry;
  * `DELETE /pxe-map/<mac>` — a rollback removing it; an entry already gone
    (404) is the state wanted, so it is success, not an error.
"""

from __future__ import annotations

import httpx

from shared.exceptions import (
    PxeMapAuthError,
    PxeMapError,
    PxeMapRequestRejectedError,
)

# Below the activity's 90s start_to_close_timeout with room for the InfraEnv
# read and one call per bond member (two today): 2 x 25s + the read < 90s.
_HTTP_TIMEOUT = httpx.Timeout(25.0)

_MAP_PATH = "/pxe-map/{mac}"


def map_key(mac: str) -> str:
    """The MAC as the PXE map keys it: lower-case hex, no separators."""
    return mac.replace(":", "").replace("-", "").lower()


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


async def _call(
    method: str, base_url: str, token: str, mac: str, json: dict | None = None
) -> httpx.Response:
    """One request to the map. The client lives for this call only, so the
    token never outlives one invocation, with a timeout below the activity's."""
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=_HTTP_TIMEOUT) as client:
            return await client.request(
                method, _MAP_PATH.format(mac=map_key(mac)), json=json, headers=_headers(token)
            )
    except httpx.HTTPError as exc:
        raise PxeMapError(f"PXE map at {base_url} unreachable: {exc!r}") from exc


def _raise_for(resp: httpx.Response, base_url: str, method: str, mac: str) -> None:
    """Classify a failed answer. Everything permanent is named, not defaulted:
    an unclassified failure retries every minute forever with the run RUNNING."""
    if resp.status_code in (401, 403):
        raise PxeMapAuthError(
            f"PXE map at {base_url} rejected our credentials ({resp.status_code}): "
            "check PXE_MAP_TOKEN"
        )
    if resp.status_code in (400, 404, 422):
        raise PxeMapRequestRejectedError(
            f"PXE map at {base_url} refused {method} {_MAP_PATH.format(mac=map_key(mac))} "
            f"({resp.status_code}): {resp.text[:500]}"
        )
    raise PxeMapError(
        f"PXE map at {base_url} failed: {resp.status_code} {resp.text[:500]}"
    )


async def put_mapping(base_url: str, token: str, mac: str, ipxe_url: str) -> None:
    """Point one MAC at one iPXE script. Raises a classified error on failure."""
    resp = await _call("PUT", base_url, token, mac, json={"ipxe_url": ipxe_url})
    if not resp.is_success:
        _raise_for(resp, base_url, "PUT", mac)


async def delete_mapping(base_url: str, token: str, mac: str) -> bool:
    """Remove one MAC's entry. True if it was removed, False if it was already gone."""
    resp = await _call("DELETE", base_url, token, mac)
    if resp.is_success:
        return True
    if resp.status_code == 404:
        return False
    _raise_for(resp, base_url, "DELETE", mac)
    return False  # unreachable: _raise_for always raises
