"""The site PXE VM's MAC -> iPXE map — the network-boot path for IPMI servers.

Split out of activities.py for the reason server_scan.py is: it holds ONE
technology (httpx against the PXE map service) and takes its endpoint and token
as PARAMETERS, so the tests drive it with respx and no environment. The
@activity.defn wrappers in activities.py own the settings, pick the site's URL
and read the iPXE script from the InfraEnv.

Two kinds of entry, served by the PXE VM's nginx under `/pxe-map/`:
  * one per MAC, `<mac>.ipxe` — what a booting host fetches;
  * one per (MCE, InfraEnv), `<mce_cluster>/<infra_env>.ipxe` — the BOOT
    ENTRY holding that InfraEnv's iPXE script URL. A MAC mapped to an InfraEnv
    chains to it, so publishing a regenerated discovery image is ONE write,
    not one per host already mapped.

The calls:
  * `PUT /pxe-map/<mac>` with `{"infra_env": {"mce_cluster", "name"}}` (or,
    for runs recorded before boot entries, `{"ipxe_url": ...}`) — an upsert, so
    a Temporal retry, a re-run and a later install of the same machine into
    another InfraEnv all simply overwrite the entry;
  * `PUT /pxe-map/<mce_cluster>/<infra_env>` with `{"ipxe_url",
    "image_created_at"}` — compare-and-set on the image time server-side, so
    of several installs racing to publish the newest image wins; an older one
    gets 409, which is success for the caller (a newer image is published);
  * `DELETE /pxe-map/<mac>` — a rollback removing it; an entry already gone
    (404) is the state wanted, so it is success, not an error;
  * `DELETE /pxe-map/<mac>?mce_cluster=<mce>` — remove-server's CONDITIONAL
    delete: the PXE VM keeps (409) a MAC chaining to another MCE's boot entry.
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
_BOOT_ENTRY_PATH = "/pxe-map/{mce_cluster}/{infra_env}"


def map_key(mac: str) -> str:
    """The MAC as the PXE map keys it: lower-case hex, no separators."""
    return mac.replace(":", "").replace("-", "").lower()


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


async def _call(
    method: str, base_url: str, token: str, path: str, json: dict | None = None
) -> httpx.Response:
    """One request to the map. The client lives for this call only, so the
    token never outlives one invocation, with a timeout below the activity's."""
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=_HTTP_TIMEOUT) as client:
            return await client.request(method, path, json=json, headers=_headers(token))
    except httpx.HTTPError as exc:
        raise PxeMapError(f"PXE map at {base_url} unreachable: {exc!r}") from exc


def _raise_for(resp: httpx.Response, base_url: str, method: str, path: str, hint: str = "") -> None:
    """Classify a failed answer. Everything permanent is named, not defaulted:
    an unclassified failure retries every minute forever with the run RUNNING."""
    if resp.status_code in (401, 403):
        raise PxeMapAuthError(
            f"PXE map at {base_url} rejected our credentials ({resp.status_code}): "
            "check PXE_MAP_TOKEN"
        )
    if resp.status_code in (400, 404, 422):
        raise PxeMapRequestRejectedError(
            f"PXE map at {base_url} refused {method} {path} "
            f"({resp.status_code}): {resp.text[:500]}{hint}"
        )
    raise PxeMapError(
        f"PXE map at {base_url} failed: {resp.status_code} {resp.text[:500]}"
    )


async def put_mapping(base_url: str, token: str, mac: str, ipxe_url: str) -> None:
    """Point one MAC straight at one iPXE script URL. Raises a classified error on failure."""
    path = _MAP_PATH.format(mac=map_key(mac))
    resp = await _call("PUT", base_url, token, path, json={"ipxe_url": ipxe_url})
    if not resp.is_success:
        _raise_for(resp, base_url, "PUT", path)


async def put_mapping_to_infra_env(
    base_url: str, token: str, mac: str, mce_cluster: str, infra_env: str
) -> None:
    """Point one MAC at an InfraEnv's boot entry. Raises a classified error on failure.

    Accepted whether or not the entry exists yet — the PXE VM only records
    which entry the MAC chains to.
    """
    path = _MAP_PATH.format(mac=map_key(mac))
    resp = await _call(
        "PUT",
        base_url,
        token,
        path,
        json={"infra_env": {"mce_cluster": mce_cluster, "name": infra_env}},
    )
    if not resp.is_success:
        _raise_for(
            resp,
            base_url,
            "PUT",
            path,
            hint=(
                " — a 422 asking for ipxe_url means that PXE VM's pxe-map-api "
                "predates InfraEnv boot entries; upgrade it"
            ),
        )


async def put_infra_env_boot(
    base_url: str,
    token: str,
    mce_cluster: str,
    infra_env: str,
    ipxe_url: str,
    image_created_at: str,
) -> bool:
    """Publish one InfraEnv's iPXE script as its boot entry.

    True when written, False when the PXE VM already held a NEWER image (409)
    — success for the caller, since that image includes everything this one
    did. Raises a classified error on any other failure.
    """
    path = _BOOT_ENTRY_PATH.format(mce_cluster=mce_cluster, infra_env=infra_env)
    resp = await _call(
        "PUT",
        base_url,
        token,
        path,
        json={"ipxe_url": ipxe_url, "image_created_at": image_created_at},
    )
    if resp.is_success:
        return True
    if resp.status_code == 409:
        return False
    _raise_for(
        resp,
        base_url,
        "PUT",
        path,
        hint=(
            " — a 404 here most likely means that PXE VM's pxe-map-api predates "
            "InfraEnv boot entries; upgrade it"
        ),
    )
    return False  # unreachable: _raise_for always raises


async def delete_mapping_for_mce(
    base_url: str, token: str, mac: str, mce_cluster: str
) -> str | None:
    """Remove one MAC's entry unless it belongs to ANOTHER MCE's boot entry.

    None when the MAC is no longer mapped (removed, or never was); the PXE VM's
    reason when it kept the entry (409 — it chains to another MCE). Raises a
    classified error on any other failure.
    """
    path = _MAP_PATH.format(mac=map_key(mac))
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=_HTTP_TIMEOUT) as client:
            resp = await client.delete(
                path, params={"mce_cluster": mce_cluster}, headers=_headers(token)
            )
    except httpx.HTTPError as exc:
        raise PxeMapError(f"PXE map at {base_url} unreachable: {exc!r}") from exc
    if resp.is_success or resp.status_code == 404:
        return None
    if resp.status_code == 409:
        return resp.text[:500]
    _raise_for(resp, base_url, "DELETE", path)
    return None  # unreachable: _raise_for always raises


async def delete_mapping(base_url: str, token: str, mac: str) -> bool:
    """Remove one MAC's entry. True if it was removed, False if it was already gone."""
    path = _MAP_PATH.format(mac=map_key(mac))
    resp = await _call("DELETE", base_url, token, path)
    if resp.is_success:
        return True
    if resp.status_code == 404:
        return False
    _raise_for(resp, base_url, "DELETE", path)
    return False  # unreachable: _raise_for always raises
