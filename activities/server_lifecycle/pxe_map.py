"""The site PXE VM's MAC -> iPXE map — the network-boot path for IPMI servers.

Split out of activities.py for the reason server_scan.py is: it holds ONE
technology (httpx against the PXE map service) and takes its endpoint and token
as PARAMETERS, so the tests drive it with respx and no environment. The
@activity.defn wrapper in activities.py owns the settings, picks the site's URL
and reads the iPXE script from the InfraEnv.

One call is the whole surface: `PUT /pxe-map/<mac>` with `{"ipxe_url": ...}`.
A PUT is an upsert, so a Temporal retry, a re-run and a later install of the
same machine into another InfraEnv all simply overwrite the entry.
"""

from __future__ import annotations

import httpx

from shared.exceptions import (
    PxeMapAuthError,
    PxeMapError,
    PxeMapRequestRejectedError,
)

# Below the activity's 90s start_to_close_timeout with room for the InfraEnv
# read and one PUT per bond member (two today): 2 x 25s + the read < 90s.
_HTTP_TIMEOUT = httpx.Timeout(25.0)

_MAP_PATH = "/pxe-map/{mac}"


def map_key(mac: str) -> str:
    """The MAC as the PXE map keys it: lower-case hex, no separators."""
    return mac.replace(":", "").replace("-", "").lower()


async def put_mapping(base_url: str, token: str, mac: str, ipxe_url: str) -> None:
    """Point one MAC at one iPXE script. Raises a classified error on failure.

    The client is created per call via `async with`, so the token never
    outlives one invocation, with an explicit timeout below the activity's.
    """
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=_HTTP_TIMEOUT) as client:
            resp = await client.put(
                _MAP_PATH.format(mac=map_key(mac)),
                json={"ipxe_url": ipxe_url},
                headers=headers,
            )
    except httpx.HTTPError as exc:
        raise PxeMapError(f"PXE map at {base_url} unreachable: {exc!r}") from exc

    if resp.is_success:
        return
    if resp.status_code in (401, 403):
        raise PxeMapAuthError(
            f"PXE map at {base_url} rejected our credentials ({resp.status_code}): "
            "check PXE_MAP_TOKEN"
        )
    if resp.status_code in (400, 404, 422):
        raise PxeMapRequestRejectedError(
            f"PXE map at {base_url} refused PUT {_MAP_PATH.format(mac=map_key(mac))} "
            f"({resp.status_code}): {resp.text[:500]}"
        )
    raise PxeMapError(
        f"PXE map at {base_url} failed: {resp.status_code} {resp.text[:500]}"
    )
