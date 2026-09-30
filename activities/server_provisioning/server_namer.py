"""The naming service — the ONE place its request is built. Fill this in.

The service already exists inside the disconnected environment: it reads the
device's cores, memory and disks from OME, rounds them its own way and renames
the OME server profile to

    ocp-dell-<model>-<region>-<cores>c-<mem>gb-<disk>tb-<service tag>

Its exact request contract is not known to this repo yet, so `request_name` is
the opening for it: it sends everything the workflow knows (ServerNameRequest)
as JSON to SERVER_NAMER_URL and treats any 2xx as "accepted". Adapt the method,
path and body below to the real API — nothing else in the workflow depends on
them. The workflow does NOT trust the answer: it reads the profile name back
from OME and checks it against the convention, the region and the service tag.

Error classification follows the other limbs: 4xx is the service refusing
this request (deterministic, ServerNamerRejectedError), anything else —
5xx, timeouts, unreachable — is retried (ServerNamerError).
"""

from __future__ import annotations

import httpx

from shared.exceptions import ServerNamerError, ServerNamerRejectedError
from shared.models.server_provisioning import ServerNameRequest

_HTTP_TIMEOUT = httpx.Timeout(60.0)
_TLS_VERIFY = False


async def request_name(base_url: str, token: str, request: ServerNameRequest) -> None:
    """Ask the naming service to rename this device's OME profile."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY) as client:
            # TODO(naming service): the real method, path and body go here.
            resp = await client.post(base_url, json=request.model_dump(), headers=headers)
    except httpx.HTTPError as exc:
        raise ServerNamerError(f"naming service unreachable: {type(exc).__name__}: {exc}") from exc
    if resp.is_success:
        return
    if 400 <= resp.status_code < 500:
        raise ServerNamerRejectedError(
            f"naming service refused {request.service_tag} ({resp.status_code}): {resp.text[:500]}"
        )
    raise ServerNamerError(f"naming service answered {resp.status_code}: {resp.text[:500]}")
