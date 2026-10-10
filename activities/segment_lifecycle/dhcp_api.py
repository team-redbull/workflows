"""The DHCP scope API (dhcp_scope_manager) — release-segment's safety net.

Split out of activities.py for the reason values_repo.py and server-lifecycle's
server_scan.py are: it holds ONE technology (httpx against the DHCP scope API)
and takes its endpoint and token as PARAMETERS rather than reading settings, so
tests drive it with respx against any base URL. The @activity.defn wrappers in
activities.py own the settings and the logging.

Two calls are the whole surface, both keyed by the scope's network address (the
segment's CIDR with the mask dropped, e.g. 10.20.90.0):

  * GET /api/v1/scopes/{network} — anonymous. 404 is a normal answer: the scope
    does not exist, which is the usual state by the time release-segment runs
    (the Argo CD cascade already deleted it).
  * DELETE /api/v1/scopes/{network} — Bearer DHCP_API_TOKEN. 204 whether or not
    the scope existed, so a retry is always safe.

This is the ONLY write the orchestrator makes to the DHCP API, and it is only
ever a delete: Crossplane remains the one creator and updater of scopes.
"""

from __future__ import annotations

from typing import NoReturn

import httpx

from shared.exceptions import DhcpApiAuthError, DhcpApiError, DhcpScopeInvalidError
from shared.models.segment_lifecycle import DhcpExclusion, DhcpScopeState

# The DHCP API runs PowerShell against a remote Windows DHCP server, each command
# capped at 60s on its side, and a DELETE chains several of them (existence,
# state, failover detach, one per exclusion, the scope itself). So this budget
# is larger than the Segments Manager's 60s and stays strictly below the
# workflow's DHCP start_to_close_timeout (180s). A DELETE cut off mid-way is
# harmless: the retry is idempotent.
_HTTP_TIMEOUT = httpx.Timeout(150.0)

# See activities.py's _TLS_VERIFY for the full reasoning: the airgapped internal
# CA is not in this image's trust store, and with unbounded retries a handshake
# failure would retry forever.
_TLS_VERIFY = False

_SCOPE_PATH = "/api/v1/scopes/{network}"


def _client(base_url: str) -> httpx.AsyncClient:
    """A fresh, per-invocation client — created inside each call."""
    return httpx.AsyncClient(base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY)


def _detail(resp: httpx.Response) -> str:
    """The API's own error text — its body is {"error": {"code", "message",
    "details"}} — falling back to the raw body."""
    try:
        body = resp.json()
    except ValueError:
        return resp.text[:500]
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict) and error.get("message"):
        return f"{error.get('code', '')}: {error['message']}".lstrip(": ")
    return resp.text[:500]


def _raise_for(action: str, resp: httpx.Response) -> NoReturn:
    """Classify a non-2xx answer. 401/403 and 400 are deterministic (the
    workflow lists them non-retryable); anything else — 500, a 503 with no DHCP
    server configured behind the API, a 504 from its PowerShell layer — is
    transient and retried."""
    if resp.status_code in (401, 403):
        raise DhcpApiAuthError(
            f"{action} unauthorized ({resp.status_code}): check DHCP_API_TOKEN "
            "against the DHCP API's own token"
        )
    if resp.status_code == 400:
        raise DhcpScopeInvalidError(f"{action} rejected: {_detail(resp)}")
    raise DhcpApiError(f"{action} failed: {resp.status_code} {_detail(resp)}")


async def fetch_scope(base_url: str, network: str) -> DhcpScopeState:
    """The scope for `network`, or found=False when there is none."""
    action = f"DHCP scope lookup for {network}"
    async with _client(base_url) as client:
        try:
            resp = await client.get(_SCOPE_PATH.format(network=network))
        except httpx.HTTPError as exc:
            raise DhcpApiError(f"{action} failed: {exc}") from exc
    if resp.status_code == 404:
        return DhcpScopeState(found=False)
    if resp.status_code != 200:
        _raise_for(action, resp)
    try:
        body = resp.json()
        # The API's own camelCase; an absent list means no exclusions.
        return DhcpScopeState(
            found=True,
            exclusions=[
                DhcpExclusion(
                    start_address=exclusion["startAddress"],
                    end_address=exclusion["endAddress"],
                )
                for exclusion in body.get("exclusions") or []
            ],
        )
    except Exception as exc:  # a malformed payload: a proxy page, a schema drift
        raise DhcpApiError(f"Invalid DHCP scope response for {network}: {exc}") from exc


async def remove_scope(base_url: str, token: str, network: str) -> None:
    """Delete the scope for `network`. 204 whether or not it existed."""
    action = f"DHCP scope delete for {network}"
    async with _client(base_url) as client:
        try:
            resp = await client.delete(
                _SCOPE_PATH.format(network=network),
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError as exc:
            raise DhcpApiError(f"{action} failed: {exc}") from exc
    if resp.status_code not in (200, 204):
        _raise_for(action, resp)
