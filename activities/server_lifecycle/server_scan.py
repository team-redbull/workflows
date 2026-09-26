"""The server-scan inventory read — this limb's only outbound HTTP call.

Split out of activities.py for the reason values_repo.py is: it holds ONE
technology (httpx against the inventory platform) and takes its endpoint and
token as PARAMETERS rather than reading settings, so the offline tests drive it
with respx against any base URL and no environment at all. The @activity.defn
wrapper in activities.py owns the settings and the logging.

One read is the whole surface: GET /servers/available, which returns servers
that are unclaimed, healthy, reachable and not in maintenance, live-rechecking
each one it hands back. A VIEWER token is enough — only server-scan's four
mutation endpoints need admin.
"""

from __future__ import annotations

from typing import Any

import httpx

from shared.exceptions import (
    AmbiguousServerNameError,
    ServerNotAvailableError,
    ServerScanAuthError,
    ServerScanError,
)
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    BmcEndpoint,
    ServerInterface,
)

# Must stay strictly below the activity start_to_close_timeout (90s). A live
# recheck inside server-scan reaches a vendor manager, so this is not a fast
# endpoint — 60s is the same budget the segment-lifecycle limb uses.
_HTTP_TIMEOUT = httpx.Timeout(60.0)

# See the segment-lifecycle limb's _TLS_VERIFY for the full reasoning: the
# airgapped internal CA is not in this image's trust store, and with unbounded
# retries a handshake failure would retry forever. Flip to True (and mount a CA
# bundle via SSL_CERT_FILE) once the certificates can be trusted.
_TLS_VERIFY = False

_AVAILABLE_PATH = "/servers/available"


def _query(request: AcquireServerRequest) -> dict[str, Any]:
    """The endpoint's query string for one request.

    `name` and `pattern` are mutually exclusive in the endpoint's own contract,
    and `count` only means anything for a pattern draw — a named lookup is a
    pool of one.
    """
    params: dict[str, Any] = {
        "health": request.health,
        "min_nic_macs": request.min_nic_macs,
    }
    if request.name is not None:
        params["name"] = request.name
    else:
        params["pattern"] = request.pattern
        params["count"] = request.count
    return params


def _detail(resp: httpx.Response) -> str:
    """server-scan's own RFC 9457 `detail`, which is what an operator acts on.

    It distinguishes "nothing matched the pattern" from "everything matching is
    CRITICAL, claimed, unreachable or in maintenance" — a difference the caller
    cannot otherwise see.
    """
    try:
        body = resp.json()
    except ValueError:
        return resp.text
    if isinstance(body, dict):
        return str(body.get("detail") or body)
    return str(body)


def _raise_for_status(resp: httpx.Response, request: AcquireServerRequest) -> None:
    """Classify a non-200 answer. Everything permanent is named, not defaulted.

    An unclassified failure retries every minute forever with the run sitting
    RUNNING rather than FAILED, so a bad token, an empty pool and an ambiguous
    name each get their own type; anything else is transient by classification.
    """
    if resp.status_code == 200:
        return
    if resp.status_code in (401, 403):
        raise ServerScanAuthError(
            f"server-scan rejected our credentials ({resp.status_code}): check "
            "SERVER_SCAN_API_TOKEN (a viewer token is sufficient)"
        )
    if resp.status_code == 404:
        raise ServerNotAvailableError(
            f"server-scan has no assignable server for this request: {_detail(resp)}"
        )
    if resp.status_code == 409:
        raise AmbiguousServerNameError(
            f"server name {request.name!r} matches more than one server-scan "
            f"document: {_detail(resp)}"
        )
    raise ServerScanError(
        f"server-scan lookup failed: {resp.status_code} {_detail(resp)}"
    )


def _to_acquired_server(item: dict[str, Any]) -> AcquiredServer:
    """Project one `AvailableServerItem` onto this domain's model.

    `nic_macs` is deliberately dropped rather than carried: for a Dell server
    it holds every NPAR partition MAC while `interfaces` is reduced to one
    entry per physical port, so keeping both would invite selecting from the
    wrong one.
    """
    bmc = item.get("bmc") or {}
    return AcquiredServer(
        id=item["id"],
        name=item["name"],
        vendor=item.get("vendor", ""),
        source_provider=item.get("source_provider"),
        bmc_vendor=item.get("bmc_vendor"),
        bmc=BmcEndpoint(
            scheme=bmc.get("scheme"),
            host=bmc.get("host") or "",
            host_is_ip=bool(bmc.get("host_is_ip", False)),
            port=bmc.get("port"),
            path=bmc.get("path"),
        ),
        interfaces=[
            ServerInterface(
                name=interface.get("name", ""),
                mac=interface.get("mac"),
                location=interface.get("location"),
                link_state=interface.get("link_state") or "UNKNOWN",
            )
            for interface in item.get("interfaces") or []
        ],
        site_id=item.get("site_id"),
        health_overall=item.get("health_overall", "UNKNOWN"),
        live_recheck_performed=bool(item.get("live_recheck_performed", False)),
    )


async def fetch_available_servers(
    base_url: str, token: str, request: AcquireServerRequest
) -> list[AcquiredServer]:
    """Ask server-scan for assignable candidates.

    The client is created per call via `async with`, with an explicit timeout
    below the activity's, so a network hang frees the worker before Temporal
    reaps the activity and the token never outlives one invocation.
    """
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    ) as client:
        resp = await client.get(
            _AVAILABLE_PATH, params=_query(request), headers=headers
        )

    _raise_for_status(resp, request)
    items = resp.json().get("items", [])
    if not items:
        # A 200 with nothing in it is the honest partial-fulfilment answer, but
        # for one install it means the same as a 404 and is classified as such.
        raise ServerNotAvailableError(
            "server-scan returned no candidates for this request "
            f"(name={request.name!r}, pattern={request.pattern!r}, "
            f"requested={request.count})"
        )
    return [_to_acquired_server(item) for item in items]
