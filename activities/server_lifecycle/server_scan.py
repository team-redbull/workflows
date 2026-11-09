"""The server-scan inventory read — this limb's only outbound HTTP call.

Split out of activities.py for the reason values_repo.py is: it holds ONE
technology (httpx against the inventory platform) and takes its endpoint and
token as PARAMETERS rather than reading settings, so the offline tests drive it
with respx against any base URL and no environment at all. The @activity.defn
wrapper in activities.py owns the settings and the logging.

Three calls are the whole surface for an install. GET /servers/available
returns servers that are unclaimed, healthy, reachable, unreserved and not in
maintenance, live-rechecking each one it hands back. POST and DELETE
/servers/{id}/reservation take and release the install lock on the one a run
chose (ADR-0035) — mutations, so the token needs server-scan's ADMIN role.
When /servers/available refuses a NAMED server, the exact-name detail read
(install_candidate_by_name) says why.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, NoReturn

import httpx

from shared.exceptions import (
    AmbiguousServerNameError,
    ServerNotAvailableError,
    ServerReservedError,
    ServerScanAuthError,
    ServerScanError,
    ServerScanRequestInvalidError,
)
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    BmcEndpoint,
    InstallCandidateRecord,
    ReleaseServerRequest,
    ReserveServerRequest,
    ServerInterface,
    ServerLockRecord,
    ServerMembership,
    ServerReservation,
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
_RESERVATION_PATH = "/servers/{server_id}/reservation"


def _query(request: AcquireServerRequest) -> dict[str, Any]:
    """The endpoint's query string for one request.

    `name` and `pattern` are mutually exclusive in the endpoint's own contract,
    and `count` only means anything for a pattern draw — a named lookup is a
    pool of one.
    """
    params: dict[str, Any] = {"health": request.health}
    if request.name is not None:
        params["name"] = request.name
    else:
        params["pattern"] = request.pattern
        params["count"] = request.count
    return params


# The most of an answer's text an error carries. Every activity error is
# recorded verbatim in history, and Temporal turns one over 2 MiB into a
# non-retryable "Failure exceeds size limit." that fails the run (CLAUDE.md §5).
_MAX_DETAIL_CHARS = 500


def _bounded(text: str) -> str:
    """The start of `text` an operator reads, saying how much was cut."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return f"{text[:_MAX_DETAIL_CHARS]}… ({len(text) - _MAX_DETAIL_CHARS} more characters cut)"


def _detail(resp: httpx.Response) -> str:
    """server-scan's own RFC 9457 `detail`, which is what an operator acts on.

    It distinguishes "nothing matched the pattern" from "everything matching is
    CRITICAL, claimed, unreachable or in maintenance" — a difference the caller
    cannot otherwise see. Bounded: whatever answers at SERVER_SCAN_URL (a proxy
    included) decides the body's size, and it goes into an activity error.
    """
    try:
        body = resp.json()
    except ValueError:
        return _bounded(resp.text)
    if isinstance(body, dict):
        return _bounded(str(body.get("detail") or body))
    return _bounded(str(body))


def _details(resp: httpx.Response) -> dict[str, Any]:
    """server-scan's RFC 9457 `details` extension, or an empty dict."""
    try:
        body = resp.json()
    except ValueError:
        return {}
    details = body.get("details") if isinstance(body, dict) else None
    return details if isinstance(details, dict) else {}


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
            "SERVER_SCAN_API_TOKEN (add-server-to-mce needs the ADMIN role, for its install lock)"
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
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    ) as client:
        resp = await client.get(
            _AVAILABLE_PATH, params=_query(request), headers=_headers(token)
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


def _headers(token: str) -> dict[str, str]:
    """The bearer header, or none where server-scan runs with auth disabled."""
    return {"Authorization": f"Bearer {token}"} if token else {}


_SEARCH_PAGE_SIZE = 50


async def _get_json(client: httpx.AsyncClient, path: str, **params: Any) -> dict[str, Any]:
    try:
        resp = await client.get(path, params=params or None)
    except httpx.HTTPError as exc:
        raise ServerScanError(f"server-scan unreachable: {type(exc).__name__}: {exc}") from exc
    if resp.status_code in (401, 403):
        raise ServerScanAuthError(
            f"server-scan rejected our credentials ({resp.status_code}): check SERVER_SCAN_API_TOKEN"
        )
    if not resp.is_success:
        raise ServerScanError(f"server-scan GET {path} failed: {resp.status_code} {_detail(resp)}")
    return resp.json()


def _server_macs(detail: dict[str, Any]) -> set[str]:
    """Every MAC a server document carries, lower-cased.

    Both lists, on purpose: `identity.nic_macs` is whole (every NPAR partition)
    and `network.interfaces` is one per physical port. For matching an Agent
    any MAC the machine owns is evidence, so the more the better.
    """
    macs = {m for m in (detail.get("identity") or {}).get("nic_macs") or [] if m}
    for iface in (detail.get("network") or {}).get("interfaces") or []:
        if iface.get("mac"):
            macs.add(iface["mac"])
    return {m.lower() for m in macs}


async def server_macs_by_name(base_url: str, token: str, server_name: str) -> list[str]:
    """Every MAC server-scan holds for the server of this exact name, or [] if unknown.

    `GET /servers?search=<name>` narrows, `GET /servers/{id}` confirms the name
    EXACTLY (case-insensitive — a search hit alone could be another machine)
    and carries the MACs. Never `/servers/available`: it hides reserved and
    claimed machines — an installed one usually is — and live-rechecks a vendor
    manager on every call. Unknown is not an error: the caller still has the
    MACs on the cluster to match by.
    """
    wanted = server_name.lower()
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY, headers=_headers(token)
    ) as client:
        page = await _get_json(client, "/servers", search=server_name, page_size=_SEARCH_PAGE_SIZE)
        macs: set[str] = set()
        for item in page.get("items", []):
            if str(item.get("name") or "").lower() not in ("", wanted):
                continue
            detail = await _get_json(client, f"/servers/{item['id']}")
            if str(detail.get("name") or "").lower() == wanted:
                macs |= _server_macs(detail)
    return sorted(macs)


async def _detail_by_exact_name(
    base_url: str, token: str, server_name: str, purpose: str
) -> dict[str, Any] | None:
    """The ONE server-scan document of this exact name (with its `id`), or None.

    `GET /servers?search=<name>` narrows, `GET /servers/{id}` confirms the name
    EXACTLY (case-insensitive — a search hit alone could be another machine).
    Two documents of one name are an ERROR, not a union: the answer is one
    machine and its `srv_…` id, which the install lock is then taken on.
    `purpose` finishes the ambiguity message ("to look up", "to install").
    """
    wanted = server_name.lower()
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY, headers=_headers(token)
    ) as client:
        page = await _get_json(client, "/servers", search=server_name, page_size=_SEARCH_PAGE_SIZE)
        matches: list[dict[str, Any]] = []
        for item in page.get("items", []):
            if str(item.get("name") or "").lower() not in ("", wanted):
                continue
            detail = await _get_json(client, f"/servers/{item['id']}")
            if str(detail.get("name") or "").lower() == wanted:
                matches.append({"id": item["id"], **detail})

    if not matches:
        return None
    if len(matches) > 1:
        raise AmbiguousServerNameError(
            f"server name {server_name!r} matches {len(matches)} server-scan documents "
            f"({', '.join(sorted(str(m.get('id')) for m in matches))}): which machine "
            f"{purpose} is ambiguous"
        )
    return matches[0]


def _membership(detail: dict[str, Any]) -> ServerMembership:
    openshift = detail.get("openshift") or {}
    maintenance = detail.get("maintenance") or {}
    return ServerMembership(
        found=True,
        server_id=str(detail.get("id")),
        server_name=detail.get("name"),
        lifecycle_state=openshift.get("lifecycle_state"),
        cluster_name=openshift.get("cluster_name") or None,
        mce_name=openshift.get("mce_name") or None,
        contested_with=openshift.get("contested_with") or None,
        maintenance=bool(maintenance.get("enabled", False)),
        maintenance_reason=maintenance.get("reason") or None,
    )


async def server_membership_by_name(
    base_url: str, token: str, server_name: str, *, now: datetime | None = None
) -> ServerMembership:
    """server-scan's record of where the server of this exact name is, and its lock.

    The same two steps as server_macs_by_name — search narrows, the detail
    confirms the name exactly — but here two documents of one name are an
    ERROR, not a union: the answer is one machine's membership and its
    `srv_…` id, which the install lock is then taken on (uninstall-server-from-hc) or
    released from (remove-server-from-mce). The lock is judged live at read time.
    """
    detail = await _detail_by_exact_name(base_url, token, server_name, "to look up")
    if detail is None:
        return ServerMembership(found=False)
    membership = _membership(detail)
    membership.lock = _lock_record(detail.get("reservation") or {}, now or datetime.now(UTC))
    return membership


def bmc_vendor_for(vendor: str | None, source_provider: str | None) -> str | None:
    """The BMC-driver vocabulary `/servers/available` puts in `bmc_vendor`.

    server-scan's own rule (`bmc_vendor_for` in its api/v1/schemas.py), because
    a server's DETAIL document carries only `identity.vendor` and the collector:
    a Cisco is CISCO (UCS, driven over IPMI) unless Intersight collected it, a
    Dell DELL, an HP HP, and a STANDALONE machine None — its driver is the
    caller's decision, which add-server-to-mce refuses to guess.
    """
    normalized = (vendor or "").lower()
    if normalized == "cisco":
        return "INTERSIGHT" if source_provider == "INTERSIGHT" else "CISCO"
    if normalized == "dell":
        return "DELL"
    if normalized == "hp":
        return "HP"
    return None


def _lock_is_live(reservation: dict[str, Any], now: datetime) -> bool:
    """server-scan's `Reservation.is_live`: held, and no expiry or one still ahead."""
    if reservation.get("holder") is None:
        return False
    expires_at = reservation.get("expires_at")
    if not expires_at:
        # A hand-edited document — `reserve` never writes this; server-scan
        # honours it, so it is live here too.
        return True
    try:
        expiry = datetime.fromisoformat(str(expires_at))
    except ValueError:
        return True  # unreadable: assume held, the safe reading
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=UTC)
    return now < expiry


def _lock_record(reservation: dict[str, Any], now: datetime) -> ServerLockRecord:
    """The document's `reservation` as the lock it is, judged live at `now`."""
    return ServerLockRecord(
        live=_lock_is_live(reservation, now),
        holder=reservation.get("holder"),
        workflow_id=reservation.get("workflow_id"),
        mce_cluster=reservation.get("mce_cluster"),
        expires_at=(
            None if reservation.get("expires_at") is None else str(reservation["expires_at"])
        ),
    )


def _acquired_from_detail(detail: dict[str, Any]) -> AcquiredServer:
    """The DETAIL document as the `AvailableServerItem` `/servers/available` would build.

    Same projection server-scan makes (`AvailableServerItem.from_server`):
    vendor from `identity`, the BMC and the interfaces from `network`, the
    driver vocabulary by bmc_vendor_for. `nic_macs` stays out for the reason
    `_to_acquired_server` gives.
    """
    identity = detail.get("identity") or {}
    network = detail.get("network") or {}
    vendor = str(identity.get("vendor") or "")
    source_provider = detail.get("source_provider")
    return _to_acquired_server(
        {
            "id": detail["id"],
            "name": detail["name"],
            "vendor": vendor,
            "source_provider": source_provider,
            "bmc_vendor": bmc_vendor_for(vendor, source_provider),
            "bmc": network.get("bmc") or {},
            "interfaces": network.get("interfaces") or [],
            "site_id": detail.get("site_id"),
            "health_overall": (detail.get("health") or {}).get("overall", "UNKNOWN"),
            "live_recheck_performed": False,
        }
    )


# The categories server-scan rolls a health verdict up from (its Health model).
_HEALTH_KEYS = ("overall", "cpu", "memory", "storage", "network", "connectivity", "power", "gpu")


async def install_candidate_by_name(
    base_url: str, token: str, server_name: str, *, now: datetime | None = None
) -> InstallCandidateRecord:
    """Everything server-scan STORES about one named server — what add-server-to-mce
    reads once `/servers/available` has refused it, to learn why.

    The same exact-name read as uninstall-server-from-hc's, never `/servers/available`,
    which is the thing that refused. Nothing is decided here: the workflow
    tells a safety refusal (a cluster's claim, maintenance, another run's lock)
    from the server's own health, which a request may waive.
    """
    detail = await _detail_by_exact_name(base_url, token, server_name, "to install")
    if detail is None:
        return InstallCandidateRecord()
    health = detail.get("health") or {}
    reachable = detail.get("reachable")
    return InstallCandidateRecord(
        membership=_membership(detail),
        server=_acquired_from_detail(detail),
        health={key: str(health[key]) for key in _HEALTH_KEYS if health.get(key) is not None},
        reachable=None if reachable is None else bool(reachable),
        lock=_lock_record(detail.get("reservation") or {}, now or datetime.now(UTC)),
    )


def _raise_for_lock_status(resp: httpx.Response, action: str) -> NoReturn:
    """Classify the answers both lock calls share; 200, 404 and 409 are the caller's."""
    if resp.status_code in (401, 403):
        raise ServerScanAuthError(
            f"server-scan refused to {action} the install lock ({resp.status_code}): "
            "SERVER_SCAN_API_TOKEN needs the ADMIN role — the reservation "
            "endpoints are mutations"
        )
    if resp.status_code in (400, 422):
        raise ServerScanRequestInvalidError(
            f"server-scan rejected the {action} request ({resp.status_code}): "
            f"{_detail(resp)}"
        )
    raise ServerScanError(
        f"server-scan {action} failed: {resp.status_code} {_detail(resp)}"
    )


def _expires_at(resp: httpx.Response) -> str | None:
    """The lock's expiry from the returned ServerDetail, when it carries one."""
    try:
        reservation = resp.json().get("reservation") or {}
    except (ValueError, AttributeError):
        return None
    return reservation.get("expires_at") if isinstance(reservation, dict) else None


async def reserve_server(
    base_url: str, token: str, request: ReserveServerRequest
) -> ServerReservation:
    """Take, or extend, the install lock on one server.

    A 409 is TWO different answers and only one of them is final. With
    `held_by` in its details, a live lock belongs to another run: permanent for
    this candidate. Without it, this caller lost a revision race to some other
    write on the document — often a collector run, not a claim — and a retry
    either wins or turns into the first kind.
    """
    body = request.model_dump(
        include={
            "holder",
            "mce_cluster",
            "infra_env",
            "namespace",
            "workflow_id",
            "ttl_seconds",
        }
    )
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    ) as client:
        resp = await client.post(
            _RESERVATION_PATH.format(server_id=request.server_id),
            json=body,
            headers=_headers(token),
        )

    if resp.status_code == 200:
        return ServerReservation(
            server_id=request.server_id, held=True, expires_at=_expires_at(resp)
        )
    if resp.status_code == 404:
        raise ServerNotAvailableError(
            f"{request.server_name} ({request.server_id}) is no longer in "
            f"server-scan's inventory: {_detail(resp)}"
        )
    if resp.status_code == 409 and _details(resp).get("held_by"):
        held = _details(resp)
        raise ServerReservedError(
            f"{request.server_name} is reserved by {held.get('held_by')!r} for "
            f"MCE {held.get('held_for_mce')!r} (run {held.get('workflow_id')!r}) "
            f"until {held.get('expires_at')}"
        )
    _raise_for_lock_status(resp, "reserve")


async def release_server(
    base_url: str, token: str, request: ReleaseServerRequest
) -> ServerReservation:
    """Release this run's install lock; nothing of ours to release is success."""
    async with httpx.AsyncClient(
        base_url=base_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    ) as client:
        resp = await client.request(
            "DELETE",
            _RESERVATION_PATH.format(server_id=request.server_id),
            json={"holder": request.holder, "workflow_id": request.workflow_id},
            headers=_headers(token),
        )

    if resp.status_code == 200:
        return ServerReservation(server_id=request.server_id, held=False)
    if resp.status_code == 404:
        return ServerReservation(
            server_id=request.server_id,
            held=False,
            detail="the server is no longer in server-scan's inventory",
        )
    if resp.status_code == 409 and _details(resp).get("held_by"):
        # Our lock lapsed and another run has since taken the machine. Clearing
        # it would hand that run's server back to the pool mid-install.
        return ServerReservation(
            server_id=request.server_id,
            held=False,
            detail=f"left alone, it is not ours: {_detail(resp)}",
        )
    # A 409 WITHOUT `held_by` is a lost revision race: a collector, membership
    # or maintenance write changed the document between server-scan's read and
    # its write, and nothing was released. Retried like any transient failure —
    # taking it as "not ours" left our lock held until its TTL.
    _raise_for_lock_status(resp, "release")
