"""Segment-connectivity activity implementations — the execution limbs.

These run in the segment-connectivity activity deployment. They talk to:
  - the team's Segments Manager (SEGMENTS_MANAGER_URL) — validate/list segments,
    unlock (Bearer token via SEGMENTS_MANAGER_API_TOKEN; GETs are public)
  - the next connectivity service (NEXT_URL) — a black box; we trust its output
    (token renewal, open firewall rules, request status)

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: unlock treats an already-unlocked segment as success;
    re-submitting identical open-rules requests converges to the same firewall
    state (worst case an orphan request id we never poll).
  * Every httpx.AsyncClient is created INSIDE the activity via `async with`,
    with an explicit timeout strictly below the workflow's
    start_to_close_timeout (30s). This frees the worker on a network hang
    before Temporal times the activity out, and keeps auth tokens scoped to a
    single invocation (no global leak).
"""

from __future__ import annotations

import asyncio

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared.exceptions import (
    BmcSegmentNotConfiguredError,
    NextApiError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentNotFoundError,
)
from shared.models.segment_connectivity import (
    BmcOpenRulesRequest,
    SegmentConnectivityFailureNotice,
    SegmentConnectivityInput,
    SegmentConnectivityRequestRef,
    SegmentConnectivityRequestsUpdate,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
    SegmentType,
)
from shared.settings import SegmentConnectivityActivitySettings

_settings = SegmentConnectivityActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (90s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(60.0)

# STUB — Phase 1: system names / comment labels for the next payload. Replace
# with real values (or configuration) when the next-service contract is final.
_SYSTEM_NAMES: dict[SegmentType, str] = {
    SegmentType.HC: "hosted-cluster",
    SegmentType.MCE: "mce",
    SegmentType.INVENTORY: "inventory",
    SegmentType.PXE: "pxe",
}
_COMMENT_LABELS: dict[SegmentType, str] = {
    SegmentType.HC: "Hosted Cluster",
    SegmentType.MCE: "MCE",
    SegmentType.INVENTORY: "Inventory",
    SegmentType.PXE: "PXE",
}

# BMC is not a SegmentType (it's not Segments-Manager-tracked — see
# get_bmc_segment), so its labels live outside the SegmentType-keyed dicts
# above rather than stretching those dicts to cover a type that can never be
# queried, listed, or given as workflow input.
_BMC_SYSTEM_NAME = "bmc"
_BMC_COMMENT_LABEL = "BMC"

# Port policy per (source, destination) type pair, straight from the ConfigMap
# (syntax validated fail-fast at worker startup by SegmentConnectivityActivitySettings).
# New type pairs: add a PORTS_<SRC>_TO_<DST> settings field + an entry here.
# Deliberately excludes BMC: _peer_types() derives Segments-Manager-queryable
# peer types from this dict's keys, and BMC segments are never queryable from
# the Segments Manager (see get_bmc_segment) — an (MCE, BMC) entry here would
# make list_peer_segments wrongly try `GET /api/segments?type=BMC`.
_PORT_PROFILES: dict[tuple[SegmentType, SegmentType], dict[str, list[str]]] = {
    (SegmentType.HC, SegmentType.MCE): _settings.ports_hc_to_mce,
    (SegmentType.MCE, SegmentType.HC): _settings.ports_mce_to_hc,
    (SegmentType.INVENTORY, SegmentType.MCE): _settings.ports_inventory_to_mce,
    (SegmentType.MCE, SegmentType.INVENTORY): _settings.ports_mce_to_inventory,
    (SegmentType.PXE, SegmentType.MCE): _settings.ports_pxe_to_mce,
    (SegmentType.MCE, SegmentType.PXE): _settings.ports_mce_to_pxe,
}


def _expand_ports(profile: dict[str, list[str]]) -> list[dict]:
    """Expand the compact ConfigMap port syntax into the next API's structure.

    {"tcp": ["30000-32767"], "udp": ["9000"]} ->
    [{"type": "range", "port_range_start": 30000, "port_range_end": 32767, "protocol": "TCP"},
     {"type": "port", "port": 9000, "protocol": "UDP"}]
    """
    ports: list[dict] = []
    for protocol, entries in profile.items():
        for entry in entries:
            if "-" in entry:
                start, end = entry.split("-", 1)
                ports.append(
                    {
                        "type": "range",
                        "port_range_start": int(start),
                        "port_range_end": int(end),
                        "protocol": protocol.upper(),
                    }
                )
            else:
                ports.append(
                    {"type": "port", "port": int(entry), "protocol": protocol.upper()}
                )
    return ports


def _segments_manager_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for the Segments Manager."""
    return httpx.AsyncClient(base_url=_settings.segments_manager_url, timeout=_HTTP_TIMEOUT)


def _segments_manager_auth() -> dict[str, str]:
    """Bearer header for mutating Segments Manager calls (GETs are public)."""
    return {"Authorization": f"Bearer {_settings.segments_manager_api_token}"}


def _raise_segments_manager_error(action: str, resp: httpx.Response) -> None:
    """Classify a non-2xx Segments Manager response: 401/403 is a deterministic
    credentials problem (non-retryable in the workflow); anything else is
    treated as transient and retried."""
    if resp.status_code in (401, 403):
        raise SegmentsManagerAuthError(
            f"{action} unauthorized ({resp.status_code}): check "
            "SEGMENTS_MANAGER_API_TOKEN"
        )
    raise SegmentsManagerError(f"{action} failed: {resp.status_code} {resp.text}")


def _next_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for the next connectivity service."""
    return httpx.AsyncClient(base_url=_settings.next_url, timeout=_HTTP_TIMEOUT)


async def _fetch_next_token(client: httpx.AsyncClient) -> str:
    """Renew a next API access token; fetched fresh inside every invocation."""
    try:
        resp = await client.post(_settings.next_token_renewal_uri, json={})
    except httpx.HTTPError as exc:
        raise NextApiError(f"Token renewal call failed: {exc}") from exc
    if resp.status_code != 200:
        raise NextApiError(f"Token renewal returned {resp.status_code}: {resp.text}")
    token = resp.json().get("access_token")
    if not token:
        raise NextApiError(f"Token renewal response missing access_token: {resp.text}")
    return token


@activity.defn
async def get_segment_site(connectivity_input: SegmentConnectivityInput) -> str:
    """Validate the input segment exists (by type + CIDR) and return its site."""
    async with _segments_manager_client() as client:
        resp = await client.get(
            "/api/segments", params={"type": connectivity_input.type.value}
        )
        if resp.status_code != 200:
            _raise_segments_manager_error("List segments", resp)
        for seg in resp.json():
            if seg.get("segment") == connectivity_input.segment:
                site = seg.get("site")
                if not site:
                    raise SegmentsManagerError(f"Segment entry missing 'site': {seg}")
                activity.logger.info(
                    "Validated segment %s (type=%s) exists in site=%s",
                    connectivity_input.segment,
                    connectivity_input.type.value,
                    site,
                )
                return site
    raise SegmentNotFoundError(
        f"Segment {connectivity_input.segment} (type={connectivity_input.type.value}) "
        "not found in the Segments Manager"
    )


def _peer_types(source_type: SegmentType) -> list[SegmentType]:
    """Destination types source_type peers with, derived from the configured
    port profiles — the port policy IS the peering topology, so a future
    segment type wires up symmetric peer-discovery as soon as its PORTS_*
    config is added, with no changes here. Sorted for stable, reproducible
    output ordering (log readability / test determinism)."""
    return sorted(
        {dest for (src, dest) in _PORT_PROFILES if src == source_type},
        key=lambda t: t.value,
    )


async def _list_segments_by_type(
    client: httpx.AsyncClient, seg_type: SegmentType, site: str
) -> list[SegmentRef]:
    resp = await client.get("/api/segments", params={"type": seg_type.value})
    if resp.status_code != 200:
        _raise_segments_manager_error(f"List {seg_type.value} segments", resp)
    refs: list[SegmentRef] = []
    for seg in resp.json():
        if seg.get("site") != site:
            continue
        cidr = seg.get("segment")
        if not cidr:
            raise SegmentsManagerError(f"{seg_type.value} segment entry missing 'segment': {seg}")
        refs.append(SegmentRef(segment=cidr, type=seg_type))
    return refs


@activity.defn
async def list_peer_segments(query: PeerSegmentsQuery) -> list[SegmentRef]:
    """Return every same-site segment eligible to peer with query.source_type."""
    peer_types = _peer_types(query.source_type)
    if not peer_types:
        # A code/config gap (a supported type with no PORTS_* profile wired
        # up yet) — not reachable with today's 4 types, but fail loudly
        # rather than silently treating it as "nothing co-located yet".
        raise ApplicationError(
            f"No peer types configured for source_type={query.source_type.value} "
            "(add PORTS_<SRC>_TO_<DST> config entries)",
            type="PeerTypesNotConfigured",
            non_retryable=True,
        )
    async with _segments_manager_client() as client:
        results = await asyncio.gather(
            *(_list_segments_by_type(client, t, query.site) for t in peer_types)
        )
    segments = [ref for group in results for ref in group]
    activity.logger.info(
        "Found %d peer segment(s) in site=%s for source_type=%s (peer types=%s)",
        len(segments),
        query.site,
        query.source_type.value,
        [t.value for t in peer_types],
    )
    return segments


async def _submit_next_open_rules(
    *,
    source_segment: str,
    source_system_name: str,
    destination_segment: str,
    destination_system_name: str,
    comment: str,
    profile: dict[str, list[str]],
) -> SegmentConnectivityRequestRef:
    """Build the next-API payload and submit it. Shared by submit_open_rules
    and submit_bmc_open_rules — everything below this point is generic over
    who the source/destination are.

    Idempotent in effect: a retry after an unacknowledged-but-accepted POST
    opens identical rules, which converge to the same firewall state (the
    duplicate request id is simply never polled).
    """
    payload = {
        "ad_groups": [],
        "comment": comment,
        "properties": {
            "source": {
                "system_name": source_system_name,
                "domain": _settings.domain,
                "addresses": [{"type": "segment", "segment": source_segment}],
            },
            "destination": {
                "system_name": destination_system_name,
                "domain": _settings.domain,
                "addresses": [{"type": "segment", "segment": destination_segment}],
            },
            "ports": _expand_ports(profile),
        },
    }

    async with _next_client() as client:
        token = await _fetch_next_token(client)
        try:
            resp = await client.post(
                _settings.next_open_rules_uri,
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError as exc:
            raise NextApiError(f"Open-rules call failed: {exc}") from exc
        if resp.status_code not in (200, 201):
            raise NextApiError(
                f"Open-rules returned {resp.status_code}: {resp.text}"
            )
        try:
            ref = SegmentConnectivityRequestRef.model_validate(resp.json())
        except Exception as exc:  # malformed payload from the black box
            raise NextApiError(f"Invalid open-rules response: {exc}") from exc

    activity.logger.info(
        "Submitted open-rules request id=%d (%s -> %s)",
        ref.id,
        source_segment,
        destination_segment,
    )
    return ref


@activity.defn
async def submit_open_rules(request: OpenRulesRequest) -> SegmentConnectivityRequestRef:
    """Submit one open-firewall-rules request to the next API."""
    profile = _PORT_PROFILES.get((request.source_type, request.destination_type))
    if profile is None:
        # Deterministic: the workflow submitted a direction the port policy
        # doesn't cover — a code/config gap, not a next-service failure.
        raise ApplicationError(
            f"No port profile configured for {request.source_type.value} -> "
            f"{request.destination_type.value} (add a PORTS_* config entry)",
            type="PortProfileMissing",
            non_retryable=True,
        )
    return await _submit_next_open_rules(
        source_segment=request.source_segment,
        source_system_name=_SYSTEM_NAMES[request.source_type],
        destination_segment=request.destination_segment,
        destination_system_name=_SYSTEM_NAMES[request.destination_type],
        comment=(
            f"{_COMMENT_LABELS[request.source_type]}: {request.source_segment} -> "
            f"{_COMMENT_LABELS[request.destination_type]}: {request.destination_segment}"
        ),
        profile=profile,
    )


@activity.defn
async def get_bmc_segment(site: str) -> str:
    """Return the site's static BMC CIDR from ConfigMap (BMC_SEGMENTS_BY_SITE).

    A pure config lookup, not an API call: BMC is not a Segments-Manager-
    tracked segment type.
    """
    bmc_segment = _settings.bmc_segments_by_site.get(site)
    if not bmc_segment:
        raise BmcSegmentNotConfiguredError(
            f"No BMC segment configured for site={site} (check BMC_SEGMENTS_BY_SITE)"
        )
    return bmc_segment


@activity.defn
async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> SegmentConnectivityRequestRef:
    """Submit the one-directional MCE -> BMC open-rules request
    (PORTS_MCE_TO_BMC)."""
    return await _submit_next_open_rules(
        source_segment=request.mce_segment,
        source_system_name=_SYSTEM_NAMES[SegmentType.MCE],
        destination_segment=request.bmc_segment,
        destination_system_name=_BMC_SYSTEM_NAME,
        comment=(
            f"{_COMMENT_LABELS[SegmentType.MCE]}: {request.mce_segment} -> "
            f"{_BMC_COMMENT_LABEL}: {request.bmc_segment}"
        ),
        profile=_settings.ports_mce_to_bmc,
    )


@activity.defn
async def check_segment_connectivity_requests(request_ids: list[int]) -> list[int]:
    """Batch-check next request statuses; return the ids still pending.

    "Still pending" is a normal return value, never an error — long-term
    waiting is the workflow's timer loop's job, not activity retries.
    """
    async with _next_client() as client:
        token = await _fetch_next_token(client)
        headers = {"Authorization": f"Bearer {token}"}

        async def _check_one(request_id: int) -> tuple[int, str]:
            try:
                resp = await client.get(
                    f"{_settings.next_check_status_uri}/{request_id}", headers=headers
                )
            except httpx.HTTPError as exc:
                raise NextApiError(
                    f"Status check for request {request_id} failed: {exc}"
                ) from exc
            if resp.status_code != 200:
                raise NextApiError(
                    f"Status check for request {request_id} returned "
                    f"{resp.status_code}: {resp.text}"
                )
            status = resp.json().get("status")
            if status not in ("pending", "complete"):
                # Deterministic: a status outside the known contract (e.g. a
                # terminal "rejected") will never become pending/complete on
                # retry — fail the workflow loudly instead of retrying forever.
                raise ApplicationError(
                    f"Request {request_id} reported unexpected status {status!r}",
                    type="UnexpectedRequestStatus",
                    non_retryable=True,
                )
            return request_id, status

        # Concurrent fan-in keeps the batch well under the 30s activity timeout.
        results = await asyncio.gather(*(_check_one(rid) for rid in request_ids))

    pending = [request_id for request_id, status in results if status != "complete"]
    activity.logger.info(
        "Connectivity requests: %d/%d still pending",
        len(pending),
        len(request_ids),
    )
    return pending


@activity.defn
async def get_next_checking_request_interval() -> int:
    """Seconds the workflow should wait between polls (operator-configured)."""
    return _settings.next_checking_request_interval_seconds


@activity.defn
async def publish_request_ids(update: SegmentConnectivityRequestsUpdate) -> None:
    """Replace the pending request ids shown beside the segment's status in the
    Segments Manager UI; an empty list removes the display.

    Idempotent: PUT semantics — the manager stores exactly the list sent, and
    re-sending the current value is a no-op ("already up to date").
    """
    async with _segments_manager_client() as client:
        resp = await client.put(
            "/api/segments/segment-connectivity-requests",
            json={
                "segment": update.segment,
                "request_ids": update.request_ids,
                "submitted_at": update.submitted_at.isoformat(),
            },
            headers=_segments_manager_auth(),
        )
        if resp.status_code == 200:
            activity.logger.info(
                "Published %d pending request id(s) for segment %s",
                len(update.request_ids),
                update.segment,
            )
            return
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {update.segment} not found in the Segments Manager"
            )
        _raise_segments_manager_error("Publish request ids", resp)


@activity.defn
async def unlock_segment(segment: str) -> None:
    """Flip the segment's status Locked -> Available in the Segments Manager.

    Idempotent: the manager answers 200 "Segment already unlocked" for a
    segment that is not Locked, which we treat as success.
    """
    async with _segments_manager_client() as client:
        resp = await client.post(
            "/api/segments/unlock",
            json={"segment": segment},
            headers=_segments_manager_auth(),
        )
        if resp.status_code == 200:
            activity.logger.info("Segment %s unlocked: %s", segment, resp.text)
            return
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {segment} not found in the Segments Manager"
            )
        _raise_segments_manager_error("Unlock", resp)


@activity.defn
async def publish_segment_connectivity_failure(notice: SegmentConnectivityFailureNotice) -> None:
    """Surface a terminal workflow failure in the Segments Manager UI.

    Two steps:
      1. Clear the pending request-ids display (replace-style PUT — the ids are
         dead the moment the workflow stops driving them; the orphaned ids
         survive inside the failure message).
      2. Publish the failure note beside the segment's status badge (the
         Segments Manager's `PUT /api/segments/segment-connectivity-failure`).
    Both calls are best-effort: any error here is swallowed by the workflow
    (leaving the display cleared), so a manager hiccup never masks the original
    workflow failure.
    """
    async with _segments_manager_client() as client:
        resp = await client.put(
            "/api/segments/segment-connectivity-requests",
            json={"segment": notice.segment, "request_ids": []},
            headers=_segments_manager_auth(),
        )
        # A 404 here means the segment itself is gone — nothing to annotate.
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {notice.segment} not found in the Segments Manager"
            )
        if resp.status_code != 200:
            _raise_segments_manager_error("Clear request ids", resp)

        resp = await client.put(
            "/api/segments/segment-connectivity-failure",
            json={"segment": notice.segment, "message": notice.message},
            headers=_segments_manager_auth(),
        )
        if resp.status_code != 200:
            _raise_segments_manager_error("Publish failure note", resp)
    activity.logger.info(
        "Published connectivity failure for segment %s: %s",
        notice.segment,
        notice.message,
    )
