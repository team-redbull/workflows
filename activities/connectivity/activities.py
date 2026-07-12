"""Connectivity activity implementations — the execution limbs.

These run in the connectivity activity deployment. They talk to:
  - the team's Segments Manager (SEGMENT_MANAGER_URL) — validate/list segments,
    unlock (Bearer token via SEGMENT_MANAGER_API_TOKEN; GETs are public)
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

from shared.exceptions import NextApiError, SegmentManagerError, SegmentNotFoundError
from shared.models.connectivity import (
    ConnectivityInput,
    ConnectivityRequestRef,
    ConnectivityRequestsUpdate,
    OpenRulesRequest,
    SegmentType,
)
from shared.settings import ConnectivityActivitySettings

_settings = ConnectivityActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (30s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(10.0)

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

# Port policy per (source, destination) type pair, straight from the ConfigMap
# (syntax validated fail-fast at worker startup by ConnectivityActivitySettings).
# New type pairs: add a PORTS_<SRC>_TO_<DST> settings field + an entry here.
_PORT_PROFILES: dict[tuple[SegmentType, SegmentType], dict[str, list[str]]] = {
    (SegmentType.HC, SegmentType.MCE): _settings.ports_hc_to_mce,
    (SegmentType.MCE, SegmentType.HC): _settings.ports_mce_to_hc,
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


def _segment_manager_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for the Segments Manager."""
    return httpx.AsyncClient(base_url=_settings.segment_manager_url, timeout=_HTTP_TIMEOUT)


def _segment_manager_auth() -> dict[str, str]:
    """Bearer header for mutating Segments Manager calls (GETs are public)."""
    return {"Authorization": f"Bearer {_settings.segment_manager_api_token}"}


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
async def validate_segment_exists(connectivity_input: ConnectivityInput) -> None:
    """Assert the input segment exists in the Segments Manager (by type + CIDR)."""
    async with _segment_manager_client() as client:
        resp = await client.get(
            "/api/segments", params={"type": connectivity_input.type.value}
        )
        if resp.status_code != 200:
            raise SegmentManagerError(
                f"List segments failed: {resp.status_code} {resp.text}"
            )
        for seg in resp.json():
            if seg.get("segment") == connectivity_input.segment:
                activity.logger.info(
                    "Validated segment %s (type=%s) exists",
                    connectivity_input.segment,
                    connectivity_input.type.value,
                )
                return
    raise SegmentNotFoundError(
        f"Segment {connectivity_input.segment} (type={connectivity_input.type.value}) "
        "not found in the Segments Manager"
    )


async def _fetch_segment_site(
    client: httpx.AsyncClient, connectivity_input: ConnectivityInput
) -> str:
    """Look up the input segment's own record and return its 'site'."""
    resp = await client.get(
        "/api/segments", params={"type": connectivity_input.type.value}
    )
    if resp.status_code != 200:
        raise SegmentManagerError(f"List segments failed: {resp.status_code} {resp.text}")
    for seg in resp.json():
        if seg.get("segment") == connectivity_input.segment:
            site = seg.get("site")
            if not site:
                raise SegmentManagerError(f"Segment entry missing 'site': {seg}")
            return site
    raise SegmentNotFoundError(
        f"Segment {connectivity_input.segment} (type={connectivity_input.type.value}) "
        "not found in the Segments Manager"
    )


@activity.defn
async def list_mce_segments(connectivity_input: ConnectivityInput) -> list[str]:
    """Return the CIDRs of MCE-type segments in the same site as the input segment."""
    async with _segment_manager_client() as client:
        site = await _fetch_segment_site(client, connectivity_input)

        resp = await client.get("/api/segments", params={"type": SegmentType.MCE.value})
        if resp.status_code != 200:
            raise SegmentManagerError(
                f"List MCE segments failed: {resp.status_code} {resp.text}"
            )
        segments: list[str] = []
        for seg in resp.json():
            if seg.get("site") != site:
                continue
            cidr = seg.get("segment")
            if not cidr:
                raise SegmentManagerError(f"MCE segment entry missing 'segment': {seg}")
            segments.append(cidr)
    activity.logger.info("Found %d MCE segment(s) in site=%s", len(segments), site)
    return segments


@activity.defn
async def submit_open_rules(request: OpenRulesRequest) -> ConnectivityRequestRef:
    """Submit one open-firewall-rules request to the next API.

    Idempotent in effect: a retry after an unacknowledged-but-accepted POST
    opens identical rules, which converge to the same firewall state (the
    duplicate request id is simply never polled).
    """
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

    payload = {
        "ad_groups": [],
        "comment": (
            f"{_COMMENT_LABELS[request.source_type]}: {request.source_segment} -> "
            f"{_COMMENT_LABELS[request.destination_type]}: {request.destination_segment}"
        ),
        "properties": {
            "source": {
                "system_name": _SYSTEM_NAMES[request.source_type],
                "domain": _settings.domain,
                "addresses": [{"type": "segment", "segment": request.source_segment}],
            },
            "destination": {
                "system_name": _SYSTEM_NAMES[request.destination_type],
                "domain": _settings.domain,
                "addresses": [{"type": "segment", "segment": request.destination_segment}],
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
            ref = ConnectivityRequestRef.model_validate(resp.json())
        except Exception as exc:  # malformed payload from the black box
            raise NextApiError(f"Invalid open-rules response: {exc}") from exc

    activity.logger.info(
        "Submitted open-rules request id=%d (%s -> %s)",
        ref.id,
        request.source_segment,
        request.destination_segment,
    )
    return ref


@activity.defn
async def check_connectivity_requests(request_ids: list[int]) -> list[int]:
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
                raise NextApiError(
                    f"Request {request_id} reported unexpected status {status!r}"
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
async def publish_request_ids(update: ConnectivityRequestsUpdate) -> None:
    """Replace the pending request ids shown beside the segment's status in the
    Segments Manager UI; an empty list removes the display.

    Idempotent: PUT semantics — the manager stores exactly the list sent, and
    re-sending the current value is a no-op ("already up to date").
    """
    async with _segment_manager_client() as client:
        resp = await client.put(
            "/api/segments/connectivity-requests",
            json={"segment": update.segment, "request_ids": update.request_ids},
            headers=_segment_manager_auth(),
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
        raise SegmentManagerError(
            f"Publish request ids failed: {resp.status_code} {resp.text}"
        )


@activity.defn
async def unlock_segment(segment: str) -> None:
    """Flip the segment's status Locked -> Available in the Segments Manager.

    Idempotent: the manager answers 200 "Segment already unlocked" for a
    segment that is not Locked, which we treat as success.
    """
    async with _segment_manager_client() as client:
        resp = await client.post(
            "/api/segments/unlock",
            json={"segment": segment},
            headers=_segment_manager_auth(),
        )
        if resp.status_code == 200:
            activity.logger.info("Segment %s unlocked: %s", segment, resp.text)
            return
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {segment} not found in the Segments Manager"
            )
        raise SegmentManagerError(
            f"Unlock failed: {resp.status_code} {resp.text}"
        )
