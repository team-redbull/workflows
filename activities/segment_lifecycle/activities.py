"""Segment-lifecycle activity implementations — the execution limbs.

These run in the `segment-lifecycle-worker` deployment. They talk to:
  - the team's Segments Manager (SEGMENTS_MANAGER_URL) — create/list segments,
    publish request ids, unlock (Bearer token via SEGMENTS_MANAGER_API_TOKEN;
    GETs are public)
  - the next connectivity service (NEXT_URL) — a black box; we trust its output
    (token renewal, open firewall rules, request status)
  - the day1 values repo (DAY1_REPO_URL) — git subprocess via
    activities/segment_lifecycle/values_repo.py (clone, append the allocation
    block, push; the token is never logged)
  - the DHCP scope API (DHCP_API_URL) — READ-ONLY: the allocate-segment
    workflow polls a scope to observe Crossplane's convergence, never writes one

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: create accepts an existing segment that matches the requested
    definition; unlock treats an already-unlocked segment as success;
    re-submitting identical open-rules requests converges to the same firewall
    state (worst case an orphan request id we never poll).
  * Every httpx.AsyncClient is created INSIDE the activity via `async with`,
    with an explicit timeout strictly below the workflow's
    start_to_close_timeout (30s). This frees the worker on a network hang
    before Temporal times the activity out, and keeps auth tokens scoped to a
    single invocation (no global leak).
  * TLS verification is disabled on every client (_TLS_VERIFY) because the
    airgapped environment's internal CA cannot be injected into this image.
"""

from __future__ import annotations

import asyncio

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from activities.segment_lifecycle import values_repo
from activities.segment_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import (
    BmcSegmentNotConfiguredError,
    DhcpApiError,
    NextApiError,
    SegmentConflictError,
    SegmentConversionConflictError,
    SegmentPoolExhaustedError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentNotFoundError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    BmcOpenRulesRequest,
    BmcRuleDirection,
    BmcSegments,
    BmcVendor,
    ClusterFileLocation,
    ClusterValuesAppendRequest,
    ConvertibleSegment,
    ConvertibleSegmentsQuery,
    DhcpExclusion,
    DhcpScopeState,
    SegmentConnectivityFailureNotice,
    InitializeSegmentInput,
    NextRequestRef,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentConnectivityRequestsUpdate,
    SegmentEntry,
    SegmentTypeUpdate,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
    SegmentType,
    ValuesCommitRef,
)
from shared.settings import SegmentLifecycleActivitySettings

_settings = SegmentLifecycleActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (90s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(60.0)

# TLS verification is OFF for every outbound call this worker makes.
#
# The airgapped environment serves its endpoints (starting with the next
# connectivity service) from an internal CA, and that CA cannot be injected
# into this pod's trust store — so httpx's default certifi bundle rejects
# every handshake with CERTIFICATE_VERIFY_FAILED and the retry policy, being
# unbounded, retries it forever. A deliberate, environment-driven decision
# recorded in ONE place: flip this to True (and mount a CA bundle, e.g. via
# SSL_CERT_FILE) the day the certificates can be trusted properly.
#
# The git subprocess has its own trust store and is switched off separately —
# see values_repo._run_git.
_TLS_VERIFY = False

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
# get_bmc_segments), so its labels live outside the SegmentType-keyed dicts
# above rather than stretching those dicts to cover a type that can never be
# queried, listed, or given as workflow input. They are keyed by hardware
# VENDOR: a site has one BMC network per vendor, and naming them apart is what
# makes the two requests an MCE run submits distinguishable in next's UI.
_BMC_SYSTEM_NAMES: dict[BmcVendor, str] = {
    BmcVendor.DELL: "dell-bmc",
    BmcVendor.CISCO: "cisco-bmc",
}
_BMC_COMMENT_LABELS: dict[BmcVendor, str] = {
    BmcVendor.DELL: "Dell BMC",
    BmcVendor.CISCO: "Cisco BMC",
}

# Port policy per (source, destination) type pair, straight from the ConfigMap
# (syntax validated fail-fast at worker startup by SegmentLifecycleActivitySettings).
# New type pairs: add a PORTS_<SRC>_TO_<DST> settings field + an entry here.
# Deliberately excludes BMC: _peer_types() derives Segments-Manager-queryable
# peer types from this dict's keys, and BMC segments are never queryable from
# the Segments Manager (see get_bmc_segments) — an (MCE, BMC) entry here would
# make list_peer_segments wrongly try `GET /api/segments?type=BMC`.
# Also excludes PXE, deliberately: PXE segments exist in the Segments Manager,
# but no connectivity is opened for them (the workflow rejects a PXE input at
# _SUPPORTED_TYPES). An (MCE, PXE) entry here would re-introduce it from the
# other side — every MCE run would discover same-site PXE peers.
_PORT_PROFILES: dict[tuple[SegmentType, SegmentType], dict[str, list[str]]] = {
    (SegmentType.HC, SegmentType.MCE): _settings.ports_hc_to_mce,
    (SegmentType.MCE, SegmentType.HC): _settings.ports_mce_to_hc,
    (SegmentType.INVENTORY, SegmentType.MCE): _settings.ports_inventory_to_mce,
    (SegmentType.MCE, SegmentType.INVENTORY): _settings.ports_mce_to_inventory,
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
    return httpx.AsyncClient(
        base_url=_settings.segments_manager_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    )


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
    return httpx.AsyncClient(
        base_url=_settings.next_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    )


async def _fetch_next_token(client: httpx.AsyncClient) -> str:
    """Renew a next API access token; fetched fresh inside every invocation.

    The endpoint is an OAuth2 client-credentials token URL, so the client id +
    password from the `next-api-credentials` Secret travel as HTTP BASIC in the
    Authorization header — NOT as a body (a request body is ignored, and one
    without the header answers 401 `{"detail": "Not authenticated"}` with
    `WWW-Authenticate: Basic`). Any `grant_type` rides along in the configured
    NEXT_TOKEN_RENEWAL_URI as a query param, which httpx's base_url join keeps.
    `auth=` is per-request on purpose: only renewal is Basic, while open-rules
    and status carry the Bearer token this returns.

    Failures here are NextApiError (retryable) — a wrong credential is
    indistinguishable from an outage at this layer.
    """
    try:
        resp = await client.post(
            _settings.next_token_renewal_uri,
            auth=(_settings.next_client_id, _settings.next_password),
        )
    except httpx.HTTPError as exc:
        raise NextApiError(f"Token renewal call failed: {exc}") from exc
    if resp.status_code != 200:
        # The URL is in the message because an air-gapped operator reading this
        # in the Temporal UI otherwise cannot tell a wrong NEXT_TOKEN_RENEWAL_URI
        # from a credential problem. No secrets in it — the creds are a header.
        raise NextApiError(
            f"Token renewal to {resp.request.url} returned "
            f"{resp.status_code}: {resp.text}"
        )
    token = resp.json().get("access_token")
    if not token:
        raise NextApiError(f"Token renewal response missing access_token: {resp.text}")
    return token


# Fields compared when a create is rejected because the CIDR already exists,
# to tell "we (or a previous attempt) already created exactly this" from "the
# CIDR belongs to a different segment". Deliberately the segment's IMMUTABLE
# identity: the Segments Manager models `dhcp` as the one field editable after
# creation (PATCH /api/segments), so a flipped dhcp flag is a legitimate later
# edit, not evidence of a conflicting definition — comparing it would fail
# re-runs for a segment an operator has since reconfigured. A difference there
# is logged instead.
_SEGMENT_IDENTITY_FIELDS = ("type", "site", "vlan_id", "epg_name")


def _segments_manager_detail(resp: httpx.Response) -> str:
    """The Segments Manager's own error text, which is what an operator has to
    act on. Falls back to the raw body for anything not shaped like its
    {"detail": ...} error (a proxy's HTML, a bare list, ...)."""
    try:
        body = resp.json()
    except ValueError:
        return resp.text
    if isinstance(body, dict) and body.get("detail") is not None:
        return str(body["detail"])
    return resp.text


async def _accept_existing_segment(
    client: httpx.AsyncClient,
    rules_input: InitializeSegmentInput,
    create_resp: httpx.Response,
) -> None:
    """Resolve a rejected create: idempotent replay, or a real failure?

    The Segments Manager answers 400 both for "this CIDR/VLAN is already
    taken" and for "this definition is invalid", so only its stored record can
    tell them apart. Look the CIDR up and decide:
      * absent          -> the definition itself was rejected (SegmentValidationError)
      * present, same   -> a previous attempt already applied it: success
      * present, differs-> two definitions for one CIDR (SegmentConflictError)
    """
    resp = await client.get(
        "/api/segments/by-segment", params={"segment": rules_input.segment}
    )
    if resp.status_code == 404:
        raise SegmentValidationError(
            f"Segments Manager rejected segment {rules_input.segment}: "
            f"{_segments_manager_detail(create_resp)}"
        )
    if resp.status_code != 200:
        # Not classifiable yet — treat as transient and let the retry policy
        # ask again rather than guessing at a terminal failure.
        _raise_segments_manager_error("Look up existing segment", resp)

    existing = resp.json()
    wanted = rules_input.model_dump(mode="json")
    differences = {
        field: (wanted[field], existing.get(field))
        for field in _SEGMENT_IDENTITY_FIELDS
        if existing.get(field) != wanted[field]
    }
    if differences:
        raise SegmentConflictError(
            f"Segment {rules_input.segment} already exists in the Segments "
            f"Manager with different attributes (field: requested != stored): "
            + ", ".join(
                f"{field}: {want!r} != {got!r}" for field, (want, got) in differences.items()
            )
        )
    if existing.get("dhcp") != wanted["dhcp"]:
        activity.logger.info(
            "Segment %s already exists with dhcp=%s (requested %s) — keeping the "
            "stored value; dhcp is editable after creation",
            rules_input.segment,
            existing.get("dhcp"),
            wanted["dhcp"],
        )
    activity.logger.info(
        "Segment %s already exists with matching attributes — treating create as done",
        rules_input.segment,
    )


@activity.defn
async def create_segment(rules_input: InitializeSegmentInput) -> None:
    """Create the segment in the Segments Manager (born Locked).

    Idempotent: see _accept_existing_segment — a create rejected because the
    CIDR is already stored succeeds when the stored segment matches.
    """
    async with _segments_manager_client() as client:
        resp = await client.post(
            "/api/segments",
            json=rules_input.model_dump(mode="json"),
            headers=_segments_manager_auth(),
        )
        if resp.status_code in (200, 201):
            activity.logger.info(
                "Created segment %s (type=%s, site=%s, vlan=%d) in the Segments Manager",
                rules_input.segment,
                rules_input.type.value,
                rules_input.site,
                rules_input.vlan_id,
            )
            return
        # 400 = the manager's own validation (bad definition OR already taken);
        # 422 = the request didn't even match its schema. Both are about THIS
        # payload, so both go through the same disambiguation.
        if resp.status_code in (400, 409, 422):
            await _accept_existing_segment(client, rules_input, resp)
            return
        _raise_segments_manager_error("Create segment", resp)


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
) -> NextRequestRef:
    """Build the next-API payload and submit it. Shared by submit_open_rules
    and submit_bmc_open_rules — everything below this point is generic over
    who the source/destination are.

    Idempotent in effect: a retry after an unacknowledged-but-accepted POST
    opens identical rules, which converge to the same firewall state (the
    duplicate request id is simply never polled).
    """
    payload = {
        "ad_groups": [_settings.next_group],
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
            ref = NextRequestRef.model_validate(resp.json())
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
async def submit_open_rules(request: OpenRulesRequest) -> NextRequestRef:
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
async def get_bmc_segments(site: str) -> BmcSegments:
    """Return the site's static BMC CIDRs (one per hardware vendor the site
    hosts) from ConfigMap (SITE_NETWORKS).

    A pure config lookup, not an API call: BMC is not a Segments-Manager-
    tracked segment type. SITE_NETWORKS is the shared site topology — the same
    structure the Segments Manager reads `pool` from — so an unknown site here
    means the site is genuinely unconfigured, not that the two drifted apart.

    A vendor key may legitimately be absent (a site with only Dell or only
    Cisco hardware); SiteNetworks requires at least one and rejects a misspelt
    key, so a site that resolves here has exactly the BMC networks it should,
    and the MCE opens rules against all of them in one fan-out.
    """
    networks = _settings.site_networks.get(site)
    if networks is None:
        raise BmcSegmentNotConfiguredError(
            f"No BMC segments configured for site={site} (check SITE_NETWORKS)"
        )
    return BmcSegments(dell=networks.dell_bmc, cisco=networks.cisco_bmc)


@activity.defn
async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> NextRequestRef:
    """Submit ONE open-rules request between an MCE segment and one vendor's
    BMC network, in the direction the request names.

    PORTS_MCE_TO_BMC is the profile for all four requests an MCE run makes:
    both vendors and both directions. The ports are the same IPMI ports
    whichever way the rule runs, and the key keeps its MCE_TO_BMC name because
    renaming it would mean shipping a new ConfigMap key ahead of the image.
    """
    mce = (
        _SYSTEM_NAMES[SegmentType.MCE],
        request.mce_segment,
        _COMMENT_LABELS[SegmentType.MCE],
    )
    bmc = (
        _BMC_SYSTEM_NAMES[request.vendor],
        request.bmc_segment,
        _BMC_COMMENT_LABELS[request.vendor],
    )
    source, destination = (
        (mce, bmc) if request.direction is BmcRuleDirection.MCE_TO_BMC else (bmc, mce)
    )
    (source_system, source_segment, source_label) = source
    (destination_system, destination_segment, destination_label) = destination

    return await _submit_next_open_rules(
        source_segment=source_segment,
        source_system_name=source_system,
        destination_segment=destination_segment,
        destination_system_name=destination_system,
        comment=(
            f"{source_label}: {source_segment} -> "
            f"{destination_label}: {destination_segment}"
        ),
        profile=_settings.ports_mce_to_bmc,
    )


@activity.defn
async def check_next_requests(request_ids: list[int]) -> list[int]:
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


# --- allocate-segment -------------------------------------------------------


@activity.defn
async def get_valid_sites() -> list[str]:
    """The Segments Manager's configured site list (public GET)."""
    async with _segments_manager_client() as client:
        resp = await client.get("/api/sites")
        if resp.status_code != 200:
            _raise_segments_manager_error("List sites", resp)
        sites = resp.json().get("sites")
    if not isinstance(sites, list) or not sites:
        raise SegmentsManagerError(f"GET /api/sites returned no site list: {resp.text}")
    activity.logger.info("Segments Manager knows %d site(s): %s", len(sites), sites)
    return sites


@activity.defn
async def locate_cluster_file(cluster: str) -> ClusterFileLocation:
    """Find the cluster's values file in the day1 values repo (fresh shallow
    clone per invocation; the site is derived from the path under the
    clusters root)."""
    location = await values_repo.locate_cluster_file(
        repo_url=_settings.day1_repo_url,
        branch=_settings.day1_branch,
        token=_settings.day1_git_token,
        cluster=cluster,
    )
    activity.logger.info(
        "Cluster %s lives at %s (site=%s)", cluster, location.relative_path, location.site
    )
    return location


@activity.defn
async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
    """Reserve a segment in the Segments Manager.

    Idempotent server-side per (cluster, site, type): a repeat call returns
    the existing allocation, so a Temporal retry can never double-allocate.
    """
    async with _segments_manager_client() as client:
        resp = await client.post(
            "/api/segments/allocate",
            json={
                "cluster_name": request.cluster,
                "site": request.site,
                "type": request.type.value,
            },
            headers=_segments_manager_auth(),
        )
        if resp.status_code == 200:
            allocation = SegmentAllocation.model_validate(resp.json())
            activity.logger.info(
                "Allocated segment %s (vlan=%d, epg=%s) to cluster %s",
                allocation.segment,
                allocation.vlan_id,
                allocation.epg_name,
                request.cluster,
            )
            return allocation
        if resp.status_code == 503:
            # A drained pool must fail loudly, not retry every minute forever —
            # it is refilled by an operator creating segments, never by waiting.
            raise SegmentPoolExhaustedError(
                f"No available {request.type.value} segment at site "
                f"{request.site}: {_segments_manager_detail(resp)}"
            )
        if resp.status_code in (400, 422):
            # The manager's own validation (e.g. a bad cluster name) — the
            # generic classifier below only knows 401/403, so without this
            # mapping a 400 would retry forever.
            raise SegmentValidationError(
                f"Segments Manager rejected the allocation request: "
                f"{_segments_manager_detail(resp)}"
            )
        _raise_segments_manager_error("Allocate segment", resp)


@activity.defn
async def get_segment(segment: str) -> SegmentEntry:
    """Read one segment back (public GET) — the verification step's read-back."""
    async with _segments_manager_client() as client:
        resp = await client.get("/api/segments/by-segment", params={"segment": segment})
        if resp.status_code == 200:
            return SegmentEntry.model_validate(resp.json())
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {segment} not found in the Segments Manager"
            )
        if resp.status_code in (400, 422):
            raise SegmentValidationError(
                f"Segments Manager rejected the segment lookup: "
                f"{_segments_manager_detail(resp)}"
            )
        _raise_segments_manager_error("Look up segment", resp)


@activity.defn
async def append_allocation_to_cluster_values(
    request: ClusterValuesAppendRequest,
) -> ValuesCommitRef:
    """Append the vlanId + dhcp_values block to the cluster's values file and
    push. The DhcpValues are derived HERE (the policy lives in this worker's
    config, unreachable from the sandboxed workflow) and returned in both the
    pushed and the already-present case, for the convergence poll."""
    # The exclusion policy is per segment type — pick this allocation's. A type
    # the map does not list excludes NOTHING: an operator lists a type when it
    # reserves part of its /24 and leaves it out otherwise, rather than writing
    # an empty list to say "nothing". The block then carries a network alone
    # and the scope distributes the DHCP API's whole derived .1-.253.
    dhcp_values = build_dhcp_values(
        request.segment, _settings.dhcp_exclusion_octet_ranges.get(request.type, [])
    )
    commit_sha, changed = await values_repo.append_allocation(
        repo_url=_settings.day1_repo_url,
        branch=_settings.day1_branch,
        token=_settings.day1_git_token,
        relative_path=request.relative_path,
        cluster=request.cluster,
        vlan_id=request.vlan_id,
        dhcp_values=dhcp_values,
    )
    if changed:
        activity.logger.info(
            "Pushed allocation for %s to %s (commit %s)",
            request.cluster,
            request.relative_path,
            commit_sha,
        )
    else:
        activity.logger.info(
            "%s already carries this exact allocation — nothing pushed",
            request.relative_path,
        )
    return ValuesCommitRef(commit_sha=commit_sha, changed=changed, dhcp_values=dhcp_values)


def _dhcp_api_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for the DHCP scope API (read-only).

    No Authorization header: that API leaves its scope GETs anonymous so this poll
    needs no credential of its own. Anything that has to WRITE there still needs a
    token — Crossplane does, and holds one per cluster.
    """
    return httpx.AsyncClient(
        base_url=_settings.dhcp_api_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    )


@activity.defn
async def get_dhcp_scope(network: str) -> DhcpScopeState:
    """Read-only observation of the DHCP API for the convergence poll.

    404 is a NORMAL answer (Crossplane has not created the scope yet), never
    an error — raising there would make the unbounded retry policy swallow
    the workflow's bounded deadline.
    """
    async with _dhcp_api_client() as client:
        try:
            resp = await client.get(f"/api/v1/scopes/{network}")
        except httpx.HTTPError as exc:
            raise DhcpApiError(f"DHCP scope lookup for {network} failed: {exc}") from exc
        if resp.status_code == 404:
            activity.logger.info("DHCP scope %s does not exist yet", network)
            return DhcpScopeState(found=False)
        if resp.status_code != 200:
            raise DhcpApiError(
                f"DHCP scope lookup for {network} returned "
                f"{resp.status_code}: {resp.text}"
            )
        try:
            body = resp.json()
            # The API's own camelCase; it always returns exclusions sorted
            # ascending, matching the order the policy validator enforces, so
            # the workflow can compare the lists directly. Absent means none.
            state = DhcpScopeState(
                found=True,
                exclusions=[
                    DhcpExclusion(
                        start_address=exclusion["startAddress"],
                        end_address=exclusion["endAddress"],
                    )
                    for exclusion in body.get("exclusions") or []
                ],
            )
        except Exception as exc:  # malformed payload from the API
            raise DhcpApiError(f"Invalid DHCP scope response for {network}: {exc}") from exc
    activity.logger.info(
        "DHCP scope %s exists (%d exclusion(s))", network, len(state.exclusions)
    )
    return state


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


# --- convert-segment --------------------------------------------------------

# The ONE status a segment may be converted from. "Allocated" is excluded by
# definition (in use by a cluster). "Locked" is excluded by policy: its
# connectivity is not established, so it may still have a LIVE
# initialize-segment run — converting it would mean cancelling that run, and
# this workflow deliberately does not go there (see convert_segment.py).
_CONVERTIBLE_STATUS = "Available"


@activity.defn
async def list_convertible_segments(
    query: ConvertibleSegmentsQuery,
) -> list[ConvertibleSegment]:
    """Every Available segment of the given type at the site (public GET).

    All three filters are server-side. Status became one of them once
    "convertible" narrowed to a SINGLE status — the manager's status query
    param takes one value, which is why the old Available-or-Locked rule had
    to be applied client-side over a wider result set.
    """
    async with _segments_manager_client() as client:
        resp = await client.get(
            "/api/segments",
            params={
                "site": query.site,
                "type": query.type.value,
                "status": _CONVERTIBLE_STATUS,
            },
        )
        if resp.status_code != 200:
            _raise_segments_manager_error(
                f"List {query.type.value} segments at {query.site}", resp
            )
    hits: list[ConvertibleSegment] = []
    for seg in resp.json():
        # Strict, not tolerant (§7): the filter is the manager's to apply, but
        # a wrong status here would put a segment into the conversion loop that
        # must never be there, so an unexpected one fails the activity rather
        # than being silently skipped.
        if seg.get("status") != _CONVERTIBLE_STATUS:
            raise SegmentsManagerError(
                f"GET /api/segments?status={_CONVERTIBLE_STATUS} returned a "
                f"segment with status {seg.get('status')!r}: {seg}"
            )
        try:
            hits.append(ConvertibleSegment.model_validate(seg))
        except ValueError as exc:
            raise SegmentsManagerError(
                f"Malformed segment entry from GET /api/segments: {seg}: {exc}"
            ) from exc
    activity.logger.info(
        "Found %d convertible %s segment(s) at site=%s",
        len(hits),
        query.type.value,
        query.site,
    )
    return hits


@activity.defn
async def convert_segment_type(update: SegmentTypeUpdate) -> None:
    """Convert the segment's type in the Segments Manager (PUT /api/segments/type).

    The manager applies the whole conversion atomically: new type, status back
    to Locked, and every segment_connectivity_* field cleared. Idempotent
    server-side (a retry finds the type already set and converges); a 409 —
    Allocated segment, or the expected_type compare-and-set lost to a
    concurrent conversion — is deterministic and non-retryable.
    """
    async with _segments_manager_client() as client:
        resp = await client.put(
            "/api/segments/type",
            json={
                "segment": update.segment,
                "type": update.type.value,
                "expected_type": update.expected_type.value,
            },
            headers=_segments_manager_auth(),
        )
        if resp.status_code == 200:
            activity.logger.info(
                "Converted segment %s: type %s -> %s (%s)",
                update.segment,
                update.expected_type.value,
                update.type.value,
                _segments_manager_detail(resp),
            )
            return
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {update.segment} not found in the Segments Manager"
            )
        if resp.status_code == 409:
            raise SegmentConversionConflictError(
                f"Segments Manager refused to convert {update.segment} to "
                f"{update.type.value}: {_segments_manager_detail(resp)}"
            )
        _raise_segments_manager_error("Convert segment type", resp)
