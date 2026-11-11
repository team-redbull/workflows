"""Segment-lifecycle activity implementations — the execution limbs.

These run in the `segment-lifecycle-worker` deployment. They talk to:
  - the team's Segments Manager (SEGMENTS_MANAGER_URL) — create segments,
    allocate one for a cluster and read it back (Bearer token via
    SEGMENTS_MANAGER_API_TOKEN; GETs are public)
  - the DHCP scope API (DHCP_API_URL) — release-segment's safety net: GET a
    scope (anonymous) and DELETE it (Bearer DHCP_API_TOKEN) if it survived the
    Argo CD cascade, via activities/segment_lifecycle/dhcp_api.py (plain
    parameters, no settings). The only write this worker makes there;
    Crossplane stays the only creator/updater of scopes.

The day1 values repo is NOT reached from here any more (2026-11-11): the
cluster file, its branch and the allocation block written into it are the
cluster-lifecycle limb's (activities/cluster_lifecycle/), which starts
allocate-segment as a child and records what it allocated. This limb runs no
git at all.

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: create accepts an existing segment that matches the requested
    definition; allocate is idempotent server-side per (cluster, site, type);
    release answers 200 for an already-released segment and the scope DELETE
    answers 204 for an absent scope.
  * Every httpx.AsyncClient is created INSIDE the activity via `async with`,
    with an explicit timeout strictly below the workflow's
    start_to_close_timeout (60s < 90s). This frees the worker on a network hang
    before Temporal times the activity out, and keeps credentials scoped to a
    single invocation (no global leak).
  * TLS verification is disabled on every client (_TLS_VERIFY) because the
    airgapped environment's internal CA cannot be injected into this image.
"""

from __future__ import annotations

import httpx
from temporalio import activity

from activities.segment_lifecycle import dhcp_api
from shared.exceptions import (
    AmbiguousInventorySegmentError,
    InventorySegmentNotFoundError,
    AmbiguousAllocationError,
    SegmentConflictError,
    SegmentPoolExhaustedError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentNotFoundError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ClusterAllocationLookup,
    ClusterAllocationLookupRequest,
    DhcpScopeState,
    InitializeSegmentInput,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
)
from shared.settings import SegmentLifecycleActivitySettings

_settings = SegmentLifecycleActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (90s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(60.0)

# TLS verification is OFF for every outbound call this worker makes.
#
# The airgapped environment serves its endpoints from an internal CA, and that
# CA cannot be injected into this pod's trust store — so httpx's default
# certifi bundle rejects every handshake with CERTIFICATE_VERIFY_FAILED and the
# retry policy, being unbounded, retries it forever. A deliberate,
# environment-driven decision recorded in ONE place: flip this to True (and
# mount a CA bundle, e.g. via SSL_CERT_FILE) the day the certificates can be
# trusted properly.
#
# The git subprocess has its own trust store and is switched off separately —
# see activities/git_repo.exec_git.
_TLS_VERIFY = False

# The most of an answer's text an error carries. Every activity error is
# recorded verbatim in history, and Temporal turns one over 2 MiB into a
# non-retryable "Failure exceeds size limit." that fails the run (CLAUDE.md §5).
_MAX_DETAIL_CHARS = 500


def _bounded(text: str) -> str:
    """The start of `text` an operator reads, saying how much was cut."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return f"{text[:_MAX_DETAIL_CHARS]}… ({len(text) - _MAX_DETAIL_CHARS} more characters cut)"


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
    raise SegmentsManagerError(f"{action} failed: {resp.status_code} {_bounded(resp.text)}")


# Fields compared when a create is rejected because the CIDR already exists,
# to tell "we (or a previous attempt) already created exactly this" from "the
# CIDR belongs to a different segment". Deliberately the segment's IMMUTABLE
# identity: the Segments Manager models `dhcp` as the one field editable after
# creation (PATCH /api/segments), so a flipped dhcp flag is a legitimate later
# edit, not evidence of a conflicting definition — comparing it would fail
# re-runs for a segment an operator has since reconfigured. A difference there
# is logged instead. `type` is not identity at all: it is allocation state the
# manager sets and clears, so an existing segment that has since been allocated
# still matches the definition that created it.
_SEGMENT_IDENTITY_FIELDS = ("site", "vlan_id", "epg_name")


def _segments_manager_detail(resp: httpx.Response) -> str:
    """The Segments Manager's own error text, which is what an operator has to
    act on. Falls back to the raw body for anything not shaped like its
    {"detail": ...} error (a proxy's HTML, a bare list, ...). Bounded: the
    answer's size is up to whatever replies, and it goes into an activity error."""
    try:
        body = resp.json()
    except ValueError:
        return _bounded(resp.text)
    if isinstance(body, dict) and body.get("detail") is not None:
        return _bounded(str(body["detail"]))
    return _bounded(resp.text)


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

    The "present, same" branch is what makes a re-triggered workflow harmless:
    the run completes against the segment that already exists rather than
    failing an operator who re-submitted the same definition.
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
    """Create the segment in the Segments Manager, Available immediately.

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
                "Created segment %s (site=%s, vlan=%d) in the Segments Manager",
                rules_input.segment,
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
        raise SegmentsManagerError(
            f"GET /api/sites returned no site list: {_bounded(resp.text)}"
        )
    activity.logger.info("Segments Manager knows %d site(s): %s", len(sites), sites)
    return sites


@activity.defn
async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
    """Reserve a segment in the Segments Manager, as request.type.

    Any Available segment at the site qualifies — the manager stamps the type
    onto the one it hands out. Idempotent server-side per (cluster, site,
    type): a repeat call returns the existing allocation, so a Temporal retry
    can never double-allocate.
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
                f"No available segment at site {request.site} to allocate as "
                f"{request.type.value}: {_segments_manager_detail(resp)}"
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


# --- shared with the server-lifecycle domain --------------------------------

# The Segments Manager's own name for an MCE's inventory network. add-server-to-mce
# tags every NMStateConfig with the VLAN of the segment carrying this type. One
# type, because there is one inventory scope per cluster: the class-per-BMC-
# protocol split (INVENTORY_REDFISH / INVENTORY_IPMI) is reverted on both sides.
_INVENTORY_SEGMENT_TYPE = "INVENTORY"


@activity.defn
async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
    """Find the INVENTORY segment allocated to one MCE cluster."""
    async with _segments_manager_client() as client:
        # `type` is the only filter of the two this endpoint accepts —
        # verified against its OpenAPI, which declares site/status/type/fresh
        # and no cluster_name. The cluster match is therefore made below,
        # client-side, and is not optional.
        resp = await client.get(
            "/api/segments", params={"type": _INVENTORY_SEGMENT_TYPE}
        )
        if resp.status_code == 404:
            # A list endpoint answers an empty result with 200 and [], so 404
            # here means the ROUTE is absent, not that there is no segment.
            _raise_segments_manager_error("List segments", resp)
        if resp.status_code != 200:
            _raise_segments_manager_error("List segments", resp)

        payload = resp.json()
        entries = payload.get("items", payload) if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            raise SegmentsManagerError(
                f"List segments returned {type(entries).__name__}, expected a list"
            )

        # The cluster match, made here because the endpoint cannot make it.
        # The type is re-checked as well: an ignored or renamed filter would
        # otherwise hand back another cluster's VLAN, which the caller has no
        # way to detect.
        matches = [
            entry
            for entry in entries
            if entry.get("cluster_name") == mce_cluster
            and entry.get("type") == _INVENTORY_SEGMENT_TYPE
        ]
        if not matches:
            raise InventorySegmentNotFoundError(
                f"MCE cluster {mce_cluster!r} has no {_INVENTORY_SEGMENT_TYPE} "
                f"segment in the Segments Manager (searched {len(entries)} segment(s))"
            )
        if len(matches) > 1:
            raise AmbiguousInventorySegmentError(
                f"MCE cluster {mce_cluster!r} has {len(matches)} "
                f"{_INVENTORY_SEGMENT_TYPE} segments "
                f"({', '.join(str(m.get('segment')) for m in matches)}); "
                "exactly one is required — resolve the duplicate allocation"
            )
        return SegmentEntry.model_validate(matches[0])


# --- release-segment --------------------------------------------------------


@activity.defn
async def find_cluster_allocation(
    request: ClusterAllocationLookupRequest,
) -> ClusterAllocationLookup:
    """The segment the Segments Manager holds for (cluster, type), if any.

    Lists Allocated segments of the type (public GET) and matches the cluster
    name EXACTLY here: the route has no cluster filter — a `cluster_name`
    parameter is silently ignored and would return every segment — and its
    /search is a substring match (`c1` would also hit `c10`). `fresh=true`
    bypasses the manager's per-process list cache, so a release that just
    happened is never mistaken for a live allocation.
    """
    async with _segments_manager_client() as client:
        resp = await client.get(
            "/api/segments",
            params={"status": "Allocated", "type": request.type.value, "fresh": "true"},
        )
        if resp.status_code != 200:
            _raise_segments_manager_error("List allocated segments", resp)
        body = resp.json()
    if not isinstance(body, list):
        raise SegmentsManagerError(
            f"GET /api/segments returned no segment list: {_bounded(resp.text)}"
        )
    matches = [
        row
        for row in body
        if isinstance(row, dict)
        and row.get("cluster_name") == request.cluster
        and row.get("type") == request.type.value
    ]
    if len(matches) > 1:
        raise AmbiguousAllocationError(
            f"The Segments Manager holds {len(matches)} Allocated "
            f"{request.type.value} segments for cluster {request.cluster} "
            f"({', '.join(sorted(str(row.get('segment')) for row in matches))}) — "
            "it allows one per (cluster, site, type); resolve it there, the "
            "run will not guess which to release"
        )
    if not matches:
        activity.logger.info(
            "No Allocated %s segment for cluster %s", request.type.value, request.cluster
        )
        return ClusterAllocationLookup(found=False)
    entry = SegmentEntry.model_validate(matches[0])
    activity.logger.info(
        "Cluster %s holds %s segment %s (vlan=%d, site=%s)",
        request.cluster,
        request.type.value,
        entry.segment,
        entry.vlan_id,
        entry.site,
    )
    return ClusterAllocationLookup(found=True, entry=entry)


@activity.defn
async def release_segment(segment: str) -> None:
    """Give the segment back to the pool. Idempotent server-side: an
    already-Available segment answers 200 "already released"."""
    async with _segments_manager_client() as client:
        resp = await client.post(
            "/api/segments/release",
            json={"segment": segment},
            headers=_segments_manager_auth(),
        )
        if resp.status_code == 200:
            try:
                message = resp.json().get("message")
            except ValueError:
                message = None
            activity.logger.info(
                "Released segment %s: %s", segment, message or "released"
            )
            return
        if resp.status_code == 404:
            raise SegmentNotFoundError(
                f"Segment {segment} not found in the Segments Manager: "
                f"{_segments_manager_detail(resp)}"
            )
        if resp.status_code in (400, 422):
            raise SegmentValidationError(
                f"Segments Manager rejected the release of {segment}: "
                f"{_segments_manager_detail(resp)}"
            )
        # A 500 is the manager's "the update changed nothing" — a release that
        # lost a race. Retried on purpose: the retry answers 200 "already
        # released".
        _raise_segments_manager_error("Release segment", resp)


@activity.defn
async def get_dhcp_scope(network: str) -> DhcpScopeState:
    """Observe the DHCP scope for `network` (anonymous GET). 404 is a normal
    answer — found=False — never an error."""
    state = await dhcp_api.fetch_scope(_settings.dhcp_api_url, network)
    if state.found:
        activity.logger.info(
            "DHCP scope %s exists (%d exclusion(s))", network, len(state.exclusions)
        )
    else:
        activity.logger.info("DHCP scope %s does not exist", network)
    return state


@activity.defn
async def delete_dhcp_scope(network: str) -> None:
    """Delete the DHCP scope for `network`. Idempotent server-side: 204 whether
    or not it existed."""
    await dhcp_api.remove_scope(_settings.dhcp_api_url, _settings.dhcp_api_token, network)
    activity.logger.info("Deleted DHCP scope %s", network)
