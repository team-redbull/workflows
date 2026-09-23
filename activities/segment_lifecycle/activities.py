"""Segment-lifecycle activity implementations — the execution limbs.

These run in the `segment-lifecycle-worker` deployment. They talk to:
  - the team's Segments Manager (SEGMENTS_MANAGER_URL) — create, list and
    convert segments, allocate one for a cluster (Bearer token via
    SEGMENTS_MANAGER_API_TOKEN; GETs are public)
  - the day1 values repo (DAY1_REPO_URL) — git subprocess via
    activities/segment_lifecycle/values_repo.py (clone, append the allocation
    block, push; the token is never logged)
  - the DHCP scope API (DHCP_API_URL) — READ-ONLY: the allocate-segment
    workflow polls a scope to observe Crossplane's convergence, never writes one

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: create accepts an existing segment that matches the requested
    definition; allocate is idempotent server-side per (cluster, site, type);
    the values-repo append is a no-op for a file already recording this
    allocation; a re-typed segment converges on a retry.
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

from activities.segment_lifecycle import values_repo
from activities.segment_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import (
    DhcpApiError,
    SegmentConflictError,
    SegmentConversionConflictError,
    SegmentPoolExhaustedError,
    SegmentsManagerAuthError,
    SegmentsManagerError,
    SegmentNotFoundError,
    SegmentValidationError,
)
from shared.models.segment_lifecycle import (
    ClusterFileLocation,
    ClusterValuesAppendRequest,
    ConvertibleSegment,
    ConvertibleSegmentsQuery,
    DhcpExclusion,
    DhcpScopeState,
    InitializeSegmentInput,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
    SegmentTypeUpdate,
    ValuesCommitRef,
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
# see values_repo._run_git.
_TLS_VERIFY = False


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


# --- convert-segment --------------------------------------------------------

# The ONE status a segment may be converted from: "Allocated" is excluded by
# definition (in use by a cluster), and Available is the only other status a
# segment can hold. Availability alone is not the whole guard, though — the
# cluster_name assertion below is the other half (see list_convertible_segments).
_CONVERTIBLE_STATUS = "Available"


@activity.defn
async def list_convertible_segments(
    query: ConvertibleSegmentsQuery,
) -> list[ConvertibleSegment]:
    """Every Available, unassigned segment of the given type at the site
    (public GET).

    All three filters are server-side; both halves of "convertible" are then
    ASSERTED on every hit rather than trusted, because a segment that reaches
    the conversion loop wrongly is re-typed under whoever holds it.
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
        # An Available segment carrying a cluster is a manager INVARIANT
        # violation, not a status we merely filtered wrong: allocation is what
        # assigns a cluster, and it sets the status in the same update. Failing
        # loudly (§7) is right for the same reason the status check is — the
        # alternative is re-typing a segment a cluster is actually using.
        if seg.get("cluster_name"):
            raise SegmentsManagerError(
                f"GET /api/segments?status={_CONVERTIBLE_STATUS} returned a "
                f"segment assigned to cluster {seg.get('cluster_name')!r}: {seg}"
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

    A re-type and nothing else: the segment stays Available, so it is
    allocatable as its new type the moment this returns. The manager's own
    guard is Available AND unassigned. Idempotent server-side (a retry finds
    the type already set and converges); a 409 — segment in use, or the
    expected_type compare-and-set lost to a concurrent conversion — is
    deterministic and non-retryable.
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
