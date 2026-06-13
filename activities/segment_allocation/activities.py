"""Segment-allocation activity implementations — the execution limbs.

These run in the segment-allocation activity deployment. They talk to:
  - the team's Segments Manager (SEGMENT_MANAGER_URL)  — read pool, create, allocate
  - the external generator / IPAM (GENERATOR_URL) — a black box; we trust its output

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: create_segment tolerates an already-existing segment; allocate is
    idempotent per (cluster, site) by the Segments Manager's contract.
  * Every httpx.AsyncClient is created INSIDE the activity via `async with`, with an
    explicit timeout strictly below the workflow's start_to_close_timeout (30s). This
    frees the worker on a network hang before Temporal times the activity out, and
    keeps the auth session cookie scoped to a single invocation (no global leak).
"""

from __future__ import annotations

import httpx
from temporalio import activity

from shared.exceptions import (
    NoSegmentAvailableError,
    SegmentGeneratorError,
    SegmentManagerError,
)
from shared.models.segment_allocation import *
from shared.settings import SegmentActivitySettings

_settings = SegmentActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (30s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(10.0)


def _segment_manager_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for the Segments Manager (own cookie jar)."""
    return httpx.AsyncClient(base_url=_settings.segment_manager_url, timeout=_HTTP_TIMEOUT)


async def _login(client: httpx.AsyncClient) -> None:
    """Authenticate; the session cookie is stored on this client's jar only."""
    resp = await client.post(
        "/api/auth/login",
        json={"username": _settings.segment_manager_user, "password": _settings.segment_manager_password},
    )
    if resp.status_code != 200:
        raise SegmentManagerError(
            f"Login failed: {resp.status_code} {resp.text}"
        )


@activity.defn
async def get_available_segment(site: str) -> SegmentSpec | None:
    """Return the first unallocated, non-released segment at the site, or None."""
    async with _segment_manager_client() as client:
        resp = await client.get("/api/segments")
        if resp.status_code != 200:
            raise SegmentManagerError(
                f"List segments failed: {resp.status_code} {resp.text}"
            )
        for seg in resp.json():
            if (
                seg.get("site") == site
                and not seg.get("cluster_name")
                and not seg.get("released", False)
            ):
                activity.logger.info(
                    "Found available segment %s at site=%s",
                    seg.get("segment"),
                    site,
                )
                return SegmentSpec(
                    site=seg["site"],
                    vlan_id=seg["vlan_id"],
                    segment=seg["segment"],
                    epg_name=seg["epg_name"],
                    dhcp=seg.get("dhcp", False),
                )
    activity.logger.info("No available segment at site=%s", site)
    return None


@activity.defn
async def request_segment(site: str) -> SegmentSpec:
    """Ask the external generator for a new valid segment at the site"""
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        try:
            resp = await client.post(_settings.segment_generator_url, json={"site": site})
        except httpx.HTTPError as exc:
            raise SegmentGeneratorError(f"Generator call failed: {exc}") from exc
        if resp.status_code != 200:
            raise SegmentGeneratorError(
                f"Generator returned {resp.status_code}: {resp.text}"
            )
        try:
            spec = SegmentSpec.model_validate(resp.json())
        except Exception as exc:  # malformed payload from the generator
            raise SegmentGeneratorError(f"Invalid generator payload: {exc}") from exc
    activity.logger.info(
        "Generated segment %s for site=%s",
        spec.segment,
        site,
    )
    return spec


@activity.defn
async def register_segment(spec: SegmentSpec) -> None:
    """Register a segment in the Segments Manager.

    Idempotent: a duplicate (already-created) segment is treated as success, so a
    retried activity does not fail on the manager's VLAN-uniqueness constraint.
    """
    async with _segment_manager_client() as client:
        await _login(client)
        resp = await client.post("/api/segments", json=spec.model_dump())
        if resp.status_code in (200, 201):
            activity.logger.info("Created segment vlan=%s", spec.vlan_id)
            return
        # Already exists -> the desired state is satisfied; treat as success.
        body = resp.text.lower()
        if resp.status_code in (400, 409) and "overlaps" in body:
            activity.logger.info(
                "Segment vlan=%s already exists; treating as created", spec.vlan_id
            )
            return
        raise SegmentManagerError(
            f"Create segment failed: {resp.status_code} {resp.text}"
        )


@activity.defn
async def allocate_segment(allocation_input: SegmentAllocationInput) -> SegmentAllocationResult:
    """Allocate a segment at the site to the cluster.

    Idempotent per the manager contract: if the cluster already holds a segment at
    the site, the existing one is returned instead of allocating a new one.
    """
    async with _segment_manager_client() as client:
        await _login(client)
        resp = await client.post(
            "/api/allocate-vlan",
            json={
                "cluster_name": allocation_input.cluster_name,
                "site": allocation_input.site,
            },
        )
        if resp.status_code == 200:
            result = SegmentAllocationResult.model_validate(resp.json())
            activity.logger.info(
                "Allocated vlan=%s to cluster=%s",
                result.vlan_id,
                result.cluster_name,
            )
            return result
        body = resp.text.lower()
        if "no segment" in body or "available" in body:
            raise NoSegmentAvailableError(
                f"No segment available at site={allocation_input.site}: {resp.text}"
            )
        raise SegmentManagerError(
            f"Allocate failed: {resp.status_code} {resp.text}"
        )
