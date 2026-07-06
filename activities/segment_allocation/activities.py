"""Segment-allocation activity implementations — the execution limbs.

These run in the segment-allocation activity deployment. Step 1 talks to the
deployments API (DEPLOYMENT_API_URL) to create a deployment and read back the
allocated segment, then commits that segment to GitOps.

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Every httpx.AsyncClient is created INSIDE the activity via `async with`, with
    an explicit timeout strictly below the workflow's start_to_close_timeout (30s).
    This frees the worker on a network hang before Temporal reaps the activity and
    keeps any auth/session state scoped to a single invocation (no global leak).
  * Idempotency: create/commit are safe to retry (see per-activity notes).
"""

from __future__ import annotations

import httpx
from temporalio import activity

from shared.exceptions import DeploymentApiError
from shared.models.segment_allocation import (
    DeploymentStatus,
    SegmentAllocationInput,
)
from shared.settings import SegmentActivitySettings

_settings = SegmentActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (30s) so a hung
# connection fails the HTTP call and releases the worker before Temporal reaps it.
_HTTP_TIMEOUT = httpx.Timeout(10.0)

# TODO(pending): auth headers for the deployments API are not finalized yet.
_API_HEADERS: dict[str, str] = {}


@activity.defn
async def create_deployment(allocation_input: SegmentAllocationInput) -> str:
    """Create a deployment and return its `uuid`.

    POSTs to the deployments collection endpoint. The response `uuid` identifies
    the deployment we then poll (see `get_deployment`).
    """
    # TODO(pending): the exact JSON payload the deployments API expects is not
    # finalized. cluster_name/site are carried through until the schema is known.
    payload = {
        "cluster_name": allocation_input.cluster_name,
        "site": allocation_input.site,
    }
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=_API_HEADERS) as client:
        try:
            resp = await client.post(_settings.deployments_endpoint, json=payload)
        except httpx.HTTPError as exc:
            raise DeploymentApiError(f"Create deployment call failed: {exc}") from exc
        if resp.status_code not in (200, 201):
            raise DeploymentApiError(
                f"Create deployment returned {resp.status_code}: {resp.text}"
            )
        try:
            uuid = resp.json()["uuid"]
        except (ValueError, KeyError, TypeError) as exc:
            raise DeploymentApiError(
                f"Create deployment response missing 'uuid': {resp.text}"
            ) from exc

    activity.logger.info(
        "Created deployment uuid=%s for cluster=%s site=%s",
        uuid,
        allocation_input.cluster_name,
        allocation_input.site,
    )
    return uuid


@activity.defn
async def get_deployment(uuid: str) -> DeploymentStatus:
    """Fetch a deployment's current status (and segment once created).

    Extracts `status` and, when present, `additionalInfo.segment`. The workflow
    polls this until status == "CREATED".
    """
    url = f"{_settings.deployments_endpoint}/{uuid}"
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=_API_HEADERS) as client:
        try:
            resp = await client.get(url)
        except httpx.HTTPError as exc:
            raise DeploymentApiError(f"Get deployment call failed: {exc}") from exc
        if resp.status_code != 200:
            raise DeploymentApiError(
                f"Get deployment {uuid} returned {resp.status_code}: {resp.text}"
            )
        try:
            body = resp.json()
            status = body["status"]
        except (ValueError, KeyError, TypeError) as exc:
            raise DeploymentApiError(
                f"Get deployment {uuid} response missing 'status': {resp.text}"
            ) from exc
        segment = (body.get("additionalInfo") or {}).get("segment")

    activity.logger.info("Deployment uuid=%s status=%s", uuid, status)
    return DeploymentStatus(status=status, segment=segment)


@activity.defn
async def commit_segment_to_git(allocation_input: SegmentAllocationInput, segment: str) -> None:
    """Commit the allocated segment to the GitOps repository.

    Must be idempotent: a retry after a partial/committed write should converge to
    the same state, never duplicate it.
    """
    # TODO(pending): Git repository, hierarchy, and commit logic are not finalized.
    # Left as a no-op (warn only) so Step 1 completes end-to-end; wire up the real
    # commit once the GitOps hierarchy is defined, and keep it idempotent.
    activity.logger.warning(
        "Segment=%s for cluster=%s site=%s NOT committed to Git yet (logic pending)",
        segment,
        allocation_input.cluster_name,
        allocation_input.site,
    )
