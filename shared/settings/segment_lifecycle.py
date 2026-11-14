"""SegmentLifecycleActivitySettings — The segment-lifecycle activity worker only: the Segments Manager and the DHCP scope API.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class SegmentLifecycleActivitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Segments Manager (GETs are public; mutating calls need the token) ---
    segments_manager_url: str
    segments_manager_api_token: str

    # The day1 repo and the DHCP exclusion policy are NOT here any more: since
    # provision-cluster owns the cluster file (2026-11-11), allocate-segment
    # allocates and verifies only, and DAY1_REPO_URL, DAY1_GIT_TOKEN and
    # DHCP_EXCLUSION_OCTET_RANGES moved to ClusterLifecycleActivitySettings.
    # The segment-lifecycle chart keeps rendering them until this image is the
    # deployed one (CLAUDE.md §8); `extra="ignore"` lets the limb ignore them.

    # --- release-segment: the DHCP scope API (dhcp_scope_manager) -----------
    # release-segment checks whether a released cluster's scope survived the
    # Argo CD cascade (GET, anonymous) and deletes it if so (DELETE, bearer
    # token). It is the ONLY write this worker makes there, and only a DELETE:
    # Crossplane stays the only creator/updater of scopes (CLAUDE.md §4).
    #
    # The token is a COPY of the DHCP API's own token (redbull-platform:
    # gitops/charts/dhcp-scope-manager/charts/dhcp-api-token/values.yaml),
    # rendered into this worker's namespace by the segment-lifecycle-worker
    # chart. The two must rotate together — a stale copy fails every release
    # with DhcpApiAuthError (non-retryable), never silently.
    dhcp_api_url: str
    dhcp_api_token: str
