"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file (see
.env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Two settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (both workers and api.py).
  - SegmentActivitySettings: needed only by the segment-allocation activity
    worker/tasks (deployments API endpoint + GitOps repo config). The workflow
    worker has no business holding these.

Do NOT import this module from inside a workflow definition (it runs in the
sandbox) — only from worker entrypoints, api.py, and activity implementations.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_host: str
    temporal_namespace: str = "default"


class SegmentActivitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Deployments API (segment allocation). Base host only; the project/group path
    # is composed per request. e.g. https://america.com
    deployment_api_url: str
    deployment_project_id: str
    deployment_group: str

    @property
    def deployments_endpoint(self) -> str:
        """Base URL for the deployments collection.

        POST here creates a deployment; GET `<endpoint>/{uuid}` reads one.
        """
        return (
            f"{self.deployment_api_url.rstrip('/')}"
            f"/api/v2/projects/{self.deployment_project_id}"
            f"/groups/{self.deployment_group}/deployments"
        )
