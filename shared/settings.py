"""Typed, fail-fast configuration via pydantic-settings.

Replaces ad-hoc os.environ.get(...) calls. Values are read from the process
environment and, if present, a .env file (see .env.example) — never hardcoded
endpoints, per the deployment-agnostic rule.

Two settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (both workers and api.py).
  - SegmentActivitySettings: needed only by the segment-allocation activity
    worker/tasks (Segments Manager + generator credentials/URLs). The workflow
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

    segment_manager_url: str
    segment_manager_user: str
    segment_manager_password: str
    segment_generator_url: str
