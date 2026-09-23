"""Test environment bootstrap.

activities/segment_lifecycle/activities.py instantiates SegmentLifecycleActivitySettings
at import time (fail-fast by design), so the full activity config must be in
the environment BEFORE any test module imports it.

The repo's .env is switched OFF for the whole suite. Real env vars do NOT
simply win over it: pydantic-settings DEEP-MERGES dict fields across sources,
so a developer's local .env would leak its own DHCP_EXCLUSION_OCTET_RANGES
types into the value set below — turning "this config is rejected" tests green
because the .env quietly supplied the missing key. Tests must depend only on
what this file sets, on a laptop and in CI alike.
"""

from __future__ import annotations

import os

from shared.settings import SegmentLifecycleActivitySettings, TemporalSettings

for _settings_class in (SegmentLifecycleActivitySettings, TemporalSettings):
    _settings_class.model_config["env_file"] = None

os.environ.update(
    {
        "TEMPORAL_HOST": "localhost:7233",
        "SEGMENTS_MANAGER_URL": "http://segments-manager.test",
        "SEGMENTS_MANAGER_API_TOKEN": "test-token",
        # --- allocate-segment: values repo + DHCP policy/API ---
        "DAY1_REPO_URL": "https://git.test/team/gitops-day1-platform-config.git",
        "DAY1_GIT_TOKEN": "test-git-token",
        "DHCP_EXCLUSION_OCTET_RANGES": '{"HC": [[1, 10], [241, 254]]}',
        "DHCP_API_URL": "http://dhcp.test",
    }
)
