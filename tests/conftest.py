"""Test environment bootstrap.

activities/segment_lifecycle/activities.py instantiates SegmentLifecycleActivitySettings
at import time (fail-fast by design), so the full activity config must be in
the environment BEFORE any test module imports it.

The repo's .env is switched OFF for the whole suite. Real env vars do NOT
simply win over it: pydantic-settings DEEP-MERGES dict fields across sources,
so a developer's local .env leaks its SITE_NETWORKS sites and PORTS_* protocols
into the values set below — turning "this config is rejected" tests green
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
        "DOMAIN": "test-domain",
        "NEXT_URL": "http://next.test",
        "NEXT_CHECKING_REQUEST_INTERVAL_SECONDS": "15",
        "NEXT_GROUP": "test-group",
        "PORTS_HC_TO_MCE": '{"tcp": ["30000-32767"], "udp": ["9000"]}',
        "PORTS_MCE_TO_HC": '{"tcp": ["6443", "30000-32767"]}',
        "PORTS_INVENTORY_TO_MCE": '{"tcp": ["30000-32767"]}',
        "PORTS_MCE_TO_INVENTORY": '{"tcp": ["6443"]}',
        "PORTS_PXE_TO_MCE": '{"udp": ["69"]}',
        "PORTS_MCE_TO_PXE": '{"tcp": ["6443"]}',
        "SITE_NETWORKS": (
            '{"site-a": {"pool": "192.11.0.0/16", '
            '"dell-bmc": "10.98.0.0/16", "cisco-bmc": "10.99.0.0/16"}}'
        ),
        "PORTS_MCE_TO_BMC": '{"tcp": ["623"]}',
        # --- allocate-segment: values repo + DHCP policy/API ---
        "DAY1_REPO_URL": "https://git.test/team/gitops-day1-platform-config.git",
        "DAY1_GIT_TOKEN": "test-git-token",
        "DHCP_EXCLUSION_OCTET_RANGES": "[[1, 10], [241, 254]]",
        "DHCP_API_URL": "http://dhcp.test",
    }
)
