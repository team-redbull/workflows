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

from shared.settings import (
    SegmentLifecycleActivitySettings,
    ServerLifecycleActivitySettings,
    ServerProvisioningActivitySettings,
    TemporalSettings,
)

for _settings_class in (
    SegmentLifecycleActivitySettings,
    ServerLifecycleActivitySettings,
    ServerProvisioningActivitySettings,
    TemporalSettings,
):
    _settings_class.model_config["env_file"] = None

os.environ.update(
    {
        "TEMPORAL_HOST": "localhost:7233",
        "SEGMENTS_MANAGER_URL": "http://segments-manager.test",
        "SEGMENTS_MANAGER_API_TOKEN": "test-token",
        # --- allocate-segment: values repo + DHCP policy ---
        "DAY1_REPO_URL": "https://git.test/team/gitops-day1-platform-config.git",
        "DAY1_GIT_TOKEN": "test-git-token",
        "DHCP_EXCLUSION_OCTET_RANGES": '{"HC": [[1, 10], [241, 254]]}',
        # --- install-server: server-scan + BMC credentials ---
        # Names this worker's queue; one deployment serves one MCE.
        "MCE_CLUSTER": "ocp4-mce-test",
        "SERVER_SCAN_URL": "http://server-scan.test/api/v1",
        "SERVER_SCAN_API_TOKEN": "test-viewer-token",
        "DELL_BMC_USERNAME": "test-dell-user",
        "DELL_BMC_PASSWORD": "test-dell-pass",
        "HP_BMC_USERNAME": "test-hp-user",
        "HP_BMC_PASSWORD": "test-hp-pass",
        "CISCO_BMC_USERNAME": "test-cisco-user",
        "CISCO_BMC_PASSWORD": "test-cisco-pass",
        # --- install-server: the site PXE map, for IPMI servers ---
        "PXE_MAP_URLS": '{"bat-yam": "http://pxe.bat-yam.test:8080"}',
        "PXE_MAP_TOKEN": "test-pxe-token",
        # --- provision-dell-server: OME, iDRAC root, templates, naming service ---
        "OME_URL": "https://ome.test",
        "OME_USERNAME": "test-ome-user",
        "OME_PASSWORD": "test-ome-pass",
        "IDRAC_ROOT_PASSWORD": "target-pass",
        "IDRAC_FACTORY_PASSWORDS": '["factory-a", "factory-b"]',
        "DELL_TEMPLATES": '{"PowerEdge R660": {"7.10.70.00": "ocp-r660-7.10.70.00"}}',
        "SERVER_NAMER_URL": "https://namer.test/rename",
        # --- release-segment: the DHCP scope API ---
        "DHCP_API_URL": "http://dhcp.test",
        "DHCP_API_TOKEN": "test-dhcp-token",
    }
)
