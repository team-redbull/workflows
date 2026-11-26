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

from shared.settings.cluster_lifecycle import ClusterLifecycleActivitySettings
from shared.settings.cluster_upgrade import ClusterUpgradeActivitySettings
from shared.settings.segment_lifecycle import SegmentLifecycleActivitySettings
from shared.settings.server_lifecycle import ServerLifecycleActivitySettings
from shared.settings.server_provisioning import ServerProvisioningActivitySettings
from shared.settings.temporal import TemporalSettings
from shared.settings.upgrade_agent import UpgradeAgentActivitySettings
from shared.settings.upgrade_gitops import UpgradeGitopsActivitySettings
from shared.settings.vm_lifecycle import VmLifecycleActivitySettings

for _settings_class in (
    ClusterLifecycleActivitySettings,
    ClusterUpgradeActivitySettings,
    SegmentLifecycleActivitySettings,
    ServerLifecycleActivitySettings,
    ServerProvisioningActivitySettings,
    TemporalSettings,
    UpgradeAgentActivitySettings,
    UpgradeGitopsActivitySettings,
    VmLifecycleActivitySettings,
):
    _settings_class.model_config["env_file"] = None

os.environ.update(
    {
        "TEMPORAL_HOST": "localhost:7233",
        "SEGMENTS_MANAGER_URL": "http://segments-manager.test",
        "SEGMENTS_MANAGER_API_TOKEN": "test-token",
        # --- the day1 repo (cluster-lifecycle, upgrade-gitops, vm-lifecycle) ---
        "DAY1_REPO_URL": "https://git.test/team/gitops-day1-platform-config.git",
        "DAY1_GIT_TOKEN": "test-git-token",
        # --- provision-cluster: the DHCP policy, the day2 sigs (GitLab, one project per sig) ---
        "DHCP_EXCLUSION_OCTET_RANGES": '{"HC": [[1, 10], [241, 254]]}',
        "DAY2_SIG_REPO_URL_TEMPLATE": "https://git.test/redbull/gitops-day2-prod/sigs/{sig}.git",
        # --- provision-cluster: the console shortcut ---
        "SHORTCUTS_API_URL": "https://shorts.test",
        "SHORTCUTS_API_TOKEN": "test-shorts-token",
        "SHORTCUT_OWNER": "redbull-workflows",
        "CLUSTER_CONSOLE_URL_TEMPLATE": "https://console-openshift-console.apps.{cluster}.test",
        # --- add-server-to-mce: server-scan + BMC credentials ---
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
        # --- add-server-to-mce's KubeVirt branch: a KV cluster's kubevirt-redfish ---
        "KUBEVIRT_BMC_HOST_TEMPLATE": "kubevirt-redfish-kubevirt-redfish.apps.{kv_cluster}.example.test",
        "KUBEVIRT_BMC_USERNAME": "test-redfish-user",
        "KUBEVIRT_BMC_PASSWORD": "test-redfish-pass",
        # --- add-server-to-mce: the site PXE map, for IPMI servers ---
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
        # --- cluster-upgrade: the upgrade agent service ---
        "UPGRADE_AGENT_URL": "http://upgrade-agent.test:8080",
        "UPGRADE_AGENT_TOKEN": "test-agent-token",
        # --- create-vm: the KubeVirt clusters' agents and the placement policy ---
        # DAY1_REPO_URL / DAY1_GIT_TOKEN above serve the vm-lifecycle limb too.
        "HEADROOM_BUFFER_PERCENT": "15",
        "KV_CLUSTER": "ocp4-prod-kv-five-az1-a",
        "VM_STORAGE_CLASS_NAME": "test-cephfs",
    }
)
