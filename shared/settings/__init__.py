"""Typed, fail-fast configuration via pydantic-settings — ONE MODULE PER WORKER.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

One module per deployment boundary, each holding one settings class:

  temporal.py             TemporalSettings — anything that connects a Temporal
                          Client (every worker and api.py)
  segment_lifecycle.py    SegmentLifecycleActivitySettings
  server_lifecycle.py     ServerLifecycleActivitySettings
  upgrade_gitops.py       UpgradeGitopsActivitySettings    } cluster-upgrade's three
  cluster_upgrade.py      ClusterUpgradeActivitySettings   } limbs, one class per
  upgrade_agent.py        UpgradeAgentActivitySettings     } credential set (§2)
  vm_lifecycle.py         VmLifecycleActivitySettings
  cluster_lifecycle.py    ClusterLifecycleActivitySettings

THIS FILE IMPORTS NOTHING, AND MUST STAY THAT WAY (checker SH007). Import a
class from ITS module — `from shared.settings.vm_lifecycle import
VmLifecycleActivitySettings` — never from the package. CI builds an image only
when a file that image loads has changed (scripts/image_changes.py follows the
imports from each entrypoint), so a re-export here would load every worker's
settings into every image, and one worker's new config key would again rebuild
and re-tag all of them. That is what this was until 2026-11-14: one
shared/settings.py, 34 commits in ten weeks, each rebuilding ten images.

Field names deliberately equal the Helm ConfigMap/Secret keys (lowercased) —
pydantic-settings matches env vars case-insensitively, so SEGMENTS_MANAGER_URL
populates segments_manager_url.

Which ConfigMap a key lives in (an ops grouping) is INDEPENDENT of which
settings class declares it (a code grouping). pydantic reads the flat process
env, so it never sees the ConfigMap boundary. SEGMENTS_MANAGER_URL lives in the
shared `workflows-orchestrator-config` ConfigMap (so future workflows reuse it
without duplication), yet stays a field on SegmentLifecycleActivitySettings —
only the activity worker requires it, and it mounts workflows-orchestrator-config
+ segment-lifecycle-config together. Keep the modules aligned with the vendored
charts in the Argo CD repo:
redbull-platform/gitops/charts/workflows-orchestrator/templates/config.yaml
    (workflows-orchestrator-config: temporal + segments-manager url)
redbull-platform/gitops/charts/segment-lifecycle-worker/templates/config.yaml
    (segment-lifecycle-config: the DHCP API URL; + the dhcp-api-token Secret)
redbull-platform/gitops/charts/cluster-lifecycle-worker/templates/config.yaml
    (cluster-lifecycle-config: the day1 repo URL, the DHCP policy,
    the day2 sig URL template; + the redbull-git-token Secret)

Do NOT import a settings module from inside a workflow definition (it runs in
the sandbox) — only from worker entrypoints, api.py, and activity
implementations.
"""
