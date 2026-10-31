"""The checks an upgrade-cluster run applies, and the rollout rules — pure, deterministic.

POLICY, like upgrade_path.py: the limbs READ (a day1 file, a HostedCluster, the
hosted cluster's health) and these functions DECIDE, so every rule is a plain
unit test and the limbs stay dumb readers. It runs inside the workflow sandbox
on every replay: no clock, no I/O, no set-iteration order in anything returned.

  * planning_checks — DESIGN §7 checks 1–4, before approval. Any failure fails
    the run, with every failure listed at once; nothing has been committed.
  * preflight_checks — checks 5–11, before approval and again (5–10) before
    every control-plane step and NodePool roll.
  * cluster_profile, nodepool_order, soak_minutes — how the workers roll (D7).
  * nodepool_roll_deadline_minutes — the machine deadline of one NodePool roll.

Design: docs/upgrade/DESIGN.md §6–§7 in the Cluster-AutoUpgrade workspace.
"""

from __future__ import annotations

import math

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.cluster_upgrade import (
        ArgoApplicationState,
        CheckFailure,
        CheckResult,
        ClusterProfile,
        Day1Cluster,
        Day2Layers,
        HostedClusterState,
        HostedHealth,
        MceState,
        PinnedReleases,
        ReleaseLayout,
        UpgradeClusterInput,
        UpgradePlan,
        UpgradeRules,
    )
    from shared.ocp_version import minor_name, minor_of, release_tag

# The upgrade type every NodePool must have: a Replace roll would reprovision
# bare-metal hosts, which this workflow is not built to watch.
_IN_PLACE = "InPlace"

# Soak after each NodePool gate passes, unless the cluster file sets one.
_ZONED_SOAK_MINUTES = 30
_GENERIC_SOAK_MINUTES = 0

# How long one InPlace batch of nodes may take: drain, the MachineConfig, a
# bare-metal reboot. The hour install-server allows a bare-metal POST and boot.
PER_BATCH_MINUTES = 60

# The waves whose clusters may skip the human approval (AGENT.md #3, #30).
_AUTO_APPROVE_WAVES = (0, 1)

_SEVERITY = {
    CheckFailure.WAIT: 0,
    CheckFailure.NEEDS_HUMAN: 1,
    CheckFailure.NOTIFY_OWNER: 1,
    CheckFailure.FAILED: 2,
}


def expand(template: str, *, image_path: str, version: str | None = None) -> str:
    """A ladder.yaml `release` template with `{imagePath}` (and `{version}`) filled in."""
    text = template.replace("{imagePath}", image_path)
    if version is not None:
        text = text.replace("{version}", version)
    return text


def release_repositories(release: ReleaseLayout) -> list[str]:
    """The two mirror repositories a release is pulled from: payload, then operator."""
    return [
        expand(release.repo, image_path=release.image_path),
        expand(release.cpo_repo, image_path=release.image_path),
    ]


# --- profile and order ---------------------------------------------------------


def cluster_profile(day1: Day1Cluster) -> ClusterProfile:
    """`upgrade.profile` when the file sets it, else zoned when any NodePool has a zone (D7)."""
    if day1.upgrade.profile is not None:
        return day1.upgrade.profile
    if any(nodepool.zone for nodepool in day1.nodepools):
        return ClusterProfile.ZONED
    return ClusterProfile.GENERIC


def soak_minutes(profile: ClusterProfile, day1: Day1Cluster) -> int:
    if day1.upgrade.soak_minutes is not None:
        return day1.upgrade.soak_minutes
    return _ZONED_SOAK_MINUTES if profile is ClusterProfile.ZONED else _GENERIC_SOAK_MINUTES


def nodepool_order(
    *, requested: list[str] | None, day1: Day1Cluster, names: list[str]
) -> list[str]:
    """The order NodePools roll in: the request's, else the file's, else by zone.

    `names` are the NodePools the HostedCluster has. An order given by the
    request or the cluster file must name each of them exactly once — a
    NodePool left out would never be upgraded, one named twice would roll
    twice. Without one: by zone label (`a`, `b`, `c`) when every NodePool has
    one, else in the order the cluster file lists them.

    Raises ValueError with the reason, which the caller reports as a failed
    planning check.
    """
    for source, order in (("the request", requested), ("upgrade.nodePoolOrder", day1.upgrade.nodepool_order)):
        if order is None:
            continue
        if sorted(order) != sorted(names) or len(set(order)) != len(order):
            raise ValueError(
                f"{source} orders NodePools {order}, but the cluster has {sorted(names)}: "
                "name each one exactly once"
            )
        return list(order)
    by_name = {nodepool.name: nodepool for nodepool in day1.nodepools}
    file_order = [nodepool.name for nodepool in day1.nodepools if nodepool.name in names]
    file_order += [name for name in names if name not in by_name]
    if file_order and all(by_name.get(name) and by_name[name].zone for name in file_order):
        return sorted(file_order, key=lambda name: (by_name[name].zone, name))
    return file_order


def _batch_size(replicas: int, max_unavailable: str | None) -> int:
    """How many nodes an InPlace roll takes down at once.

    HyperShift's own rule (`getInPlaceMaxUnavailable`, hypershift
    release-4.20 `hypershift-operator/controllers/nodepool/capi.go:1034`):
    unset means 1; a percentage is scaled to the replicas rounding DOWN; and 0
    is raised to 1.
    """
    if max_unavailable is None:
        return 1
    text = max_unavailable.strip()
    if text.endswith("%"):
        count = math.floor(replicas * int(text[:-1]) / 100)
    else:
        count = int(text)
    return max(1, count)


def nodepool_roll_deadline_minutes(
    replicas: int | None, max_unavailable: str | None, *, per_batch_minutes: int = PER_BATCH_MINUTES
) -> int:
    """The whole budget of one NodePool roll: one `per_batch_minutes` per batch of nodes."""
    nodes = replicas or 1
    batches = math.ceil(nodes / _batch_size(nodes, max_unavailable))
    return batches * per_batch_minutes


# --- planning: checks 1-4, before approval ------------------------------------


def planning_checks(
    *,
    request: UpgradeClusterInput,
    rules: UpgradeRules,
    mce: MceState,
    day1: Day1Cluster,
    hosted: HostedClusterState,
    plan: UpgradePlan,
    pinned: PinnedReleases,
    day2: Day2Layers,
) -> list[CheckResult]:
    """Every planning check, passed or not, in DESIGN §7 order. All are FAILED-kind."""
    return [
        _rules_match_mce(rules, mce, plan),
        _mirror_has_every_step(plan, pinned, rules.release),
        _day1_file_upgradeable(rules, day1, hosted),
        _git_matches_cluster(day1, hosted),
        _day2_layers_exist(day2),
        _auto_approve_allowed(request, day1),
    ]


def _failed_check(name: str, passed: bool, expected: str, observed: str) -> CheckResult:
    return CheckResult(
        name=name, passed=passed, expected=expected, observed=observed,
        on_failure=CheckFailure.FAILED,
    )


def _rules_match_mce(rules: UpgradeRules, mce: MceState, plan: UpgradePlan) -> CheckResult:
    name = "rules-match-mce"
    expected = f"supported-versions = {sorted(rules.supported_hosted_versions)}"
    if not mce.supported_versions_found:
        return _failed_check(name, False, expected, "ConfigMap hypershift/supported-versions not found")
    if sorted(set(mce.supported_versions)) != sorted(set(rules.supported_hosted_versions)):
        return _failed_check(
            name, False, expected,
            f"the MCE reports {sorted(mce.supported_versions)}: fix upgrade/mces/<mce>.yaml or the MCE",
        )
    outside = [
        step.version for step in plan.steps
        if minor_name(minor_of(step.version)) not in mce.supported_versions
    ]
    if outside:
        return _failed_check(
            name, False, "every step's minor supported by this MCE",
            f"not supported here: {', '.join(outside)}",
        )
    return _failed_check(name, True, expected, "matches")


def image_set_snippet(versions: list[str]) -> str:
    """The oc-mirror ImageSetConfiguration that mirrors exactly these releases."""
    lines = [
        "kind: ImageSetConfiguration",
        "apiVersion: mirror.openshift.io/v2alpha1",
        "mirror:",
        "  platform:",
        "    channels:",
    ]
    for version in versions:
        lines += [
            f"      - name: stable-{minor_name(minor_of(version))}",
            f"        minVersion: {version}",
            f"        maxVersion: {version}",
        ]
    return "\n".join(lines)


def _mirror_has_every_step(
    plan: UpgradePlan, pinned: PinnedReleases, release: ReleaseLayout
) -> CheckResult:
    name = "mirror-has-every-step"
    versions = step_versions(plan)
    unpinned = [v for v in versions if v not in pinned.pinned and v not in pinned.missing]
    missing = [*pinned.missing, *unpinned]
    expected = f"every step in {expand(release.repo, image_path=release.image_path)}, payload complete"
    if not missing and not pinned.problems:
        return _failed_check(name, True, expected, "all pinned")
    observed = []
    if missing:
        tags = ", ".join(release_tag(v) for v in missing)
        observed.append(f"mirror {tags} first:\n{image_set_snippet(missing)}")
    observed.extend(pinned.problems)
    return _failed_check(name, False, expected, "\n".join(observed))


def _day1_file_upgradeable(
    rules: UpgradeRules, day1: Day1Cluster, hosted: HostedClusterState
) -> CheckResult:
    name = "day1-file-upgradeable"
    problems = list(day1.problems)
    day1_names = sorted(nodepool.name for nodepool in day1.nodepools)
    hosted_names = sorted(nodepool.name for nodepool in hosted.nodepools)
    if day1_names != hosted_names:
        problems.append(f"the file lists NodePools {day1_names}, the cluster has {hosted_names}")
    for nodepool in hosted.nodepools:
        if nodepool.upgrade_type != _IN_PLACE:
            problems.append(f"NodePool {nodepool.name} upgradeType is {nodepool.upgrade_type}, not {_IN_PLACE}")
    if day1.image_path != rules.release.image_path:
        problems.append(
            f"imagePath is {day1.image_path}: move {day1.path} to the full mirror "
            f"{rules.release.image_path} first"
        )
    if day1.image_hc is not None and day1.image_hc != rules.release.image_path:
        problems.append(f"imageHc is {day1.image_hc}, not {rules.release.image_path}")
    return _failed_check(
        name, not problems, f"{day1.path} parses, InPlace NodePools, the full mirror",
        "; ".join(problems) or "ok",
    )


def effective_workertag(day1: Day1Cluster, nodepool: str) -> str | None:
    """The release a NodePool runs per its day1 file: its own tag, the file's, else mastertag."""
    entry = next((np for np in day1.nodepools if np.name == nodepool), None)
    if entry is not None and entry.workertag:
        return entry.workertag
    return day1.top_level_workertag or day1.mastertag


def _git_matches_cluster(day1: Day1Cluster, hosted: HostedClusterState) -> CheckResult:
    """Git is intent, the cluster is truth: a plan from one must hold for the other."""
    name = "git-matches-cluster"
    problems = []
    if hosted.control_plane_version is None:
        problems.append("the HostedCluster is mid-upgrade (no Completed version)")
    elif day1.mastertag != hosted.control_plane_version:
        problems.append(f"mastertag is {day1.mastertag}, the control plane runs {hosted.control_plane_version}")
    for nodepool in hosted.nodepools:
        written = effective_workertag(day1, nodepool.name)
        if written != nodepool.version:
            problems.append(f"NodePool {nodepool.name}: the file says {written}, it runs {nodepool.version}")
    return _failed_check(
        name, not problems, "the cluster runs what its day1 file says", "; ".join(problems) or "agree"
    )


def _day2_layers_exist(day2: Day2Layers) -> CheckResult:
    observed = "; ".join(
        f"create ocp-versions/{gap.version}/ for {gap.chart} in {gap.sig} first" for gap in day2.missing
    )
    return _failed_check(
        "day2-layers", not day2.missing, "every pinned day2 chart has a layer for every step",
        observed or "all present",
    )


def _auto_approve_allowed(request: UpgradeClusterInput, day1: Day1Cluster) -> CheckResult:
    name = "auto-approve-wave"
    if not request.auto_approve:
        return _failed_check(name, True, "", "approval by a person")
    wave = day1.upgrade.wave
    return _failed_check(
        name, wave in _AUTO_APPROVE_WAVES,
        f"upgrade.wave in {list(_AUTO_APPROVE_WAVES)} for auto_approve",
        f"upgrade.wave is {wave}",
    )


def step_versions(plan: UpgradePlan) -> list[str]:
    """Each release the plan deploys, once, in plan order."""
    seen: list[str] = []
    for step in plan.steps:
        if step.version not in seen:
            seen.append(step.version)
    return seen


# --- pre-flight: checks 5-11 -------------------------------------------------


def preflight_checks(
    *,
    argo: ArgoApplicationState,
    hosted: HostedClusterState,
    health: HostedHealth,
    mce: MceState,
    release: ReleaseLayout,
    include_idms: bool,
) -> list[CheckResult]:
    """Checks 5–10 (and 11 when `include_idms`), passed or not, in DESIGN §7 order."""
    checks = [
        CheckResult(
            name="argo-app-clean",
            passed=argo.found and argo.sync_status == "Synced" and argo.health_status == "Healthy",
            expected="Application Synced and Healthy",
            observed=(
                f"{argo.sync_status}/{argo.health_status}"
                + (f", out of sync: {', '.join(argo.out_of_sync)}" if argo.out_of_sync else "")
                if argo.found else "Application not found"
            ),
            on_failure=CheckFailure.NEEDS_HUMAN,
        ),
        CheckResult(
            name="operators-healthy",
            passed=health.reachable and not health.operators_unhealthy,
            expected="every ClusterOperator Available and not Degraded",
            observed=(
                ", ".join(health.operators_unhealthy) or "all healthy"
                if health.reachable else "the hosted cluster's API is unreachable"
            ),
            on_failure=CheckFailure.NEEDS_HUMAN,
        ),
        CheckResult(
            name="cluster-version-upgradeable",
            passed=health.cluster_version_upgradeable is True,
            # Never answered by setting hypershift.openshift.io/force-upgrade-to
            # (AGENT.md #13): a person decides why the cluster says no.
            expected="ClusterVersion Upgradeable=True",
            observed=health.upgradeable_message or str(health.cluster_version_upgradeable),
            on_failure=CheckFailure.NEEDS_HUMAN,
        ),
        CheckResult(
            name="control-plane-idle",
            passed=hosted.progressing is False and hosted.conditions.get("ClusterVersionSucceeding") == "True",
            expected="not Progressing, ClusterVersionSucceeding=True",
            observed=(
                f"Progressing={hosted.progressing}, "
                f"ClusterVersionSucceeding={hosted.conditions.get('ClusterVersionSucceeding')}"
            ),
            on_failure=CheckFailure.WAIT,
        ),
        CheckResult(
            name="nodes-healthy",
            passed=not health.nodes_not_ready and not health.nodepools_short and health.pending_csrs == 0,
            expected="all Nodes Ready, NodePools at desired replicas, no pending CSRs",
            observed="; ".join(
                part for part in (
                    f"NotReady: {', '.join(health.nodes_not_ready)}" if health.nodes_not_ready else "",
                    f"short: {', '.join(health.nodepools_short)}" if health.nodepools_short else "",
                    f"{health.pending_csrs} pending CSRs" if health.pending_csrs else "",
                ) if part
            ) or "healthy",
            on_failure=CheckFailure.NEEDS_HUMAN,
        ),
        CheckResult(
            name="pdbs-allow-disruption",
            passed=not health.blocking_pdbs,
            expected="no PodDisruptionBudget at disruptionsAllowed 0 on the nodes in scope",
            observed=(
                f"their owners must allow a disruption: {', '.join(health.blocking_pdbs)}"
                if health.blocking_pdbs else "none blocking"
            ),
            on_failure=CheckFailure.NOTIFY_OWNER,
        ),
        CheckResult(
            name="webhooks-have-endpoints",
            passed=not health.dead_webhooks,
            expected="no failurePolicy Fail webhook without endpoints",
            observed=", ".join(health.dead_webhooks) or "none",
            on_failure=CheckFailure.NEEDS_HUMAN,
        ),
    ]
    if include_idms:
        checks.append(_idms_covers_release(hosted, health, mce, release))
    return checks


def _idms_covers_release(
    hosted: HostedClusterState, health: HostedHealth, mce: MceState, release: ReleaseLayout
) -> CheckResult:
    """The release's repositories are mirrored on all three paths that pull it.

    The HostedCluster's imageContentSources (its control plane), the IDMS
    inside the hosted cluster (its nodes, from the day2 chart), and the MCE's
    own IDMS (its HyperShift operator reading the payload). Whether nodes'
    registries.conf really carries it needs a read without exec (O10).
    """
    needed = release_repositories(release)
    gaps = [
        f"{where} lacks {repo}"
        for where, mirrors in (
            ("HostedCluster imageContentSources", hosted.image_content_mirrors),
            ("the hosted cluster's IDMS", health.idms_mirrors),
            ("the MCE's IDMS", mce.idms_mirrors),
        )
        for repo in needed
        if repo not in mirrors
    ]
    return CheckResult(
        name="idms-covers-release",
        passed=not gaps,
        expected=f"{', '.join(needed)} mirrored for the control plane, the nodes and the MCE",
        observed="; ".join(gaps) or "covered",
        on_failure=CheckFailure.FAILED,
    )


# --- reading results -------------------------------------------------------------


def failed(checks: list[CheckResult]) -> list[CheckResult]:
    return [check for check in checks if not check.passed]


def worst(checks: list[CheckResult]) -> CheckFailure | None:
    """The most severe consequence among the failed checks, or None when all passed."""
    failures = failed(checks)
    if not failures:
        return None
    return max((check.on_failure for check in failures), key=_SEVERITY.__getitem__)


def describe(checks: list[CheckResult]) -> str:
    """One line per failed check: what it wanted, what it saw."""
    return "\n".join(
        f"{check.name}: expected {check.expected}; observed {check.observed}" for check in failed(checks)
    )
