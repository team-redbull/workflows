"""The checks an upgrade-cluster run applies, and the rollout rules — pure, deterministic.

POLICY, like upgrade_path.py: the limbs READ (a day1 file, a HostedCluster, the
hosted cluster's health) and these functions DECIDE, so every rule is a plain
unit test and the limbs stay dumb readers. It runs inside the workflow sandbox
on every replay: no clock, no I/O, no set-iteration order in anything returned.

  * mce_of_day1_path — the hosting MCE, from where day1 keeps the cluster file.
  * planning_checks — DESIGN §7 checks 1–4, before approval. Any failure fails
    the run, with every failure listed at once; nothing has been committed.
  * preflight_checks — checks 5–11, before approval and again (5–10) before
    every control-plane step and NodePool roll.
  * cluster_profile, nodepool_order, soak_minutes — how the workers roll (D7).
  * nodepool_roll_deadline_minutes — the machine deadline of one NodePool roll.
  * tier1_enabled — whether the agent may run tier-1 actions itself (AGENT.md #25).
  * pending_admin_gates, review_admin_gates — the admin acks a control-plane
    minor step needs, and whether the run may give them itself (D16).
  * worst, agent_checks, owner_checks — what a set of failed checks asks of
    the run: whose turn it is, and what the agent may look at.

What a limb reads and this module also matches — the day1 path, a release's
repositories, HyperShift's and the CVO's strings — is in shared/upgrade_layout.py,
so the two sides cannot drift apart.

Design: docs/upgrade/DESIGN.md §6–§7 in the Cluster-AutoUpgrade workspace.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.cluster_upgrade import (
        AdminAckRule,
        AdminAckState,
        ApiUsage,
        ArgoApplicationState,
        CheckFailure,
        CheckResult,
        ClusterProfile,
        Day1Cluster,
        Day2Layers,
        HostedClusterState,
        HostedHealth,
        MceState,
        NodePoolMode,
        PinnedReleases,
        ReleaseLayout,
        StepKind,
        UpgradeClusterInput,
        UpgradePlan,
        UpgradeRules,
    )
    from shared.ocp_version import minor_name, minor_of, release_tag
    from shared.upgrade_layout import (
        ADMIN_ACK_REQUIRED,
        DAY1_CLUSTER_FILE,
        IN_PLACE,
        cpo_repository,
        release_repositories,
        release_repository,
    )

# Soak after each NodePool gate passes, unless the cluster file sets one.
_ZONED_SOAK_MINUTES = 30
_GENERIC_SOAK_MINUTES = 0

# How long one InPlace batch of nodes may take: drain, the MachineConfig, a
# bare-metal reboot. The hour add-server-to-mce allows a bare-metal POST and boot.
PER_BATCH_MINUTES = 60

# The waves whose clusters may run without a person: lab and canary. Three
# things are allowed there and nowhere else — auto-approve (AGENT.md #3, #30),
# auto NodePool mode, which skips the customer's sign-off between NodePools
# (D19, D20), and the agent's tier-1 actions (AGENT.md #25, tier1_enabled).
# ONE set on purpose: each is the run acting where no person looks first, and
# lab and canary are the clusters where that may cost something. Graduating a
# wave is one reviewed change here, for all three.
_UNATTENDED_WAVES = (0, 1)

# The CVO's Upgradeable=False reason while its own update runs (4.19 and later).
_UPDATE_IN_PROGRESS = "UpdateInProgress"
# ADMIN_ACK_REQUIRED (shared/upgrade_layout.py), its reason while an admin gate
# of the current minor has no ack, stops no pre-flight: the run gives or asks
# for the ack itself, right before a control-plane minor step (D16), and
# HyperShift never gates a NodePool on Upgradeable — a NodePool rolled 4.17 ->
# 4.19 under it on the sandbox (CONTEXT §8.13).

# How the CVO recognises an admin gate and the minor it belongs to
# (adminAckGateFmt, gateApplicableToCurrentVersion, pkg/cvo/upgradeable.go,
# cluster-version-operator release-4.19). A gate applies while the cluster's
# current version has that minor.
_ADMIN_GATE = re.compile(r"^ack-[4-5][.]([0-9]{1,})-[^-]")

# Which consequence of a failed check wins (worst). Every rank distinct, so the
# answer never depends on the order the checks were listed in. NEEDS_HUMAN
# outranks NOTIFY_OWNER: a platform fault next to a customer's PDB is still a
# platform fault, which the agent diagnoses before a person is asked; only a
# run whose sole problem is the owner's goes straight to the owner.
_SEVERITY = {
    CheckFailure.WAIT: 0,
    CheckFailure.NOTIFY_OWNER: 1,
    CheckFailure.NEEDS_HUMAN: 2,
    CheckFailure.FAILED: 3,
}

# What a check built from the hosted cluster's API says when that API did not answer.
_HOSTED_UNREACHABLE = "the hosted cluster's API is unreachable"


# --- profile and order ---------------------------------------------------------


def mce_of_day1_path(path: str) -> str | None:
    """The MCE whose folder holds this cluster file, or None for a path outside that layout.

    The layout is shared/upgrade_layout.DAY1_CLUSTER_FILE, the one the hub limb
    finds the file by. The folder name is the canonical MCE name (with its
    `ocp4-` prefix), the one every MCE limb's queue carries.
    """
    match = DAY1_CLUSTER_FILE.fullmatch(path)
    return match.group("mce") if match else None


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


_WHOLE_NUMBER = re.compile(r"^[0-9]+$")
_PERCENTAGE = re.compile(r"^([0-9]+)%$")


def max_unavailable_value(value: int | str) -> str:
    """A maxUnavailable a person may ask for, as the text the cluster file gets (D28).

    A whole number of nodes, 1 or more, or a whole percentage, 1% to 100%.
    HyperShift would take 0 and raise it to 1 (`_batch_size`), so 0 is refused
    rather than written. Raises ValueError with the reason.
    """
    if isinstance(value, bool):
        raise ValueError(f"maxUnavailable {value!r} is not a number of nodes or a percentage")
    text = str(value).strip()
    if _WHOLE_NUMBER.match(text) and int(text) >= 1:
        return str(int(text))
    percentage = _PERCENTAGE.match(text)
    if percentage and 1 <= int(percentage.group(1)) <= 100:
        return f"{int(percentage.group(1))}%"
    raise ValueError(
        f"maxUnavailable {value!r} must be a whole number of nodes (1 or more) or a percentage (1% to 100%)"
    )


def max_unavailable_overrides(requested: dict[str, str] | None, plan: UpgradePlan) -> dict[str, str]:
    """The request's maxUnavailable per NodePool, for the NodePools this plan rolls (D28).

    Each is written with that NodePool's workertag commit, so a NodePool the
    plan never rolls would never get it: naming one is refused, as is a value
    `max_unavailable_value` refuses. Raises ValueError with the reason, which
    the caller reports as a failed planning check.
    """
    if not requested:
        return {}
    # A NodePool may roll in two steps (a skew step, then the final one): once is enough here.
    rolled = list(dict.fromkeys(
        name for step in plan.steps if step.kind is StepKind.WORKERS for name in step.nodepools
    ))
    not_rolled = sorted(set(requested) - set(rolled))
    if not_rolled:
        raise ValueError(
            f"max_unavailable names {', '.join(not_rolled)}, which this plan does not roll "
            f"(it rolls {', '.join(rolled) or 'no NodePool'}): its maxUnavailable would never be written"
        )
    return {name: max_unavailable_value(requested[name]) for name in rolled if name in requested}


def _batch_size(replicas: int, max_unavailable: str | None) -> int:
    """How many nodes an InPlace roll takes down at once.

    HyperShift's own rule (`getInPlaceMaxUnavailable`, hypershift
    release-4.20 `hypershift-operator/controllers/nodepool/capi.go:1034`):
    unset means 1; a percentage is scaled to the replicas rounding DOWN; and 0
    is raised to 1.

    TOTAL — it never raises. It runs in workflow code, where an exception fails
    the workflow TASK and the run hangs (CLAUDE.md §5), and the value is the
    NodePool's, read off the MCE: nothing the run can refuse. Anything it cannot
    read as a whole number or a whole percentage ("33.5%", "", "a") counts as
    1 at a time, the slowest roll, which gives the longest deadline: a deadline
    too long only waits, one too short would hand a healthy roll to the agent.
    """
    if max_unavailable is None:
        return 1
    text = max_unavailable.strip()
    try:
        count = math.floor(replicas * int(text[:-1]) / 100) if text.endswith("%") else int(text)
    except ValueError:
        return 1
    return max(1, count)


def nodepool_roll_deadline_minutes(replicas: int | None, max_unavailable: str | None) -> int:
    """The whole budget of one NodePool roll: PER_BATCH_MINUTES per batch of nodes."""
    nodes = replicas or 1
    batches = math.ceil(nodes / _batch_size(nodes, max_unavailable))
    return batches * PER_BATCH_MINUTES


# Lab clusters only (wave 0): with no day2 Application, nobody has a layer to
# sync after a control-plane step, so the run does not wait for confirm-day2
# (the owner, 2026-11-01, D27). A canary (1) and every production wave wait.
_LAB_WAVES = (0,)


def day2_confirm_may_be_skipped(wave: int | None) -> bool:
    """Whether confirm-day2 may be skipped when the cluster has no day2 Application: lab clusters only (D27)."""
    return wave in _LAB_WAVES


def tier1_enabled(wave: int | None) -> bool:
    """Whether the agent runs tier-1 actions itself, judge-reviewed, within its budget (AGENT.md #25).

    Lab and canary only, the unattended waves. Everywhere else every action is
    a proposal a person approves, until the fault-injection evals are green and
    the same fix has been approved repeatedly. Settled at planning from the
    cluster file's `upgrade.wave`; a file with no wave is production.
    """
    return wave in _UNATTENDED_WAVES


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
        _auto_nodepool_mode_allowed(request, day1, plan),
    ]


def _planning_check(name: str, passed: bool, expected: str, observed: str) -> CheckResult:
    """One planning check's result, passed or not: a planning failure always fails the run."""
    return CheckResult(
        name=name, passed=passed, expected=expected, observed=observed,
        on_failure=CheckFailure.FAILED,
    )


def _rules_match_mce(rules: UpgradeRules, mce: MceState, plan: UpgradePlan) -> CheckResult:
    name = "rules-match-mce"
    expected = f"supported-versions = {sorted(rules.supported_hosted_versions)}"
    if not mce.supported_versions_found:
        return _planning_check(name, False, expected, "ConfigMap hypershift/supported-versions not found")
    if sorted(set(mce.supported_versions)) != sorted(set(rules.supported_hosted_versions)):
        return _planning_check(
            name, False, expected,
            f"the MCE reports {sorted(mce.supported_versions)}: fix upgrade/mces/<mce>.yaml or the MCE",
        )
    outside = [
        step.version for step in plan.steps
        if minor_name(minor_of(step.version)) not in mce.supported_versions
    ]
    if outside:
        return _planning_check(
            name, False, "every step's minor supported by this MCE",
            f"not supported here: {', '.join(outside)}",
        )
    return _planning_check(name, True, expected, "matches")


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
    expected = f"every step in {release_repository(release)}, payload complete"
    # A layout with a CPO repo pins the operator image too; one without (a public
    # source, D25) runs the payload's own CPO, so no operator digest is expected.
    operator = cpo_repository(release)
    no_operator = [
        v for v in versions
        if operator is not None and v in pinned.pinned and pinned.pinned[v].control_plane_operator is None
    ]
    problems = [
        *pinned.problems,
        *(f"{v}: no control-plane-operator digest pinned, though the layout names {release.cpo_repo}"
          for v in no_operator),
    ]
    if not missing and not problems:
        return _planning_check(name, True, expected, "all pinned")
    observed = []
    if missing:
        tags = ", ".join(release_tag(v) for v in missing)
        observed.append(f"mirror {tags} first:\n{image_set_snippet(missing)}")
    observed.extend(problems)
    return _planning_check(name, False, expected, "\n".join(observed))


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
        if nodepool.upgrade_type != IN_PLACE:
            problems.append(f"NodePool {nodepool.name} upgradeType is {nodepool.upgrade_type}, not {IN_PLACE}")
    if day1.image_path != rules.release.image_path:
        problems.append(
            f"imagePath is {day1.image_path}: move {day1.path} to the full mirror "
            f"{rules.release.image_path} first"
        )
    if day1.image_hc is not None and day1.image_hc != rules.release.image_path:
        problems.append(f"imageHc is {day1.image_hc}, not {rules.release.image_path}")
    problems += _release_layout_problems(rules.release, day1)
    return _planning_check(
        name, not problems, f"{day1.path} parses, InPlace NodePools, the full mirror",
        "; ".join(problems) or "ok",
    )


def _release_layout_problems(release: ReleaseLayout, day1: Day1Cluster) -> list[str]:
    """The chart and the rules must build the same image references (check 3).

    The chart renders `<imagePath>/<releaseRepo>:<tag>` and, unless `cpoRepo`
    is "", a CPO annotation on `<imagePath>/<cpoRepo>`; the run pins what the
    rules' `repo`/`cpoRepo` say. Disagreeing, the cluster would run images
    nobody pinned. Only for a reader that reports the two: the hub limb always
    does, never as None; an older one's payloads (in history too) lack the
    keys and decode to None, and the check then stays as it was.
    """
    if day1.release_repo is None or day1.cpo_repo is None:
        return []
    problems = []
    rendered = f"{release.image_path}/{day1.release_repo}"
    pinned = release_repository(release)
    if rendered != pinned:
        problems.append(
            f"releaseRepo {day1.release_repo} renders {rendered}, but the rules pin {pinned}: "
            "set the cluster's releaseRepo to match upgrade/ladder.yaml's release.repo"
        )
    pinned_cpo = cpo_repository(release)
    if pinned_cpo is None and day1.cpo_repo:
        problems.append(
            f"cpoRepo {day1.cpo_repo} renders a control-plane-operator image the rules do not pin "
            '(release has no cpoRepo): set the cluster\'s cpoRepo to ""'
        )
    elif pinned_cpo is not None:
        rendered_cpo = f"{release.image_path}/{day1.cpo_repo}" if day1.cpo_repo else "no CPO image"
        if rendered_cpo != pinned_cpo:
            problems.append(f"cpoRepo renders {rendered_cpo}, but the rules pin {pinned_cpo}")
    return problems


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
    return _planning_check(
        name, not problems, "the cluster runs what its day1 file says", "; ".join(problems) or "agree"
    )


def _day2_layers_exist(day2: Day2Layers) -> CheckResult:
    observed = "; ".join(
        f"create ocp-versions/{gap.version}/ for {gap.chart} in {gap.sig} first" for gap in day2.missing
    )
    return _planning_check(
        "day2-layers", not day2.missing, "every pinned day2 chart has a layer for every step",
        observed or "all present",
    )


def _auto_approve_allowed(request: UpgradeClusterInput, day1: Day1Cluster) -> CheckResult:
    name = "auto-approve-wave"
    if not request.auto_approve:
        return _planning_check(name, True, "", "approval by a person")
    wave = day1.upgrade.wave
    return _planning_check(
        name, wave in _UNATTENDED_WAVES,
        f"upgrade.wave in {list(_UNATTENDED_WAVES)} for auto_approve",
        f"upgrade.wave is {wave}",
    )


def _auto_nodepool_mode_allowed(request: UpgradeClusterInput, day1: Day1Cluster, plan: UpgradePlan) -> CheckResult:
    """Auto mode rolls NodePools without stopping, so the customer gets no turn to check (D19):
    only lab and canary clusters may run it (D20). A plan that rolls no workers is not affected."""
    name = "auto-mode-wave"
    if request.nodepool_mode is not NodePoolMode.AUTO or not any(s.kind is StepKind.WORKERS for s in plan.steps):
        return _planning_check(name, True, "", "the customer signs off before each NodePool")
    wave = day1.upgrade.wave
    return _planning_check(
        name, wave in _UNATTENDED_WAVES,
        f"upgrade.wave in {list(_UNATTENDED_WAVES)} for nodepool_mode auto; elsewhere the customer runs "
        "their readiness checks and signs off with continue",
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
    """Checks 5–10 (and 11 when `include_idms`), passed or not, in DESIGN §7 order.

    `health` is read through the hosted cluster's own API. When that API did not
    answer (`reachable` False, every list empty), nothing built from it passes:
    each such check fails NEEDS_HUMAN saying so — an unread PDB list is not a
    customer's to clear, and an empty node list is not a healthy one.
    """
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
        _read_from_hosted(health, CheckResult(
            name="operators-healthy",
            passed=not health.operators_unhealthy,
            expected="every ClusterOperator Available and not Degraded",
            observed=", ".join(health.operators_unhealthy) or "all healthy",
            on_failure=CheckFailure.NEEDS_HUMAN,
        )),
        _read_from_hosted(health, _cluster_version_upgradeable(health)),
        CheckResult(
            name="control-plane-idle",
            # Progressing alone is not enough: HyperShift drops it to False as soon
            # as the CVO takes the new release, long before the rollout ends
            # (isProgressing, hostedcluster_controller.go, release-4.20; seen live
            # on a 4.17 -> 4.18 upgrade). The CVO's own conditions and a Completed
            # history entry say the rollout is over.
            passed=(
                hosted.progressing is False
                and hosted.control_plane_version is not None
                and hosted.conditions.get("ClusterVersionProgressing") == "False"
                and hosted.conditions.get("ClusterVersionSucceeding") == "True"
            ),
            expected="not Progressing, version Completed, ClusterVersionProgressing=False, ClusterVersionSucceeding=True",
            observed=(
                f"Progressing={hosted.progressing}, "
                f"version={hosted.control_plane_version or 'not Completed'}, "
                f"ClusterVersionProgressing={hosted.conditions.get('ClusterVersionProgressing')}, "
                f"ClusterVersionSucceeding={hosted.conditions.get('ClusterVersionSucceeding')}"
            ),
            on_failure=CheckFailure.WAIT,
        ),
        _read_from_hosted(health, CheckResult(
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
        )),
        _read_from_hosted(health, CheckResult(
            name="pdbs-allow-disruption",
            passed=not health.blocking_pdbs,
            expected="no PodDisruptionBudget at disruptionsAllowed 0 on the nodes in scope",
            observed=(
                f"their owners must allow a disruption: {', '.join(health.blocking_pdbs)}"
                if health.blocking_pdbs else "none blocking"
            ),
            on_failure=CheckFailure.NOTIFY_OWNER,
        )),
        _read_from_hosted(health, CheckResult(
            name="webhooks-have-endpoints",
            passed=not health.dead_webhooks,
            expected="no failurePolicy Fail webhook without endpoints",
            observed=", ".join(health.dead_webhooks) or "none",
            on_failure=CheckFailure.NEEDS_HUMAN,
        )),
    ]
    if include_idms:
        checks.append(_idms_covers_release(hosted, health, mce, release))
    return checks


def _read_from_hosted(health: HostedHealth, check: CheckResult) -> CheckResult:
    """`check` as built from `health` — or, when the hosted API did not answer, failed for that.

    Its name and what it expects stay, so a person reads which checks went
    unread; it is NEEDS_HUMAN whatever it would otherwise be.
    """
    if health.reachable:
        return check
    return check.model_copy(update={
        "passed": False, "observed": _HOSTED_UNREACHABLE, "on_failure": CheckFailure.NEEDS_HUMAN,
    })


def upgradeable_reasons(health: HostedHealth) -> list[str]:
    """Every reason the CVO gives for Upgradeable=False; empty when it is True."""
    if health.cluster_version_upgradeable is True:
        return []
    if health.upgradeable_reasons:
        return list(health.upgradeable_reasons)
    return [health.upgradeable_reason] if health.upgradeable_reason else []


def _cluster_version_upgradeable(health: HostedHealth) -> CheckResult:
    """Check 6's Upgradeable half. A person decides why the cluster says no — except for two reasons.

    Never answered by setting hypershift.openshift.io/force-upgrade-to
    (AGENT.md #13). UpdateInProgress, the CVO's own "an update is still
    running", clears by itself, so it waits like control-plane-idle does.
    AdminAckRequired is the admin-ack step's to answer (D16), right before the
    next control-plane minor step.
    """
    reasons = upgradeable_reasons(health)
    blocking = [reason for reason in reasons if reason != ADMIN_ACK_REQUIRED]
    observed = (
        (f"{', '.join(reasons)}: " if reasons else "")
        + (health.upgradeable_message or str(health.cluster_version_upgradeable))
    )
    if reasons and not blocking:
        observed += " (the run gives or asks for the ack before the next control-plane minor step)"
    return CheckResult(
        name="cluster-version-upgradeable",
        passed=health.cluster_version_upgradeable is True or (bool(reasons) and not blocking),
        expected="ClusterVersion Upgradeable=True, or only an admin ack missing",
        observed=observed,
        on_failure=(
            CheckFailure.WAIT if blocking and all(reason == _UPDATE_IN_PROGRESS for reason in blocking)
            else CheckFailure.NEEDS_HUMAN
        ),
    )


def _idms_covers_release(
    hosted: HostedClusterState, health: HostedHealth, mce: MceState, release: ReleaseLayout
) -> CheckResult:
    """The release's repositories are mirrored on all three paths that pull it.

    The HostedCluster's imageContentSources (its control plane), the IDMS
    inside the hosted cluster (its nodes, from the day2 chart), and the MCE's
    own IDMS (its HyperShift operator reading the payload). Whether nodes'
    registries.conf really carries it needs a read without exec (O10).

    A layout that is not mirrored (`release.mirrored: false`, a connected lab
    pulling from the public source, D25) has nothing to cover: it passes,
    saying why, so the reason is in the run's record and not a silent skip.
    """
    if not release.mirrored:
        return CheckResult(
            name="idms-covers-release",
            passed=True,
            expected="the release's repositories mirrored for the control plane, the nodes and the MCE",
            observed=(
                f"not applicable: releases come straight from {release_repository(release)} "
                "(release.mirrored: false), no mirror to cover"
            ),
            on_failure=CheckFailure.FAILED,
        )
    needed = release_repositories(release)
    # The hosted cluster's own IDMS is judged only when its API answered: an
    # unread one is neither a gap to fail the run on nor coverage to pass it on.
    paths = [
        ("HostedCluster imageContentSources", hosted.image_content_mirrors),
        *([("the hosted cluster's IDMS", health.idms_mirrors)] if health.reachable else []),
        ("the MCE's IDMS", mce.idms_mirrors),
    ]
    gaps = [f"{where} lacks {repo}" for where, mirrors in paths for repo in needed if repo not in mirrors]
    unread = [] if health.reachable else [f"the hosted cluster's IDMS not read: {_HOSTED_UNREACHABLE}"]
    return CheckResult(
        name="idms-covers-release",
        passed=not gaps and not unread,
        expected=f"{', '.join(needed)} mirrored for the control plane, the nodes and the MCE",
        observed="; ".join([*gaps, *unread]) or "covered",
        # A gap read off the HostedCluster or the MCE is real, and fails a run
        # not yet approved. With none, an unread hosted cluster is a person's
        # to look at, like every other check built from it.
        on_failure=CheckFailure.NEEDS_HUMAN if unread and not gaps else CheckFailure.FAILED,
    )


# --- admin acks (D16) -------------------------------------------------------------


@dataclass(frozen=True)
class AdminGateReview:
    """What the run makes of one pending admin gate (review_admin_gates).

    Brain-only: decided and used within one workflow task, never an activity's
    argument or result, a query's payload or history, so not a shared model.
    """

    gate: str
    message: str  # what the gate says, as admin-gates holds it
    listed: bool  # in the rules file's adminAcks
    in_use: list[ApiUsage] = field(default_factory=list)  # callers that count


def admin_gate_minor(gate: str) -> int | None:
    """The minor an admin gate belongs to (19 for `ack-4.19-...`), or None when the CVO would not read it."""
    match = _ADMIN_GATE.match(gate)
    return int(match.group(1)) if match else None


def pending_admin_gates(state: AdminAckState, control_plane_version: str) -> dict[str, str]:
    """The admin gates the CVO enforces now and nobody has acked: gate -> its message.

    The CVO's own rule (checkAdminGate, pkg/cvo/upgradeable.go, release-4.19):
    a gate applies while its `ack-4.<minor>-` matches the minor of the cluster's
    current version, and is acked only by the exact string "true" in admin-acks.
    A gate name the CVO cannot parse is pending too: the CVO reports it as an
    error, which a person has to look at.
    """
    minor = minor_of(control_plane_version)
    pending = {}
    for gate, message in sorted(state.gates.items()):
        gate_minor = admin_gate_minor(gate)
        if gate_minor is not None and gate_minor != minor:
            continue
        if state.acks.get(gate) == "true":
            continue
        pending[gate] = message
    return pending


def admin_ack_apis(rules: list[AdminAckRule]) -> list[str]:
    """Every API the listed gates cover, once each, in rule order: what the reader reports callers of."""
    apis: list[str] = []
    for rule in rules:
        apis.extend(api for api in rule.apis if api not in apis)
    return apis


def review_admin_gates(
    pending: dict[str, str],
    rules: list[AdminAckRule],
    usage: list[ApiUsage],
    ignored_users: list[str],
) -> list[AdminGateReview]:
    """For each pending gate: is it in the rules file, and who still calls its APIs.

    A caller counts unless its user is in `ignored_users` (they walk every
    registered API, Red Hat KCS 6955985). Strict on purpose: one request from
    anyone else is enough to ask a person, because the ack is what lets the
    removal happen.
    """
    by_gate = {rule.gate: rule for rule in rules}
    reviews = []
    for gate, message in pending.items():
        rule = by_gate.get(gate)
        in_use = [] if rule is None else [
            use for use in usage
            if use.api in rule.apis and use.username not in ignored_users and use.requests > 0
        ]
        reviews.append(AdminGateReview(gate=gate, message=message, listed=rule is not None, in_use=in_use))
    return reviews


def admin_acks_on_path(plan: UpgradePlan, rules: list[AdminAckRule]) -> list[str]:
    """The listed admin acks the plan's control-plane minor steps will meet, for the approver.

    Only what the rules file knows: a gate it does not list shows up at the
    step itself, and waits for a person there.
    """
    found = []
    version = plan.control_plane
    for step in plan.steps:
        if step.kind is not StepKind.CONTROL_PLANE:
            continue
        if minor_of(step.version) > minor_of(version):
            found += [
                f"{rule.gate} before {step.version}"
                for rule in rules if admin_gate_minor(rule.gate) == minor_of(version)
            ]
        version = step.version
    return found


def describe_api_usage(usage: list[ApiUsage]) -> str:
    """One line per caller: the API, who, with what, how often."""
    return "\n".join(
        f"  {use.api}: {use.username}"
        + (f" ({use.user_agent})" if use.user_agent else "")
        + f", {use.requests} requests" + (f" ({', '.join(use.verbs)})" if use.verbs else "")
        for use in usage
    )


# --- reading results -------------------------------------------------------------


def failed(checks: list[CheckResult]) -> list[CheckResult]:
    return [check for check in checks if not check.passed]


def worst(checks: list[CheckResult]) -> CheckFailure | None:
    """The most severe consequence among the failed checks, or None when all passed.

    NOTIFY_OWNER only when nothing but owners' checks (and waits) failed: next
    to any platform failure it is NEEDS_HUMAN (_SEVERITY).
    """
    failures = failed(checks)
    if not failures:
        return None
    return max((check.on_failure for check in failures), key=_SEVERITY.__getitem__)


def owner_checks(checks: list[CheckResult]) -> list[CheckResult]:
    """The failed checks only their owner can clear (a customer's PDB, AGENT.md #12): for the owner's message."""
    return [check for check in failed(checks) if check.on_failure is CheckFailure.NOTIFY_OWNER]


def agent_checks(checks: list[CheckResult]) -> list[CheckResult]:
    """The failed checks the agent may diagnose: every one but an owner's, which never goes to the agent."""
    return [check for check in failed(checks) if check.on_failure is not CheckFailure.NOTIFY_OWNER]


def describe(checks: list[CheckResult]) -> str:
    """One line per failed check: what it wanted, what it saw."""
    return "\n".join(
        f"{check.name}: expected {check.expected}; observed {check.observed}" for check in failed(checks)
    )
