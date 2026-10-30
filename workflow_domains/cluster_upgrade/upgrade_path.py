"""The upgrade path from a cluster's current versions to its target — pure, deterministic.

POLICY, not orchestration: it makes no activity call, reads no clock and touches
no I/O, so it is unit-tested with no Temporal environment at all. It stays in the
DOMAIN folder rather than `shared/` because only the brain plans a path; it runs
inside the workflow sandbox on every replay, so it must never grow a clock, a
random or a set-iteration order.

The rules, in the order they are applied:

  * The CONTROL PLANE moves through every minor and never skips one. From its
    current release, the next step is:
      - in the TARGET minor: the target, after any `must` rung between the two;
      - else, if its own minor has a rung newer than it: that minor's EXIT rung
        (the first `must` rung, else the newest rung) — a release is only known
        to upgrade into the next minor from the rung the ladder verified, the
        way 4.15.0 went 4.15.0 → 4.15.41 → 4.16.27;
      - else: the next minor's ENTRY rung (its first `must` rung, else its
        newest), or the target if that minor is the target's.
    A minor the path must enter with no rung at all is refused: nobody verified
    the way into it.
  * WORKERS may skip minors but never fall more than `max_worker_skew` minors
    behind the control plane (team rule 2; HyperShift itself allows 3). Before
    a control-plane step that would break that, every lagging NodePool first
    moves to the control plane's current release. A worker step always goes to
    the control plane's release, never past it: a NodePool may not be newer
    than its HostedCluster, patch included.
  * A rung with `data_plane_required` rolls the workers right after the control
    plane reaches it.
  * At the end, if `workers_to_target`, every lagging NodePool follows to the
    target.

Refusals raise ApplicationError typed UpgradePathRefusedError: raised from
workflow code, a bare exception would fail the workflow TASK and retry forever.

Design: docs/upgrade/DESIGN.md §5.2 in the Cluster-AutoUpgrade workspace.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import UpgradePathRefusedError
    from shared.models.cluster_upgrade import (
        LadderRung,
        StepKind,
        StepReason,
        UpgradePlan,
        UpgradeStep,
    )
    from shared.ocp_version import minor_name, parse_release, release_version


def resolve_upgrade_path(
    *,
    control_plane: str,
    nodepools: dict[str, str],
    target: str,
    ladder: list[LadderRung],
    max_worker_skew: int,
    workers_to_target: bool = True,
) -> UpgradePlan:
    """The ordered steps that take a cluster from its current versions to `target`.

    `nodepools` maps each NodePool's name to its current release, in the order
    worker steps should list them. Versions may be spelled `X.Y.Z` or
    `X.Y.Z-x86_64`; the plan carries bare `X.Y.Z`.

    An empty plan means there is nothing to do. Raises ApplicationError
    (UpgradePathRefusedError) when no path is allowed; nothing has changed then.
    """
    try:
        current = release_version(control_plane)
        goal = release_version(target)
        lagging = {name: release_version(v) for name, v in nodepools.items()}
        rungs = _ordered_rungs(ladder)
    except ValueError as err:
        raise _refused(str(err)) from None

    if max_worker_skew < 1:
        raise _refused(f"maxWorkerSkew must be at least 1, got {max_worker_skew}")
    if _key(goal) < _key(current):
        raise _refused(
            f"target {goal} is older than the control plane's {current}: "
            "there is no downgrade"
        )
    for name, version in lagging.items():
        if _key(version) > _key(current):
            raise _refused(
                f"NodePool {name} is at {version}, newer than its control plane's "
                f"{current}; HyperShift never allows that, so the cluster needs "
                "a human first"
            )
        gap = _minor(current) - _minor(version)
        if gap > max_worker_skew:
            raise _refused(
                f"NodePool {name} at {version} is already {gap} minors behind the "
                f"control plane's {current}, beyond maxWorkerSkew {max_worker_skew}; "
                "bring it within the limit by hand first"
            )

    data_plane_required = {rung.version for rung in rungs if rung.data_plane_required}
    steps: list[UpgradeStep] = []

    while _key(current) < _key(goal):
        hop, reason = _next_control_plane_step(current, goal, rungs)
        if _minor(hop) - _slowest_minor(lagging) > max_worker_skew:
            steps.append(_workers_to(current, lagging, StepReason.SKEW))
            lagging = _moved(lagging, current)
        steps.append(UpgradeStep(kind=StepKind.CONTROL_PLANE, version=hop, reason=reason))
        current = hop
        if current in data_plane_required and _behind(lagging, current):
            steps.append(_workers_to(current, lagging, StepReason.RUNG_REQUIRED))
            lagging = _moved(lagging, current)

    if workers_to_target and _behind(lagging, current):
        steps.append(_workers_to(current, lagging, StepReason.FINAL))

    return UpgradePlan(
        control_plane=release_version(control_plane),
        nodepools={name: release_version(v) for name, v in nodepools.items()},
        target=goal,
        max_worker_skew=max_worker_skew,
        steps=steps,
    )


def recommended_target(
    minor: int, ladder: list[LadderRung], available: list[str]
) -> str | None:
    """AGENT.md #8: the newest release of `minor` that is BOTH in the mirror and a rung.

    `available` is the release tags the mirror lists (either spelling; anything
    unparseable is ignored — a mirror holds other tags too). None when no
    release of that minor qualifies, which the caller reports as "mirror a rung
    of 4.<minor> first".
    """
    rung_versions = set()
    for rung in ladder:
        try:
            rung_versions.add(release_version(rung.version))
        except ValueError:
            continue
    candidates = []
    for tag in available:
        try:
            version = release_version(tag)
        except ValueError:
            continue
        if _minor(version) == minor and version in rung_versions:
            candidates.append(version)
    return max(candidates, key=_key) if candidates else None


def _next_control_plane_step(
    current: str, goal: str, rungs: list[LadderRung]
) -> tuple[str, StepReason]:
    current_minor, goal_minor = _minor(current), _minor(goal)

    if current_minor == goal_minor:
        between = [
            r
            for r in rungs
            if r.must and _key(current) < _key(r.version) < _key(goal)
        ]
        return (between[0].version, StepReason.LADDER) if between else (goal, StepReason.TARGET)

    newer_here = [
        r for r in rungs if _minor(r.version) == current_minor and _key(r.version) > _key(current)
    ]
    if newer_here:
        return _exit_or_entry(newer_here), StepReason.LADDER

    next_minor = current_minor + 1
    in_next = [r for r in rungs if _minor(r.version) == next_minor]
    if not in_next:
        raise _refused(
            f"the ladder has no rung for {minor_name(next_minor)}, so the control "
            f"plane at {current} has no verified way toward {goal}: add a verified "
            f"{minor_name(next_minor)} release to upgrade/ladder.yaml"
        )
    if next_minor == goal_minor:
        before_goal = [r for r in in_next if r.must and _key(r.version) < _key(goal)]
        return (
            (before_goal[0].version, StepReason.LADDER)
            if before_goal
            else (goal, StepReason.TARGET)
        )
    return _exit_or_entry(in_next), StepReason.LADDER


def _exit_or_entry(rungs_of_one_minor: list[LadderRung]) -> str:
    """The first `must` rung of a minor, else its newest rung (input is ascending)."""
    musts = [r for r in rungs_of_one_minor if r.must]
    return (musts[0] if musts else rungs_of_one_minor[-1]).version


def _ordered_rungs(ladder: list[LadderRung]) -> list[LadderRung]:
    """The ladder with bare versions, ascending; ValueError on a bad or repeated version."""
    normalised = [
        rung.model_copy(update={"version": release_version(rung.version)}) for rung in ladder
    ]
    seen: set[str] = set()
    for rung in normalised:
        if rung.version in seen:
            raise ValueError(f"the ladder lists {rung.version} twice")
        seen.add(rung.version)
    return sorted(normalised, key=lambda rung: _key(rung.version))


def _workers_to(version: str, lagging: dict[str, str], reason: StepReason) -> UpgradeStep:
    return UpgradeStep(
        kind=StepKind.WORKERS,
        version=version,
        reason=reason,
        nodepools=[name for name, v in lagging.items() if _key(v) < _key(version)],
    )


def _moved(lagging: dict[str, str], version: str) -> dict[str, str]:
    return {name: version if _key(v) < _key(version) else v for name, v in lagging.items()}


def _behind(lagging: dict[str, str], version: str) -> bool:
    return any(_key(v) < _key(version) for v in lagging.values())


def _slowest_minor(lagging: dict[str, str]) -> int:
    # A cluster with no NodePools has no workers to hold the control plane back.
    return min((_minor(v) for v in lagging.values()), default=10**6)


def _key(version: str) -> tuple[int, int, int]:
    return parse_release(version)


def _minor(version: str) -> int:
    return parse_release(version)[1]


def _refused(message: str) -> ApplicationError:
    return ApplicationError(message, type=UpgradePathRefusedError.__name__)
