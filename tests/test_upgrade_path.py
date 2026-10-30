"""Unit tests for the upgrade-path resolver and the OCP version helpers.

Both are pure, so they need no Temporal harness. The resolver decides what a
cluster's control plane and NodePools will run for weeks, so beside the worked
examples there is a brute-force check: every start, target and skew limit in a
grid is planned and the plan REPLAYED step by step against the invariants —
no control-plane step skips a minor, no NodePool is ever newer than its control
plane, the gap never exceeds the limit, and no `must` rung is skipped.
"""

from __future__ import annotations

import itertools

import pytest
from temporalio.exceptions import ApplicationError

from shared.exceptions import UpgradePathRefusedError
from shared.models.cluster_upgrade import LadderRung, StepKind, StepReason, UpgradePlan
from shared.ocp_version import (
    minor_name,
    minor_of,
    parse_release,
    release_tag,
    release_version,
)
from workflow_domains.cluster_upgrade.upgrade_path import (
    recommended_target,
    resolve_upgrade_path,
)

# The day1 dev ladder (upgrade/ladder.yaml): every consecutive pair is an edge
# in Red Hat's public stable-4.16 … stable-4.21 graphs as read on 2026-10-31.
LADDER = [
    LadderRung(version=v, must=True)
    for v in ("4.15.41", "4.16.55", "4.17.58", "4.18.56", "4.19.53", "4.20.44", "4.21.40")
]


def _plan(control_plane: str, target: str, *, pools=None, skew=2, ladder=None, **kw) -> UpgradePlan:
    return resolve_upgrade_path(
        control_plane=control_plane,
        nodepools=pools if pools is not None else {"pool-a": control_plane},
        target=target,
        ladder=ladder if ladder is not None else LADDER,
        max_worker_skew=skew,
        **kw,
    )


def _steps(plan: UpgradePlan) -> list[tuple[str, str, str]]:
    return [(s.kind.value, s.version, s.reason.value) for s in plan.steps]


def _refusal(**kw) -> str:
    with pytest.raises(ApplicationError) as caught:
        _plan(**kw)
    assert caught.value.type == UpgradePathRefusedError.__name__
    return str(caught.value)


class TestOcpVersion:
    def test_both_spellings_the_chart_renders_are_accepted(self) -> None:
        assert parse_release("4.16.27") == (4, 16, 27)
        assert parse_release("4.16.27-x86_64") == (4, 16, 27)
        assert release_version("4.16.27-x86_64") == "4.16.27"

    def test_the_tag_is_what_hc_release_tag_renders(self) -> None:
        assert release_tag("4.20.44") == "4.20.44-x86_64"
        assert release_tag("4.20.44-x86_64") == "4.20.44-x86_64"

    @pytest.mark.parametrize("text", ["4.20.9-multi", "4.21.0-rc.1", "latest", "4.20", "5.0.1"])
    def test_anything_the_design_never_validated_is_refused(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_release(text)

    def test_minor_helpers(self) -> None:
        assert minor_of("4.16.27") == 16
        assert minor_name(16) == "4.16"


class TestTheLadder:
    def test_four_sixteen_to_four_twenty_one_with_the_team_skew_of_two(self) -> None:
        # The DESIGN §5.2 worked example on the real ladder.
        plan = _plan("4.16.27", "4.21.40")
        assert _steps(plan) == [
            ("control-plane", "4.16.55", "ladder"),  # exit rung of its own minor first
            ("control-plane", "4.17.58", "ladder"),
            ("control-plane", "4.18.56", "ladder"),  # workers 2 behind: allowed
            ("workers", "4.18.56", "skew"),  # 4.19 would make it 3
            ("control-plane", "4.19.53", "ladder"),
            ("control-plane", "4.20.44", "ladder"),
            ("workers", "4.20.44", "skew"),
            ("control-plane", "4.21.40", "target"),
            ("workers", "4.21.40", "final"),
        ]

    def test_the_observed_four_fifteen_path(self) -> None:
        # The team saw an InPlace NodePool go 4.15.0 -> 4.15.41 -> 4.16.x.
        plan = _plan("4.15.0", "4.16.55")
        assert _steps(plan) == [
            ("control-plane", "4.15.41", "ladder"),
            ("control-plane", "4.16.55", "target"),
            ("workers", "4.16.55", "final"),
        ]

    def test_a_z_stream_needs_no_rung(self) -> None:
        # No 4.22 rung exists (4.21 -> 4.22 is candidate-only), yet a cluster
        # already on 4.22 can still take a z-stream.
        plan = _plan("4.22.8", "4.22.20")
        assert _steps(plan) == [
            ("control-plane", "4.22.20", "target"),
            ("workers", "4.22.20", "final"),
        ]

    def test_entering_a_minor_without_a_rung_is_refused(self) -> None:
        message = _refusal(control_plane="4.21.40", target="4.22.20")
        assert "no rung for 4.22" in message

    def test_the_tag_spelling_is_accepted_and_the_plan_is_bare(self) -> None:
        plan = _plan("4.20.44-x86_64", "4.21.40-x86_64")
        assert plan.control_plane == "4.20.44"
        assert plan.target == "4.21.40"
        assert all("-" not in s.version for s in plan.steps)


class TestWorkers:
    def test_only_the_lagging_nodepools_move_in_their_given_order(self) -> None:
        plan = _plan(
            "4.18.56",
            "4.20.44",
            pools={"zone-c": "4.16.55", "zone-a": "4.18.56", "zone-b": "4.16.55"},
        )
        workers = [s for s in plan.steps if s.kind is StepKind.WORKERS]
        # The skew step before 4.19 moves only the two that would fall 3 behind…
        assert (workers[0].reason, workers[0].version, workers[0].nodepools) == (
            StepReason.SKEW,
            "4.18.56",
            ["zone-c", "zone-b"],
        )
        # …and the final step moves all three, in the order given.
        assert workers[-1].nodepools == ["zone-c", "zone-a", "zone-b"]

    def test_a_skew_of_one_rolls_the_workers_every_minor(self) -> None:
        plan = _plan("4.17.58", "4.19.53", skew=1)
        assert _steps(plan) == [
            ("control-plane", "4.18.56", "ladder"),
            ("workers", "4.18.56", "skew"),
            ("control-plane", "4.19.53", "target"),
            ("workers", "4.19.53", "final"),
        ]

    def test_workers_to_target_false_still_obeys_the_skew(self) -> None:
        plan = _plan("4.16.55", "4.21.40", workers_to_target=False)
        reasons = [s.reason for s in plan.steps if s.kind is StepKind.WORKERS]
        assert reasons and all(r is StepReason.SKEW for r in reasons)

    def test_already_at_target_with_workers_behind_moves_only_the_workers(self) -> None:
        plan = _plan("4.20.44", "4.20.44", pools={"pool-a": "4.19.53"})
        assert _steps(plan) == [("workers", "4.20.44", "final")]
        stay = _plan("4.20.44", "4.20.44", pools={"pool-a": "4.19.53"}, workers_to_target=False)
        assert stay.steps == []

    def test_nothing_to_do_is_an_empty_plan(self) -> None:
        assert _plan("4.20.44", "4.20.44").steps == []

    def test_a_cluster_with_no_nodepools_moves_only_its_control_plane(self) -> None:
        plan = _plan("4.16.55", "4.19.53", pools={})
        assert all(s.kind is StepKind.CONTROL_PLANE for s in plan.steps)


class TestRungFlags:
    def test_a_must_rung_in_the_target_minor_is_not_skipped(self) -> None:
        ladder = LADDER[:-2] + [
            LadderRung(version="4.20.10", must=True),
            LadderRung(version="4.20.44"),
        ]
        plan = _plan("4.19.53", "4.20.50", ladder=ladder)
        assert [s.version for s in plan.steps if s.kind is StepKind.CONTROL_PLANE] == [
            "4.20.10",
            "4.20.50",
        ]

    def test_a_non_must_rung_in_the_target_minor_is_skipped(self) -> None:
        ladder = LADDER[:-2] + [LadderRung(version="4.20.44", must=False)]
        plan = _plan("4.19.53", "4.20.50", ladder=ladder)
        assert [s.version for s in plan.steps if s.kind is StepKind.CONTROL_PLANE] == ["4.20.50"]

    def test_data_plane_required_rolls_the_workers_at_that_rung(self) -> None:
        ladder = [
            r.model_copy(update={"data_plane_required": r.version == "4.17.58"}) for r in LADDER
        ]
        plan = _plan("4.16.55", "4.18.56", ladder=ladder)
        assert _steps(plan) == [
            ("control-plane", "4.17.58", "ladder"),
            ("workers", "4.17.58", "rung-required"),
            ("control-plane", "4.18.56", "target"),
            ("workers", "4.18.56", "final"),
        ]


class TestRefusals:
    def test_a_downgrade(self) -> None:
        assert "no downgrade" in _refusal(control_plane="4.20.44", target="4.19.53")

    def test_a_nodepool_newer_than_its_control_plane_even_by_a_patch(self) -> None:
        message = _refusal(control_plane="4.18.5", target="4.18.56", pools={"p": "4.18.6"})
        assert "newer than its control plane" in message

    def test_a_cluster_already_out_of_policy(self) -> None:
        message = _refusal(control_plane="4.19.53", target="4.20.44", pools={"p": "4.16.55"})
        assert "3 minors behind" in message

    def test_a_version_the_chart_would_not_render_as_x86_64(self) -> None:
        message = _refusal(control_plane="4.20.9-multi", target="4.20.44")
        assert "not an OCP release version" in message

    def test_a_repeated_rung(self) -> None:
        ladder = LADDER + [LadderRung(version="4.20.44-x86_64")]
        assert "twice" in _refusal(control_plane="4.19.53", target="4.20.44", ladder=ladder)

    def test_a_skew_limit_below_one(self) -> None:
        assert "at least 1" in _refusal(control_plane="4.19.53", target="4.20.44", skew=0)


def _replay(plan: UpgradePlan, ladder: list[LadderRung]) -> None:
    """Walk a plan step by step and assert every invariant at every point."""
    cp = plan.control_plane
    pools = dict(plan.nodepools)
    control_plane_versions = []
    for step in plan.steps:
        if step.kind is StepKind.CONTROL_PLANE:
            assert parse_release(step.version) > parse_release(cp), "control plane went backward"
            assert minor_of(step.version) <= minor_of(cp) + 1, f"skipped a minor: {step.version}"
            cp = step.version
            control_plane_versions.append(cp)
        else:
            assert step.version == cp, "workers moved somewhere other than the control plane"
            assert step.nodepools, "an empty worker step"
            for name in step.nodepools:
                assert parse_release(pools[name]) < parse_release(step.version)
                pools[name] = step.version
        for version in pools.values():
            assert parse_release(version) <= parse_release(cp), "a NodePool got ahead"
            assert minor_of(cp) - minor_of(version) <= plan.max_worker_skew, "skew exceeded"
    assert cp == plan.target
    start, end = parse_release(plan.control_plane), parse_release(plan.target)
    musts = {r.version for r in ladder if r.must and start < parse_release(r.version) <= end}
    skipped = musts - set(control_plane_versions)
    assert not skipped, f"skipped must rungs: {skipped}"


_STARTS = [
    "4.15.0", "4.15.41", "4.16.27", "4.16.55", "4.17.3",
    "4.18.56", "4.19.53", "4.20.18", "4.20.44", "4.21.40",
]
_TARGETS = ["4.16.55", "4.17.58", "4.18.56", "4.19.60", "4.20.44", "4.20.50", "4.21.40", "4.21.45"]


class TestInvariants:
    @pytest.mark.parametrize("skew", [1, 2, 3])
    def test_every_plan_in_the_grid_replays_cleanly(self, skew: int) -> None:
        planned = 0
        for start, target in itertools.product(_STARTS, _TARGETS):
            if parse_release(target) < parse_release(start):
                continue
            behind = f"4.{max(15, minor_of(start) - skew)}.0"
            for pools in ({"a": start, "b": start}, {"a": start, "b": behind}):
                for to_target in (True, False):
                    plan = _plan(start, target, pools=pools, skew=skew, workers_to_target=to_target)
                    _replay(plan, LADDER)
                    if to_target:
                        assert all(v == target for v in _final_pools(plan).values())
                    planned += 1
        assert planned > 200

    def test_the_same_input_gives_the_same_plan(self) -> None:
        assert _plan("4.16.27", "4.21.40") == _plan("4.16.27", "4.21.40")


def _final_pools(plan: UpgradePlan) -> dict[str, str]:
    pools = dict(plan.nodepools)
    for step in plan.steps:
        if step.kind is StepKind.WORKERS:
            pools.update({name: step.version for name in step.nodepools})
    return pools


class TestRecommendedTarget:
    def test_the_newest_release_that_is_both_mirrored_and_a_rung(self) -> None:
        ladder = LADDER + [LadderRung(version="4.20.50")]
        mirror = [
            "4.20.18-x86_64",
            "4.20.44-x86_64",
            "4.20.50-x86_64",
            "4.20.60-x86_64",  # mirrored, but not a rung
            "4.20.44-x86_64-hypershift",  # the CPO tag: not a release
        ]
        assert recommended_target(20, ladder, mirror) == "4.20.50"

    def test_none_when_no_rung_of_that_minor_is_mirrored(self) -> None:
        assert recommended_target(21, LADDER, ["4.21.39-x86_64"]) is None
