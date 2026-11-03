"""update-nodepool's pure rules: which NodePools are a kind's, balance, and the spread of a new total."""

from __future__ import annotations

import pytest

from workflow_domains.server_lifecycle import nodepool_replicas

KIND = "cisco-m6-bat-yam-64c-512gb"


@pytest.mark.parametrize(
    "infra_env",
    [KIND, f"{KIND}-zone-a", f"{KIND}-zone-c", f"{KIND}-b2", f"{KIND}-anything-at-all"],
)
def test_the_kind_is_the_infraenv_or_it_followed_by_a_dash(infra_env):
    assert nodepool_replicas.belongs_to_kind(infra_env, KIND)


@pytest.mark.parametrize("infra_env", [f"{KIND}x", f"{KIND}x-zone-a", "cisco-m6", "", None, f"x-{KIND}"])
def test_anything_else_is_another_kind(infra_env):
    assert not nodepool_replicas.belongs_to_kind(infra_env, KIND)


def test_a_shorter_kind_also_takes_the_longer_ones_that_start_with_it():
    """Nothing after the dash is interpreted — the result lists what was selected, for this reason."""
    assert nodepool_replicas.belongs_to_kind(f"{KIND}-zone-a", "cisco-m6-bat-yam")


@pytest.mark.parametrize(
    ("replicas", "ok"),
    [([], True), ([3], True), ([3, 3, 3], True), ([3, 2, 3], True), ([0, 1], True), ([3, 1], False), ([5, 2, 2], False)],
)
def test_balanced_is_at_most_one_replica_apart(replicas, ok):
    assert nodepool_replicas.balanced(replicas) is ok


def _pools(*counts: int) -> list[tuple[str, str, int]]:
    return [(f"np-{zone}", f"{KIND}-zone-{zone}", count) for zone, count in zip("abc", counts)]


@pytest.mark.parametrize(
    ("counts", "target", "expected"),
    [
        # Up: the fewest first, ties ascending (zone-a before zone-b).
        ((3, 3, 2), 10, (4, 3, 3)),
        ((3, 3, 3), 4 * 3, (4, 4, 4)),
        ((2, 2, 2), 7, (3, 2, 2)),
        ((0, 0, 0), 2, (1, 1, 0)),
        ((0, 0, 0), 4, (2, 1, 1)),
        # Down: the most first, ties descending — so zone-a keeps the extra.
        ((4, 3, 3), 9, (3, 3, 3)),
        ((3, 3, 3), 7, (3, 2, 2)),
        ((1, 1, 1), 2, (1, 1, 0)),
        ((2, 2, 1), 0, (0, 0, 0)),
        # No change.
        ((3, 3, 2), 8, (3, 3, 2)),
    ],
)
def test_the_spread_keeps_a_balanced_start_balanced(counts, target, expected):
    planned = nodepool_replicas.plan(_pools(*counts), target)
    assert tuple(planned[f"np-{zone}"] for zone in "abc") == expected
    assert sum(planned.values()) == target
    assert nodepool_replicas.balanced(list(planned.values()))


def test_going_up_no_nodepool_loses_a_replica_and_down_none_gains_one():
    up = nodepool_replicas.plan(_pools(5, 2, 2), 12)
    assert all(up[f"np-{z}"] >= c for z, c in zip("abc", (5, 2, 2)))
    down = nodepool_replicas.plan(_pools(2, 2, 5), 6)
    assert all(down[f"np-{z}"] <= c for z, c in zip("abc", (2, 2, 5)))


def test_ties_go_by_infraenv_then_nodepool_name_not_by_list_order():
    pools = [("np-z", f"{KIND}-zone-b", 1), ("np-y", f"{KIND}-zone-a", 1)]
    assert nodepool_replicas.plan(pools, 3) == {"np-y": 2, "np-z": 1}
    assert nodepool_replicas.plan(pools, 1) == {"np-y": 1, "np-z": 0}


def test_no_nodepools_plans_nothing_and_a_negative_total_is_refused():
    assert nodepool_replicas.plan([], 3) == {}
    with pytest.raises(ValueError):
        nodepool_replicas.plan(_pools(1, 1), -1)
