"""update-nodepool's rules: which NodePools are a kind's, and where each one's replicas go — pure.

Sandbox-safe like bond_selection.py: no I/O, no clock, nothing iterated in an
order that is not fixed, because the workflow runs it on every replay.

Only runs recorded with an `infra_env_kind` use it: the API takes one
NodePool by name since 2026-11-18 (the operator: "no need infraenv kind").
Delete it with the workflow's by-kind branch once
`WorkflowType='UpdateNodePoolWorkflow' AND ExecutionStatus='Running'` lists
no run started before that day — and its goldens with it.

  * belongs_to_kind — a NodePool is of InfraEnv kind K when the InfraEnv its
    agentLabelSelector names is `K`, or `K-` followed by anything
    (`K-zone-a`, `K-b2`). Nothing after the dash is interpreted, so a shorter
    kind also takes the longer kinds that start with it; the run's result
    lists every NodePool selected.
  * spread / balanced — the rule a scale keeps: no two NodePools of a kind
    more than ONE replica apart (decided with the operator, 2026-11-03). A
    start that already breaks it is refused, never repaired.
  * plan — the new replicas for a new total, one replica at a time: up to the
    NodePool with the fewest, down from the one with the most. Ties go by
    InfraEnv name, then NodePool name — ascending going up, descending going
    down — so from a balanced start every step stays balanced and the first
    NodePool in that order keeps the extra one.
"""

from __future__ import annotations


def belongs_to_kind(infra_env: str | None, kind: str) -> bool:
    return bool(infra_env) and (infra_env == kind or str(infra_env).startswith(f"{kind}-"))


def spread(replicas: list[int]) -> int:
    """How far apart the fullest and the emptiest NodePool are (0 for none or one)."""
    return max(replicas) - min(replicas) if replicas else 0


def balanced(replicas: list[int]) -> bool:
    return spread(replicas) <= 1


def plan(nodepools: list[tuple[str, str, int]], target: int) -> dict[str, int]:
    """Each NodePool's replicas for `target` in all: `nodepools` is (name, infra_env, replicas) each.

    Moves only in the direction of the change: going up no NodePool loses a
    replica, going down none gains one. `target` below 0 is the caller's
    error (the API refuses it).
    """
    if target < 0:
        raise ValueError(f"a total of {target} replicas")
    order = {name: index for index, (name, _infra_env, _replicas) in enumerate(
        sorted(nodepools, key=lambda np: (np[1], np[0]))
    )}
    counts = {name: replicas for name, _infra_env, replicas in nodepools}
    if not counts:
        return {}
    total = sum(counts.values())
    while total < target:
        name = min(counts, key=lambda n: (counts[n], order[n]))
        counts[name] += 1
        total += 1
    while total > target:
        name = max(counts, key=lambda n: (counts[n], order[n]))
        counts[name] -= 1
        total -= 1
    return counts
