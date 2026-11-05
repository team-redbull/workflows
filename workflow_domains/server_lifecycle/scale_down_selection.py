"""scale-down-nodepool's choice of the servers that leave — pure.

Sandbox-safe like bond_selection.py: the limb reports each Machine of a
NodePool with what the policies rank by (read_scale_down_candidates), and this
module decides, on every replay the same way.

The policy is the NodePool's own `redbull.io/scale-down-policy` label or
annotation, never the request's (decided with the operator, 2026-11-03):

  * absent — no policy: nothing is marked, CAPI's MachineSet picks at random;
  * `fewest-pods` — the fewest workload pods, the least for the drain to move;
  * `least-requested` — the lowest share of CPU / memory its pods request;
  * `least-utilized` — the lowest live CPU / memory use (metrics.k8s.io);
  * anything else — refused before anything is written.

THE CHOICE IS MADE SO CAPI MAKES THE SAME ONE, in every version it runs. CAPI's
MachineSet ranks Machines on a scale-down — HyperShift sets no delete policy,
so Random — and takes the top ones, ties by Machine NAME. A marked Machine
outranks every healthy one everywhere, and an unhealthy one only from CAPI 1.9
on (kubernetes-sigs/cluster-api#11431): in 4.18's CAPI marked and unhealthy
tie, so CAPI would take an unmarked unhealthy Machine whose name sorts first.
So the order is:

  1. CAPI-UNHEALTHY Machines (`capi_healthy` False: CAPI's own
     isMachineHealthy — no nodeRef, a failure, NodeHealthy or
     HealthCheckSucceeded False), by NAME, whatever the policy;
  2. a Machine already carrying THIS run's mark (a re-run keeps its choice);
  3. a Node the hosted cluster does not have, or has NotReady or cordoned,
     while CAPI still counts the Machine healthy;
  4. the policy's reading; ties by name.

With CAPI-unhealthy Machines first in CAPI's own order, the Machines CAPI
deletes are exactly the marked ones whichever priority the mark has: when
there are at least as many unhealthy Machines as removals, the marked ones
are the first unhealthy by name; when fewer, every unhealthy one is marked
too, so the marked set IS CAPI's top group.

A NodePool whose Machines span two MachineSets is mid-rollout (a Replace
NodePool's MachineDeployment), where the mark may be passed by: refused.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import (
        NodePoolReplicasDriftError,
        ScaleDownMarkConflictError,
        ScaleDownPolicyInvalidError,
        ScaleDownRolloutInProgressError,
    )
    from shared.models.server_lifecycle import ScaleDownChoice, ScaleDownMachine, ScaleDownPolicy

_POLICIES = {policy.value: policy for policy in ScaleDownPolicy}


@dataclass(frozen=True)
class Refusal:
    """Why the scale-down must not mark anything: the WORKFLOW-RAISED type, and the message."""

    error: type[Exception]
    message: str


def policy_of(value: str | None) -> ScaleDownPolicy | None:
    """The NodePool's policy; None when it names none. ValueError for a value that is not a policy."""
    if value is None or value == "":
        return None
    policy = _POLICIES.get(value.strip())
    if policy is None:
        raise ValueError(value)
    return policy


def invalid_policies(policies: dict[str, str | None]) -> Refusal | None:
    """Every NodePool whose `redbull.io/scale-down-policy` names no policy, at once."""
    bad = []
    for name, value in sorted(policies.items()):
        try:
            policy_of(value)
        except ValueError:
            bad.append(f"{name}: {value!r}")
    if not bad:
        return None
    return Refusal(
        ScaleDownPolicyInvalidError,
        "redbull.io/scale-down-policy must be one of "
        + ", ".join(sorted(_POLICIES))
        + " (or absent, for CAPI's random choice); not "
        + "; ".join(bad)
        + ". Nothing was marked or written",
    )


def refusal(
    machines: list[ScaleDownMachine],
    shrink: dict[str, tuple[int, int]],
    mark: str,
) -> Refusal | None:
    """Why these Machines cannot be chosen from, or None. `shrink` is NodePool -> (replicas now, to remove)."""
    for nodepool, (replicas, _remove) in sorted(shrink.items()):
        own = [m for m in machines if m.nodepool == nodepool]
        deleting = [m.name for m in own if m.deleting and m.delete_mark != mark]
        if deleting:
            return Refusal(
                NodePoolReplicasDriftError,
                f"NodePool {nodepool} already has Machines deleting ({', '.join(deleting)}): a scale "
                "is in flight. Nothing was marked or written; run again once it has settled",
            )
        foreign = [f"{m.name} ({m.delete_mark!r})" for m in own if m.delete_mark is not None and m.delete_mark != mark]
        if foreign:
            return Refusal(
                ScaleDownMarkConflictError,
                f"NodePool {nodepool}: {', '.join(foreign)} already carry a cluster.x-k8s.io/delete-machine "
                "mark this run did not write, and CAPI would delete those first. Nothing was marked or "
                "written; remove the mark (or let the run that wrote it finish), then run again",
            )
        if len(own) != replicas:
            return Refusal(
                NodePoolReplicasDriftError,
                f"NodePool {nodepool} has {replicas} replicas but {len(own)} Machines carry its "
                "hypershift.openshift.io/nodePool annotation. Nothing was marked or written",
            )
        machine_sets = sorted({m.machine_set for m in own if m.machine_set})
        if len(machine_sets) > 1:
            return Refusal(
                ScaleDownRolloutInProgressError,
                f"NodePool {nodepool}'s Machines belong to {len(machine_sets)} MachineSets "
                f"({', '.join(machine_sets)}): a rollout is in progress, and its MachineDeployment may "
                "scale down the MachineSet the marked servers are not in. Nothing was marked or written; "
                "run again once the rollout has finished",
            )
    return None


# The tiers, best candidate first (the module docstring says why this order).
_CAPI_UNHEALTHY, _OWN_MARK, _NODE_BROKEN, _BY_POLICY = range(4)


def _tier(machine: ScaleDownMachine, mark: str) -> tuple[int, str]:
    if not machine.capi_healthy:
        why = "no Node" if machine.node is None else "unhealthy for CAPI"
        return _CAPI_UNHEALTHY, why
    if machine.delete_mark == mark:
        return _OWN_MARK, "already marked by this run"
    if not machine.node_found:
        return _NODE_BROKEN, f"Node {machine.node} not in the hosted cluster"
    if not machine.ready:
        return _NODE_BROKEN, "NotReady"
    if machine.unschedulable:
        return _NODE_BROKEN, "cordoned"
    return _BY_POLICY, ""


def _metric(machine: ScaleDownMachine, policy: ScaleDownPolicy) -> tuple[int, str]:
    """The policy's reading of one healthy Machine: lower leaves first; unknown sorts last."""
    if policy is ScaleDownPolicy.FEWEST_PODS:
        return machine.workload_pods, f"{machine.workload_pods} workload pods"
    value = machine.requested_permille if policy is ScaleDownPolicy.LEAST_REQUESTED else machine.used_permille
    word = "requested" if policy is ScaleDownPolicy.LEAST_REQUESTED else "used"
    if value is None:
        return 10**9, f"{word} unknown"
    return value, f"{word} {value / 10:.1f}%"


def choose(
    machines: list[ScaleDownMachine],
    shrink: dict[str, tuple[int, int]],
    policies: dict[str, ScaleDownPolicy],
    mark: str,
) -> list[ScaleDownChoice]:
    """The Machines each NodePool loses, best candidate first, by its own policy."""
    chosen: list[ScaleDownChoice] = []
    for nodepool, (_replicas, remove) in sorted(shrink.items()):
        if remove <= 0:
            continue
        policy = policies[nodepool]
        ranked = []
        for machine in (m for m in machines if m.nodepool == nodepool):
            tier, why = _tier(machine, mark)
            value, reading = _metric(machine, policy)
            ranked.append(((tier, value if tier == _BY_POLICY else 0, machine.name), machine, why or reading))
        ranked.sort(key=lambda item: item[0])
        for _key, machine, reason in ranked[:remove]:
            chosen.append(
                ScaleDownChoice(
                    nodepool=nodepool, machine=machine.name, node=machine.node,
                    policy=policy.value, reason=reason,
                )
            )
    return chosen
