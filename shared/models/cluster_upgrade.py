"""Typed state for the cluster-upgrade workflows — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model, never
an untyped dict.

None of these models carries `extra="forbid"`: they are decoded from Temporal
history on every replay — and an upgrade run lives for weeks, through deploys —
so a field removed later would wedge in-flight runs. Strictness about unknown
fields belongs on the API-edge request models in the router.

Versions are carried as bare `X.Y.Z` strings (see `shared/ocp_version.py`);
the `-x86_64` mirror tag is derived where an image reference is built, never
stored, so there is one spelling in history.

Design: docs/upgrade/DESIGN.md §5 in the Cluster-AutoUpgrade workspace.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class LadderRung(BaseModel):
    """One verified release on the upgrade ladder (day1 `upgrade/ladder.yaml`).

    A rung is a release the team has checked upgrades cleanly into the next
    minor. `must` rungs are never skipped; `data_plane_required` makes every
    NodePool roll to this release before the control plane moves past it.
    """

    version: str
    must: bool = False
    data_plane_required: bool = False


class StepKind(str, Enum):
    """What a step moves: the HostedCluster, or NodePools."""

    CONTROL_PLANE = "control-plane"
    WORKERS = "workers"


class StepReason(str, Enum):
    """Why the resolver put a step in the plan — shown to the approver."""

    LADDER = "ladder"  # an intermediate rung of the ladder
    TARGET = "target"  # the requested target release
    SKEW = "skew"  # workers must move before the control plane passes the skew limit
    RUNG_REQUIRED = "rung-required"  # the rung has dataPlane: required
    FINAL = "final"  # workers follow the control plane to the target at the end


class UpgradeStep(BaseModel):
    """One step of a plan: move the control plane, or a set of NodePools, to `version`."""

    kind: StepKind
    version: str
    reason: StepReason
    # WORKERS only: every NodePool below `version` when the step runs, in the
    # order the cluster's profile will roll them. Empty for CONTROL_PLANE.
    nodepools: list[str] = Field(default_factory=list)


class UpgradePlan(BaseModel):
    """The ordered steps from a cluster's current versions to its target.

    `pinned` maps each release in the plan to its payload digest. Mirror
    validation fills it, and every sync re-checks it (DESIGN D2), so a tag that
    moves in the mirror after approval stops the run instead of deploying
    something nobody approved.
    """

    control_plane: str
    nodepools: dict[str, str]
    target: str
    max_worker_skew: int
    steps: list[UpgradeStep]
    pinned: dict[str, str] = Field(default_factory=dict)
