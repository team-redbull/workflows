"""provision-cluster's runs in flight are guarded by REPLAY: a workflow-code change that would
make a recorded history non-deterministic fails here, before it ships (CLAUDE.md §5).

A run of this workflow waits DAYS for a person's merge, continuing as new
every _POLLS_PER_RUN polls, so there are always runs in flight. The only cure
for a non-deterministic change is `workflow.patched()`. Two sources of
histories:

  1. histories recorded HERE, from scenarios on the mock limbs of
     tests/test_provision_cluster_workflow.py, replayed at once;
  2. GOLDEN histories in tests/histories/provision_cluster/, recorded by
     earlier versions of the code and replayed on the current one — the runs
     a deploy would find in flight. The `continued` golden is the SECOND run
     of a continued chain: the history a restored state replays.

A golden is written only with UPDATE_REPLAY_HISTORIES=1 and only when its
file does not exist: never re-recorded to make a change pass.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.consts import PROVISION_CLUSTER_WORKFLOW_QUEUE
from shared.exceptions import ClusterValuesRejectedError, GitProviderAuthError
from shared.models.cluster_lifecycle import ProvisionClusterRunArgs
from tests.test_provision_cluster_workflow import INPUT, _error, _Harness, _target, make_mocks, make_segment_mocks
from workflow_domains.cluster_lifecycle.provision_cluster import _POLLS_PER_RUN, ProvisionClusterWorkflow
from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "provision_cluster"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


@dataclass
class Scenario:
    """One run: the mock limbs' behaviour, and how it must end."""

    cluster: dict[str, Any] = field(default_factory=dict)
    segment: dict[str, Any] = field(default_factory=dict)
    # The ApplicationError type the run fails with; None = it completes.
    fails_with: str | None = None


SCENARIOS: dict[str, Callable[[], Scenario]] = {
    # Inspect, the child, publish, the merge request, merged at once.
    "provision": lambda: Scenario(),
    # Three timers before the merge, then day2 in two sigs.
    "merge-wait-day2": lambda: Scenario(
        cluster={"states": ["open", "open", "open", "merged"], "target": _target(day2_env="prep", day2_sigs=["redbull", "data"]),
                 "register_results": {"redbull": True, "data": False}},
    ),
    # The file refused at inspection: nothing else ran.
    "rejected": lambda: Scenario(
        cluster={"inspect_error": ClusterValuesRejectedError("`mastertag` is required")},
        fails_with=ClusterValuesRejectedError.__name__,
    ),
    # The child failed (unknown site): the parent fails with its type.
    "child-failed": lambda: Scenario(segment={"valid_sites": ("elsewhere",)}, fails_with="AllocateSegmentFailedError"),
    # Closed without merging.
    "closed": lambda: Scenario(cluster={"states": ["open", "closed"]}, fails_with="MergeRequestClosedError"),
    # The provider refused the token at the merge request.
    "provider-auth": lambda: Scenario(cluster={"ensure_error": GitProviderAuthError("401")}, fails_with=GitProviderAuthError.__name__),
    # A re-run: nothing changed, everything found.
    "rerun": lambda: Scenario(cluster={"changed": False, "mr_created": False}),
    # Enough polls to continue as new: the recorded history is the CONTINUED run's.
    "continued": lambda: Scenario(cluster={"states": ["open"] * (_POLLS_PER_RUN + 2) + ["merged"]}),
}

_SCRUBBED = {"identity": "provision-cluster-replay", "stackTrace": ""}


def _scrubbed(node: Any) -> Any:
    """The history with every worker `identity` and failure `stackTrace` neutralised."""
    if isinstance(node, dict):
        return {key: (_SCRUBBED[key] if key in _SCRUBBED else _scrubbed(value)) for key, value in node.items()}
    if isinstance(node, list):
        return [_scrubbed(item) for item in node]
    return node


def _replayer() -> Replayer:
    # Both classes: a provision-cluster history carries the child start by TYPE.
    return Replayer(workflows=[ProvisionClusterWorkflow, AllocateSegmentWorkflow], data_converter=pydantic_data_converter)


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, cluster_acts = make_mocks(**scenario.cluster)
    _, segment_acts = make_segment_mocks(**scenario.segment)
    async with _Harness(cluster_acts, segment_acts) as client:
        handle = await client.start_workflow(
            ProvisionClusterWorkflow.run, ProvisionClusterRunArgs(input=INPUT), id=f"provision-cluster-replay-{name}",
            task_queue=PROVISION_CLUSTER_WORKFLOW_QUEUE,
        )
        if scenario.fails_with is None:
            await asyncio.wait_for(handle.result(), timeout=120)
        else:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await asyncio.wait_for(handle.result(), timeout=120)
            assert _error(excinfo.value).type == scenario.fails_with
        return await handle.fetch_history()


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_scenario_records_replays(scenario):
    history = await _record(scenario)
    await _replayer().replay_workflow(history)
    golden = GOLDEN / f"{scenario}.json"
    if _RECORD and not golden.exists():
        GOLDEN.mkdir(parents=True, exist_ok=True)
        golden.write_text(
            json.dumps(
                {"workflow_id": history.workflow_id, "history": _scrubbed(json.loads(history.to_json()))},
                separators=(",", ":"), sort_keys=True,
            )
            + "\n"
        )


def test_there_are_golden_histories():
    """No committed history protects nothing in flight."""
    assert sorted(GOLDEN.glob("*.json"))


@pytest.mark.parametrize("path", sorted(GOLDEN.glob("*.json")), ids=lambda p: p.stem)
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
