"""The temporary cluster-upgrade page (workflow_domains/cluster_upgrade/ui.py) and its demo data.

demo-runs.json is what the real workflow returned in six scenarios on the fake
fleet: the run list, and each run's status as GET /workflows/runs/{id} shapes
it. This test checks it still decodes as today's models, so the page cannot
drift from the API. `UPDATE_UI_DEMO=1` regenerates it.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.consts import UPGRADE_TARGETS_WORKFLOW_QUEUE
from shared.models.cluster_upgrade import (
    ClusterUpgradeSettings,
    ContinueNodePool,
    HumanDecision,
    NodePoolMode,
    ProposedAction,
    UpgradeClusterProgress,
    UpgradeClusterResult,
    UpgradePhase,
    UpgradeTargets,
    UpgradeTargetsInput,
    UpgradeTargetsRunArgs,
    WatchResult,
)
from tests.cluster_upgrade_fakes import CLUSTER, MCE, WORKFLOW_ID, Fleet, Harness, request, verdict
from tests.test_upgrade_cluster_workflow import ALICE, DAY2, KYVERNO, Run, _approve_through_control_plane
from workflow_domains.cluster_upgrade import ui
from workflow_domains.cluster_upgrade.upgrade_targets import UpgradeTargetsWorkflow
from workflow_domains.routers import runs as runs_router

DEMO = ui.UI_DIR / "demo-runs.json"
DEMO_MCE = "ocp4-demo-mce-site1-a"
PRODUCTION = ClusterUpgradeSettings(wave=2)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(ui.router)
    return TestClient(app)


def test_the_page_is_served_whole_with_nothing_from_outside(client):
    response = client.get("/ui/cluster-upgrade")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")
    page = response.text
    assert "<title>Cluster upgrades</title>" in page
    # Air-gapped: no script, stylesheet or font from another host.
    assert "http://" not in page and "https://" not in page
    # It calls the API it is served by, on the routes the routers really have.
    for path in ("/workflows/runs", "/workflows/cluster-upgrade/upgrade-cluster", "/workflows/cluster-upgrade/upgrade-targets",
                 "/ui/cluster-upgrade/demo-runs.json"):
        assert path in page


def test_the_demo_data_is_served_and_decodes_as_the_api_returns_it(client):
    response = client.get("/ui/cluster-upgrade/demo-runs.json")
    assert response.status_code == 200
    demo = response.json()
    assert [run["workflow_id"] for run in demo["list"]] == list(demo["runs"])
    phases = set()
    for status in demo["runs"].values():
        assert status["workflow_type"] == "UpgradeClusterWorkflow"
        if status["status"] == "RUNNING":
            phases.add(UpgradeClusterProgress.model_validate(status["progress"]).phase)
        else:
            assert UpgradeClusterResult.model_validate(status["result"]).outcome.value == "done"
    assert phases == {UpgradePhase.AWAITING_APPROVAL, UpgradePhase.CONTROL_PLANE_STEP, UpgradePhase.AWAITING_CONTINUE,
                      UpgradePhase.NEEDS_HUMAN, UpgradePhase.AWAITING_ADMIN_ACK}
    # The "New upgrade" form's choices, as GET /upgrade-targets returns them (D22).
    targets = {cluster: UpgradeTargets.model_validate(t) for cluster, t in demo["targets"].items()}
    assert targets["demo-media-site2"].supported_minors == ["4.20", "4.19", "4.18"]
    assert [t.version for t in targets["demo-media-site2"].targets] == ["4.20.44"]
    # Demo names only: nothing of the fakes' production-looking names is left.
    text = DEMO.read_text()
    assert CLUSTER not in text and MCE not in text


class _Execution:
    def __init__(self, workflow_id: str, status: str, started: datetime) -> None:
        from temporalio.client import WorkflowExecutionStatus

        self.id, self.workflow_type = workflow_id, "UpgradeClusterWorkflow"
        self.status = WorkflowExecutionStatus[status]
        self.start_time, self.close_time = started, None


class _ListingClient:
    def __init__(self, executions: list[_Execution]) -> None:
        self.executions, self.queries = executions, []

    def list_workflows(self, query: str, *, limit: int):
        self.queries.append((query, limit))

        async def _iterate():
            for execution in self.executions[:limit]:
                yield execution

        return _iterate()


def test_the_run_list_is_one_workflow_type_newest_first():
    fake = _ListingClient([
        _Execution("upgrade-cluster-a", "COMPLETED", datetime(2026, 10, 30, tzinfo=UTC)),
        _Execution("upgrade-cluster-b", "RUNNING", datetime(2026, 10, 31, tzinfo=UTC)),
    ])
    app = FastAPI()
    app.include_router(runs_router.router)
    app.dependency_overrides[runs_router.get_temporal_client] = lambda: fake
    client = TestClient(app)

    response = client.get("/workflows/runs", params={"workflow_type": "UpgradeClusterWorkflow", "limit": 20})
    assert response.status_code == 200
    assert [(run["workflow_id"], run["status"]) for run in response.json()] == [
        ("upgrade-cluster-b", "RUNNING"), ("upgrade-cluster-a", "COMPLETED"),
    ]
    assert fake.queries == [("WorkflowType = 'UpgradeClusterWorkflow'", 20)]
    # The type goes into the visibility query, so it must be a class name and nothing else.
    bad = client.get("/workflows/runs", params={"workflow_type": "X' OR WorkflowType != '"})
    assert bad.status_code == 422 and len(fake.queries) == 1


# --- regenerating demo-runs.json (UPDATE_UI_DEMO=1) -------------------------------------


async def _awaiting_approval(h: Harness) -> None:
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)


async def _mid_control_plane(h: Harness) -> None:
    h.fleet.block_control_plane_watch = True
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.CONTROL_PLANE_STEP)


async def _customer_turn(h: Harness) -> None:
    await h.start(request())
    await _approve_through_control_plane(h)
    await h.until(UpgradePhase.AWAITING_CONTINUE)
    await h.decide(Run.continue_nodepool, ContinueNodePool(by="the customer"))
    await h.until(UpgradePhase.AWAITING_CONTINUE)


async def _agent_proposal(h: Harness) -> None:
    h.fleet.np_watch_script = [WatchResult.STALLED]
    h.fleet.verdicts = [verdict(
        "needs_human",
        diagnosis=("The drain of worker-2 has waited 12 min on PDB openshift-console/downloads "
                   "(disruptionsAllowed 0): both its pods are in CrashLoopBackOff on the starved node, "
                   "so the eviction API refuses them and the drain would wait for ever."),
        actions_proposed=[ProposedAction(
            action_id="a-1", kind="delete_pod", target="openshift-console/downloads-7d9f8-x2k4p",
            args={"uid": "8c1e0a52"}, rationale="its controller recreates it elsewhere and the drain goes on",
        )],
    )]
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.AWAITING_CONTINUE)
    await h.decide(Run.continue_nodepool, ContinueNodePool(by="the customer"))
    await h.until(UpgradePhase.NEEDS_HUMAN)


async def _admin_ack_caller(h: Harness) -> None:
    h.fleet.api_usage = [KYVERNO]
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
    await h.decide(Run.confirm_day2, DAY2)
    await h.until(UpgradePhase.AWAITING_ADMIN_ACK)


async def _done_in_the_lab(h: Harness) -> None:
    await h.start(request(nodepool_mode=NodePoolMode.AUTO, auto_approve=True))
    for _ in range(2):
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    await h.result()


# (demo cluster, fleet, drive)
_SCENARIOS = [
    ("demo-shop-site1", lambda: Fleet(upgrade=PRODUCTION), _awaiting_approval),
    ("demo-payments-site1", lambda: Fleet(upgrade=PRODUCTION), _mid_control_plane),
    ("demo-search-site1", lambda: Fleet(upgrade=PRODUCTION,
                                        nodepools={"zone-a": "4.18.56", "zone-b": "4.18.56", "zone-c": "4.18.56"},
                                        zones={"zone-a": "a", "zone-b": "b", "zone-c": "c"}), _customer_turn),
    ("demo-analytics-site1", lambda: Fleet(control_plane="4.20.44", upgrade=PRODUCTION,
                                           nodepools={"np-a": "4.19.53", "np-b": "4.19.53"}), _agent_proposal),
    ("demo-billing-site1", lambda: Fleet(upgrade=PRODUCTION), _admin_ack_caller),
    ("demo-lab-a-site1", lambda: Fleet(upgrade=ClusterUpgradeSettings(wave=0)), _done_in_the_lab),
]


def _media_fleet() -> Fleet:
    """A cluster with no run yet, on an MCE that supports up to 4.20 only."""
    fleet = Fleet(control_plane="4.19.53", upgrade=PRODUCTION)
    fleet.supported = ["4.20", "4.19", "4.18"]
    return fleet


# Clusters the demo's "New upgrade" form knows: every run's, plus ones with no run.
_TARGET_ONLY = [("demo-media-site2", _media_fleet), ("demo-maps-site2", lambda: Fleet(control_plane="4.20.44"))]


async def _targets(h: Harness) -> dict[str, Any]:
    targets = await h.client.execute_workflow(
        UpgradeTargetsWorkflow.run, UpgradeTargetsRunArgs(input=UpgradeTargetsInput(cluster=CLUSTER)),
        id=f"upgrade-targets-{CLUSTER}", task_queue=UPGRADE_TARGETS_WORKFLOW_QUEUE,
    )
    return targets.model_dump(mode="json")


def _renamed(value: Any, cluster: str) -> Any:
    text = json.dumps(value)
    for old, new in ((WORKFLOW_ID, f"upgrade-cluster-{cluster}"), (CLUSTER, cluster), (MCE, DEMO_MCE)):
        text = text.replace(old, new)
    return json.loads(text)


async def _record_demo() -> dict[str, Any]:
    listing, statuses, targets = [], {}, {}
    for cluster, make_fleet in _TARGET_ONLY:
        async with Harness(make_fleet()) as h:
            targets[cluster] = _renamed(await _targets(h), cluster)
    for cluster, make_fleet, drive in _SCENARIOS:
        async with Harness(make_fleet()) as h:
            targets[cluster] = _renamed(await _targets(h), cluster)
            await drive(h)
            description = await h.run().describe()
            status = description.status.name if description.status else "UNKNOWN"
            run: dict[str, Any] = {"workflow_id": WORKFLOW_ID, "workflow_type": "UpgradeClusterWorkflow",
                                   "status": status}
            if status == "RUNNING":
                run["progress"] = (await h.progress()).model_dump(mode="json")
            else:
                run["result"] = (await h.result()).model_dump(mode="json")
            listing.append(_renamed({
                "workflow_id": WORKFLOW_ID, "workflow_type": "UpgradeClusterWorkflow", "status": status,
                "start_time": description.start_time.isoformat() if description.start_time else None,
                "close_time": description.close_time.isoformat() if description.close_time else None,
            }, cluster))
            statuses[f"upgrade-cluster-{cluster}"] = _renamed(run, cluster)
    return {"generated_by": "UPDATE_UI_DEMO=1 pytest tests/test_cluster_upgrade_ui.py", "list": listing,
            "runs": statuses, "targets": targets}


@pytest.mark.skipif(not os.environ.get("UPDATE_UI_DEMO"), reason="set UPDATE_UI_DEMO=1 to regenerate demo-runs.json")
async def test_regenerate_the_demo_data():
    DEMO.write_text(json.dumps(await _record_demo(), indent=1) + "\n")
