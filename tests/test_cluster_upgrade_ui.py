"""The temporary cluster-upgrade page (workflow_domains/cluster_upgrade/ui.py).

It shows only the runs the API reports, so these check the page against the
API and the models: what it is served as, the routes it calls, and that it has
words for every phase a run can report.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.models.cluster_upgrade import UpgradeOutcome, UpgradePhase
from workflow_domains.cluster_upgrade import ui
from workflow_domains.routers import runs as runs_router

PAGE = ui.UI_DIR / "index.html"


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
    for path in ("/workflows/runs", "/workflows/cluster-upgrade/upgrade-cluster", "/workflows/cluster-upgrade/upgrade-targets"):
        assert path in page


def test_the_page_has_words_for_every_phase_a_run_reports():
    """A running run's `progress.phase` and a finished one's `result.outcome` are looked up in PHASES."""
    page = PAGE.read_text()
    phases = re.search(r"const PHASES = \{(.*?)\n\};", page, re.S)
    decisions = re.search(r"const DECISIONS = \{(.*?)\n\};", page, re.S)
    assert phases is not None and decisions is not None
    named = set(re.findall(r'"([a-z0-9-]+)": \[', phases.group(1)))
    assert named == {phase.value for phase in UpgradePhase}
    assert {outcome.value for outcome in UpgradeOutcome} <= named
    # A decision is offered in a phase the workflow has.
    assert set(re.findall(r'"([a-z0-9-]+)": \[', decisions.group(1))) <= named


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
