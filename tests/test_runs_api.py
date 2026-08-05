"""Run-status API tests: real routes, a stand-in Temporal client.

This endpoint is deliberately domain-agnostic (see workflow_domains/routers/runs.py),
so what matters here is that it reports every terminal state without knowing
which workflow it is looking at — including a workflow that exposes no
`progress` query at all.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from workflow_domains.routers import runs as router_module
from workflow_domains.routers.deps import get_temporal_client

_NOT_FOUND = RPCError("not found", RPCStatusCode.NOT_FOUND, b"")


class _FakeDescription:
    def __init__(self, status: WorkflowExecutionStatus | None) -> None:
        self.status = status
        self.workflow_type = "OpenSegmentRulesWorkflow"


class _FakeHandle:
    def __init__(
        self,
        status: WorkflowExecutionStatus | None,
        *,
        describe_error: Exception | None = None,
        progress: dict | None = None,
        query_error: Exception | None = None,
        result: object = None,
        result_error: Exception | None = None,
    ) -> None:
        self._status = status
        self._describe_error = describe_error
        self._progress = progress
        self._query_error = query_error
        self._result = result
        self._result_error = result_error

    async def describe(self) -> _FakeDescription:
        if self._describe_error is not None:
            raise self._describe_error
        return _FakeDescription(self._status)

    async def query(self, query: str, **_kwargs):
        assert query == "progress"
        if self._query_error is not None:
            raise self._query_error
        return self._progress

    async def result(self):
        if self._result_error is not None:
            raise self._result_error
        return self._result


class _FakeClient:
    def __init__(self, handle: _FakeHandle) -> None:
        self._handle = handle

    def get_workflow_handle(self, _workflow_id: str) -> _FakeHandle:
        return self._handle


@pytest.fixture
def make_client():
    def _make(handle: _FakeHandle) -> TestClient:
        app = FastAPI()
        app.include_router(router_module.router)
        app.dependency_overrides[get_temporal_client] = lambda: _FakeClient(handle)
        return TestClient(app)

    return _make


def test_running_reports_live_progress(make_client):
    handle = _FakeHandle(
        WorkflowExecutionStatus.RUNNING,
        progress={"phase": "awaiting-completion", "total_requests": 4, "pending_requests": 2},
    )
    response = make_client(handle).get("/workflows/runs/open-segment-rules-HC-10.0.0.0")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert body["workflow_type"] == "OpenSegmentRulesWorkflow"
    assert body["progress"]["pending_requests"] == 2


def test_progress_is_best_effort(make_client):
    """A down worker — or a workflow with no `progress` query, which this
    domain-agnostic route must tolerate — degrades to status-only, never 500."""
    handle = _FakeHandle(
        WorkflowExecutionStatus.RUNNING, query_error=RuntimeError("no such query")
    )
    response = make_client(handle).get("/workflows/runs/some-other-workflow-1")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert "progress" not in body  # excluded, not null


def test_completed_returns_the_result(make_client):
    handle = _FakeHandle(
        WorkflowExecutionStatus.COMPLETED,
        result={"segment": "10.0.0.0/24", "request_ids": [1, 2]},
    )
    response = make_client(handle).get("/workflows/runs/open-segment-rules-HC-10.0.0.0")

    assert response.json()["result"]["request_ids"] == [1, 2]


def test_failed_surfaces_the_root_cause(make_client):
    """The wrappers Temporal raises say nothing useful — the ApplicationError
    underneath carries the real message, so the chain is walked to the root."""
    failure = WorkflowFailureError(
        cause=ApplicationError("Segment 10.0.0.0/24 not found", type="SegmentNotFoundError")
    )
    handle = _FakeHandle(WorkflowExecutionStatus.FAILED, result_error=failure)
    response = make_client(handle).get("/workflows/runs/open-segment-rules-HC-10.0.0.0")

    body = response.json()
    assert body["status"] == "FAILED"
    assert "Segment 10.0.0.0/24 not found" in body["error"]


def test_unknown_workflow_id_is_404(make_client):
    handle = _FakeHandle(None, describe_error=_NOT_FOUND)
    response = make_client(handle).get("/workflows/runs/nope")

    assert response.status_code == 404
