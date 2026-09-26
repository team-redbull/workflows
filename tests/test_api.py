"""Trigger-API tests: real routes, a stand-in Temporal client.

The API is now the single entry point for a segment's whole lifecycle, so what
matters here is the fan-out and reporting contract — which segments got a
workflow, and under which deterministic id. The workflow itself is covered by
test_workflow.py; nothing here starts a real one.

The router is mounted on a bare app rather than workflow_domains.api:app so the test
never runs that module's lifespan, which would try to connect to Temporal.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import (
    ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
    INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
)
from shared.models.segment_lifecycle import AllocateSegmentRunArgs, SegmentType
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.segment_lifecycle import router as router_module


def _definition(segment: str, vlan_id: int) -> dict:
    return {
        "segment": segment,
        "site": "site-a",
        "vlan_id": vlan_id,
        "epg_name": "EPG_TEST_01",
    }


class _FakeHandle:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id
        self.result_run_id = "run-1"


class _FakeClient:
    """Records start_workflow calls; raises for ids in `already_started`."""

    def __init__(
        self,
        already_started: set[str] = frozenset(),
        fail: bool = False,
        expected_queue: str = INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
    ) -> None:
        self.started: list[str] = []
        self.args: list = []
        self._already_started = set(already_started)
        self._fail = fail
        self._expected_queue = expected_queue

    async def start_workflow(self, _run, args, *, id: str, task_queue: str):
        assert task_queue == self._expected_queue
        if self._fail:
            raise RuntimeError("temporal unavailable")
        if id in self._already_started:
            raise WorkflowAlreadyStartedError(id, "InitializeSegmentWorkflow")
        # A second start of the same id within one request must also conflict,
        # exactly as the server would.
        self._already_started.add(id)
        self.started.append(id)
        self.args.append(args)
        return _FakeHandle(id)


@pytest.fixture
def make_client():
    def _make(fake: _FakeClient) -> TestClient:
        app = FastAPI()
        app.include_router(router_module.router)
        app.dependency_overrides[get_temporal_client] = lambda: fake
        return TestClient(app)

    return _make


def test_start_uses_the_deterministic_workflow_id(make_client):
    fake = _FakeClient()
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment",
        json=_definition("130.154.20.0/24", 100),
    )

    assert response.status_code == 202
    # CIDR mask dropped: the same network requested with a different mask dedups.
    assert response.json()["workflow_id"] == "initialize-segment-130.154.20.0"
    assert fake.started == ["initialize-segment-130.154.20.0"]


def test_start_rejects_a_type_in_the_definition(make_client):
    """A segment is born typeless — allocate-segment stamps the type on. The
    Segments Manager would reject a type on create (extra="forbid"), so the
    route refuses it up front rather than start a run bound to fail."""
    response = make_client(_FakeClient()).post(
        "/workflows/segment-lifecycle/initialize-segment",
        json={**_definition("10.0.0.0/24", 100), "type": "HC"},
    )
    assert response.status_code == 422


def test_start_rejects_an_incomplete_definition(make_client):
    """The definition must be complete here — the workflow creates the segment,
    so a missing vlan_id can no longer be filled in by the Segments Manager."""
    response = make_client(_FakeClient()).post(
        "/workflows/segment-lifecycle/initialize-segment",
        json={"segment": "10.0.0.0/24", "site": "site-a"},
    )
    assert response.status_code == 422


def test_start_conflicts_when_already_running(make_client):
    fake = _FakeClient(already_started={"initialize-segment-10.0.0.0"})
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment",
        json=_definition("10.0.0.0/24", 100),
    )
    assert response.status_code == 409


def test_routes_are_scoped_to_the_workflow_not_the_domain():
    """The domain prefix must stay free for the domain's OTHER workflows: no
    route may sit on the bare `/workflows/<domain>`, and none may claim a
    `{workflow_id}`-style catch-all under it (that swallows every sibling's
    path). Status lives on the shared /workflows/runs router instead."""
    paths = {route.path for route in router_module.router.routes}

    assert paths == {
        "/workflows/segment-lifecycle/initialize-segment",
        "/workflows/segment-lifecycle/initialize-segment/bulk",
        "/workflows/segment-lifecycle/allocate-segment",
    }


def test_bulk_starts_one_workflow_per_segment(make_client):
    fake = _FakeClient()
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment/bulk",
        json={
            "segments": [
                _definition("10.0.0.0/24", 100),
                _definition("10.1.0.0/24", 101),
            ]
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert (body["started"], body["already_running"], body["failed"]) == (2, 0, 0)
    assert sorted(fake.started) == [
        "initialize-segment-10.0.0.0",
        "initialize-segment-10.1.0.0",
    ]


def test_bulk_reports_per_segment_instead_of_failing_the_batch(make_client):
    """One already-running segment must not stop the others — the whole point
    of reporting per item rather than with a single status code."""
    fake = _FakeClient(already_started={"initialize-segment-10.0.0.0"})
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment/bulk",
        json={
            "segments": [
                _definition("10.0.0.0/24", 100),
                _definition("10.1.0.0/24", 101),
            ]
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert (body["started"], body["already_running"], body["failed"]) == (1, 1, 0)
    outcomes = {item["segment"]: item["status"] for item in body["results"]}
    assert outcomes == {"10.0.0.0/24": "already_running", "10.1.0.0/24": "started"}
    assert fake.started == ["initialize-segment-10.1.0.0"]


def test_bulk_duplicate_cidrs_collapse_onto_one_workflow(make_client):
    fake = _FakeClient()
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment/bulk",
        json={"segments": [_definition("10.0.0.0/24", 100)] * 2},
    )

    body = response.json()
    assert (body["started"], body["already_running"]) == (1, 1)
    assert fake.started == ["initialize-segment-10.0.0.0"]


def test_bulk_start_failure_is_reported_not_raised(make_client):
    fake = _FakeClient(fail=True)
    response = make_client(fake).post(
        "/workflows/segment-lifecycle/initialize-segment/bulk",
        json={"segments": [_definition("10.0.0.0/24", 100)]},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["failed"] == 1
    assert "temporal unavailable" in body["results"][0]["error"]


def test_bulk_rejects_an_empty_batch(make_client):
    response = make_client(_FakeClient()).post(
        "/workflows/segment-lifecycle/initialize-segment/bulk", json={"segments": []}
    )
    assert response.status_code == 422


# --- allocate-segment ------------------------------------------------------

_ALLOCATE = "/workflows/segment-lifecycle/allocate-segment"
_ALLOCATE_BODY = {"cluster": "ocp4-prep-a", "values_branch": "feature/ocp4-prep-a"}


def test_allocate_start_uses_the_deterministic_id_and_carries_the_branch(make_client):
    fake = _FakeClient(expected_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE)
    response = make_client(fake).post(_ALLOCATE, json=_ALLOCATE_BODY)

    assert response.status_code == 202
    assert response.json()["workflow_id"] == "allocate-segment-HC-ocp4-prep-a"
    (run_args,) = fake.args
    assert isinstance(run_args, AllocateSegmentRunArgs)
    assert run_args.input.type == SegmentType.HC
    assert run_args.input.values_branch == "feature/ocp4-prep-a"
    # The branch survives the trip through the boundary model — what the
    # data converter serializes is AllocateSegmentRunArgs, not the request.
    decoded = AllocateSegmentRunArgs.model_validate_json(run_args.model_dump_json())
    assert decoded.input.values_branch == "feature/ocp4-prep-a"


def test_allocate_requires_the_values_branch(make_client):
    """No configured fallback exists: a request without a branch would have
    nowhere to record the allocation, so it never starts a run."""
    fake = _FakeClient(expected_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE)
    response = make_client(fake).post(_ALLOCATE, json={"cluster": "ocp4-prep-a"})
    assert response.status_code == 422
    assert fake.started == []


@pytest.mark.parametrize(
    "branch", ["", "-x", "--upload-pack=touch /tmp/x", "a..b", "has space", "a~1", "x:y"]
)
def test_allocate_rejects_a_branch_git_could_misread(make_client, branch):
    """The branch reaches git as an argument from an API with no auth: an
    option-shaped or malformed value is refused before any run starts."""
    fake = _FakeClient(expected_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE)
    response = make_client(fake).post(
        _ALLOCATE, json={**_ALLOCATE_BODY, "values_branch": branch}
    )
    assert response.status_code == 422
    assert fake.started == []


def test_allocate_rejects_unknown_fields(make_client):
    """A misspelt or retired field (a `site`, a `branch`) is refused rather
    than silently ignored."""
    response = make_client(
        _FakeClient(expected_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE)
    ).post(_ALLOCATE, json={**_ALLOCATE_BODY, "site": "site1"})
    assert response.status_code == 422


def test_allocate_dedups_across_branches(make_client):
    """The id carries cluster and type, never the branch: the Segments
    Manager allocates per (cluster, site, type), so two branches for one
    cluster must collide on one run rather than race for one segment."""
    fake = _FakeClient(expected_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE)
    client = make_client(fake)
    assert client.post(_ALLOCATE, json=_ALLOCATE_BODY).status_code == 202
    response = client.post(
        _ALLOCATE, json={**_ALLOCATE_BODY, "values_branch": "feature/other"}
    )
    assert response.status_code == 409
    assert fake.started == ["allocate-segment-HC-ocp4-prep-a"]
