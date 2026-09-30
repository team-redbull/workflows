"""provision-dell-server trigger routes: real router, a stand-in Temporal client.

What matters is the edge contract: the region is resolved from the iDRAC prefix
before a run exists, the dedup id is the normalized IP, and a bulk request
reports every machine on its own.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import PROVISION_DELL_SERVER_WORKFLOW_QUEUE
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.server_provisioning import router as router_module

PATH = "/workflows/server-provisioning/provision-dell-server"


class _FakeHandle:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id
        self.result_run_id = "run-1"


class _FakeClient:
    def __init__(self, already_started: set[str] = frozenset(), fail: bool = False) -> None:
        self.started: list[str] = []
        self.args: list = []
        self._already_started = set(already_started)
        self._fail = fail

    async def start_workflow(self, _run, args, *, id: str, task_queue: str):
        assert task_queue == PROVISION_DELL_SERVER_WORKFLOW_QUEUE
        if self._fail:
            raise RuntimeError("temporal unavailable")
        if id in self._already_started:
            raise WorkflowAlreadyStartedError(id, "ProvisionDellServerWorkflow")
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


def test_single_resolves_the_region_from_the_prefix(make_client):
    fake = _FakeClient()
    resp = make_client(fake).post(PATH, json={"idrac_ip": "134.1.2.1"})
    assert resp.status_code == 202
    assert resp.json()["workflow_id"] == "provision-dell-server-134.1.2.1"
    assert fake.args[0].input.region == "israel"


def test_an_explicit_region_wins(make_client):
    fake = _FakeClient()
    make_client(fake).post(PATH, json={"idrac_ip": "192.0.2.10", "region": "tlv"})
    assert fake.args[0].input.region == "tlv"


def test_an_unknown_prefix_is_422_and_starts_nothing(make_client):
    fake = _FakeClient()
    resp = make_client(fake).post(PATH, json={"idrac_ip": "192.0.2.10"})
    assert resp.status_code == 422
    assert fake.started == []


@pytest.mark.parametrize("body", [{"idrac_ip": "not-an-ip"}, {"idrac_ip": "1.1.1.1", "vlan": 5}])
def test_a_malformed_body_is_422(make_client, body):
    assert make_client(_FakeClient()).post(PATH, json=body).status_code == 422


def test_a_run_already_in_flight_is_409(make_client):
    fake = _FakeClient(already_started={"provision-dell-server-1.1.1.1"})
    assert make_client(fake).post(PATH, json={"idrac_ip": "1.1.1.1"}).status_code == 409


def test_bulk_reports_every_machine_on_its_own(make_client):
    fake = _FakeClient(already_started={"provision-dell-server-2.2.2.2"})
    resp = make_client(fake).post(
        f"{PATH}/bulk",
        json={
            "servers": [
                {"idrac_ip": "1.1.1.1"},
                {"idrac_ip": "2.2.2.2"},
                {"idrac_ip": "192.0.2.10"},
                {"idrac_ip": "1.1.1.1"},
            ]
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert [r["status"] for r in body["results"]] == [
        "started",
        "already_running",
        "rejected",
        "already_running",
    ]
    assert (body["started"], body["already_running"], body["rejected"], body["failed"]) == (1, 2, 1, 0)
    assert body["results"][0]["region"] == "region1"


def test_bulk_survives_temporal_failing(make_client):
    resp = make_client(_FakeClient(fail=True)).post(
        f"{PATH}/bulk", json={"servers": [{"idrac_ip": "1.1.1.1"}]}
    )
    assert resp.json()["results"][0]["status"] == "failed"
