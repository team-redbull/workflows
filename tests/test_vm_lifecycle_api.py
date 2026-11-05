"""create-vm trigger: real route, a stand-in Temporal client (tests/test_server_lifecycle_api.py's shape)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import CREATE_VM_WORKFLOW_QUEUE
from shared.models.vm_lifecycle import CreateVmInput
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.vm_lifecycle import router as router_module

PATH = "/workflows/vm-lifecycle/create-vm"
BODY = {"mce_cluster": "ocp4-prod-mce-bat-yam-a", "infra_env": "x201-zone-a-8"}
SLOT = "create-vm-ocp4-prod-mce-bat-yam-a_x201-zone-a-8_{}"


class _FakeHandle:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id
        self.result_run_id = "run-1"


class _FakeClient:
    def __init__(self) -> None:
        self.args: list = []
        self.ids: list[str] = []
        self.running: set[str] = set()
        self.fail_on: str | None = None

    async def start_workflow(self, _run, args, *, id: str, task_queue: str):
        assert task_queue == CREATE_VM_WORKFLOW_QUEUE
        if id == self.fail_on:
            raise RuntimeError("Temporal unavailable")
        if id in self.running:
            raise WorkflowAlreadyStartedError(id, "CreateVmWorkflow")
        self.running.add(id)
        self.args.append(args)
        self.ids.append(id)
        return _FakeHandle(id)


@pytest.fixture
def fake() -> _FakeClient:
    return _FakeClient()


@pytest.fixture
def client(fake: _FakeClient) -> TestClient:
    app = FastAPI()
    app.include_router(router_module.router)
    app.dependency_overrides[get_temporal_client] = lambda: fake
    return TestClient(app)


def test_one_request_is_one_run_in_the_first_free_slot(client, fake):
    response = client.post(PATH, json=BODY)
    assert response.status_code == 202
    assert response.json() == {"workflow_id": SLOT.format(1), "run_id": "run-1"}


def test_the_run_carries_what_the_edge_derived(client, fake):
    client.post(PATH, json=BODY)
    assert fake.args[0].input == CreateVmInput(
        mce_cluster="ocp4-prod-mce-bat-yam-a", infra_env="x201-zone-a-8", namespace="inventory",
        site="bat-yam", zone="a", cpu=8, memory_gib=64,
    )


def test_an_explicit_namespace_is_kept(client, fake):
    assert client.post(PATH, json={**BODY, "namespace": "redfish-vms"}).status_code == 202
    assert fake.args[0].input.namespace == "redfish-vms"


@pytest.mark.parametrize("typed", ["prod-mce-bat-yam-a", "ocp4-prod-mce-bat-yam-a", "  prod-mce-bat-yam-a "])
def test_the_mce_is_canonical(client, fake, typed):
    assert client.post(PATH, json={**BODY, "mce_cluster": typed}).status_code == 202
    assert fake.args[0].input.mce_cluster == "ocp4-prod-mce-bat-yam-a"


@pytest.mark.parametrize(
    ("body", "words"),
    [
        ({**BODY, "infra_env": "x201-zone-a-metal"}, "bare metal"),
        ({**BODY, "infra_env": "cisco-m6-bat-yam-64c-512gb"}, "convention"),
        ({**BODY, "mce_cluster": "ocp4-mce-bat-yam-01"}, "ocp4-<env>-mce-<site>-<letter>"),
        ({**BODY, "namespace": ""}, "namespace"),
        ({**BODY, "count": 0}, "count"),
        ({**BODY, "count": 21}, "count"),
        ({**BODY, "vlan_id": 5}, "vlan_id"),
        ({**BODY, "site": "bat-yam"}, "site"),
        ({"infra_env": "x201-zone-a-8"}, "mce_cluster"),
        # The VM name `ocp-<infraenv>-<6 hex>` must fit a Kubernetes name; refused at the edge,
        # never as a ValueError inside the run.
        ({**BODY, "infra_env": "x" * 50 + "-zone-a-8"}, "DNS-1123"),
    ],
)
def test_anything_else_is_422_and_starts_nothing(client, fake, body, words):
    response = client.post(PATH, json=body)
    assert response.status_code == 422
    assert words in response.text
    assert fake.args == []


def test_asking_again_while_the_first_runs_adds_a_second_vm(client, fake):
    assert client.post(PATH, json=BODY).json()["workflow_id"] == SLOT.format(1)
    assert client.post(PATH, json=BODY).json()["workflow_id"] == SLOT.format(2)


def test_a_finished_slot_is_free_again(client, fake):
    client.post(PATH, json=BODY)
    fake.running.clear()
    assert client.post(PATH, json=BODY).json()["workflow_id"] == SLOT.format(1)


def test_every_slot_busy_is_409(client, fake):
    fake.running = {SLOT.format(n) for n in range(1, 101)}
    response = client.post(PATH, json=BODY)
    assert response.status_code == 409 and "_100" in response.json()["detail"]


def test_a_start_temporal_refuses_is_502_with_its_reason(client, fake):
    fake.fail_on = SLOT.format(1)
    response = client.post(PATH, json=BODY)
    assert response.status_code == 502 and "Temporal unavailable" in response.json()["detail"]


class TestCount:
    def test_count_runs_each_on_its_own_slot_with_a_report(self, client, fake):
        response = client.post(PATH, json={**BODY, "count": 3})
        assert response.status_code == 202
        report = response.json()
        assert (report["started"], report["already_running"], report["failed"]) == (3, 0, 0)
        assert [(r["slot"], r["workflow_id"], r["status"]) for r in report["results"]] == [
            (n, SLOT.format(n), "started") for n in (1, 2, 3)
        ]
        assert all(a.input == fake.args[0].input for a in fake.args)

    def test_a_busy_slot_is_stepped_over(self, client, fake):
        fake.running = {SLOT.format(2)}
        report = client.post(PATH, json={**BODY, "count": 2}).json()
        assert [(r["slot"], r["status"]) for r in report["results"]] == [(1, "started"), (2, "already_running"), (3, "started")]
        assert (report["started"], report["already_running"]) == (2, 1)

    def test_a_list_of_one_is_still_a_report(self, client, fake):
        report = client.post(PATH, json={**BODY, "count": 1}).json()
        assert report["started"] == 1 and len(report["results"]) == 1

    def test_a_refused_start_stops_the_scan_and_is_reported(self, client, fake):
        fake.fail_on = SLOT.format(2)
        response = client.post(PATH, json={**BODY, "count": 3})
        assert response.status_code == 202
        report = response.json()
        assert (report["started"], report["failed"]) == (1, 1)
        assert report["results"][-1]["status"] == "failed" and "Temporal unavailable" in report["results"][-1]["error"]


def test_every_run_field_is_sent_or_derived():
    """CreateVmRequest.run_input must fill EVERY CreateVmInput field: the ones the caller sends
    and the ones the edge derives. A field added to the input and forgotten here would reach
    the run as its default — or never validate."""
    request_fields = set(router_module.CreateVmRequest.model_fields) - {"count"}
    derived = {"site", "zone", "cpu", "memory_gib"}
    assert request_fields | derived == set(CreateVmInput.model_fields)
    produced = router_module.CreateVmRequest(**BODY).run_input()
    assert set(produced.model_fields_set) == set(CreateVmInput.model_fields)
