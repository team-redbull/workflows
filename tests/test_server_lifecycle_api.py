"""install-server trigger: real route, a stand-in Temporal client.

The router is mounted on a bare app so workflow_domains.api's lifespan, which
would connect to Temporal, never runs.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.consts import INSTALL_SERVER_WORKFLOW_QUEUE
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.server_lifecycle import router as router_module

PATH = "/workflows/server-lifecycle/install-server"
BODY = {"infra_env": "cisco-m6-bat-yam-64c-512gb", "mce_cluster": "ocp4-mce-bat-yam-01"}


class _FakeHandle:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id
        self.result_run_id = "run-1"


class _FakeClient:
    def __init__(self) -> None:
        self.args: list = []

    async def start_workflow(self, _run, args, *, id: str, task_queue: str):
        assert task_queue == INSTALL_SERVER_WORKFLOW_QUEUE
        self.args.append(args)
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


def test_without_a_namespace_the_run_targets_inventory(client, fake):
    response = client.post(PATH, json=BODY)

    assert response.status_code == 202
    # The default is written INTO the run's input, so a later change of the
    # default never changes what a recorded run targets.
    assert fake.args[0].input.namespace == "inventory"


def test_an_explicit_namespace_is_kept(client, fake):
    response = client.post(PATH, json={**BODY, "namespace": "multicluster-engine"})

    assert response.status_code == 202
    assert fake.args[0].input.namespace == "multicluster-engine"


def test_an_empty_namespace_is_refused(client):
    assert client.post(PATH, json={**BODY, "namespace": ""}).status_code == 422


@pytest.mark.parametrize("typed", ["prod-mce-bat-yam", "ocp4-prod-mce-bat-yam"])
def test_the_mce_is_accepted_with_or_without_its_ocp4_prefix(client, fake, typed):
    response = client.post(PATH, json={**BODY, "mce_cluster": typed})

    assert response.status_code == 202
    # The run always carries the canonical name — the one the Segments
    # Manager knows and the one the MCE's worker builds its queue from.
    assert fake.args[0].input.mce_cluster == "ocp4-prod-mce-bat-yam"


def test_a_vlan_is_still_refused(client):
    # The edge model stays strict: the VLAN comes from the MCE's segment.
    assert client.post(PATH, json={**BODY, "vlan_id": 24}).status_code == 422


class TestInstallServerBulk:
    PATH = "/workflows/server-lifecycle/install-server/bulk"

    def test_count_runs_start_each_on_its_own_slot_id(self, client, fake):
        response = client.post(self.PATH, json={**BODY, "count": 3})

        assert response.status_code == 202
        body = response.json()
        assert body["started"] == 3 and body["already_running"] == 0
        assert [r["workflow_id"] for r in body["results"]] == [
            "install-server-cisco-m6-bat-yam-64c-512gb_1",
            "install-server-cisco-m6-bat-yam-64c-512gb_2",
            "install-server-cisco-m6-bat-yam-64c-512gb_3",
        ]
        # Pool draws, never a named server; inventory by default.
        assert all(a.input.server_name is None for a in fake.args)
        assert all(a.input.namespace == "inventory" for a in fake.args)

    def test_each_run_draws_spares_for_the_races_it_will_lose(self, client, fake):
        client.post(self.PATH, json={**BODY, "count": 4})
        assert {a.input.candidate_count for a in fake.args} == {6}

    def test_the_spares_are_capped_and_an_explicit_count_wins(self, client, fake):
        client.post(self.PATH, json={**BODY, "count": 20})
        assert {a.input.candidate_count for a in fake.args} == {20}
        fake.args.clear()
        client.post(self.PATH, json={**BODY, "count": 2, "candidate_count": 9})
        assert {a.input.candidate_count for a in fake.args} == {9}

    def test_a_slot_still_running_is_reported_not_restarted(self, client, fake):
        from temporalio.exceptions import WorkflowAlreadyStartedError

        running = {"install-server-cisco-m6-bat-yam-64c-512gb_2"}
        original = fake.start_workflow

        async def start_workflow(run, args, *, id: str, task_queue: str):
            if id in running:
                raise WorkflowAlreadyStartedError(id, "InstallServerWorkflow")
            return await original(run, args, id=id, task_queue=task_queue)

        fake.start_workflow = start_workflow
        body = client.post(self.PATH, json={**BODY, "count": 3}).json()

        assert (body["started"], body["already_running"]) == (2, 1)
        assert [r["status"] for r in body["results"]] == [
            "started", "already_running", "started",
        ]

    def test_the_mce_is_canonical(self, client, fake):
        client.post(self.PATH, json={**BODY, "mce_cluster": "prod-mce-bat-yam", "count": 1})
        assert fake.args[0].input.mce_cluster == "ocp4-prod-mce-bat-yam"

    @pytest.mark.parametrize(
        "extra",
        [{"count": 0}, {"count": 21}, {"count": 2, "server_name": "ocp-x"}, {"count": 2, "vlan_id": 24}, {}],
    )
    def test_invalid_requests_are_422(self, client, extra):
        assert client.post(self.PATH, json={**BODY, **extra}).status_code == 422


class TestRemoveServer:
    PATH = "/workflows/server-lifecycle/remove-server"
    BODY = {"server_name": "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001",
            "mce_cluster": "ocp4-mce-bat-yam-01"}

    @pytest.fixture
    def client(self, fake: _FakeClient) -> TestClient:
        from shared.consts import REMOVE_SERVER_WORKFLOW_QUEUE

        async def start_workflow(_run, args, *, id: str, task_queue: str):
            assert task_queue == REMOVE_SERVER_WORKFLOW_QUEUE
            fake.args.append(args)
            return _FakeHandle(id)

        fake.start_workflow = start_workflow
        app = FastAPI()
        app.include_router(router_module.router)
        app.dependency_overrides[get_temporal_client] = lambda: fake
        return TestClient(app)

    def test_without_a_namespace_it_removes_from_inventory(self, client, fake):
        response = client.post(self.PATH, json=self.BODY)
        assert response.status_code == 202
        assert fake.args[0].input.namespace == "inventory"
        # One id per machine, however its serial was typed.
        assert response.json()["workflow_id"] == "remove-server-ocp-cisco-m6-bat-yam-64c-512gb-fch0001"

    def test_an_explicit_namespace_is_kept(self, client, fake):
        client.post(self.PATH, json={**self.BODY, "namespace": "lab-kv"})
        assert fake.args[0].input.namespace == "lab-kv"

    @pytest.mark.parametrize("typed", ["prod-mce-bat-yam", "ocp4-prod-mce-bat-yam"])
    def test_the_mce_is_accepted_with_or_without_its_ocp4_prefix(self, client, fake, typed):
        response = client.post(self.PATH, json={**self.BODY, "mce_cluster": typed})
        assert response.status_code == 202
        assert fake.args[0].input.mce_cluster == "ocp4-prod-mce-bat-yam"

    def test_unknown_fields_are_refused(self, client):
        assert client.post(self.PATH, json={**self.BODY, "force": True}).status_code == 422


class TestPublishInfraEnvBoot:
    PATH = "/workflows/server-lifecycle/publish-infraenv-boot"
    BODY = {"mce_cluster": "mce-bat-yam-01", "infra_env": "cisco-m6-bat-yam-64c-512gb"}

    @pytest.fixture
    def client(self, fake: _FakeClient) -> TestClient:
        from shared.consts import PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE

        async def start_workflow(_run, args, *, id: str, task_queue: str):
            assert task_queue == PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE
            fake.args.append(args)
            return _FakeHandle(id)

        fake.start_workflow = start_workflow
        app = FastAPI()
        app.include_router(router_module.router)
        app.dependency_overrides[get_temporal_client] = lambda: fake
        return TestClient(app)

    def test_the_run_carries_the_canonical_mce_and_inventory_by_default(self, client, fake):
        response = client.post(self.PATH, json=self.BODY)
        assert response.status_code == 202
        run_input = fake.args[0].input
        # The MCE name is the entry's directory on the PXE VM, so both
        # spellings must land on one file.
        assert run_input.mce_cluster == "ocp4-mce-bat-yam-01"
        assert run_input.namespace == "inventory"
        assert response.json()["workflow_id"] == (
            "publish-infraenv-boot-ocp4-mce-bat-yam-01-cisco-m6-bat-yam-64c-512gb"
        )

    def test_an_explicit_namespace_is_kept(self, client, fake):
        client.post(self.PATH, json={**self.BODY, "namespace": "lab-kv"})
        assert fake.args[0].input.namespace == "lab-kv"

    @pytest.mark.parametrize("missing", ["mce_cluster", "infra_env"])
    def test_both_names_are_required(self, client, missing):
        body = {k: v for k, v in self.BODY.items() if k != missing}
        assert client.post(self.PATH, json=body).status_code == 422

    def test_unknown_fields_are_refused(self, client):
        assert client.post(self.PATH, json={**self.BODY, "ipxe_url": "http://x"}).status_code == 422
