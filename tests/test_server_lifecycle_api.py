"""install-server trigger: real route, a stand-in Temporal client.

The router is mounted on a bare app so workflow_domains.api's lifespan, which
would connect to Temporal, never runs.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import INSTALL_SERVER_WORKFLOW_QUEUE
from shared.models.server_lifecycle import InstallServerInput
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


POOL_ID = "install-server-cisco-m6-bat-yam-64c-512gb"
SLOT = POOL_ID + "_{}"
NAMES = ["ocp-cisco-m6-bat-yam-64c-512gb-WZP1", "ocp-cisco-m6-bat-yam-64c-512gb-WZP2"]


def _named_id(name: str) -> str:
    return f"install-server-name-{name.lower()}"


def _with_temporal(fake, *, running: set[str], fail_on: str | None = None) -> set[str]:
    """Make the fake behave as Temporal does: one RUNNING workflow per id."""
    original = fake.start_workflow

    async def start_workflow(run, args, *, id: str, task_queue: str):
        if id == fail_on:
            raise RuntimeError("Temporal unavailable")
        if id in running:
            raise WorkflowAlreadyStartedError(id, "InstallServerWorkflow")
        running.add(id)
        return await original(run, args, id=id, task_queue=task_queue)

    fake.start_workflow = start_workflow
    return running


def test_without_count_it_is_one_run_on_the_pool_id(client, fake):
    response = client.post(PATH, json=BODY)

    assert response.status_code == 202
    assert response.json() == {"workflow_id": POOL_ID, "run_id": "run-1"}
    # The pool picks the server; the run's own default number of candidates.
    assert fake.args[0].input.server_name is None
    assert fake.args[0].input.candidate_count == 3


def test_an_explicit_candidate_count_is_the_run_s(client, fake):
    client.post(PATH, json={**BODY, "candidate_count": 7})
    assert fake.args[0].input.candidate_count == 7


def test_a_second_single_install_while_one_runs_is_409_and_points_at_count(client, fake):
    # The reported failure: asking again for the same InfraEnv answers 409.
    # That stays (it is what stops a retried POST installing two servers), but
    # it now says how to install several.
    _with_temporal(fake, running={POOL_ID})
    response = client.post(PATH, json=BODY)

    assert response.status_code == 409
    assert "`count`" in response.json()["detail"]


@pytest.mark.parametrize(
    "extra",
    [{}, {"count": 2}, {"server_names": NAMES[0]}, {"server_names": NAMES}],
)
def test_every_run_input_is_exactly_install_server_input(client, fake, extra):
    # `count` and a list of names are how many runs the ROUTE starts; a run's
    # input is decoded from history on every replay and must stay exactly
    # InstallServerInput, one server per run.
    assert client.post(PATH, json={**BODY, **extra}).status_code == 202
    assert fake.args and all(type(a.input) is InstallServerInput for a in fake.args)


def test_every_run_field_can_be_sent():
    # The request is its own model, not a subclass of the run's input, so a
    # field added to InstallServerInput must be added to it too.
    # `server_name` is the one exception: the request takes `server_names`.
    run_fields = set(InstallServerInput.model_fields) - {"server_name"}
    assert run_fields <= set(router_module.InstallServerRequest.model_fields)


def test_the_old_server_name_field_points_at_server_names(client, fake):
    response = client.post(PATH, json={**BODY, "server_name": NAMES[0]})

    assert response.status_code == 422
    assert "`server_names`" in response.text
    assert not fake.args


@pytest.mark.parametrize(
    "extra",
    [
        {"count": 2},
        {"candidate_count": 5},
        {"count": 2, "candidate_count": 5},
    ],
)
@pytest.mark.parametrize("names", [NAMES[0], NAMES])
def test_count_and_candidate_count_are_only_for_the_pool(client, fake, extra, names):
    # Both size a draw from the pool; a named server is drawn alone.
    response = client.post(PATH, json={**BODY, "server_names": names, **extra})

    assert response.status_code == 422
    assert "send one or the other" in response.text
    assert not fake.args


class TestInstallServerCount:
    """install-server with `count`: N servers from ONE InfraEnv's pool, in parallel."""

    def test_count_runs_start_each_on_its_own_slot_id(self, client, fake):
        response = client.post(PATH, json={**BODY, "count": 3})

        assert response.status_code == 202
        body = response.json()
        assert body["started"] == 3 and body["already_running"] == 0
        assert [(r["slot"], r["workflow_id"]) for r in body["results"]] == [
            (1, SLOT.format(1)), (2, SLOT.format(2)), (3, SLOT.format(3)),
        ]
        # Pool draws: the run picks the machine, so the report names none.
        assert all(r["server_name"] is None for r in body["results"])
        assert all(a.input.server_name is None for a in fake.args)
        assert all(a.input.namespace == "inventory" for a in fake.args)

    def test_a_single_install_in_flight_does_not_block_count(self, client, fake):
        # What the reporter needed: more servers while one install is running.
        _with_temporal(fake, running={POOL_ID})
        body = client.post(PATH, json={**BODY, "count": 2}).json()

        assert body["started"] == 2
        assert [r["workflow_id"] for r in body["results"]] == [SLOT.format(1), SLOT.format(2)]

    def test_each_run_draws_spares_for_the_races_it_will_lose(self, client, fake):
        client.post(PATH, json={**BODY, "count": 4})
        assert {a.input.candidate_count for a in fake.args} == {6}

    def test_the_spares_are_capped_and_an_explicit_candidate_count_wins(self, client, fake):
        client.post(PATH, json={**BODY, "count": 20})
        assert {a.input.candidate_count for a in fake.args} == {20}
        fake.args.clear()
        client.post(PATH, json={**BODY, "count": 2, "candidate_count": 9})
        assert {a.input.candidate_count for a in fake.args} == {9}
        fake.args.clear()
        # An explicit value equal to the run's default is still the caller's.
        client.post(PATH, json={**BODY, "count": 4, "candidate_count": 3})
        assert {a.input.candidate_count for a in fake.args} == {3}

    def test_a_busy_slot_is_stepped_over_and_count_new_runs_still_start(self, client, fake):
        _with_temporal(fake, running={SLOT.format(2)})
        body = client.post(PATH, json={**BODY, "count": 3}).json()

        assert (body["started"], body["already_running"]) == (3, 1)
        assert [(r["slot"], r["status"]) for r in body["results"]] == [
            (1, "started"), (2, "already_running"), (3, "started"), (4, "started"),
        ]

    def test_a_second_batch_while_the_first_runs_adds_more(self, client, fake):
        # A batch of 5, then another 5 while the first installs: ten runs on
        # ten different ids. Which MACHINE each gets is the install lock's job.
        running = _with_temporal(fake, running=set())
        first = client.post(PATH, json={**BODY, "count": 5}).json()
        second = client.post(PATH, json={**BODY, "count": 5}).json()

        assert first["started"] == 5 and second["started"] == 5
        assert [r["workflow_id"] for r in second["results"] if r["status"] == "started"] == [
            SLOT.format(n) for n in range(6, 11)
        ]
        assert len(running) == 10

    def test_spares_count_the_runs_already_on_the_pool(self, client, fake):
        _with_temporal(fake, running={SLOT.format(n) for n in (1, 2, 3)})
        client.post(PATH, json={**BODY, "count": 2})
        # 3 busy + 2 new + 2 spare.
        assert {a.input.candidate_count for a in fake.args} == {7}

    def test_a_start_temporal_refuses_stops_the_scan_and_is_reported(self, client, fake):
        _with_temporal(fake, running=set(), fail_on=SLOT.format(2))
        body = client.post(PATH, json={**BODY, "count": 3}).json()

        assert (body["started"], body["failed"]) == (1, 1)
        assert [r["status"] for r in body["results"]] == ["started", "failed"]
        assert "Temporal unavailable" in body["results"][1]["error"]

    def test_the_mce_is_canonical(self, client, fake):
        client.post(PATH, json={**BODY, "mce_cluster": "prod-mce-bat-yam", "count": 1})
        assert fake.args[0].input.mce_cluster == "ocp4-prod-mce-bat-yam"

    @pytest.mark.parametrize(
        "extra",
        [{"count": 0}, {"count": 21}, {"candidate_count": 0}, {"candidate_count": 21},
         {"count": 2, "vlan_id": 24}],
    )
    def test_invalid_requests_are_422(self, client, extra):
        assert client.post(PATH, json={**BODY, **extra}).status_code == 422


class TestInstallServerByName:
    """install-server with `server_names`: SPECIFIC machines, one run per name."""

    def test_one_name_is_one_run_on_its_named_id(self, client, fake):
        response = client.post(PATH, json={**BODY, "server_names": NAMES[0]})

        assert response.status_code == 202
        assert response.json() == {"workflow_id": _named_id(NAMES[0]), "run_id": "run-1"}
        assert fake.args[0].input.server_name == NAMES[0]

    def test_one_name_already_installing_is_409(self, client, fake):
        _with_temporal(fake, running={_named_id(NAMES[0])})
        response = client.post(PATH, json={**BODY, "server_names": NAMES[0]})

        assert response.status_code == 409
        assert NAMES[0] in response.json()["detail"]

    def test_a_list_starts_one_run_per_name(self, client, fake):
        response = client.post(PATH, json={**BODY, "server_names": NAMES})

        assert response.status_code == 202
        body = response.json()
        assert body["started"] == 2
        assert [(r["server_name"], r["workflow_id"], r["slot"]) for r in body["results"]] == [
            (name, _named_id(name), None) for name in NAMES
        ]
        assert sorted(a.input.server_name for a in fake.args) == NAMES
        assert {a.input.infra_env for a in fake.args} == {BODY["infra_env"]}
        assert {a.input.namespace for a in fake.args} == {"inventory"}

    def test_a_list_of_one_is_still_a_report(self, client, fake):
        # The reply follows what was sent: a list is always answered per run,
        # so a caller iterating `results` never meets the single shape.
        body = client.post(PATH, json={**BODY, "server_names": NAMES[:1]}).json()
        assert body["started"] == 1 and len(body["results"]) == 1

    def test_a_name_already_installing_is_reported_and_the_rest_start(self, client, fake):
        _with_temporal(fake, running={_named_id(NAMES[0])})
        body = client.post(PATH, json={**BODY, "server_names": NAMES}).json()

        assert (body["started"], body["already_running"]) == (1, 1)
        assert {r["server_name"]: r["status"] for r in body["results"]} == {
            NAMES[0]: "already_running", NAMES[1]: "started",
        }

    def test_a_name_and_a_list_holding_it_are_one_run(self, client, fake):
        # The same machine asked for twice is ONE run, never two.
        _with_temporal(fake, running=set())
        assert client.post(PATH, json={**BODY, "server_names": NAMES[0]}).status_code == 202
        body = client.post(PATH, json={**BODY, "server_names": NAMES}).json()

        assert [r["status"] for r in body["results"]] == ["already_running", "started"]

    def test_named_runs_and_pool_runs_do_not_collide(self, client, fake):
        # 5 from the pool, then 5 by name: ten runs on ten ids. The ids cannot
        # tell whether a named machine is one a pool run already took — the
        # install lock does, and the named run fails rather than install twice.
        names = [f"ocp-cisco-m6-bat-yam-64c-512gb-WZP{n}" for n in range(1, 6)]
        running = _with_temporal(fake, running=set())
        pool = client.post(PATH, json={**BODY, "count": 5}).json()
        named = client.post(PATH, json={**BODY, "server_names": names}).json()

        assert pool["started"] == 5 and named["started"] == 5
        assert len(running) == 10

    def test_a_refused_start_does_not_stop_the_others(self, client, fake):
        _with_temporal(fake, running=set(), fail_on=_named_id(NAMES[0]))
        body = client.post(PATH, json={**BODY, "server_names": NAMES}).json()

        assert (body["started"], body["failed"]) == (1, 1)
        assert "Temporal unavailable" in body["results"][0]["error"]

    def test_the_mce_is_canonical(self, client, fake):
        client.post(PATH, json={**BODY, "server_names": NAMES, "mce_cluster": "prod-mce-bat-yam"})
        assert {a.input.mce_cluster for a in fake.args} == {"ocp4-prod-mce-bat-yam"}

    def test_a_name_listed_twice_is_refused(self, client, fake):
        # Case-insensitively: the run id lowercases the name.
        names = [NAMES[0], NAMES[0].lower()]
        response = client.post(PATH, json={**BODY, "server_names": names})

        assert response.status_code == 422
        assert not fake.args

    @pytest.mark.parametrize(
        "server_names",
        ["", [], [""], [f"ocp-x-{n}" for n in range(21)], 5, {"name": "ocp-x"}],
    )
    def test_invalid_names_are_422(self, client, fake, server_names):
        response = client.post(PATH, json={**BODY, "server_names": server_names})

        assert response.status_code == 422
        assert not fake.args
        # ONE error saying what is wrong — not one per shape (string, list)
        # the value failed to be, which put "Input should be a valid string"
        # in front of a list's real complaint.
        errors = response.json()["detail"]
        assert len(errors) == 1
        assert "valid string" not in errors[0]["msg"]


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
