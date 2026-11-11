"""provision-cluster's trigger: real route, a stand-in Temporal client.

The body is the hosted-cluster YAML itself. Pinned: the OpenAPI document
offers `application/yaml` with EXAMPLE_CLUSTER_FILE as the example (what
Swagger pre-fills); POSTing that example starts a run whose input carries the
text byte for byte; `cluster` is canonicalised and `mce` checked; the edge
refuses only what could not be a cluster file (empty, too large, not UTF-8,
not one YAML mapping) and a bad name; a duplicate is a 409.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import PROVISION_CLUSTER_WORKFLOW_QUEUE
from shared.models.cluster_lifecycle import ProvisionClusterRunArgs
from workflow_domains.cluster_lifecycle import router as router_module
from workflow_domains.cluster_lifecycle.router import EXAMPLE_CLUSTER_FILE, MAX_BODY_BYTES
from workflow_domains.routers.deps import get_temporal_client

PATH = "/workflows/cluster-lifecycle/provision-cluster"
QUERY = {"cluster": "prep-e2e-site2-a", "mce": "ocp4-prep-mce-site2-a"}
YAML = {"content-type": "application/yaml"}


class _FakeHandle:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id
        self.result_run_id = "run-1"


class _FakeClient:
    def __init__(self) -> None:
        self.args: list = []
        self.ids: list[str] = []
        self.running: set[str] = set()

    async def start_workflow(self, _run, args, *, id: str, task_queue: str):
        assert task_queue == PROVISION_CLUSTER_WORKFLOW_QUEUE
        if id in self.running:
            raise WorkflowAlreadyStartedError(id, "ProvisionClusterWorkflow")
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


def test_the_example_starts_a_run_with_the_text_byte_for_byte(client, fake):
    response = client.post(PATH, params=QUERY, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 202, response.text
    assert response.json() == {"workflow_id": "provision-cluster-ocp4-prep-e2e-site2-a", "run_id": "run-1"}
    (args,) = fake.args
    assert isinstance(args, ProvisionClusterRunArgs)
    assert args.state is None
    assert (args.input.cluster, args.input.mce) == ("ocp4-prep-e2e-site2-a", "ocp4-prep-mce-site2-a")
    assert args.input.values == EXAMPLE_CLUSTER_FILE
    # The text survives the trip through the boundary model unchanged.
    assert ProvisionClusterRunArgs.model_validate_json(args.model_dump_json()).input.values == EXAMPLE_CLUSTER_FILE


def test_swagger_shows_the_example_as_a_yaml_body(client):
    schema = client.get("/openapi.json").json()
    body = schema["paths"][PATH]["post"]["requestBody"]
    assert body["required"] is True
    assert set(body["content"]) == {"application/yaml"}
    assert body["content"]["application/yaml"]["example"] == EXAMPLE_CLUSTER_FILE
    parameters = {p["name"]: p for p in schema["paths"][PATH]["post"]["parameters"]}
    assert set(parameters) == {"cluster", "mce"}
    assert all(p["in"] == "query" and p["required"] for p in parameters.values())


def test_the_example_itself_is_a_cluster_file_the_run_would_accept():
    """What Swagger pre-fills must pass the run's own validator: tests/test_cluster_values.py
    runs EXAMPLE_CLUSTER_FILE through it; here only that the edge accepts it."""
    assert router_module.parse_cluster_file(EXAMPLE_CLUSTER_FILE.encode()) == EXAMPLE_CLUSTER_FILE


@pytest.mark.parametrize("cluster", ["ocp4-prep-e2e-site2-a", " prep-e2e-site2-a ", "prep-e2e-site2-a"])
def test_the_cluster_is_canonicalised_to_its_stem(client, fake, cluster):
    response = client.post(PATH, params={**QUERY, "cluster": cluster}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 202
    assert fake.ids == ["provision-cluster-ocp4-prep-e2e-site2-a"]


@pytest.mark.parametrize("cluster", ["", "Prep-E2E", "prep e2e", "prep_e2e", "-prep", "prep-", "ocp4-", "a" * 25, "ocp4-" + "a" * 25, "a/b"])
def test_a_cluster_name_that_is_not_a_short_dns_label_is_refused(client, fake, cluster):
    response = client.post(PATH, params={**QUERY, "cluster": cluster}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 422, response.text
    assert fake.ids == []


def test_a_24_character_cluster_name_is_the_longest_accepted(client, fake):
    name = "a" * 24
    assert client.post(PATH, params={**QUERY, "cluster": name}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML).status_code == 202
    assert fake.ids == [f"provision-cluster-ocp4-{name}"]


@pytest.mark.parametrize("mce", ["ocp4-prep-mce-site2-a", "ocp4-mce-site2", "ocp4-mce-site4-prep", " ocp4-mce-site2 "])
def test_any_folder_shaped_mce_is_accepted_verbatim(client, fake, mce):
    response = client.post(PATH, params={**QUERY, "mce": mce}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 202
    assert fake.args[-1].input.mce == mce.strip()


@pytest.mark.parametrize("mce", ["", "OCP4-mce", "ocp4 mce", "../ocp4-mce", "ocp4-mce/x", "ocp4_mce", "*"])
def test_an_mce_that_is_not_a_folder_name_is_refused(client, fake, mce):
    response = client.post(PATH, params={**QUERY, "mce": mce}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 422
    assert fake.ids == []


@pytest.mark.parametrize("missing", ["cluster", "mce"])
def test_both_query_parameters_are_required(client, fake, missing):
    params = {k: v for k, v in QUERY.items() if k != missing}
    assert client.post(PATH, params=params, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML).status_code == 422
    assert fake.ids == []


@pytest.mark.parametrize(
    ("body", "said"),
    [
        (b"", "empty"),
        (b"   \n\n", "empty"),
        (b"x" * (MAX_BODY_BYTES + 1), "at most"),
        ("mastertag: 4.22.8\n".encode("utf-16"), "UTF-8"),
        (b"mastertag: [\n", "not valid YAML"),
        (b"- a\n- b\n", "YAML mapping"),
        (b"just text\n", "YAML mapping"),
        (b"{}\n", "YAML mapping"),
        (b"a: 1\n---\nb: 2\n", "ONE YAML document"),
    ],
    ids=["empty", "blank", "too-large", "utf16", "broken-yaml", "list", "scalar", "empty-mapping", "two-documents"],
)
def test_a_body_that_could_not_be_a_cluster_file_is_refused(client, fake, body, said):
    response = client.post(PATH, params=QUERY, content=body, headers=YAML)
    assert response.status_code == 422, response.text
    assert said in response.json()["detail"]
    assert fake.ids == []


def test_a_json_object_body_is_accepted_as_the_yaml_it_is(client, fake):
    """JSON is YAML: a client that sends `{"mastertag": ...}` sends a mapping.
    The run's validator decides the rest."""
    response = client.post(PATH, params=QUERY, content=b'{"mastertag": "4.22.8"}', headers={"content-type": "application/json"})
    assert response.status_code == 202
    assert fake.args[0].input.values == '{"mastertag": "4.22.8"}'


def test_the_content_type_is_not_enforced(client, fake):
    """`curl --data-binary` without a header is still the file."""
    response = client.post(PATH, params=QUERY, content=EXAMPLE_CLUSTER_FILE.encode())
    assert response.status_code == 202


def test_a_second_request_while_the_run_waits_is_a_409_naming_the_id(client, fake):
    assert client.post(PATH, params=QUERY, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML).status_code == 202
    response = client.post(PATH, params={**QUERY, "cluster": "ocp4-prep-e2e-site2-a"}, content=EXAMPLE_CLUSTER_FILE.encode(), headers=YAML)
    assert response.status_code == 409
    assert "provision-cluster-ocp4-prep-e2e-site2-a" in response.json()["detail"]
    assert fake.ids == ["provision-cluster-ocp4-prep-e2e-site2-a"]


def test_the_route_is_the_only_one_under_the_domain():
    assert {route.path for route in router_module.router.routes} == {PATH}
