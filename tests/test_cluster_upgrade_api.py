"""cluster-upgrade routes: the real router, a stand-in Temporal client.

The router is mounted on a bare app so workflow_domains.api's lifespan, which
would connect to Temporal, never runs. What is pinned here is the EDGE: what a
start request may say, the run input it becomes, and how each decision's answer
from the run turns into HTTP. Whether the run takes a decision is the run's
validators' business, tested in test_upgrade_cluster_workflow.py.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.client import WorkflowUpdateFailedError, WorkflowUpdateRPCTimeoutOrCancelledError
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.consts import UPGRADE_CLUSTER_WORKFLOW_QUEUE
from shared.exceptions import UpgradeUpdateRefusedError
from shared.models.cluster_upgrade import (
    ActionDecision,
    ContinueNodePool,
    HumanDecision,
    NodePoolMode,
    UpgradeClusterInput,
)
from workflow_domains.cluster_upgrade import router as router_module
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow
from workflow_domains.routers.deps import get_temporal_client

PATH = "/workflows/cluster-upgrade/upgrade-cluster"
RUN = "upgrade-cluster-ocp4-prod-mce-site1-a_ocp4-prod-herzi-site1"
BODY = {
    "mce_cluster": "prod-mce-site1-a",
    "cluster": "ocp4-prod-herzi-site1",
    "target_version": "4.20.44",
    "requested_by": "alice",
}


class _FakeHandle:
    def __init__(self, client: _FakeClient, workflow_id: str) -> None:
        self._client = client
        self.id = workflow_id
        self.result_run_id = "run-1"

    async def execute_update(self, update, arg, *, rpc_timeout=None):
        self._client.updates.append((self.id, update, arg))
        if self._client.update_error is not None:
            raise self._client.update_error


class _FakeClient:
    def __init__(self) -> None:
        self.starts: list[tuple[str, object]] = []
        self.updates: list[tuple[str, object, object]] = []
        self.start_error: Exception | None = None
        self.update_error: Exception | None = None

    async def start_workflow(self, run, args, *, id: str, task_queue: str):
        assert run == UpgradeClusterWorkflow.run
        assert task_queue == UPGRADE_CLUSTER_WORKFLOW_QUEUE
        if self.start_error is not None:
            raise self.start_error
        self.starts.append((id, args))
        return _FakeHandle(self, id)

    def get_workflow_handle(self, workflow_id: str) -> _FakeHandle:
        return _FakeHandle(self, workflow_id)


@pytest.fixture
def fake() -> _FakeClient:
    return _FakeClient()


@pytest.fixture
def client(fake: _FakeClient) -> TestClient:
    app = FastAPI()
    app.include_router(router_module.router)
    app.dependency_overrides[get_temporal_client] = lambda: fake
    return TestClient(app)


class TestStart:
    def test_a_start_is_one_run_per_cluster_on_its_canonical_mce(self, client, fake):
        response = client.post(PATH, json=BODY)

        assert response.status_code == 202
        assert response.json() == {"workflow_id": RUN, "run_id": "run-1"}
        ((workflow_id, args),) = fake.starts
        assert workflow_id == RUN
        # The run's input is the history model itself, never the edge subclass,
        # with the defaults written into it.
        assert type(args.input) is UpgradeClusterInput
        assert args.input.mce_cluster == "ocp4-prod-mce-site1-a"
        assert args.input.nodepool_mode is NodePoolMode.MANUAL
        assert args.resume is None

    def test_a_tag_spelling_becomes_the_bare_version(self, client, fake):
        assert client.post(PATH, json={**BODY, "target_version": "4.20.44-x86_64"}).status_code == 202
        assert fake.starts[0][1].input.target_version == "4.20.44"

    def test_a_minor_alone_is_a_target(self, client, fake):
        body = {**BODY, "target_minor": "4.21"}
        del body["target_version"]
        assert client.post(PATH, json=body).status_code == 202
        assert fake.starts[0][1].input.target_minor == "4.21"

    @pytest.mark.parametrize(
        "change",
        [
            {"target_minor": "4.21"},  # both targets
            {"target_version": None},  # neither
            {"target_version": "4.21.0-rc.1"},
            {"target_version": "latest"},
            {"target_version": None, "target_minor": "4.x"},
            {"nodepool_order": ["np-a", "np-a"]},
            {"nodepool_order": []},
            {"requested_by": ""},
            {"server_names": ["x"]},  # unknown fields are refused, not ignored
        ],
    )
    def test_a_request_the_run_could_not_act_on_is_refused_at_the_edge(self, client, fake, change):
        assert client.post(PATH, json={**BODY, **change}).status_code == 422
        assert fake.starts == []

    def test_a_second_start_while_the_cluster_upgrades_is_a_409(self, client, fake):
        fake.start_error = WorkflowAlreadyStartedError(RUN, "UpgradeClusterWorkflow")
        response = client.post(PATH, json=BODY)
        assert response.status_code == 409
        assert RUN in response.json()["detail"]


class TestDecisions:
    @pytest.mark.parametrize(
        ("route", "body", "update", "arg"),
        [
            ("approve", {"by": "alice"}, UpgradeClusterWorkflow.approve, HumanDecision(by="alice")),
            ("continue", {"by": "alice", "nodepool": "np-b"}, UpgradeClusterWorkflow.continue_nodepool,
             ContinueNodePool(by="alice", nodepool="np-b")),
            ("confirm-day2", {"by": "day2"}, UpgradeClusterWorkflow.confirm_day2, HumanDecision(by="day2")),
            ("pause", {"by": "alice", "reason": "freeze"}, UpgradeClusterWorkflow.pause,
             HumanDecision(by="alice", reason="freeze")),
            ("resume", {"by": "alice"}, UpgradeClusterWorkflow.resume, HumanDecision(by="alice")),
            ("abort", {"by": "alice", "reason": "escalation"}, UpgradeClusterWorkflow.abort,
             HumanDecision(by="alice", reason="escalation")),
            ("retry", {"by": "alice"}, UpgradeClusterWorkflow.retry, HumanDecision(by="alice")),
            ("actions/act-1/approve", {"by": "alice"}, UpgradeClusterWorkflow.approve_action,
             ActionDecision(by="alice", action_id="act-1")),
            ("actions/act-1/reject", {"by": "alice"}, UpgradeClusterWorkflow.reject_action,
             ActionDecision(by="alice", action_id="act-1")),
        ],
    )
    def test_each_route_is_one_update_on_the_run(self, client, fake, route, body, update, arg):
        response = client.post(f"{PATH}/{RUN}/{route}", json=body)

        assert response.status_code == 200
        ((workflow_id, sent_update, sent_arg),) = fake.updates
        assert (workflow_id, sent_update) == (RUN, update)
        # The history model, not the edge one, so the run decodes what it declares.
        assert type(sent_arg) is type(arg) and sent_arg == arg

    def test_a_decision_the_run_refuses_is_a_409_with_its_reason(self, client, fake):
        reason = "continue is not waited for: the run is awaiting-approval, waiting for approve: …"
        fake.update_error = WorkflowUpdateFailedError(
            ApplicationError(reason, type=UpgradeUpdateRefusedError.__name__)
        )
        response = client.post(f"{PATH}/{RUN}/continue", json={"by": "alice"})
        assert response.status_code == 409
        assert response.json()["detail"] == reason

    def test_a_run_that_is_not_running_is_a_404(self, client, fake):
        fake.update_error = RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice"}).status_code == 404

    def test_a_run_whose_worker_does_not_answer_is_a_504(self, client, fake):
        fake.update_error = WorkflowUpdateRPCTimeoutOrCancelledError()
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice"}).status_code == 504

    def test_a_decision_route_reaches_only_upgrade_runs(self, client, fake):
        response = client.post(f"{PATH}/remove-server-ocp-x/abort", json={"by": "alice", "reason": "x"})
        assert response.status_code == 404
        assert fake.updates == []

    @pytest.mark.parametrize("body", [{"by": "alice"}, {"by": "alice", "reason": ""}, {"by": ""}])
    def test_an_abort_names_who_and_why(self, client, fake, body):
        assert client.post(f"{PATH}/{RUN}/abort", json=body).status_code == 422
        assert fake.updates == []

    def test_a_decision_with_unknown_fields_is_refused(self, client, fake):
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice", "force": True}).status_code == 422
