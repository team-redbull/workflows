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
RUN = "upgrade-cluster-ocp4-prod-herzi-site1"
BODY = {
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
        self.target_reads: list[tuple[str, str]] = []
        self.targets = None
        self.targets_error: Exception | None = None

    async def start_workflow(self, run, args, *, id: str, task_queue: str):
        assert run == UpgradeClusterWorkflow.run
        assert task_queue == UPGRADE_CLUSTER_WORKFLOW_QUEUE
        if self.start_error is not None:
            raise self.start_error
        self.starts.append((id, args))
        return _FakeHandle(self, id)

    def get_workflow_handle(self, workflow_id: str) -> _FakeHandle:
        return _FakeHandle(self, workflow_id)

    async def execute_workflow(self, run, args, *, id: str, task_queue: str, execution_timeout, id_conflict_policy):
        from shared.consts import UPGRADE_TARGETS_WORKFLOW_QUEUE
        from temporalio.common import WorkflowIDConflictPolicy
        from workflow_domains.cluster_upgrade.upgrade_targets import UpgradeTargetsWorkflow

        assert run == UpgradeTargetsWorkflow.run and task_queue == UPGRADE_TARGETS_WORKFLOW_QUEUE
        # Two asks for one cluster share one read; a stuck read ends on its own.
        assert id_conflict_policy is WorkflowIDConflictPolicy.USE_EXISTING and execution_timeout.total_seconds() == 60
        self.target_reads.append((id, args.input.cluster))
        if self.targets_error is not None:
            raise self.targets_error
        return self.targets


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
    def test_a_start_is_one_run_per_cluster(self, client, fake):
        response = client.post(PATH, json=BODY)

        assert response.status_code == 202
        assert response.json() == {"workflow_id": RUN, "run_id": "run-1"}
        ((workflow_id, args),) = fake.starts
        assert workflow_id == RUN
        # The run's input is the history model itself, never the edge subclass,
        # with the defaults written into it.
        assert type(args.input) is UpgradeClusterInput
        assert args.input.nodepool_mode is NodePoolMode.MANUAL
        assert args.resume is None

    def test_the_mce_is_not_asked_for_and_says_why(self, client, fake):
        # The run reads it from sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml.
        response = client.post(PATH, json={**BODY, "mce_cluster": "prod-mce-site1-a"})
        assert response.status_code == 422 and "finds the hosting MCE" in response.text
        assert fake.starts == []

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
            # The day1 file's name, held to the same rule as upgrade-targets'.
            {"cluster": ""},
            {"cluster": "C1; drop"},
            {"cluster": "Ocp4-prod"},
            {"cluster": "-ocp4"},
            {"cluster": "ocp4/../x"},
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
            ("approve-admin-ack", {"by": "carol", "reason": "kyverno moved to v1"},
             UpgradeClusterWorkflow.approve_admin_ack, HumanDecision(by="carol", reason="kyverno moved to v1")),
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

    @pytest.mark.parametrize("cause", [
        ApplicationError("cannot decode the argument", type="PayloadConversionError"),
        ApplicationError("handler crashed"),  # no type at all
        RuntimeError("something else"),
    ])
    def test_an_update_that_failed_any_other_way_is_a_500_naming_the_cause(self, client, fake, cause):
        # Only the validator's refusal means "not waiting for that"; anything else
        # is a fault, and a 409 would tell the caller to try again later.
        fake.update_error = WorkflowUpdateFailedError(cause)
        response = client.post(f"{PATH}/{RUN}/approve", json={"by": "alice"})
        assert response.status_code == 500
        detail = response.json()["detail"]
        assert RUN in detail and "approve" in detail and str(getattr(cause, "message", cause)) in detail

    def test_a_run_that_is_not_running_is_a_404(self, client, fake):
        fake.update_error = RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice"}).status_code == 404

    def test_a_run_whose_worker_does_not_answer_is_a_504(self, client, fake):
        fake.update_error = WorkflowUpdateRPCTimeoutOrCancelledError()
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice"}).status_code == 504

    def test_a_decision_route_reaches_only_upgrade_runs(self, client, fake):
        response = client.post(f"{PATH}/remove-server-from-mce-ocp-x/abort", json={"by": "alice", "reason": "x"})
        assert response.status_code == 404
        assert fake.updates == []

    @pytest.mark.parametrize("body", [{"by": "alice"}, {"by": "alice", "reason": ""}, {"by": ""}])
    def test_an_abort_names_who_and_why(self, client, fake, body):
        assert client.post(f"{PATH}/{RUN}/abort", json=body).status_code == 422
        assert fake.updates == []

    @pytest.mark.parametrize("body", [{"by": "carol"}, {"by": "carol", "reason": ""}])
    def test_an_admin_ack_approval_says_why(self, client, fake, body):
        assert client.post(f"{PATH}/{RUN}/approve-admin-ack", json=body).status_code == 422
        assert fake.updates == []

    def test_a_decision_with_unknown_fields_is_refused(self, client, fake):
        assert client.post(f"{PATH}/{RUN}/approve", json={"by": "alice", "force": True}).status_code == 422


class TestUpgradeTargets:
    """GET /upgrade-targets: the "New upgrade" form's dropdown (D22)."""

    PATH = "/workflows/cluster-upgrade/upgrade-targets"

    def test_the_targets_the_read_returned(self, client, fake):
        from shared.models.cluster_upgrade import UpgradeTarget, UpgradeTargets

        fake.targets = UpgradeTargets(
            cluster="c1", mce_cluster="ocp4-m", mce_version="2.17.3", control_plane_version="4.19.53",
            supported_minors=["4.20", "4.19"], ladder_revision="rev-1",
            targets=[UpgradeTarget(version="4.20.44", minor="4.20", steps=2, control_plane_steps=1, newest_in_minor=True)],
        )
        response = client.get(self.PATH, params={"cluster": "c1"})
        assert response.status_code == 200
        assert [t["version"] for t in response.json()["targets"]] == ["4.20.44"]
        assert fake.target_reads == [("upgrade-targets-c1", "c1")]

    @pytest.mark.parametrize(("cause", "status", "needle"), [
        (ApplicationError("no c1.yaml in day1", type="ClusterFileNotFoundError"), 404, "no c1.yaml"),
        (ApplicationError("upgrade/mces/ocp4-m.yaml is missing", type="UpgradeRulesInvalidError"), 409, "is missing"),
    ])
    def test_what_day1_refuses_is_named(self, client, fake, cause, status, needle):
        from temporalio.client import WorkflowFailureError

        fake.targets_error = WorkflowFailureError(cause=cause)
        response = client.get(self.PATH, params={"cluster": "c1"})
        assert response.status_code == status and needle in response.json()["detail"]

    def test_no_answer_in_a_minute_is_a_504(self, client, fake):
        from temporalio.client import WorkflowFailureError
        from temporalio.exceptions import TimeoutError as TemporalTimeoutError

        fake.targets_error = WorkflowFailureError(cause=TemporalTimeoutError("timed out", type=None, last_heartbeat_details=[]))
        response = client.get(self.PATH, params={"cluster": "c1"})
        assert response.status_code == 504 and "upgrade-gitops limb" in response.json()["detail"]

    def test_a_cluster_name_is_checked_before_anything_runs(self, client, fake):
        assert client.get(self.PATH, params={"cluster": "C1; drop"}).status_code == 422
        assert fake.target_reads == []

    def test_both_routes_hold_the_cluster_to_one_rule(self, client):
        schema = client.app.openapi()
        start = schema["components"]["schemas"]["UpgradeClusterRequest"]["properties"]["cluster"]
        [query] = schema["paths"][self.PATH]["get"]["parameters"]
        assert start["pattern"] == query["schema"]["pattern"]
        # The file's name, never the HostedCluster's (CLAUDE.md §4).
        for description in (start["description"], query["description"]):
            assert description.startswith("The day1 cluster file's name (also its Argo CD Application)")


class TestMountedOnTheApi:
    """The routes answer on the real entrypoint, workflow_domains.api:app (DESIGN O18).

    The page at /ui/cluster-upgrade calls them on the API that serves it. A
    plain TestClient never enters the app's lifespan, so no Temporal is dialled.
    """

    @pytest.fixture
    def api(self, fake):
        from workflow_domains.api import app

        app.dependency_overrides[get_temporal_client] = lambda: fake
        yield TestClient(app)
        app.dependency_overrides.pop(get_temporal_client, None)

    def test_a_start_answers_202(self, api, fake):
        response = api.post(PATH, json=BODY)
        assert response.status_code == 202 and response.json()["workflow_id"] == RUN
        assert [workflow_id for workflow_id, _ in fake.starts] == [RUN]

    def test_a_decision_answers_with_what_the_run_said(self, api, fake):
        response = api.post(f"{PATH}/{RUN}/approve", json={"by": "alice"})
        assert response.status_code == 200 and response.json() == {"workflow_id": RUN, "decision": "approve"}
        assert fake.updates[0][:2] == (RUN, UpgradeClusterWorkflow.approve)

    def test_the_targets_answer(self, api, fake):
        from shared.models.cluster_upgrade import UpgradeTargets

        fake.targets = UpgradeTargets(cluster="c1", mce_cluster="ocp4-m", mce_version="2.17.3",
                                      control_plane_version="4.19.53", supported_minors=["4.20"], ladder_revision="r")
        response = api.get("/workflows/cluster-upgrade/upgrade-targets", params={"cluster": "c1"})
        assert response.status_code == 200 and response.json()["mce_cluster"] == "ocp4-m"

    def test_every_route_the_page_calls_is_in_the_api(self, api):
        from workflow_domains.api import app
        from workflow_domains.cluster_upgrade import ui

        page = (ui.UI_DIR / "index.html").read_text()
        paths = set(app.openapi()["paths"])
        for path in ("/workflows/cluster-upgrade/upgrade-cluster", "/workflows/cluster-upgrade/upgrade-targets",
                     "/workflows/runs"):
            assert path in page and path in paths
        decisions = {p.rsplit("/", 1)[-1] for p in paths if p.startswith(f"{PATH}/{{workflow_id}}/")}
        assert decisions >= {"approve", "approve-admin-ack", "continue", "confirm-day2", "pause", "resume",
                             "abort", "retry"}
        assert api.get("/ui/cluster-upgrade").status_code == 200

