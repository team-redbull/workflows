"""provision-cluster: real ProvisionClusterWorkflow AND the real AllocateSegmentWorkflow
child, mock activities on both limb queues, time-skipping env.

Four workers, as production has them: the two workflow queues on the brain,
the cluster-lifecycle and the segment-lifecycle activity queues on their
limbs. What is pinned is classification and what each activity received, not
the sequence of calls — and the merge wait: a durable timer with no deadline,
continued as new every _POLLS_PER_RUN polls with the state carried over, so a
continued run calls nothing twice.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
    CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
    PROVISION_CLUSTER_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
)
from shared.exceptions import (
    AmbiguousMceFolderError,
    ClusterAlreadyProvisionedError,
    ClusterFileConflictError,
    ClusterValuesRejectedError,
    GitProviderAuthError,
    GitProviderError,
    GitProviderRequestRejectedError,
    MceFolderNotFoundError,
    SegmentPoolExhaustedError,
    ValuesRepoGitError,
)
from shared.models.cluster_lifecycle import (
    ClusterFilePublishRequest,
    ClusterFileRef,
    ClusterTarget,
    ClusterTargetRequest,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
    ProvisionClusterInput,
    ProvisionClusterRunArgs,
    ProvisionClusterState,
)
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentResult,
    AllocateSegmentRunArgs,
    DhcpExclusion,
    DhcpValues,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
    SegmentType,
)
from shared.workflow_ids import allocate_segment_workflow_id, provision_cluster_workflow_id
from workflow_domains.cluster_lifecycle.provision_cluster import _POLLS_PER_RUN, ProvisionClusterWorkflow
from workflow_domains.cluster_lifecycle.router import EXAMPLE_CLUSTER_FILE
from workflow_domains.segment_lifecycle.allocate_segment import AllocateSegmentWorkflow

CLUSTER = "ocp4-prep-e2e-site2-a"
MCE = "ocp4-prep-mce-site2-a"
SITE = "site2"
MCE_DIR = f"sites/{SITE}/mces/{MCE}"
PATH = f"{MCE_DIR}/hostedClusters/{CLUSTER}.yaml"
BRANCH = f"provision-cluster/{CLUSTER}"
SEGMENT = "10.20.90.0/24"
VLAN_ID = 23
MR_URL = "https://gitlab.example/p/-/merge_requests/42"
INPUT = ProvisionClusterInput(cluster=CLUSTER, mce=MCE, values=EXAMPLE_CLUSTER_FILE)
DHCP = DhcpValues(network="10.20.90.0", exclusions=[DhcpExclusion(start_address="10.20.90.1", end_address="10.20.90.10")])
CONFIGS = ["nm-conf", "worker-kubeletconfig-reserved", "nic-ring-buffer", "workers-chrony-configuration"]


def _target(**overrides) -> ClusterTarget:
    fields = dict(site=SITE, mce_dir=MCE_DIR, cluster_name="prep-e2e-site2-a", mastertag="4.22.8",
                  config_names=list(CONFIGS), day2_env=None, day2_sigs=[], revision="rev1")
    fields.update(overrides)
    return ClusterTarget(**fields)


def make_mocks(
    *,
    target: ClusterTarget | None = None,
    inspect_error: Exception | None = None,
    publish_error: Exception | None = None,
    publish_errors_before_success: int = 0,
    changed: bool = True,
    ensure_error: Exception | None = None,
    mr_created: bool = True,
    states: list[str] | None = None,
    state_errors_before_success: int = 0,
    register_results: dict[str, bool] | None = None,
    register_error: Exception | None = None,
):
    """The cluster-lifecycle limb's mocks + a call recorder.

    `states` is the sequence the merge request reads back; the last entry
    repeats (`["open", "open", "merged"]`; default `["merged"]`).
    `register_results` maps a sig to whether its folder was created (default
    True for every sig).
    """
    calls: dict[str, list] = {k: [] for k in ("inspect", "publish", "ensure", "state", "register", "order")}
    remaining_states = list(states or ["merged"])
    publish_failures = [publish_errors_before_success]
    state_failures = [state_errors_before_success]

    @activity.defn
    async def inspect_cluster_target(request: ClusterTargetRequest) -> ClusterTarget:
        calls["inspect"].append(request)
        calls["order"].append("inspect")
        if inspect_error is not None:
            raise inspect_error
        return target if target is not None else _target()

    @activity.defn
    async def publish_cluster_file(request: ClusterFilePublishRequest) -> ClusterFileRef:
        calls["publish"].append(request)
        calls["order"].append("publish")
        if publish_error is not None:
            raise publish_error
        if publish_failures[0] > 0:
            publish_failures[0] -= 1
            raise ValuesRepoGitError("push rejected: the branch moved")
        return ClusterFileRef(relative_path=PATH, branch=request.branch, commit_sha="c" * 40 if changed else None,
                              changed=changed, dhcp_values=DHCP)

    @activity.defn
    async def ensure_merge_request(request: MergeRequestRequest) -> MergeRequestRef:
        calls["ensure"].append(request)
        calls["order"].append("ensure")
        if ensure_error is not None:
            raise ensure_error
        return MergeRequestRef(url=MR_URL, identifier="42", created=mr_created)

    @activity.defn
    async def get_merge_request_state(merge_request: MergeRequestRef) -> MergeRequestState:
        calls["state"].append(merge_request)
        calls["order"].append("state")
        if state_failures[0] > 0:
            state_failures[0] -= 1
            raise GitProviderError("502 from the provider")
        current = remaining_states.pop(0) if len(remaining_states) > 1 else remaining_states[0]
        return MergeRequestState(state=current, merged_sha="m" * 40 if current == "merged" else None)

    @activity.defn
    async def register_cluster_in_day2_sig(request: Day2SigRegistrationRequest) -> Day2SigRegistration:
        calls["register"].append(request)
        calls["order"].append("register")
        if register_error is not None:
            raise register_error
        created = (register_results or {}).get(request.sig, True)
        path = f"sites/{request.site}/{request.env}/mces/{request.mce}/{request.cluster}"
        return Day2SigRegistration(sig=request.sig, path=path, created=created, commit_sha="d" * 40 if created else None)

    return calls, [inspect_cluster_target, publish_cluster_file, ensure_merge_request, get_merge_request_state,
                   register_cluster_in_day2_sig]


def make_segment_mocks(*, valid_sites: tuple[str, ...] = (SITE, "site4"), allocate_error: Exception | None = None,
                       gate: asyncio.Event | None = None):
    """The segment-lifecycle limb's mocks for the REAL allocate-segment child.
    `gate`: get_valid_sites waits on it — a way to hold a child open."""
    calls: dict[str, list] = {k: [] for k in ("sites", "allocate", "verify")}

    @activity.defn
    async def get_valid_sites() -> list[str]:
        calls["sites"].append(None)
        if gate is not None:
            await gate.wait()
        return list(valid_sites)

    @activity.defn
    async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
        calls["allocate"].append(request)
        if allocate_error is not None:
            raise allocate_error
        return SegmentAllocation(vlan_id=VLAN_ID, segment=SEGMENT, epg_name="EPG_HC_23")

    @activity.defn
    async def get_segment(segment: str) -> SegmentEntry:
        calls["verify"].append(segment)
        return SegmentEntry(segment=segment, site=SITE, vlan_id=VLAN_ID, status="Allocated", type="HC", cluster_name=CLUSTER)

    return calls, [get_valid_sites, allocate_segment, get_segment]


class _Harness:
    def __init__(self, cluster_activities, segment_activities) -> None:
        self._cluster_activities = cluster_activities
        self._segment_activities = segment_activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=PROVISION_CLUSTER_WORKFLOW_QUEUE, workflows=[ProvisionClusterWorkflow]),
            Worker(client, task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE, workflows=[AllocateSegmentWorkflow]),
            Worker(client, task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE, activities=self._cluster_activities),
            Worker(client, task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, activities=self._segment_activities),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


def _harness(cluster_kwargs: dict | None = None, segment_kwargs: dict | None = None):
    cluster_calls, cluster_acts = make_mocks(**(cluster_kwargs or {}))
    segment_calls, segment_acts = make_segment_mocks(**(segment_kwargs or {}))
    return cluster_calls, segment_calls, _Harness(cluster_acts, segment_acts)


async def _start(client: Client, run_args: ProvisionClusterRunArgs | None = None, workflow_id: str | None = None):
    return await client.start_workflow(
        ProvisionClusterWorkflow.run,
        run_args or ProvisionClusterRunArgs(input=INPUT),
        id=workflow_id or f"test-{uuid.uuid4()}",
        task_queue=PROVISION_CLUSTER_WORKFLOW_QUEUE,
    )


async def _execute(client: Client, run_args: ProvisionClusterRunArgs | None = None, workflow_id: str | None = None):
    handle = await _start(client, run_args, workflow_id)
    return await asyncio.wait_for(handle.result(), timeout=60)


def _error(exc: WorkflowFailureError) -> ApplicationError:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


# --- the happy path ----------------------------------------------------------------------


async def test_the_whole_run_from_text_to_merged_file():
    calls, segment_calls, harness = _harness()
    async with harness as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "publish", "ensure", "state"]
    # 1. inspected with the input verbatim
    assert calls["inspect"] == [ClusterTargetRequest(cluster=CLUSTER, mce=MCE, values=EXAMPLE_CLUSTER_FILE)]
    # 2. the REAL child allocated at the folder's site, as HC, and verified
    assert segment_calls["allocate"] == [SegmentAllocationRequest(cluster=CLUSTER, site=SITE, type=SegmentType.HC)]
    assert segment_calls["verify"] == [SEGMENT]
    # 3. the publish got the text verbatim, the allocation and the branch
    publish = calls["publish"][0]
    assert (publish.cluster, publish.mce_dir, publish.branch, publish.values) == (CLUSTER, MCE_DIR, BRANCH, EXAMPLE_CLUSTER_FILE)
    assert (publish.vlan_id, publish.segment, publish.type) == (VLAN_ID, SEGMENT, SegmentType.HC)
    # 4. the merge request names the branch and tells the reviewer what to wait for
    ensure = calls["ensure"][0]
    assert ensure.branch == BRANCH
    assert ensure.title == f"Provision hosted cluster {CLUSTER} on {MCE}"
    for said in (PATH, SEGMENT, "vlan `23`", "`4.22.8`", "nm-conf", "pipeline", "mcConfigMap", "sync"):
        assert said in ensure.description, said
    # 5. one read: merged at once
    assert calls["state"] == [MergeRequestRef(url=MR_URL, identifier="42", created=True)]
    # 6. no sigs listed: no day2 call
    assert calls["register"] == []
    # 7. the result
    assert (result.cluster, result.cluster_name, result.site, result.mce, result.mastertag) == (
        CLUSTER, "prep-e2e-site2-a", SITE, MCE, "4.22.8")
    assert result.config_names == CONFIGS
    assert (result.vlan_id, result.segment, result.epg_name, result.network) == (VLAN_ID, SEGMENT, "EPG_HC_23", "10.20.90.0")
    assert (result.branch, result.cluster_file, result.commit_sha, result.file_changed) == (BRANCH, PATH, "c" * 40, True)
    assert (result.merge_request_url, result.merge_request_created, result.merged_sha) == (MR_URL, True, "m" * 40)
    assert result.day2 == []


async def test_a_rerun_converges_file_present_merge_request_open_folders_present():
    calls, _, harness = _harness(
        {"changed": False, "mr_created": False, "target": _target(day2_env="prep", day2_sigs=["redbull"]),
         "register_results": {"redbull": False}}
    )
    async with harness as client:
        result = await _execute(client)
    assert (result.file_changed, result.commit_sha, result.merge_request_created) == (False, None, False)
    assert result.day2 == [Day2SigRegistration(sig="redbull", path=f"sites/{SITE}/prep/mces/{MCE}/{CLUSTER}", created=False, commit_sha=None)]


async def test_an_empty_config_list_is_a_legal_cluster():
    calls, _, harness = _harness({"target": _target(config_names=[])})
    async with harness as client:
        result = await _execute(client)
    assert result.config_names == []


# --- phase 1: what the limb refuses ---------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        MceFolderNotFoundError("no sites/<site>/mces/ocp4-x folder"),
        AmbiguousMceFolderError("mces/ocp4-x under 2 sites"),
        ClusterAlreadyProvisionedError(f"{PATH} already exists"),
        ClusterValuesRejectedError("`mastertag` is required; `owner.merkaz` is required"),
    ],
    ids=lambda e: type(e).__name__,
)
async def test_a_refused_target_fails_after_one_attempt_and_starts_no_child(error):
    calls, segment_calls, harness = _harness({"inspect_error": error})
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    failure = _error(excinfo.value)
    assert failure.type == type(error).__name__
    assert str(error) in failure.message
    assert len(calls["inspect"]) == 1
    assert segment_calls["sites"] == [] and segment_calls["allocate"] == []
    assert calls["publish"] == [] and calls["ensure"] == []


async def test_a_day1_outage_at_inspection_is_outwaited():
    outage = [2]

    calls, segment_acts = {}, None
    cluster_calls, cluster_acts = make_mocks()
    real_inspect = cluster_acts[0]

    @activity.defn(name="inspect_cluster_target")
    async def flaky(request: ClusterTargetRequest) -> ClusterTarget:
        if outage[0] > 0:
            outage[0] -= 1
            raise ValuesRepoGitError("clone timed out")
        return await real_inspect(request)

    cluster_acts[0] = flaky
    segment_calls, segment_acts = make_segment_mocks()
    async with _Harness(cluster_acts, segment_acts) as client:
        result = await _execute(client)
    assert result.vlan_id == VLAN_ID and outage[0] == 0


# --- phase 2: the child ---------------------------------------------------------------------


async def test_a_child_already_running_for_the_cluster_is_a_named_failure():
    gate = asyncio.Event()
    calls, segment_calls, harness = _harness(segment_kwargs={"gate": gate})
    child_id = allocate_segment_workflow_id(SegmentType.HC, CLUSTER)
    async with harness as client:
        # Someone started allocate-segment by hand; it is held open at its first activity.
        blocker = await client.start_workflow(
            AllocateSegmentWorkflow.run,
            AllocateSegmentRunArgs(input=AllocateSegmentInput(cluster=CLUSTER, site=SITE)),
            id=child_id, task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
        )
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
        gate.set()
        await asyncio.wait_for(blocker.result(), timeout=60)
    failure = _error(excinfo.value)
    assert failure.type == "AllocateSegmentAlreadyRunningError"
    assert child_id in failure.message
    assert calls["publish"] == []


async def test_a_child_that_fails_carries_its_type_into_the_parents_failure():
    calls, segment_calls, harness = _harness(segment_kwargs={"valid_sites": ("site4",)})
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    failure = _error(excinfo.value)
    assert failure.type == "AllocateSegmentFailedError"
    assert "UnknownSite" in failure.message and SITE in failure.message
    assert segment_calls["allocate"] == []
    assert calls["publish"] == [] and calls["ensure"] == []


async def test_a_childs_permanent_activity_error_surfaces_the_same_way():
    calls, segment_calls, harness = _harness(segment_kwargs={"allocate_error": SegmentPoolExhaustedError("pool drained")})
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    failure = _error(excinfo.value)
    assert failure.type == "AllocateSegmentFailedError"
    assert "SegmentPoolExhaustedError" in failure.message and "pool drained" in failure.message
    assert len(segment_calls["allocate"]) == 1
    assert calls["publish"] == []


async def test_the_child_runs_on_its_own_id_so_a_manual_run_dedups_with_it():
    calls, segment_calls, harness = _harness()
    child_id = allocate_segment_workflow_id(SegmentType.HC, CLUSTER)
    async with harness as client:
        await _execute(client)
        child = client.get_workflow_handle(child_id, result_type=AllocateSegmentResult)
        child_result = await child.result()
    assert child_result == AllocateSegmentResult(cluster=CLUSTER, site=SITE, type=SegmentType.HC, vlan_id=VLAN_ID,
                                                 segment=SEGMENT, epg_name="EPG_HC_23")


# --- phases 3 and 4: the branch and the merge request ---------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "error_type", "attempted"),
    [
        ({"publish_error": ClusterFileConflictError("a different file")}, "ClusterFileConflictError", "publish"),
        ({"publish_error": MceFolderNotFoundError("gone from main")}, "MceFolderNotFoundError", "publish"),
        ({"publish_error": ClusterAlreadyProvisionedError("appeared on main")}, "ClusterAlreadyProvisionedError", "publish"),
        ({"ensure_error": GitProviderAuthError("401")}, "GitProviderAuthError", "ensure"),
        ({"ensure_error": GitProviderRequestRejectedError("no main")}, "GitProviderRequestRejectedError", "ensure"),
    ],
)
async def test_each_permanent_error_fails_the_run_after_one_attempt(kwargs, error_type, attempted):
    calls, _, harness = _harness(kwargs)
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    assert _error(excinfo.value).type == error_type
    assert len(calls[attempted]) == 1
    assert calls["state"] == [] and calls["register"] == []


async def test_a_rejected_push_is_retried_from_a_fresh_clone():
    calls, _, harness = _harness({"publish_errors_before_success": 2})
    async with harness as client:
        result = await _execute(client)
    assert len(calls["publish"]) == 3 and result.file_changed


async def test_a_provider_outage_while_waiting_is_outwaited():
    calls, _, harness = _harness({"state_errors_before_success": 3})
    async with harness as client:
        result = await _execute(client)
    assert len(calls["state"]) == 4 and result.merged_sha


# --- phase 5: the human wait -------------------------------------------------------------------


async def test_the_run_waits_for_the_merge_on_a_timer_and_reports_it():
    calls, _, harness = _harness({"states": ["open", "open", "open", "merged"]})
    async with harness as client:
        handle = await _start(client)
        # Let the run reach the wait: three open reads are three 5-minute timers.
        result = await asyncio.wait_for(handle.result(), timeout=60)
        progress = await handle.query(ProvisionClusterWorkflow.progress)
    assert len(calls["state"]) == 4
    assert result.merged_sha == "m" * 40
    assert progress.phase == "completed"
    assert progress.polls == 4
    assert progress.merge_request_url == MR_URL and progress.merge_request_state == "merged"
    assert isinstance(progress.waited_since, datetime)
    assert progress.branch == BRANCH and progress.allocate_segment_workflow_id == allocate_segment_workflow_id(SegmentType.HC, CLUSTER)


async def test_a_merge_request_closed_without_merging_fails_the_run():
    calls, _, harness = _harness({"states": ["open", "closed"], "target": _target(day2_env="prep", day2_sigs=["redbull"])})
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    failure = _error(excinfo.value)
    assert failure.type == "MergeRequestClosedError"
    assert MR_URL in failure.message
    assert calls["register"] == []  # never registered in day2


async def test_the_wait_continues_as_new_every_n_polls_and_calls_nothing_twice():
    states = ["open"] * (_POLLS_PER_RUN + 10) + ["merged"]
    calls, segment_calls, harness = _harness({"states": states, "target": _target(day2_env="prep", day2_sigs=["redbull"])})
    workflow_id = provision_cluster_workflow_id(CLUSTER)
    async with harness as client:
        handle = await _start(client, workflow_id=workflow_id)
        first_run_id = handle.result_run_id
        result = await asyncio.wait_for(handle.result(), timeout=120)
        first = await client.get_workflow_handle(workflow_id, run_id=first_run_id).fetch_history()
        latest = await handle.fetch_history()
        progress = await handle.query(ProvisionClusterWorkflow.progress)

    # The first run ended by continuing as new; the latest is another run.
    assert any(e.HasField("workflow_execution_continued_as_new_event_attributes") for e in first.events)
    assert latest.run_id != first_run_id
    # Phases 1–4 ran ONCE across every run; the child was started once.
    assert len(calls["inspect"]) == 1 and len(calls["publish"]) == 1 and len(calls["ensure"]) == 1
    assert len(segment_calls["allocate"]) == 1
    # Every poll counted, across runs.
    assert len(calls["state"]) == _POLLS_PER_RUN + 11
    assert progress.polls == _POLLS_PER_RUN + 11
    # And the continued run finished the job: day2 after the merge, the full result.
    assert len(calls["register"]) == 1
    assert (result.vlan_id, result.merged_sha, result.file_changed) == (VLAN_ID, "m" * 40, True)
    assert result.day2[0].created


async def test_a_continued_run_restores_its_state_and_skips_what_was_done():
    """The shape a continue-as-new hands over: every phase's outcome in the state."""
    state = ProvisionClusterState(
        target=_target(day2_env="prep", day2_sigs=["redbull", "data"]),
        allocation=AllocateSegmentResult(cluster=CLUSTER, site=SITE, type=SegmentType.HC, vlan_id=VLAN_ID, segment=SEGMENT, epg_name="E"),
        published=ClusterFileRef(relative_path=PATH, branch=BRANCH, commit_sha="c" * 40, changed=True, dhcp_values=DHCP),
        merge_request=MergeRequestRef(url=MR_URL, identifier="42", created=True),
        waited_since=datetime(2026, 11, 11, 9, 0, 0),
        polls=7,
    )
    calls, segment_calls, harness = _harness({"register_results": {"redbull": True, "data": False}})
    async with harness as client:
        handle = await _start(client, ProvisionClusterRunArgs(input=INPUT, state=state))
        result = await asyncio.wait_for(handle.result(), timeout=60)
        progress = await handle.query(ProvisionClusterWorkflow.progress)
    assert calls["order"] == ["state", "register", "register"]
    assert segment_calls["sites"] == []
    assert sorted(r.sig for r in calls["register"]) == ["data", "redbull"]
    assert all((r.site, r.env, r.mce, r.cluster) == (SITE, "prep", MCE, CLUSTER) for r in calls["register"])
    assert progress.polls == 8 and progress.waited_since == state.waited_since
    assert {r.sig: r.created for r in result.day2} == {"redbull": True, "data": False}
    assert (result.commit_sha, result.file_changed, result.epg_name) == ("c" * 40, True, "E")


async def test_progress_during_the_wait_names_the_merge_request():
    calls, _, harness = _harness({"states": ["open"] * 3 + ["merged"]})
    async with harness as client:
        handle = await _start(client)
        # The first read is immediate; the run is then on its first timer.
        for _ in range(100):
            if calls["state"]:
                break
            await asyncio.sleep(0.05)
        progress = await handle.query(ProvisionClusterWorkflow.progress)
        assert progress.phase == "awaiting-merge"
        assert progress.merge_request_url == MR_URL
        assert progress.merge_request_state == "open"
        assert progress.waited_since is not None and progress.polls >= 1
        await asyncio.wait_for(handle.result(), timeout=60)


# --- phase 6: day2 -----------------------------------------------------------------------------


async def test_day2_registration_runs_after_the_merge_once_per_sig():
    calls, _, harness = _harness(
        {"states": ["open", "merged"], "target": _target(day2_env="prep", day2_sigs=["redbull", "data"]),
         "register_results": {"redbull": True, "data": False}}
    )
    async with harness as client:
        result = await _execute(client)
    assert calls["order"][:4] == ["inspect", "publish", "ensure", "state"]
    assert calls["order"].index("register") > calls["order"].index("state", 1)  # after the MERGED read
    assert sorted(r.sig for r in calls["register"]) == ["data", "redbull"]
    assert {r.sig: r.created for r in result.day2} == {"redbull": True, "data": False}


async def test_sigs_without_an_environment_fail_after_the_merge_pushing_nothing_to_day2():
    calls, _, harness = _harness({"target": _target(day2_env=None, day2_sigs=["redbull"])})
    async with harness as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    assert _error(excinfo.value).type == "Day2EnvMissingError"
    assert calls["register"] == [] and len(calls["state"]) == 1


async def test_a_sig_repo_outage_is_outwaited():
    outage = [2]
    cluster_calls, cluster_acts = make_mocks(target=_target(day2_env="prep", day2_sigs=["redbull"]))
    real_register = cluster_acts[4]

    @activity.defn(name="register_cluster_in_day2_sig")
    async def flaky(request: Day2SigRegistrationRequest) -> Day2SigRegistration:
        if outage[0] > 0:
            outage[0] -= 1
            raise ValuesRepoGitError("push rejected")
        return await real_register(request)

    cluster_acts[4] = flaky
    _, segment_acts = make_segment_mocks()
    async with _Harness(cluster_acts, segment_acts) as client:
        result = await _execute(client)
    assert result.day2[0].created and outage[0] == 0


# --- the id -------------------------------------------------------------------------------------


async def test_a_second_start_on_the_same_cluster_while_it_waits_is_already_started():
    from temporalio.exceptions import WorkflowAlreadyStartedError

    calls, _, harness = _harness({"states": ["open"] * 2 + ["merged"]})
    workflow_id = provision_cluster_workflow_id(CLUSTER)
    async with harness as client:
        handle = await _start(client, workflow_id=workflow_id)
        with pytest.raises(WorkflowAlreadyStartedError):
            await _start(client, workflow_id=workflow_id)
        await asyncio.wait_for(handle.result(), timeout=60)
