"""publish-infraenv-boot: real PublishInfraEnvBootWorkflow, mock activities, time-skipping env.

Two activity queues, as for install-server: the site comes from the
segment-lifecycle limb, everything else from the target MCE's worker. The
time-skipping environment collapses the publish poll's durable timers.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import InfraEnvNotFoundError, PxeSiteNotConfiguredError
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    InfraEnvBootRequest,
    InfraEnvBootState,
    InfraEnvRef,
    InfraEnvState,
    PublishInfraEnvBootInput,
    PublishInfraEnvBootRunArgs,
)
from workflow_domains.server_lifecycle.publish_infraenv_boot import (
    PublishInfraEnvBootWorkflow,
)

MCE = "ocp4-mce-bat-yam-01"
QUEUE = server_lifecycle_activity_queue(MCE)
INFRA_ENV = "cisco-m6-bat-yam-64c-512gb"
NS = "inventory"
INPUT = PublishInfraEnvBootInput(mce_cluster=MCE, infra_env=INFRA_ENV, namespace=NS)
SEGMENT = SegmentEntry(
    segment="10.20.90.0/24",
    site="bat-yam",
    vlan_id=24,
    status="Allocated",
    type=SegmentType.INVENTORY.value,
    cluster_name=MCE,
)


def make_mocks(
    *,
    segment: SegmentEntry = SEGMENT,
    infra_env_missing: bool = False,
    ready_after_polls: int = 0,
    never_ready: bool = False,
    superseded: bool = False,
    publish_error: Exception | None = None,
):
    calls: dict[str, list] = {
        k: [] for k in ("get_inventory_segment", "check_infra_env", "publish_infra_env_boot", "order")
    }

    @activity.defn
    async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
        calls["get_inventory_segment"].append(mce_cluster)
        calls["order"].append("segment")
        return segment

    @activity.defn
    async def check_infra_env(ref: InfraEnvRef) -> InfraEnvState:
        calls["check_infra_env"].append(ref)
        calls["order"].append("check")
        if infra_env_missing:
            raise InfraEnvNotFoundError(f"InfraEnv {ref.name} does not exist in {ref.namespace}")
        return InfraEnvState(name=ref.name, namespace=ref.namespace)

    @activity.defn
    async def publish_infra_env_boot(request: InfraEnvBootRequest) -> InfraEnvBootState:
        calls["publish_infra_env_boot"].append(request)
        calls["order"].append("publish")
        if publish_error is not None:
            raise publish_error
        if never_ready or len(calls["publish_infra_env_boot"]) <= ready_after_polls:
            return InfraEnvBootState(
                published=False, detail="InfraEnv condition ImageCreated is not True: regenerating"
            )
        return InfraEnvBootState(
            published=True, image_created_at="2026-10-19T10:00:05Z", superseded=superseded
        )

    return calls, [get_inventory_segment], [check_infra_env, publish_infra_env_boot]


class _Harness:
    def __init__(self, segment_activities, server_activities) -> None:
        self._segment_activities = segment_activities
        self._server_activities = server_activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(
                client,
                task_queue=PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
                workflows=[PublishInfraEnvBootWorkflow],
            ),
            Worker(client, task_queue=QUEUE, activities=self._server_activities),
            Worker(
                client,
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                activities=self._segment_activities,
            ),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client):
    return await asyncio.wait_for(
        client.execute_workflow(
            PublishInfraEnvBootWorkflow.run,
            PublishInfraEnvBootRunArgs(input=INPUT),
            id=f"test-{uuid.uuid4()}",
            task_queue=PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
        ),
        timeout=60,
    )


def _error(exc: WorkflowFailureError) -> ApplicationError:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


async def test_the_current_image_is_published_to_the_mces_site():
    calls, seg, srv = make_mocks()
    async with _Harness(seg, srv) as client:
        result = await _execute(client)

    assert calls["order"] == ["segment", "check", "publish"]
    assert calls["get_inventory_segment"] == [MCE]
    assert calls["check_infra_env"] == [InfraEnvRef(name=INFRA_ENV, namespace=NS)]
    # Tied to no host: the InfraEnv's current image, at the segment's site.
    assert calls["publish_infra_env_boot"] == [
        InfraEnvBootRequest(namespace=NS, infra_env=INFRA_ENV, mce_cluster=MCE, site="bat-yam")
    ]
    assert calls["publish_infra_env_boot"][0].server_name is None
    assert (result.site, result.image_created_at, result.superseded) == (
        "bat-yam", "2026-10-19T10:00:05Z", False,
    )


async def test_an_image_mid_regeneration_is_waited_for():
    calls, seg, srv = make_mocks(ready_after_polls=3)
    async with _Harness(seg, srv) as client:
        result = await _execute(client)
    assert len(calls["publish_infra_env_boot"]) == 4
    assert result.image_created_at == "2026-10-19T10:00:05Z"


async def test_a_newer_image_already_on_the_vm_is_a_superseded_success():
    _, seg, srv = make_mocks(superseded=True)
    async with _Harness(seg, srv) as client:
        result = await _execute(client)
    assert result.superseded is True


async def test_no_ready_image_within_the_deadline_fails_naming_the_cause():
    calls, seg, srv = make_mocks(never_ready=True)
    async with _Harness(seg, srv) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    error = _error(excinfo.value)
    assert error.type == "InfraEnvImageNotUpdatedError"
    assert "ImageCreated is not True: regenerating" in error.message
    assert "Nothing was written" in error.message
    # Bounded: 10 min of 10 s polls.
    assert 55 <= len(calls["publish_infra_env_boot"]) <= 62


async def test_a_missing_infraenv_fails_before_anything_is_published():
    calls, seg, srv = make_mocks(infra_env_missing=True)
    async with _Harness(seg, srv) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    assert _error(excinfo.value).type == "InfraEnvNotFoundError"
    assert len(calls["check_infra_env"]) == 1
    assert calls["publish_infra_env_boot"] == []


async def test_a_site_without_a_pxe_vm_fails_once():
    calls, seg, srv = make_mocks(publish_error=PxeSiteNotConfiguredError("add it to PXE_MAP_URLS"))
    async with _Harness(seg, srv) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    assert _error(excinfo.value).type == "PxeSiteNotConfiguredError"
    assert len(calls["publish_infra_env_boot"]) == 1


async def test_a_segment_allocated_to_another_cluster_is_refused():
    calls, seg, srv = make_mocks(segment=SEGMENT.model_copy(update={"cluster_name": "ocp4-mce-other"}))
    async with _Harness(seg, srv) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    assert _error(excinfo.value).type == "InventorySegmentMismatchError"
    assert calls["check_infra_env"] == [] and calls["publish_infra_env_boot"] == []
