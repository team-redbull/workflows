"""The test clock the vm-lifecycle workflow tests stand on (tests/manual_time.py), pinned.

Those tests jump a Temporal test server's clock by hand, and two of its
habits decide how. If an SDK upgrade changes either, these fail first and say
which — rather than a hundred workflow tests failing for no visible reason.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import timedelta

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from tests.manual_time import ManualTime

QUEUE = "test-manual-time"


@activity.defn
async def never_picked_up() -> str:
    return "unreachable"


@workflow.defn
class WaitsOnADeadQueue:
    @workflow.run
    async def run(self, seconds: int) -> str:
        try:
            await workflow.execute_activity(
                never_picked_up, task_queue="a-queue-nobody-polls", start_to_close_timeout=timedelta(seconds=10),
                schedule_to_start_timeout=timedelta(seconds=seconds), retry_policy=RetryPolicy(maximum_interval=timedelta(seconds=1)),
            )
        except ActivityError:
            return f"gave up after {seconds}s"
        return "answered"


@workflow.defn
class Idle:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.done)
        return "finished"


@pytest.fixture
async def clock():
    async with ManualTime() as manual:
        async with Worker(manual.client, task_queue=QUEUE, workflows=[WaitsOnADeadQueue, Idle],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            yield manual


async def test_a_jump_is_exact_and_costs_no_real_time(clock):
    before, started = await clock.now(), time.monotonic()
    await clock.advance(timedelta(hours=3))
    assert (await clock.now()) - before == pytest.approx(timedelta(hours=3), abs=timedelta(seconds=1))
    assert time.monotonic() - started < 2


async def test_an_idle_workflow_survives_any_jump_and_any_wait_for_another_run(clock):
    """Left to skip on its own, the server jumps an idle run to its ten-year timeout."""
    idle = await clock.client.start_workflow(Idle.run, id=f"idle-{uuid.uuid4()}", task_queue=QUEUE)
    other = await clock.client.start_workflow(WaitsOnADeadQueue.run, 5, id=f"w-{uuid.uuid4()}", task_queue=QUEUE)
    assert await clock.result(other, outstanding=1) == "gave up after 5s"
    await clock.advance(timedelta(days=400))
    assert (await idle.describe()).status == WorkflowExecutionStatus.RUNNING
    await idle.signal(Idle.finish)
    assert await clock.result(idle) == "finished"


async def test_the_server_skips_no_time_under_an_outstanding_activity(clock):
    handle = await clock.client.start_workflow(WaitsOnADeadQueue.run, 3600, id=f"w-{uuid.uuid4()}", task_queue=QUEUE)
    await asyncio.sleep(0.3)  # the activity is scheduled, on a queue nobody polls
    with pytest.raises(AssertionError, match="task\\(s\\) are outstanding"):
        await clock.advance(timedelta(minutes=30))
    await handle.terminate()


async def test_told_of_the_outstanding_task_the_jump_is_exact_again(clock):
    handle = await clock.client.start_workflow(WaitsOnADeadQueue.run, 30, id=f"w-{uuid.uuid4()}", task_queue=QUEUE)
    await asyncio.sleep(0.3)
    before, started = await clock.now(), time.monotonic()
    await clock.advance(timedelta(seconds=29), outstanding=1)
    assert (await clock.now()) - before == pytest.approx(timedelta(seconds=29), abs=timedelta(seconds=1))
    assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING  # one second short of its timeout
    await clock.advance(timedelta(seconds=2), outstanding=1)
    assert await clock.result(handle) == "gave up after 30s"
    assert time.monotonic() - started < 5
