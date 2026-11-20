"""A Temporal test server whose clock moves only when the test says so.

`WorkflowEnvironment.start_time_skipping()` skips time by itself while a
result is awaited — to the NEXT timer of ANY workflow. With a long-lived
workflow that is idle (the zone lock) the next "timer" is its ten-year
execution timeout, and one stray jump closes it. So here automatic skipping
is off for the whole test, and time advances in two explicit ways:

  * `advance(duration)` — a jump the test asks for (a lease's five minutes);
  * `results(...)` / `result(...)` — wait for runs, nudging the clock forward
    by a small `step` each time they are still going, so timers of a few
    seconds (a retry's backoff) pass without the test naming each one.

The test server never skips time while a task is outstanding (an activity on
a queue nobody polls or still running, a workflow task nobody serves): a jump
asked for then just waits. `advance` jumps past them all the same, by
releasing one more of the server's time-skipping locks per task it finds in its
way (its docstring says how).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

_ADVANCE_POLL_SECONDS = 0.15
_ADVANCE_PATIENCE_SECONDS = 20.0
_MAX_SLEEP_CALLS = 12


class ManualTime:
    # True: a REAL Temporal dev server on the wall clock instead (`start_local`), for what the
    # time-skipping test server does not do as Temporal does — it never times out an activity
    # whose worker died while running it, and never lets a cached run go from a workflow
    # worker that is gone. `advance` then really waits, so such tests shrink the workflow's waits.
    real_time = False

    async def __aenter__(self) -> ManualTime:
        if self.real_time:
            self.env = await WorkflowEnvironment.start_local()
        else:
            self.env = await WorkflowEnvironment.start_time_skipping()
        config = self.env.client.config()
        config["data_converter"] = pydantic_data_converter
        self.client = Client(**config)
        self._no_auto_skip = self.env.auto_time_skipping_disabled()
        self._no_auto_skip.__enter__()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._no_auto_skip.__exit__(*exc)
        await self.env.__aexit__(*exc)

    async def now(self) -> datetime:
        return await self.env.get_current_time()

    async def advance(self, duration: timedelta) -> None:
        """Jump the server's clock by `duration`, exactly.

        THE TEST SERVER DOES NOT SKIP TIME WHILE A TASK IS OUTSTANDING — an
        activity scheduled on a queue nobody polls or still running or between
        two attempts, a workflow task nobody serves. Each holds one lock on its
        time skipping, and a sleep call releases only one: the call then just
        waits, the clock moving at the wall clock's pace. So while a jump has
        NOT landed, one more sleep call is sent, for what is still missing —
        each releases one more lock until its own end, which is the same
        instant for all of them, and takes it back on the server there. The
        clock jumps exactly to the target and is locked again, with nothing
        for the client to restore and no call ever cut off (observed on the
        SDK's test server, 2026-11-20; tests/test_manual_time.py pins it).

        Never more calls than are seen to be needed: with nothing outstanding
        a second sleep call is refused by the server, or jumps a second time.
        """
        if self.real_time:
            await asyncio.sleep(duration.total_seconds())
            return
        target = await self.env.get_current_time() + duration
        calls = [asyncio.ensure_future(self.env.sleep(duration))]
        loop = asyncio.get_running_loop()
        give_up = loop.time() + _ADVANCE_PATIENCE_SECONDS
        try:
            while True:
                done, _ = await asyncio.wait(calls, timeout=_ADVANCE_POLL_SECONDS)
                if done:
                    break  # a call ran its whole span (or the server refused it): look at the clock below
                remaining = target - await self.env.get_current_time()
                if remaining <= timedelta(0):
                    break
                if len(calls) >= _MAX_SLEEP_CALLS or loop.time() >= give_up:
                    raise AssertionError(
                        f"the test server did not jump {duration}: {len(calls)} sleep calls are waiting and "
                        f"{remaining} is still missing — more tasks are outstanding than a test should have"
                    )
                calls.append(asyncio.ensure_future(self.env.sleep(remaining)))
        finally:
            # Every call ends at the target, on the server; none is cancelled (a cancelled
            # one would keep its lock released there until its span had passed anyway).
            await asyncio.gather(*calls, return_exceptions=True)

    async def results(
        self, *handles: WorkflowHandle, step: timedelta = timedelta(seconds=1), settle: float = 0.1,
        limit: float = 60.0, return_exceptions: bool = False,
    ) -> list[Any]:
        """Every run's result, in order; the clock is nudged by `step` while any is still going.

        `limit` is REAL seconds: a run that never ends fails the test instead of hanging it.
        """
        tasks = [asyncio.ensure_future(handle.result()) for handle in handles]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        try:
            while True:
                _, pending = await asyncio.wait(tasks, timeout=settle)
                if not pending:
                    break
                if loop.time() >= deadline:
                    raise AssertionError(f"{len(pending)} of {len(tasks)} runs were still going after {limit:g}s")
                if step > timedelta(0) and not self.real_time:
                    await self.advance(step)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        if not return_exceptions:
            for outcome in gathered:
                if isinstance(outcome, BaseException):
                    raise outcome
        return list(gathered)

    async def result(self, handle: WorkflowHandle, **kwargs: Any) -> Any:
        return (await self.results(handle, **kwargs))[0]

    async def failure(self, handle: WorkflowHandle, **kwargs: Any) -> WorkflowFailureError:
        """The WorkflowFailureError a run ends with; an AssertionError if it completes."""
        outcome = (await self.results(handle, return_exceptions=True, **kwargs))[0]
        assert isinstance(outcome, WorkflowFailureError), f"the run did not fail: {outcome!r}"
        return outcome
