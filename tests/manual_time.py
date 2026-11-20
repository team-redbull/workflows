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
asked for then takes its whole length on the wall clock. `advance` jumps past
the tasks it is told of (`outstanding=`) and fails rather than hide one it was
not told of.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment

_ADVANCE_ATTEMPT_SECONDS = 5.0
_NUDGE = timedelta(seconds=2)


class ManualTime:
    async def __aenter__(self) -> ManualTime:
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

    async def advance(self, duration: timedelta, *, outstanding: int = 0) -> None:
        """Jump the server's clock by `duration`, exactly.

        THE TEST SERVER DOES NOT SKIP TIME WHILE A TASK IS OUTSTANDING — an
        activity scheduled on a queue nobody polls or still running, a workflow
        task nobody serves. Each holds one lock on its time skipping, and a
        sleep call releases only one: the call then takes its whole duration
        on the wall clock. Say how many tasks are `outstanding` and that many
        MORE sleep calls are sent at once, each releasing one lock for the same
        span and taking it back on the server at its end — so the clock jumps,
        and is locked again with nothing for the client to restore (observed
        on the SDK's test server, 2026-11-20; `tests/test_manual_time.py` pins it).

        A jump that still hangs FAILS here rather than being cut off: a sleep
        call given up on leaves its lock released for the rest of the test.
        """
        before = await self.env.get_current_time()
        # A nudge of a second or two is let run its length on the wall clock, never cut off.
        patience = _ADVANCE_ATTEMPT_SECONDS * 3 if duration <= _NUDGE else _ADVANCE_ATTEMPT_SECONDS
        try:
            await asyncio.wait_for(
                asyncio.gather(*(self.env.sleep(duration) for _ in range(outstanding + 1))), patience
            )
        except (TimeoutError, RPCError) as err:
            moved = await self.env.get_current_time() - before
            raise AssertionError(
                f"the test server did not jump {duration} (it moved {moved} at the wall clock's pace): more than "
                f"{outstanding} task(s) are outstanding, and it never skips time under one it was not told of"
            ) from err

    async def results(
        self, *handles: WorkflowHandle, step: timedelta = timedelta(seconds=1), settle: float = 0.1,
        limit: float = 60.0, return_exceptions: bool = False, outstanding: int = 0,
    ) -> list[Any]:
        """Every run's result, in order; the clock is nudged by `step` while any is still going.

        `limit` is REAL seconds: a run that never ends fails the test instead of hanging it.
        `outstanding` is `advance`'s: how many tasks nobody serves the nudges must jump past.
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
                if step > timedelta(0):
                    await self.advance(step, outstanding=outstanding)
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
