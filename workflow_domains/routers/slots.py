"""Fill the first free SLOTS of a pool with new runs — the walk add-server-to-mce invented.

A workflow whose subject is only known INSIDE the run (the machine a pool
draw picks, the VM name a create draws) cannot be keyed on it. It is a slot
of its pool instead: `<workflow>-<mce>_<pool>_<slot>`, `_1` up to `max_slots`.
This walks the slots from `_1`, steps over every one whose run is still
going (Temporal allows one running workflow per id, and answers
WorkflowAlreadyStartedError for it), starts `count` new runs in the free
ones, and stops at the first start Temporal refuses for any other reason —
every further slot would fail the same way. A finished slot is free again.

Domain-agnostic: the caller supplies the id builder and the start. create-vm
uses it; add-server-to-mce keeps its own walk (its `candidate_count` depends
on the busy count) until a follow-up migrates it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from temporalio.exceptions import WorkflowAlreadyStartedError

from workflow_domains.routers.models import SlotStart


async def fill_free_slots(
    count: int,
    max_slots: int,
    workflow_id: Callable[[int], str],
    start: Callable[[str], Awaitable],
) -> list[SlotStart]:
    """One SlotStart per slot looked at, in order; `start(workflow_id)` returns the handle."""
    results: list[SlotStart] = []
    started = 0
    for slot in range(1, max_slots + 1):
        if started == count:
            break
        wf_id = workflow_id(slot)
        try:
            handle = await start(wf_id)
        except WorkflowAlreadyStartedError:
            results.append(SlotStart(slot=slot, workflow_id=wf_id, status="already_running"))
            continue
        except Exception as exc:  # noqa: BLE001 — reported, not raised
            results.append(SlotStart(slot=slot, workflow_id=wf_id, status="failed", error=str(exc)))
            break
        started += 1
        results.append(SlotStart(slot=slot, workflow_id=handle.id, status="started", run_id=handle.result_run_id or ""))
    return results


def count_statuses(results: list[SlotStart]) -> dict[str, int]:
    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return counts
