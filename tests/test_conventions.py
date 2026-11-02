"""The repo's conventions, enforced: scripts/check_conventions.py in whole-repo mode.

CLAUDE.md says WHY each rule exists; the checker says WHAT can be verified from
source; this test makes a slip a red `pytest` (and a red CI run) instead of a
workflow stuck RUNNING in production or a limb that crash-loops at startup.

The same script runs from the Claude Code PostToolUse hook in
.claude/settings.json on every Edit/Write. This is the copy that also catches a
teammate who never opens Claude Code.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# `scripts` is a namespace package reachable through pytest.ini's `pythonpath = .`.
from scripts import check_conventions

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def checker():
    return check_conventions


def _rules(violations) -> set[str]:
    return {v.rule for v in violations if v.severity == "error"}


def test_repo_follows_its_conventions(checker):
    violations = checker.run_full(REPO_ROOT)
    errors = [v.format() for v in violations if v.severity == "error"]
    warnings = [v.format() for v in violations if v.severity == "warning"]
    if warnings:
        # Warnings are advice (a missing docs page, an undocumented env key);
        # they never fail CI but they should be seen — `pytest -s` shows them.
        print("convention warnings:\n" + "\n".join(warnings))
    assert not errors, (
        "convention violations — rationale in CLAUDE.md, checklists in .claude/rules/:\n"
        + "\n".join(errors)
    )


# --- the checker checks itself: a silent checker would pass everything -----------


def test_checker_catches_workflow_sandbox_and_routing_slips(checker):
    source = '''
import time
from temporalio import workflow
from temporalio.common import RetryPolicy
from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE

with workflow.unsafe.imports_passed_through():
    from shared.interfaces.segment_lifecycle import create_segment

_RP = RetryPolicy(maximum_attempts=3, non_retryable_error_types=["NoSuchError"])


@workflow.defn
class ProbeWorkflow:
    @workflow.run
    async def run(self, a, b=None):
        time.sleep(1)
        await workflow.execute_activity(create_segment, a, retry_policy=_RP)
        raise ValueError("plain exceptions hang the run")
'''
    rules = _rules(checker.run_source("workflow_domains/probe/probe.py", source))
    assert {"WF001", "WF003", "WF004", "WF005", "WF007", "WF008", "WF009", "WF010"} <= rules


def test_checker_accepts_the_canonical_workflow_shape(checker):
    source = (REPO_ROOT / "workflow_domains/segment_lifecycle/allocate_segment.py").read_text()
    violations = checker.run_source("workflow_domains/segment_lifecycle/allocate_segment.py", source)
    assert _rules(violations) == set()


def test_checker_catches_activity_slips(checker):
    source = '''
import logging
import httpx
from temporalio import activity

log = logging.getLogger(__name__)


@activity.defn
def sync_probe() -> None:
    pass


@activity.defn
async def no_timeout() -> None:
    async with httpx.AsyncClient(base_url="http://x") as client:
        await client.get("/")
'''
    rules = _rules(checker.run_source("activities/probe/activities.py", source))
    assert {"AC001", "AC003", "AC005"} <= rules


def test_checker_catches_router_slips(checker):
    source = '''
from fastapi import APIRouter
router = APIRouter(prefix="/workflows/probe")


@router.post("")
async def start(client):
    return await client.start_workflow("ProbeWorkflow", {}, id=f"probe-{1}", task_queue="probe-workflow")


@router.get("/{workflow_id}")
async def status(workflow_id: str):
    return {}
'''
    rules = _rules(checker.run_source("workflow_domains/probe/router.py", source))
    assert {"RT002", "RT003", "RT005", "RT006", "RT007"} <= rules


def test_checker_keeps_shared_lightweight_and_boundary_models_lenient(checker):
    source = '''
import httpx
from pydantic import BaseModel, ConfigDict


class ProbeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
'''
    rules = _rules(checker.run_source("shared/models/probe.py", source))
    assert {"SH001", "SH002"} <= rules


def test_a_documented_exception_needs_a_reason(checker):
    """`# conventions: allow <RULE> — <reason>` suppresses that rule on the
    statement below it; without a reason it suppresses nothing."""
    template = '''
from datetime import timedelta
from temporalio.common import RetryPolicy

{comment}
_AGENT = RetryPolicy(maximum_interval=timedelta(minutes=1), maximum_attempts=2)
'''
    path = "workflow_domains/probe/dispatch.py"
    reasoned = template.format(comment="# conventions: allow WF007 — the one bounded retry, CLAUDE.md §5")
    bare = template.format(comment="# conventions: allow WF007")
    assert "WF007" not in _rules(checker.run_source(path, reasoned))
    assert "WF007" in _rules(checker.run_source(path, bare))


def test_a_capped_activity_is_allowed_only_as_a_caught_best_effort_step(checker):
    source = '''
from datetime import timedelta
from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE
    from shared.interfaces.segment_lifecycle import get_segment


async def best_effort(policy):
    try:
        await workflow.execute_activity(
            get_segment, "x", task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=timedelta(seconds=90),
            schedule_to_close_timeout=timedelta(minutes=5), retry_policy=policy,
        )
    except ActivityError:
        return None


async def capped(policy):
    await workflow.execute_activity(
        get_segment, "x", task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        start_to_close_timeout=timedelta(seconds=90),
        schedule_to_close_timeout=timedelta(minutes=5), retry_policy=policy,
    )
'''
    violations = checker.run_source("workflow_domains/probe/dispatch.py", source)
    capped_at = [v.line for v in violations if v.rule == "WF005"]
    capped_def = source[: source.index("async def capped")].count("\n") + 1
    assert len(capped_at) == 1 and capped_at[0] > capped_def


def test_only_routes_that_start_work_must_answer_202(checker):
    source = '''
from fastapi import APIRouter
router = APIRouter(prefix="/workflows/probe")


async def _decide(client, workflow_id):
    return await client.get_workflow_handle(workflow_id).execute_update("approve")


@router.post("/probe-run/{workflow_id}/approve")
async def approve(workflow_id: str, client):
    return await _decide(client, workflow_id)
'''
    assert "RT007" not in _rules(checker.run_source("workflow_domains/probe/router.py", source))


def test_the_hook_names_the_docs_pages_of_the_edited_code(checker):
    reminder = checker.docs_reminder(REPO_ROOT / "workflow_domains/segment_lifecycle/allocate_segment.py")
    assert reminder is not None
    assert "docs/site/segment-lifecycle/allocate-segment.html" in reminder
    assert checker.docs_reminder(REPO_ROOT / "tests/test_api.py") is None
