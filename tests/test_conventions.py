"""The repo's conventions, enforced: scripts/check_conventions.py in whole-repo mode.

CLAUDE.md says WHY each rule exists; the checker says WHAT can be verified from
source; this test makes a slip a red `pytest` (and a red CI run) instead of a
workflow stuck RUNNING in production or a limb that crash-loops at startup.

The same script runs from the Claude Code PostToolUse hook in
.claude/settings.json on every Edit/Write. This is the copy that also catches a
teammate who never opens Claude Code.
"""

from __future__ import annotations

import ast

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


def test_checker_catches_a_whole_response_body_in_an_error(checker):
    # 2026-11-09: a > 2 MiB body copied into an activity error failed a run as
    # Temporal's non-retryable "Failure exceeds size limit.".
    unbounded = '''
def _detail(resp):
    return resp.text


def _fail(response):
    raise RuntimeError(f"failed: {response.status_code} {response.text}")
'''
    violations = checker.run_source("activities/probe/client.py", unbounded)
    assert sorted(v.line for v in violations if v.rule == "AC007") == [3, 7]

    bounded = '''
def _detail(resp):
    return _bounded(resp.text)


def _fail(response):
    raise RuntimeError(f"failed: {response.status_code} {response.text[:500]}")
'''
    assert "AC007" not in _rules(checker.run_source("activities/probe/client.py", bounded))


def test_git_runs_only_through_git_repo(checker):
    # CLAUDE.md §5: one git client, a fresh_clone per attempt, worked through its Clone.
    slips = '''
import asyncio
import subprocess

import git
from activities import git_repo
from activities.git_repo import run_git


async def own_git(dest):
    await asyncio.create_subprocess_exec("git", "clone", "x", str(dest))
    subprocess.run(["git", "status"], check=True)


async def low_level(dest):
    await git_repo.clone(repo_url="x", branch="main", token="", dest=dest, missing_message="-")
    await run_git(["rev-parse", "HEAD"], cwd=dest, secrets=[])
'''
    violations = checker.run_source("activities/probe/repo.py", slips)
    assert sorted(v.line for v in violations if v.rule == "AC008") == [5, 11, 12, 16, 17]

    canonical = '''
import asyncio

from activities import git_repo


async def publish(repo_url, token):
    async with git_repo.fresh_clone(repo_url=repo_url, token=token, repo="day1", prefix="p-") as day1:
        await day1.commit_and_push(paths=["a"], message="m", author=git_repo.UPGRADE_AUTHOR)
    await asyncio.create_subprocess_exec("oc", "adm", "release", "info")
'''
    assert "AC008" not in _rules(checker.run_source("activities/probe/repo.py", canonical))
    assert "AC008" not in _rules(checker.run_source("activities/git_repo.py", slips))


def test_a_fresh_clone_is_the_writers_and_a_read_takes_the_snapshot(checker):
    # CLAUDE.md §5: a fresh_clone / fresh_main block must reach commit_and_push; a read is a snapshot.
    reads = '''
from activities import git_repo
from activities.day1_repo import fresh_main


async def read_day2(repo_url, token):
    async with git_repo.fresh_clone(repo_url=repo_url, token=token, repo="day2", prefix="r-") as day2:
        return (day2.path / "a").read_text()


async def read_day1(repo_url, token):
    async with fresh_main(repo_url=repo_url, token=token, prefix="r-") as day1:
        return await day1.head()


async def read_then_write_elsewhere(repo_url, token):
    async with fresh_main(repo_url=repo_url, token=token, prefix="r-") as day1:
        text = (day1.path / "a").read_text()
    async with fresh_main(repo_url=repo_url, token=token, prefix="w-") as day1:
        await day1.commit_and_push(paths=["a"], message=text, author=git_repo.UPGRADE_AUTHOR)
'''
    violations = checker.run_source("activities/probe/repo.py", reads)
    assert sorted(v.line for v in violations if v.rule == "AC008") == [7, 12, 17]
    assert all("snapshot" in v.message for v in violations if v.rule == "AC008")

    canonical = '''
from activities import day1_repo, git_repo


async def read_day2(repo_url, token):
    async with git_repo.snapshot(repo_url=repo_url, token=token, repo="day2") as day2:
        return (day2.path / "a").read_text()


async def read_day1(repo_url, token):
    async with day1_repo.main_snapshot(repo_url=repo_url, token=token) as day1:
        return day1.revision


async def write(repo_url, token, already):
    async with day1_repo.fresh_main(repo_url=repo_url, token=token, prefix="w-") as day1:
        if already(day1.path):
            return None
        return await day1.commit_and_push(paths=["a"], message="m", author=git_repo.UPGRADE_AUTHOR)


async def rerun_check(repo_url, token, branch):
    # conventions: allow AC008 — the writer's own "already there" half, on its per-cluster branch
    async with git_repo.fresh_clone(repo_url=repo_url, token=token, repo="day1", prefix="c-", branch=branch) as day1:
        return (day1.path / "a").is_file()
'''
    assert "AC008" not in _rules(checker.run_source("activities/probe/repo.py", canonical))


def test_day1s_layout_is_written_down_only_in_the_day1_reader(checker):
    # CLAUDE.md §5: a module that spells day1's folders is growing its own walk of day1.
    walks = '''
import re
from pathlib import Path

SITES_ROOT = "sites"
HOSTED_CLUSTERS = "hostedClusters"
_FILE = re.compile(r"sites/(?P<site>[^/]+)/mces/(?P<mce>[^/]+)/hostedClusters/(?P<name>[^/]+)[.]yaml")


def seed_folder(clone: Path, site: str, seed: str) -> Path:
    return clone / "sites" / site / "mces" / seed


def cluster_file(site: str, mce: str, cluster: str) -> str:
    return f"sites/{site}/mces/{mce}/hostedClusters/{cluster}.yaml"


def every_cluster(clone: Path):
    return clone.glob("sites/*/mces/*/hostedClusters/*.yaml")


def folder(site: str, mce: str) -> str:
    return "/".join(["sites", site, "mces", mce])
'''
    violations = [v for v in checker.run_source("activities/probe/day1.py", walks) if v.rule == "AC009"]
    assert sorted(v.line for v in violations) == [5, 6, 7, 11, 11, 15, 19, 23]
    assert all("Day1" in v.message and "cluster_file_path" in v.message for v in violations)
    in_a_workflow = [v for v in checker.run_source("workflow_domains/probe/place_vm.py", walks) if v.rule == "AC009"]
    assert len(in_a_workflow) == len(violations)

    # The reader itself, and code outside the limbs and the brain, may spell it.
    assert "AC009" not in _rules(checker.run_source("activities/day1_repo.py", walks))
    assert "AC009" not in _rules(checker.run_source("shared/upgrade_layout.py", walks))

    canonical = '''
"""Reads `sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml` through Day1."""
from activities import day1_repo


class Reader:
    """Knows `sites/` only by name."""

    async def kv_clusters(self, repo_url, token, site, zone):
        """The zone's clusters, from `sites/<site>/mces/ocp4-seed-<site>-<N>/hostedClusters/`."""
        async with day1_repo.main_snapshot(repo_url=repo_url, token=token) as day1:
            return day1.kv_clusters(site=site, zone=zone)


def where(site, mce, cluster):
    return day1_repo.cluster_file_path(day1_repo.mce_dir(site, mce), cluster)


def refuse(path):
    raise ValueError(f"{path} is not under sites/<site>/mces/<mce>/hostedClusters/: no MCE to run on")


def not_day1(response, text):
    sites = response.json().get("sites")
    websites = "websites/index.html"
    return sites, websites, text.split("/"), "a/b".join(["sites", "mces"])


def day2_folder(sig, cluster):
    # conventions: allow AC009 — day2's tree, not day1's
    return sig.glob("sites/*/*/mces/*/" + cluster)
'''
    assert "AC009" not in _rules(checker.run_source("activities/probe/day1.py", canonical))
    bare = canonical.replace("# conventions: allow AC009 — day2's tree, not day1's", "# conventions: allow AC009")
    assert "AC009" in _rules(checker.run_source("activities/probe/day1.py", bare))


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


_PROBE_ROW = {
    "image-name": "probe-worker",
    "dockerfile": "activities/probe/Dockerfile",
    "chart-repo": "team-redbull/redbull-platform",
    "helm-values-path": "gitops/charts/probe-worker/values.yaml",
    "helm-image-path": "activityWorker.image",
}


def test_a_limb_image_is_version_tagged_never_built_image_only(checker):
    """A limb is a row of .github/image-builds.json, built by the reusable workflow (vX.Y.Z on
    main); a job building its Dockerfile straight with docker/build-push-action is refused."""
    image_only = """
  build-probe-worker:
    steps:
      - uses: docker/build-push-action@v5
        with:
          file: activities/probe/Dockerfile
          tags: ghcr.io/${{ github.repository_owner }}/probe-worker:${{ steps.tag.outputs.tag }}
"""
    assert checker.limb_build_problems("probe", "", [_PROBE_ROW]) == []
    problems = checker.limb_build_problems("probe", image_only, [])
    assert len(problems) == 1 and "image-only" in problems[0] and "vX.Y.Z" in problems[0]
    missing = checker.limb_build_problems("probe", "", [])
    assert len(missing) == 1 and "no build" in missing[0] and ".github/image-builds.json" in missing[0]


def test_a_limb_row_carries_its_own_image_name_and_chart(checker):
    """A row copied from another limb and left on that limb's name or chart: the script answers
    with THIS limb's name and finds no row, or the bump lands in the other limb's chart."""
    copied = {**_PROBE_ROW, "image-name": "other-worker"}
    problems = checker.limb_build_problems("probe", "", [copied])
    assert len(problems) == 1 and "`probe-worker`" in problems[0]
    wrong_chart = {**_PROBE_ROW, "helm-values-path": "gitops/charts/other-worker/values.yaml"}
    problems = checker.limb_build_problems("probe", "", [wrong_chart])
    assert len(problems) == 1 and "gitops/charts/probe-worker/values.yaml" in problems[0]


def test_the_build_rows_must_be_readable(checker):
    assert checker.image_builds('[{"image-name": "probe-worker"}]') == ([{"image-name": "probe-worker"}], None)
    assert "missing" in checker.image_builds(None)[1]
    assert "not JSON" in checker.image_builds("{")[1]
    assert "list of rows" in checker.image_builds('{"image-name": "probe-worker"}')[1]


def test_the_settings_package_re_exports_nothing(checker):
    """SH007: a re-export loads every worker's settings into every image."""
    source = "from shared.settings.temporal import TemporalSettings\n"
    assert "SH007" in _rules(checker.run_source("shared/settings/__init__.py", source))
    assert "SH007" not in _rules(checker.run_source("shared/settings/__init__.py", '"""Docstring only."""\n'))
    assert "SH007" not in _rules(checker.run_source("shared/settings/temporal.py", source))


def test_a_settings_class_in_any_module_of_the_package_is_checked(checker):
    """SH006 and DL004 follow the split: they must not go quiet on shared/settings/<limb>.py."""
    source = "from pydantic_settings import BaseSettings\n\n\nclass ProbeActivitySettings(BaseSettings):\n    probe_url: str\n"
    assert "SH006" in _rules(checker.run_source("shared/settings/probe.py", source))
    classes = [node for node in ast.parse(source).body if isinstance(node, ast.ClassDef)]
    assert checker.undocumented_settings_keys(classes, "OTHER_KEY=x\n") == [
        "`PROBE_URL` (ProbeActivitySettings) is not documented in .env.example (§8)"
    ]
    assert checker.undocumented_settings_keys(classes, "# PROBE_URL=x\n") == []


def test_every_settings_class_of_the_repo_is_found(checker):
    """DL004/DL005 read shared/settings/*.py: one class per module, none missed."""
    found = sorted(node.name for node in checker._settings_classes())
    modules = sorted(p.stem for p in (REPO_ROOT / "shared" / "settings").glob("*.py") if p.stem != "__init__")
    assert len(found) == len(modules) == 9, (found, modules)
    assert "TemporalSettings" in found and "ClusterLifecycleActivitySettings" in found


def test_the_hook_names_the_docs_pages_of_the_edited_code(checker):
    reminder = checker.docs_reminder(REPO_ROOT / "workflow_domains/segment_lifecycle/allocate_segment.py")
    assert reminder is not None
    assert "docs/site/segment-lifecycle/allocate-segment.html" in reminder
    assert checker.docs_reminder(REPO_ROOT / "tests/test_api.py") is None


def test_a_documented_exception_may_sit_above_a_decorated_class(checker):
    source = '''
from temporalio import workflow


# conventions: allow WF014 — a synchronous question answered in the response
@workflow.defn
class ProbeWorkflow:
    @workflow.run
    async def run(self, run_args):
        return None
'''
    warnings = {v.rule for v in checker.run_source("workflow_domains/probe/probe.py", source)}
    assert "WF014" not in warnings
