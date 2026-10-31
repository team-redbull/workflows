"""The hub limb's day2 check, its Brain writer, and the activities' optional modes.

day2 and Brain are local bare repos shaped like the mock gitops-day2-prod
and redbull-brain; Brain's `scripts/brain.py` is a stand-in with the real
one's command line and output shape. The activities are called directly with
their settings patched: no worker, no network.
"""

from __future__ import annotations

import dataclasses
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from temporalio.testing import ActivityEnvironment

import activities.upgrade_gitops.activities as limb
from activities import git_repo
from activities.upgrade_gitops import brain_repo, day2_repo
from shared.exceptions import ValuesRepoGitError
from shared.models.cluster_upgrade import (
    AgentVerdict,
    Day2LayersRequest,
    IncidentRecord,
    IncidentRequest,
    ProposedAction,
    StepKind,
    StepReason,
    ToolCallRecord,
    UpgradeStep,
)
from tests.test_git_repo import SECRET, serve_over_https

CLUSTER = "ocp4-prod-alpha-site1"
MCE = "ocp4-prod-mce-site1-a"
NOW = datetime(2026, 11, 2, 9, 30, tzinfo=UTC)
# Shaped like an OpenShift OAuth token, built here so no token-shaped literal sits in the repo.
FAKE_TOKEN = "sha256" + "~" + "x" * 30


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _bare_repo(tmp_path: Path, name: str, files: dict[str, str]) -> Path:
    bare = tmp_path / f"{name}.git"
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / f"{name}-seed"
    _git("clone", "-q", str(bare), str(seed), cwd=tmp_path)
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return bare


# --- day2 ----------------------------------------------------------------------------------


@pytest.fixture
def day2(tmp_path: Path) -> Path:
    cluster = f"sigs/redbull/sites/site1/prod/mces/{MCE}/{CLUSTER}"
    return _bare_repo(tmp_path, "day2", {
        f"{cluster}/cluster-roles/values.yaml": "{}\n",
        f"{cluster}/logging/values.yaml": "{}\n",
        "sigs/redbull/defaults/hosted-clusters/kyverno/values.yaml": "{}\n",
        f"sigs/redbull/sites/site1/prod/mces/{MCE}/ocp4-prod-other-site1/gpu/values.yaml": "{}\n",
        # cluster-roles is not version-pinned; logging and kyverno are, gpu is not this cluster's.
        "sigs/redbull/operators/cluster-roles/values.yaml": "{}\n",
        "sigs/redbull/operators/logging/ocp-versions/4.19.53/values.yaml": "{}\n",
        "sigs/redbull/operators/kyverno/ocp-versions/4.19.53/values.yaml": "{}\n",
        "sigs/redbull/operators/kyverno/ocp-versions/4.20.44/values.yaml": "{}\n",
        "sigs/redbull/operators/gpu/ocp-versions/4.18.56/values.yaml": "{}\n",
    })


class TestDay2Layers:
    async def test_every_pinned_chart_of_the_cluster_needs_a_layer_per_version(self, day2):
        layers = await day2_repo.check_layers(
            repo_url=str(day2), token="", cluster=CLUSTER, versions=["4.19.53", "4.20.44"]
        )
        assert [(gap.sig, gap.chart, gap.version) for gap in layers.missing] == [("redbull", "logging", "4.20.44")]

    async def test_a_cluster_day2_does_not_know_still_gets_the_fleet_defaults(self, day2):
        layers = await day2_repo.check_layers(repo_url=str(day2), token="", cluster="ocp4-lab-new", versions=["4.21.40"])
        assert [(gap.chart, gap.version) for gap in layers.missing] == [("kyverno", "4.21.40")]

    async def test_not_configured_is_a_note_not_a_pass_in_silence(self, monkeypatch):
        monkeypatch.setattr(limb._settings, "day2_repo_url", "")
        layers = await limb.check_day2_layers(Day2LayersRequest(cluster=CLUSTER, versions=["4.19.53"]))
        assert layers.missing == [] and "DAY2_REPO_URL is not set" in layers.note


# --- Brain -----------------------------------------------------------------------------------

# Stands in for redbull-brain's scripts/brain.py, with the real one's output
# shape: `validate` prints an ERROR line naming any page that still holds a real
# cluster name, then its summary line, and exits 1 on an error; `index`
# rewrites INDEX.md. A marker in a page's text makes it misbehave.
BRAIN_PY = """\
import pathlib, re, sys, time
root = pathlib.Path(__file__).resolve().parent.parent
pages = sorted(root.glob("incidents/*/*.md"))
text = "".join(p.read_text() for p in pages)
if sys.argv[1] == "validate":
    if "HANG-VALIDATE" in text:
        time.sleep(30)
    if "CRASH-VALIDATE" in text:
        raise RuntimeError("the validator itself broke")
    if "EXIT-2" in text:
        sys.exit(2)
    bad = [p for p in pages if re.search(r"ocp4-(prod|prep|test)-[a-z0-9]", p.read_text()) or "REFUSE-ME" in p.read_text()]
    for p in bad:
        print(f"ERROR:   {p.relative_to(root)}: looks like a real cluster name")
    others = 100 if "NOISY" in text else 1 if "OTHERS-BROKEN" in text else 0
    for n in range(others):
        print(f"ERROR:   knowledge/hypershift/other-{n:03}.md: missing 'owners' (someone else's page)")
    print(f"\\n{len(pages)} pages checked — {len(bad) + others} errors, 0 warnings")
    sys.exit(1 if bad or others else 0)
if sys.argv[1] == "index":
    if "BREAK-INDEX" in text:
        sys.exit("index: cannot render")
    (root / "INDEX.md").write_text("# Team brain index\\n" + "".join(f"- {p.relative_to(root)}\\n" for p in pages))
"""


@pytest.fixture
def brain(tmp_path: Path) -> Path:
    return _bare_repo(tmp_path, "brain", {
        "scripts/brain.py": BRAIN_PY, "INDEX.md": "# Team brain index\n", "incidents/2026/.gitkeep": "",
    })


async def _write(brain: Path, **verdict_fields) -> IncidentRecord:
    return await brain_repo.write_incident(repo_url=str(brain), token="", request=_incident(**verdict_fields), now=NOW)


def _incident(**verdict_fields) -> IncidentRequest:
    verdict = AgentVerdict(
        diagnosis=verdict_fields.pop(
            "diagnosis",
            f"image-registry on {CLUSTER} is Degraded: the pruner Job failed. The MCE {MCE} pulls from "
            f"10.20.30.40 and registry.corp.internal; token {FAKE_TOKEN}",
        ),
        confidence="observed",
        verdict=verdict_fields.pop("verdict", "needs_human"),
        actions_taken=[ToolCallRecord(tool="read_pods", tier=0, result=f"pods of {CLUSTER}", at=NOW)],
        actions_proposed=[ProposedAction(action_id="a1", kind="rerun_cronjob",
                                         target="openshift-image-registry/image-pruner", rationale="re-run it")],
        **verdict_fields,
    )
    return IncidentRequest(
        workflow_id=f"upgrade-cluster-{CLUSTER}", invocation_id=f"upgrade-cluster-{CLUSTER}/1234",
        mce_cluster=MCE, cluster=CLUSTER, phase="preflight",
        step=UpgradeStep(kind=StepKind.CONTROL_PLANE, version="4.20.44", reason=StepReason.LADDER),
        verdict=verdict,
    )


class TestTheIncidentPage:
    def test_no_real_name_address_host_or_token_reaches_the_page(self):
        page = brain_repo.render(_incident(), NOW)
        for leak in (CLUSTER, MCE, "prod-alpha-site1", "10.20.30.40", "registry.corp.internal", FAKE_TOKEN):
            assert leak not in page, leak
        assert "<cluster>" in page and "<mce>" in page and "<ip>" in page and "<host>" in page
        assert "<redacted>" in page

    def test_the_frontmatter_brain_requires(self):
        head = brain_repo.render(_incident(), NOW).split("---")[1]
        for line in ("type: incident", "area: hypershift", "confidence: observed", "last_verified: 2026-11-02",
                     "applies_to: [hosted-cp, ocp-4.20]", "severity: sev3", "status: open",
                     "clusters: [<cluster>]", "started: 2026-11-02T09:30+00:00", "owners: [upgrade-agent]"):
            assert line in head, line
        assert "title: 2026-11-02 upgrade agent at preflight: needs_human" in head

    def test_no_link_or_template_placeholder_a_ci_warning_would_flag(self):
        page = brain_repo.render(_incident(diagnosis="see [[knowledge/x.md]] and [y](z.md) YYYY-MM-DD <Title>"), NOW)
        assert "[[" not in page and "](" not in page and "YYYY-MM-DD" not in page and "<Title>" not in page

    def test_the_path_follows_brains_layout_and_is_stable_per_invocation(self):
        path = brain_repo.incident_path(_incident(), NOW)
        assert path.startswith("incidents/2026/2026-11-02-hypershift-upgrade-preflight-") and path.endswith(".md")
        assert path == brain_repo.incident_path(_incident(verdict="retry_gate"), NOW)


class TestWritingToBrain:
    async def test_the_page_and_the_regenerated_index_are_pushed_to_main(self, brain):
        record = await _write(brain)
        assert record.note == "" and record.path.startswith("incidents/2026/")
        files = _git("show", "--name-only", "--format=%s", "main", cwd=brain).split()
        assert record.path in files and "INDEX.md" in files
        commit = _git("log", "-1", "--format=%an <%ae>|%s", "main", cwd=brain).strip()
        assert commit == (
            "cluster-upgrade-workflow <cluster-upgrade-workflow@redbull.local>|"
            "incident(hypershift): upgrade agent needs_human at preflight"
        )

    async def test_a_retry_finds_its_page_and_writes_nothing(self, brain):
        first = await _write(brain)
        head = _git("rev-parse", "main", cwd=brain).strip()
        later = datetime(2026, 11, 3, 1, 0, tzinfo=UTC)  # a retry the next day
        again = await brain_repo.write_incident(repo_url=str(brain), token="", request=_incident(), now=later)
        assert again.path == first.path and _git("rev-parse", "main", cwd=brain).strip() == head

    async def test_a_page_brains_validator_refuses_is_not_pushed_and_the_run_is_told(self, brain):
        head = _git("rev-parse", "main", cwd=brain).strip()
        record = await _write(brain, diagnosis="REFUSE-ME")
        assert record.path == "" and "Brain's validator refused" in record.note
        assert _git("rev-parse", "main", cwd=brain).strip() == head

    async def test_an_error_on_someone_elses_page_does_not_hold_ours_back(self, brain):
        record = await _write(brain, diagnosis="OTHERS-BROKEN")
        assert record.note == "" and record.path in _git("show", "--name-only", "--format=", "main", cwd=brain).split()

    async def test_the_refusal_is_read_in_the_whole_output_not_its_tail(self, brain):
        """Our page's ERROR line comes first, then far more than a message's tail of other pages' errors."""
        head = _git("rev-parse", "main", cwd=brain).strip()
        record = await _write(brain, diagnosis="NOISY REFUSE-ME")
        assert record.path == "" and "Brain's validator refused" in record.note
        assert _git("rev-parse", "main", cwd=brain).strip() == head


class TestBrainsValidatorFailsClosed:
    """No answer from brain.py is not a pass: the page is not pushed, and the run is told why."""

    async def _not_pushed(self, brain: Path, diagnosis: str) -> str:
        head = _git("rev-parse", "main", cwd=brain).strip()
        record = await _write(brain, diagnosis=diagnosis)
        assert record.path == "" and _git("rev-parse", "main", cwd=brain).strip() == head
        return record.note

    async def test_a_validator_that_times_out(self, brain, monkeypatch):
        monkeypatch.setattr(brain_repo, "VALIDATE_TIMEOUT_SECONDS", 1.0)
        note = await self._not_pushed(brain, "HANG-VALIDATE")
        assert "did not answer (exit 124)" in note and "timed out" in note

    async def test_a_validator_that_crashes(self, brain):
        note = await self._not_pushed(brain, "CRASH-VALIDATE")
        assert "did not answer (exit 1)" in note and "the validator itself broke" in note

    async def test_a_validator_exiting_otherwise_than_0_or_1(self, brain):
        assert "did not answer (exit 2)" in await self._not_pushed(brain, "EXIT-2")

    async def test_a_failed_index(self, brain):
        note = await self._not_pushed(brain, "BREAK-INDEX")
        assert "index failed (exit 1)" in note and "cannot render" in note

    async def test_a_brain_with_no_validator(self, tmp_path):
        bare = _bare_repo(tmp_path, "brainless", {"INDEX.md": "# Team brain index\n"})
        record = await _write(bare)
        assert record.path == "" and "no scripts/brain.py" in record.note
        assert _git("log", "--format=%s", "main", cwd=bare).split("\n")[0] == "seed"


def _reject_every_push(bare: Path) -> None:
    hook = bare / "hooks" / "pre-receive"
    hook.write_text(f"#!/bin/sh\necho 'main is protected, {SECRET}' >&2\nexit 1\n")
    hook.chmod(0o755)


class TestTheActivityNeverHoldsTheGate:
    """The gate's diagnosis waits on write_incident, so it ALWAYS returns: a note, never an error retried forever."""

    @pytest.fixture
    def configured(self, monkeypatch):
        def configure(url: str, token: str = "") -> ActivityEnvironment:
            monkeypatch.setattr(limb._settings, "brain_repo_url", url)
            monkeypatch.setattr(limb._settings, "brain_git_token", token)
            return ActivityEnvironment()

        return configure

    @staticmethod
    def _at(env: ActivityEnvironment, attempt: int) -> ActivityEnvironment:
        env.info = dataclasses.replace(env.info, attempt=attempt)
        return env

    async def test_brain_unreachable_gets_fresh_clones_then_a_note(self, configured, tmp_path):
        env = configured(str(tmp_path / "gone.git"))
        for attempt in range(1, limb.BRAIN_ATTEMPTS):
            with pytest.raises(ValuesRepoGitError):
                await self._at(env, attempt).run(limb.write_incident, _incident())
        record = await self._at(env, limb.BRAIN_ATTEMPTS).run(limb.write_incident, _incident())
        assert record.path == "" and f"after {limb.BRAIN_ATTEMPTS} attempts" in record.note

    async def test_a_push_rejected_every_time_ends_in_a_note_without_the_token(self, configured, tmp_path, monkeypatch):
        serve_over_https(tmp_path, monkeypatch)
        bare = _bare_repo(tmp_path, "brain", {"scripts/brain.py": BRAIN_PY, "INDEX.md": "# Team brain index\n"})
        _reject_every_push(bare)
        env = configured("https://git.test/brain.git", SECRET)
        with pytest.raises(ValuesRepoGitError) as caught:
            await self._at(env, 1).run(limb.write_incident, _incident())
        assert SECRET not in str(caught.value)
        record = await self._at(env, limb.BRAIN_ATTEMPTS).run(limb.write_incident, _incident())
        assert record.path == "" and "main is protected, ***" in record.note and SECRET not in record.note
        assert _git("log", "--format=%s", "main", cwd=bare).strip() == "seed"

    async def test_a_brain_with_no_main_is_a_note_at_once(self, configured, tmp_path):
        empty = tmp_path / "empty.git"
        _git("init", "-q", "--bare", "--initial-branch=main", str(empty), cwd=tmp_path)
        record = await self._at(configured(str(empty)), 1).run(limb.write_incident, _incident())
        assert record.path == "" and "the Brain repo has no `main` branch" in record.note

    async def test_anything_else_going_wrong_is_a_note_too(self, configured, monkeypatch):
        async def broken(**kwargs):
            raise KeyError("severity")

        env = configured("https://git.test/brain.git")
        monkeypatch.setattr(limb.brain_repo, "write_incident", broken)
        record = await self._at(env, 1).run(limb.write_incident, _incident())
        assert record.path == "" and "KeyError" in record.note

    async def test_not_configured_records_a_note_and_pushes_nothing(self, monkeypatch):
        monkeypatch.setattr(limb._settings, "brain_repo_url", "")
        record = await limb.write_incident(_incident())
        assert record.path == "" and "BRAIN_REPO_URL is not set" in record.note


class TestTheSettings:
    def test_an_optional_repo_may_be_empty_but_never_plain_http(self, monkeypatch):
        from shared.settings import UpgradeGitopsActivitySettings

        monkeypatch.setenv("BRAIN_REPO_URL", "")
        assert UpgradeGitopsActivitySettings().brain_repo_url == ""
        monkeypatch.setenv("BRAIN_REPO_URL", "http://git.test/brain.git")
        with pytest.raises(ValueError, match="https://"):
            UpgradeGitopsActivitySettings()

    def test_the_commits_name_the_upgrade_and_no_environment_changes_that(self):
        from activities.segment_lifecycle import values_repo
        from shared.settings import UpgradeGitopsActivitySettings

        assert git_repo.UPGRADE_AUTHOR == git_repo.GitIdentity(
            name="cluster-upgrade-workflow", email="cluster-upgrade-workflow@redbull.local"
        )
        assert git_repo.UPGRADE_AUTHOR.name != values_repo.GIT_USER_NAME
        assert not {"commit_author_name", "commit_author_email"} & set(UpgradeGitopsActivitySettings.model_fields)
