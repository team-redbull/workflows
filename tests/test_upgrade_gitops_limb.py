"""The hub limb's day2 check, its Brain writer, and the activities' optional modes.

day2 and Brain are local bare repos shaped like the mock gitops-day2-prod
and redbull-brain; Brain's `scripts/brain.py` is a stand-in with the real
one's command line and output shape. The activities are called directly with
their settings patched: no worker, no network.
"""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

import activities.upgrade_gitops.activities as limb
from activities.upgrade_gitops import brain_repo, day1_repo, day2_repo
from shared.models.cluster_upgrade import (
    AgentVerdict,
    Day2LayersRequest,
    IncidentRequest,
    ProposedAction,
    StepKind,
    StepReason,
    ToolCallRecord,
    UpgradeStep,
)

CLUSTER = "ocp4-prod-alpha-site1"
MCE = "ocp4-prod-mce-site1-a"
AUTHOR = day1_repo.GitIdentity(name="cluster-upgrade-workflow", email="cluster-upgrade-workflow@test")
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

# Stands in for redbull-brain's scripts/brain.py: `validate` prints an ERROR line
# naming any page that still holds a real cluster name, `index` rewrites INDEX.md.
BRAIN_PY = """\
import pathlib, re, sys
root = pathlib.Path(__file__).resolve().parent.parent
pages = sorted(root.glob("incidents/*/*.md"))
if sys.argv[1] == "validate":
    bad = [p for p in pages if re.search(r"ocp4-(prod|prep|test)-[a-z0-9]", p.read_text()) or "REFUSE-ME" in p.read_text()]
    for p in bad:
        print(f"ERROR:   {p.relative_to(root)}: looks like a real cluster name")
    print(f"{len(pages)} pages checked — {len(bad)} errors, 0 warnings")
    sys.exit(1 if bad else 0)
if sys.argv[1] == "index":
    (root / "INDEX.md").write_text("# Team brain index\\n" + "".join(f"- {p.relative_to(root)}\\n" for p in pages))
"""


@pytest.fixture
def brain(tmp_path: Path) -> Path:
    return _bare_repo(tmp_path, "brain", {
        "scripts/brain.py": BRAIN_PY, "INDEX.md": "# Team brain index\n", "incidents/2026/.gitkeep": "",
    })


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
        record = await brain_repo.write_incident(
            repo_url=str(brain), token="", request=_incident(), author=AUTHOR, now=NOW
        )
        assert record.note == "" and record.path.startswith("incidents/2026/")
        files = _git("show", "--name-only", "--format=%s", "main", cwd=brain).split()
        assert record.path in files and "INDEX.md" in files
        subject = _git("log", "-1", "--format=%s", "main", cwd=brain).strip()
        assert subject == "incident(hypershift): upgrade agent needs_human at preflight"

    async def test_a_retry_finds_its_page_and_writes_nothing(self, brain):
        first = await brain_repo.write_incident(repo_url=str(brain), token="", request=_incident(), author=AUTHOR, now=NOW)
        head = _git("rev-parse", "main", cwd=brain).strip()
        later = datetime(2026, 11, 3, 1, 0, tzinfo=UTC)  # a retry the next day
        again = await brain_repo.write_incident(repo_url=str(brain), token="", request=_incident(), author=AUTHOR, now=later)
        assert again.path == first.path and _git("rev-parse", "main", cwd=brain).strip() == head

    async def test_a_page_brains_validator_refuses_is_not_pushed_and_the_run_is_told(self, brain):
        head = _git("rev-parse", "main", cwd=brain).strip()
        record = await brain_repo.write_incident(
            repo_url=str(brain), token="", request=_incident(diagnosis="REFUSE-ME"), author=AUTHOR, now=NOW
        )
        assert record.path == "" and "Brain's validator refused" in record.note
        assert _git("rev-parse", "main", cwd=brain).strip() == head

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

    def test_the_commits_name_the_upgrade_not_segment_allocation(self):
        from shared.settings import UpgradeGitopsActivitySettings

        assert UpgradeGitopsActivitySettings().commit_author_name == "cluster-upgrade-workflow"
