"""scripts/sync_platform_charts.sh: redbull-platform's copies of cluster-upgrade's per-MCE
charts follow deploy/charts/, and keep the image tag and chart version CI's bump wrote.

Needs mikefarah yq v4, as the script does: GitHub's runners have it, so CI's `test` job
(which gates every image and every push to redbull-platform) always runs these.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "sync_platform_charts.sh"
CHARTS = ("cluster-upgrade-worker", "upgrade-agent-worker")


def _has_mikefarah_yq() -> bool:
    if shutil.which("yq") is None:
        return False
    done = subprocess.run(["yq", "--version"], capture_output=True, text=True)
    return "mikefarah" in done.stdout + done.stderr


# The tests that bump or keep a tag need yq; the first copy and the refusal do not.
needs_yq = pytest.mark.skipif(not _has_mikefarah_yq(), reason="needs mikefarah yq v4 (GitHub's runners have it)")


def _sync(platform: Path) -> str:
    done = subprocess.run(["bash", str(SCRIPT), str(platform)], check=True, capture_output=True, text=True)
    return done.stdout


def _yq(expression: str, path: Path) -> None:
    subprocess.run(["yq", "-i", expression, str(path)], check=True)


def _bump(chart_dir: Path, version: str) -> None:
    """What ghcr-build-push.yml does to a chart on a main build, step for step."""
    values, chart = chart_dir / "values.yaml", chart_dir / "Chart.yaml"
    _yq(f'.image.repository = "ghcr.io/team-redbull/{chart_dir.name}"', values)
    _yq(f'.image.tag = "{version}"', values)
    _yq(f'.appVersion = "{version}"', chart)
    _yq(f'.version = "{version.removeprefix("v")}"', chart)


def _files(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): p.read_text() for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def platform(tmp_path: Path) -> Path:
    (tmp_path / "gitops" / "charts").mkdir(parents=True)
    return tmp_path


def _copy(platform: Path, chart: str) -> Path:
    return platform / "gitops" / "charts" / chart


def test_a_first_sync_copies_each_chart_verbatim(platform):
    out = _sync(platform)
    for chart in CHARTS:
        assert _files(_copy(platform, chart)) == _files(REPO_ROOT / "deploy" / "charts" / chart)
        assert f"{chart}: first copy" in out


@needs_yq
def test_an_unchanged_chart_comes_out_exactly_as_the_last_bump_left_it(platform):
    _sync(platform)
    for chart in CHARTS:
        _bump(_copy(platform, chart), "v1.0.7")
    bumped = {chart: _files(_copy(platform, chart)) for chart in CHARTS}
    _sync(platform)
    for chart in CHARTS:
        assert _files(_copy(platform, chart)) == bumped[chart], f"{chart}: a no-op sync changed the copy"


@needs_yq
def test_a_changed_chart_is_replaced_and_keeps_the_tag_and_version(platform):
    _sync(platform)
    for chart in CHARTS:
        copy = _copy(platform, chart)
        _bump(copy, "v1.0.7")
        (copy / "templates" / "removed-upstream.yaml").write_text("kind: ConfigMap\n")
        deployment = copy / "templates" / "deployment.yaml"
        deployment.write_text(deployment.read_text() + "# edited in redbull-platform by hand\n")
    _sync(platform)
    for chart in CHARTS:
        copy = _copy(platform, chart)
        source = REPO_ROOT / "deploy" / "charts" / chart
        assert not (copy / "templates" / "removed-upstream.yaml").exists()
        assert (copy / "templates" / "deployment.yaml").read_text() == (source / "templates" / "deployment.yaml").read_text()
        values, chart_yaml = (copy / "values.yaml").read_text(), (copy / "Chart.yaml").read_text()
        assert 'tag: "v1.0.7"' in values
        assert f"repository: ghcr.io/team-redbull/{chart}" in values
        assert "version: 1.0.7" in chart_yaml and 'appVersion: "v1.0.7"' in chart_yaml


def test_the_script_refuses_a_directory_that_is_not_redbull_platform(tmp_path):
    done = subprocess.run(["bash", str(SCRIPT), str(tmp_path)], capture_output=True, text=True)
    assert done.returncode != 0 and "no gitops/charts/" in done.stderr
