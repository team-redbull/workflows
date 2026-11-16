""".github/image-builds.json and the `build` job: every image but the brain, built side by side.

scripts/image_changes.py decides WHICH images a push builds (tests/test_image_changes.py);
its `--matrix` hands the `build` job the rows of those images, so the workflow never
decides again. What is pinned here:

  * the rows are the images the script finds, minus the brain (its own job, first), each
    with that image's Dockerfile and its own chart in redbull-platform;
  * the matrix holds exactly the rows of the names it is given, refuses a name without one,
    and is nothing at all when there is no row to build;
  * build.yml's `build` job passes every key of a row to the reusable workflow, cancels no
    image for another's failure and waits for the brain and the chart sync.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from scripts import image_changes

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILDS = image_changes.load_builds(REPO_ROOT)
IMAGES = {image.name: image for image in image_changes.discover_images(REPO_ROOT)}
BRAIN = image_changes.BRAIN_IMAGE
BUILD_YML = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "build.yml").read_text(encoding="utf-8"))
JOBS = BUILD_YML["jobs"]


def _matrix(*names: str) -> list[str]:
    return [row["image-name"] for row in image_changes.build_matrix(list(names), BUILDS)["include"]]


# --- the rows ----------------------------------------------------------------------------


def test_every_image_but_the_brain_has_exactly_one_row():
    assert sorted(row["image-name"] for row in BUILDS) == sorted(set(IMAGES) - {BRAIN})


@pytest.mark.parametrize("row", BUILDS, ids=lambda row: row["image-name"])
def test_a_row_builds_its_images_dockerfile_into_its_own_chart(row):
    """The chart directory is named after the image: the reusable workflow tags each release
    `<chart>/vX.Y.Z` in redbull-platform, and the release notes read the tags by image name."""
    assert row["dockerfile"] == IMAGES[row["image-name"]].dockerfile
    assert row["helm-values-path"] == f"gitops/charts/{row['image-name']}/values.yaml"
    assert row["helm-image-path"] in {"image", "activityWorker.image"}


def test_the_brain_job_bumps_the_brains_own_chart():
    brain = JOBS["build-workflow-brain"]["with"]
    assert brain["image-name"] == BRAIN
    assert brain["dockerfile"] == IMAGES[BRAIN].dockerfile
    assert brain["helm-values-path"] == f"gitops/charts/{BRAIN}/values.yaml"


@pytest.mark.parametrize("text", [
    '{"image-name": "x"}',                                                        # not a list
    '[{"image-name": "x", "dockerfile": "d", "helm-values-path": "v"}]',           # a key missing
    '[{"image-name": "x", "dockerfile": "d", "helm-values-path": "v", "helm-image-path": ""}]',
    '[{"image-name": "x", "dockerfile": "d", "helm-values-path": "v", "helm-image-path": "image", "extra": "y"}]',
])
def test_a_malformed_file_is_refused(tmp_path, text):
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github" / "image-builds.json").write_text(text)
    with pytest.raises(ValueError):
        image_changes.load_builds(tmp_path)


def test_an_image_with_two_rows_is_refused(tmp_path):
    row = {"image-name": "x", "dockerfile": "d", "helm-values-path": "v", "helm-image-path": "image"}
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github" / "image-builds.json").write_text(json.dumps([row, row]))
    with pytest.raises(ValueError, match="two rows"):
        image_changes.load_builds(tmp_path)


# --- the matrix --------------------------------------------------------------------------


def test_the_matrix_is_the_rows_of_the_names_given_in_the_files_order():
    assert _matrix("workflows-docs", BRAIN, "cluster-lifecycle-worker") == ["cluster-lifecycle-worker", "workflows-docs"]
    assert image_changes.build_matrix(["vm-lifecycle-worker"], BUILDS)["include"] == [
        row for row in BUILDS if row["image-name"] == "vm-lifecycle-worker"
    ]


def test_the_brain_alone_or_nothing_is_an_empty_matrix():
    assert _matrix(BRAIN) == [] and _matrix() == []


def test_every_image_is_every_row():
    assert _matrix(*IMAGES) == [row["image-name"] for row in BUILDS]


def test_a_name_without_a_row_is_refused_not_skipped():
    with pytest.raises(ValueError, match="no-such-worker"):
        image_changes.build_matrix(["vm-lifecycle-worker", "no-such-worker"], BUILDS)


def _run_matrix(stdin: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "scripts/image_changes.py", "--matrix"], cwd=REPO_ROOT,
                          input=stdin, capture_output=True, text=True, timeout=60)


def test_the_cli_prints_the_matrix_on_one_line():
    result = _run_matrix('["workflows-orchestrator", "vm-lifecycle-worker"]')
    assert result.returncode == 0
    assert result.stdout.count("\n") == 1                           # one GITHUB_OUTPUT line
    assert [row["image-name"] for row in json.loads(result.stdout)["include"]] == ["vm-lifecycle-worker"]


@pytest.mark.parametrize("stdin", ['[]', '["workflows-orchestrator"]', ''])
def test_the_cli_prints_nothing_when_no_row_is_built(stdin):
    """GitHub refuses a matrix with no entries: `build` is skipped on an empty output."""
    result = _run_matrix(stdin)
    assert result.returncode == 0 and result.stdout.strip() == ""


def test_the_cli_fails_on_a_name_without_a_row():
    result = _run_matrix('["no-such-worker"]')
    assert result.returncode != 0 and "no-such-worker" in result.stderr


# --- build.yml ---------------------------------------------------------------------------


def test_the_build_job_hands_every_key_of_a_row_to_the_reusable_workflow():
    job = JOBS["build"]
    assert job["uses"].startswith("team-redbull/.github/.github/workflows/ghcr-build-push.yml@")
    assert job["strategy"]["matrix"] == "${{ fromJSON(needs.changes.outputs.matrix) }}"
    for key in image_changes.BUILD_KEYS:
        assert job["with"][key] == f"${{{{ matrix.{key} }}}}"
    assert job["with"]["chart-repo"] == JOBS["build-workflow-brain"]["with"]["chart-repo"] == "team-redbull/redbull-platform"


def test_one_images_failure_cancels_no_other():
    assert JOBS["build"]["strategy"]["fail-fast"] is False


def test_the_images_wait_for_the_suite_the_brain_and_the_chart_sync():
    job = JOBS["build"]
    assert set(job["needs"]) == {"changes", "test", "build-workflow-brain", "sync-upgrade-charts"}
    # A skipped brain (a push that does not touch it) must not skip the others; a failed
    # one, a red test or a failed sync must.
    assert "!cancelled() && !failure()" in job["if"] and "needs.changes.outputs.matrix != ''" in job["if"]
    assert set(JOBS["build-workflow-brain"]["needs"]) == {"changes", "test"}
    assert set(JOBS["sync-upgrade-charts"]["needs"]) == {"changes", "test"}


def test_the_changes_job_takes_the_matrix_from_the_script():
    job = JOBS["changes"]
    assert job["outputs"]["matrix"] == "${{ steps.filter.outputs.matrix }}"
    script = next(step["run"] for step in job["steps"] if step.get("id") == "filter")
    assert "python3 scripts/image_changes.py --matrix" in script
    assert 'echo "matrix=${matrix}"' in script
