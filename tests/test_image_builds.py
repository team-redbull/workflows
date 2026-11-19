""".github/image-builds.json and the two build jobs: the brain first, every other image side by side.

scripts/image_changes.py decides WHICH images a push builds (tests/test_image_changes.py);
its `--first` hands `build-workflow-brain` the brain's row and its `--matrix` hands the
`build` job every other listed image's, so the workflow never decides again. What is
pinned here:

  * the rows are the images the script finds, one each, with that image's Dockerfile and
    its own chart in redbull-platform;
  * every input an image's build gets is in its row, so build.yml holds none: a change to
    build.yml is no image's change (tests/test_image_changes.py);
  * each matrix holds exactly the rows of the names it is given, refuses a name without
    one, and is nothing at all when there is no row to build;
  * both jobs pass every key of a row to the reusable workflow; `build` cancels no image
    for another's failure and waits for the brain and the chart sync.
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


def test_every_image_has_exactly_one_row():
    assert sorted(row["image-name"] for row in BUILDS) == sorted(IMAGES)


@pytest.mark.parametrize("row", BUILDS, ids=lambda row: row["image-name"])
def test_a_row_builds_its_images_dockerfile_into_its_own_chart(row):
    """The chart directory is named after the image: the reusable workflow tags each release
    `<chart>/vX.Y.Z` in redbull-platform, and the release notes read the tags by image name."""
    assert row["dockerfile"] == IMAGES[row["image-name"]].dockerfile
    assert row["chart-repo"] == "team-redbull/redbull-platform"
    assert row["helm-values-path"] == f"gitops/charts/{row['image-name']}/values.yaml"
    assert row["helm-image-path"] in {"image", "activityWorker.image"}


def test_the_brain_row_bumps_the_brains_own_chart():
    brain = next(row for row in BUILDS if row["image-name"] == BRAIN)
    assert brain == {"image-name": BRAIN, "dockerfile": "workflow_domains/Dockerfile",
                     "chart-repo": "team-redbull/redbull-platform",
                     "helm-values-path": f"gitops/charts/{BRAIN}/values.yaml", "helm-image-path": "image"}


@pytest.mark.parametrize("text", [
    '{"image-name": "x"}',                                                        # not a list
    '[{"image-name": "x", "dockerfile": "d", "chart-repo": "c", "helm-values-path": "v"}]',           # a key missing
    '[{"image-name": "x", "dockerfile": "d", "chart-repo": "c", "helm-values-path": "v", "helm-image-path": ""}]',
    '[{"image-name": "x", "dockerfile": "d", "chart-repo": "c", "helm-values-path": "v", "helm-image-path": "image", '
    '"extra": "y"}]',
])
def test_a_malformed_file_is_refused(tmp_path, text):
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github" / "image-builds.json").write_text(text)
    with pytest.raises(ValueError):
        image_changes.load_builds(tmp_path)


def test_an_image_with_two_rows_is_refused(tmp_path):
    row = {"image-name": "x", "dockerfile": "d", "chart-repo": "c", "helm-values-path": "v", "helm-image-path": "image"}
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


def test_the_first_matrix_is_the_brains_row_or_nothing():
    first = image_changes.first_matrix
    assert [row["image-name"] for row in first([BRAIN, "vm-lifecycle-worker"], BUILDS)["include"]] == [BRAIN]
    assert first(["vm-lifecycle-worker"], BUILDS)["include"] == [] and first([], BUILDS)["include"] == []


def test_every_image_is_every_row_across_the_two_matrices():
    both = image_changes.first_matrix(list(IMAGES), BUILDS)["include"] + image_changes.build_matrix(list(IMAGES), BUILDS)["include"]
    assert sorted(row["image-name"] for row in both) == sorted(row["image-name"] for row in BUILDS)


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


def test_the_cli_prints_the_brains_matrix_with_first():
    result = subprocess.run([sys.executable, "scripts/image_changes.py", "--first"], cwd=REPO_ROOT,
                            input='["workflows-orchestrator", "vm-lifecycle-worker"]', capture_output=True, text=True,
                            timeout=60)
    assert result.returncode == 0 and result.stdout.count("\n") == 1
    assert [row["image-name"] for row in json.loads(result.stdout)["include"]] == [BRAIN]


@pytest.mark.parametrize("stdin", ['[]', '["workflows-orchestrator"]', ''])
def test_the_cli_prints_nothing_when_no_row_is_built(stdin):
    """GitHub refuses a matrix with no entries: `build` is skipped on an empty output."""
    result = _run_matrix(stdin)
    assert result.returncode == 0 and result.stdout.strip() == ""


def test_the_cli_fails_on_a_name_without_a_row():
    result = _run_matrix('["no-such-worker"]')
    assert result.returncode != 0 and "no-such-worker" in result.stderr


# --- build.yml ---------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "output"), [("build-workflow-brain", "first"), ("build", "matrix")])
def test_both_build_jobs_hand_every_key_of_a_row_and_nothing_else(name, output):
    """build.yml holds no image's build input: every one comes from the row."""
    job = JOBS[name]
    assert job["uses"].startswith("team-redbull/.github/.github/workflows/ghcr-build-push.yml@")
    assert job["strategy"]["matrix"] == f"${{{{ fromJSON(needs.changes.outputs.{output}) }}}}"
    assert job["if"].endswith(f"needs.changes.outputs.{output} != '' }}}}")
    assert job["with"] == {key: f"${{{{ matrix.{key} }}}}" for key in image_changes.BUILD_KEYS}


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
    assert job["outputs"]["first"] == "${{ steps.filter.outputs.first }}"
    script = next(step["run"] for step in job["steps"] if step.get("id") == "filter")
    assert "python3 scripts/image_changes.py --matrix" in script and "python3 scripts/image_changes.py --first" in script
    assert 'echo "matrix=${matrix}"' in script and 'echo "first=${first}"' in script
    # The rule compares the rows with the diff base's: without --base every row counts as changed.
    assert 'python3 scripts/image_changes.py --base "${base}"' in script
