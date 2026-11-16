"""scripts/image_changes.py — which images a push must build (CI's `changes` job).

A build that was needed and skipped ships a stale image and nothing reports it,
so what is pinned here is SAFETY first, precision second:

  * everything each entrypoint REALLY loads (a fresh interpreter, sys.modules)
    is in the set the script derives statically — the script may over-build,
    never under-build;
  * what it cannot read as a Python module is built wide; the files that decide
    every build affect every image;
  * the names it answers with are the names build.yml's jobs ask for;
  * and the precision itself, on representative files: one worker's settings
    or activity code is that worker's image only.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import image_changes

REPO_ROOT = Path(__file__).resolve().parent.parent
IMAGES = image_changes.discover_images(REPO_ROOT)
BY_NAME = {image.name: image for image in IMAGES}
PYTHON_IMAGES = [image for image in IMAGES if image.entrypoints]
LIMBS = sorted(name for name in BY_NAME if name.endswith("-worker"))
BRAIN = "workflows-orchestrator"
DOCS = "workflows-docs"


def _affected(*paths: str) -> list[str]:
    return sorted(image_changes.affected_images(list(paths), IMAGES, REPO_ROOT))


# --- the images, and their names ----------------------------------------------------------


def test_every_image_of_the_repo_is_found():
    assert sorted(BY_NAME) == sorted([BRAIN, DOCS, *LIMBS])
    assert len(LIMBS) == len(list((REPO_ROOT / "activities").glob("*/Dockerfile"))) == 8
    assert BY_NAME[BRAIN].entrypoints == ("workflow_domains.main_worker_init", "workflow_domains.api")
    for limb in LIMBS:
        folder = limb.removesuffix("-worker").replace("-", "_")
        assert BY_NAME[limb].entrypoints == (f"activities.{folder}.worker_init",)
        assert BY_NAME[limb].copied_files == (f"activities/{folder}/requirements.txt",)
        assert set(BY_NAME[limb].copied_dirs) == {"shared/", "activities/"}
    assert BY_NAME[DOCS].entrypoints == () and "docs/site/" in BY_NAME[DOCS].copied_dirs


def test_the_names_are_the_ones_build_yml_asks_for():
    """Each build job asks `contains(fromJSON(...images...), '<name>')`: a name the script
    never answers is an image that is never built."""
    build_yml = (REPO_ROOT / ".github" / "workflows" / "build.yml").read_text(encoding="utf-8")
    asked = set(re.findall(r"contains\(fromJSON\(needs\.changes\.outputs\.images \|\| '\[\]'\), '([a-z0-9-]+)'\)", build_yml))
    built = {
        name for name in re.findall(r"^      image-name: ([a-z0-9-]+)$", build_yml, re.M)
    }
    assert asked == built == set(BY_NAME)
    # No job is left on the retired per-folder flags.
    assert not re.search(r"outputs\.(brain|limb|docs)\b", build_yml)


def test_the_brain_serves_the_api_from_the_same_image():
    """The chart starts the brain image a second time with uvicorn: api.py and the
    routers are loaded by nothing main_worker_init imports."""
    loaded = BY_NAME[BRAIN].loaded
    assert "workflow_domains/api.py" in loaded
    assert "workflow_domains/routers/runs.py" in loaded
    assert "workflow_domains/cluster_lifecycle/router.py" in loaded


# --- safety: the static set covers what really loads --------------------------------------

_PROBE = """
import json, os, sys
import importlib
for module in {modules!r}:
    importlib.import_module(module)
root = {root!r} + os.sep
files = sorted(
    os.path.relpath(m.__file__, {root!r}).replace(os.sep, "/")
    for m in list(sys.modules.values())
    if getattr(m, "__file__", None) and os.path.abspath(m.__file__).startswith(root)
    and not os.path.relpath(m.__file__, {root!r}).startswith((".venv", "tests", "venv"))
)
print(json.dumps(files))
"""


@pytest.mark.parametrize("image", PYTHON_IMAGES, ids=lambda image: image.name)
def test_everything_an_entrypoint_really_loads_is_in_the_static_set(image):
    """A fresh interpreter imports the image's entrypoints; every repo file then in
    sys.modules must be in the set the script derived from the source alone."""
    probe = _PROBE.format(root=str(REPO_ROOT), modules=list(image.entrypoints))
    # The suite's environment (tests/conftest.py set every settings key in os.environ),
    # but NOT conftest itself: it imports every settings module, which is exactly what
    # an image must not be seen to load.
    result = subprocess.run([sys.executable, "-c", probe], cwd=REPO_ROOT, capture_output=True, text=True, timeout=120,
                            env={**os.environ, "PYTHONPATH": str(REPO_ROOT)})
    assert result.returncode == 0, result.stderr[-2000:]
    really_loaded = set(json.loads(result.stdout.strip().splitlines()[-1]))
    assert really_loaded, "the probe loaded nothing — it proves nothing"
    missing = really_loaded - image.loaded
    assert not missing, f"{image.name} loads files the static graph missed: {sorted(missing)}"


def test_a_lazy_import_and_a_relative_import_are_followed(tmp_path):
    (tmp_path / "pkg" / "sub").mkdir(parents=True)
    (tmp_path / "pkg" / "__init__.py").write_text("")
    (tmp_path / "pkg" / "sub" / "__init__.py").write_text("")
    (tmp_path / "pkg" / "entry.py").write_text(
        "from pkg import eager\n\n\ndef run():\n    from pkg.sub import lazy\n    import pkg.plain\n"
    )
    (tmp_path / "pkg" / "eager.py").write_text("from . import sibling\nfrom .sub.deep import thing\n")
    (tmp_path / "pkg" / "sibling.py").write_text("")
    (tmp_path / "pkg" / "plain.py").write_text("")
    (tmp_path / "pkg" / "unused.py").write_text("")
    (tmp_path / "pkg" / "sub" / "lazy.py").write_text("")
    (tmp_path / "pkg" / "sub" / "deep.py").write_text("thing = 1\n")
    loaded = image_changes.loaded_files(tmp_path, ("pkg.entry",))
    assert loaded == {
        "pkg/__init__.py", "pkg/entry.py", "pkg/eager.py", "pkg/sibling.py", "pkg/plain.py",
        "pkg/sub/__init__.py", "pkg/sub/lazy.py", "pkg/sub/deep.py",
    }


def test_no_image_loads_code_by_a_name_built_at_run_time():
    """The graph follows `import` statements. importlib / __import__ with a computed name
    would load a file it cannot see — none exists; adding one needs a rule here first."""
    pattern = re.compile(r"\b(importlib|__import__|import_module|runpy|pkgutil)\b")
    offenders = [
        path for image in PYTHON_IMAGES for path in sorted(image.loaded)
        if pattern.search((REPO_ROOT / path).read_text(encoding="utf-8"))
    ]
    assert offenders == []


# --- safety: when unsure, build wide ------------------------------------------------------


@pytest.mark.parametrize("path", image_changes.GLOBAL_TRIGGERS)
def test_what_decides_every_build_affects_every_image(path):
    assert _affected(path) == sorted(BY_NAME)


def test_a_data_file_in_a_copied_folder_is_built_wide():
    # The temporary UI pages are read from disk at run time: no import names them.
    assert _affected("workflow_domains/cluster_upgrade/ui/index.html") == [BRAIN]
    assert _affected("activities/server_provisioning/some-template.j2") == LIMBS
    assert _affected("shared/some-data.json") == sorted([BRAIN, *LIMBS])


def test_a_deleted_or_renamed_module_is_built_wide():
    assert _affected("activities/segment_lifecycle/values_repo.py") == LIMBS          # gone since 2026-11-11
    assert _affected("shared/settings.py") == sorted([BRAIN, *LIMBS])                 # gone since the split
    assert _affected("workflow_domains/no_such_module.py") == [BRAIN]


def test_a_module_that_does_not_parse_is_built_wide(tmp_path):
    (tmp_path / "activities" / "probe").mkdir(parents=True)
    (tmp_path / "activities" / "__init__.py").write_text("")
    (tmp_path / "activities" / "probe" / "__init__.py").write_text("")
    (tmp_path / "activities" / "probe" / "worker_init.py").write_text("x = 1\n")
    (tmp_path / "activities" / "probe" / "broken.py").write_text("def (:\n")
    image = image_changes.Image(
        name="probe-worker", dockerfile="activities/probe/Dockerfile",
        entrypoints=("activities.probe.worker_init",), copied_dirs=("activities/",),
    )
    image.loaded = image_changes.loaded_files(tmp_path, image.entrypoints)
    affected = image_changes.affected_images(["activities/probe/broken.py"], [image], tmp_path)
    assert list(affected) == ["probe-worker"] and "built wide" in affected["probe-worker"][0]


# --- precision: an image is built for what it loads ---------------------------------------


# Workers that also load ANOTHER worker's settings module, because they register some
# of its activities: the server-lifecycle worker runs cluster-upgrade's
# read_hosted_cluster / sync_argo_application for update-nodepool (CLAUDE.md §2, §4),
# so cluster-upgrade's settings are its settings too.
_ALSO_LOADED_BY = {"cluster-upgrade-worker": ["server-lifecycle-worker"]}


@pytest.mark.parametrize("limb", LIMBS)
def test_a_workers_own_files_build_that_worker_only(limb):
    folder = limb.removesuffix("-worker").replace("-", "_")
    assert _affected(f"shared/settings/{folder}.py") == sorted([limb, *_ALSO_LOADED_BY.get(limb, [])])
    assert _affected(f"activities/{folder}/Dockerfile") == [limb]
    assert _affected(f"activities/{folder}/requirements.txt") == [limb]
    assert limb in _affected(f"activities/{folder}/activities.py")
    assert BRAIN not in _affected(f"activities/{folder}/activities.py")
    assert limb in _affected(f"activities/{folder}/worker_init.py")


@pytest.mark.parametrize(("path", "expected"), [
    # one worker's activity code: that worker
    ("activities/cluster_lifecycle/git_provider.py", ["cluster-lifecycle-worker"]),
    ("activities/vm_lifecycle/placement.py", ["vm-lifecycle-worker"]),
    ("activities/segment_lifecycle/dhcp_api.py", ["segment-lifecycle-worker"]),
    # the shared git client: the four workers that use git (server-lifecycle for
    # update-nodepool's day1 read and replicas commit, through upgrade-gitops's day1_repo)
    ("activities/git_repo.py", ["cluster-lifecycle-worker", "server-lifecycle-worker",
                                "upgrade-gitops-worker", "vm-lifecycle-worker"]),
    ("activities/day1_repo.py", ["cluster-lifecycle-worker", "server-lifecycle-worker",
                                 "upgrade-gitops-worker", "vm-lifecycle-worker"]),
    ("activities/upgrade_gitops/day1_repo.py", ["server-lifecycle-worker", "upgrade-gitops-worker"]),
    # one limb's models: the brain (it builds the requests) and that limb
    ("shared/models/cluster_lifecycle.py", ["cluster-lifecycle-worker", "workflows-orchestrator"]),
    # an interface is the BRAIN's stub: the limb registers its implementation by
    # name and never imports it (tests/test_contract.py keeps the two equal)
    ("shared/interfaces/cluster_lifecycle.py", ["workflows-orchestrator"]),
    # workflow code and the API: the brain
    ("workflow_domains/cluster_lifecycle/provision_cluster.py", ["workflows-orchestrator"]),
    ("workflow_domains/routers/runs.py", ["workflows-orchestrator"]),
    ("workflow_domains/Dockerfile", ["workflows-orchestrator"]),
    # the docs site: what its Dockerfile copies, nothing else under docs/
    ("docs/site/index.html", ["workflows-docs"]),
    ("docs/nginx.conf", ["workflows-docs"]),
    ("docs/Dockerfile", ["workflows-docs"]),
    ("docs/design/provision-cluster.md", []),
    # no image at all
    ("README.md", []),
    ("CLAUDE.md", []),
    ("tests/test_settings.py", []),
    ("scripts/check_conventions.py", []),
    ("deploy/charts/cluster-upgrade-worker/values.yaml", []),
    (".env.example", []),
])
def test_representative_files(path, expected):
    assert _affected(path) == sorted(expected)


@pytest.mark.parametrize("path", [
    "shared/exceptions.py", "shared/consts.py", "shared/settings/temporal.py",
    "shared/temporal_client.py", "shared/logging_config.py", "shared/settings/__init__.py",
])
def test_what_every_image_loads_builds_every_python_image(path):
    """The shared core: a change there is everyone's change. shared/exceptions.py is the
    one that changes often, and it stays ONE file on purpose (CLAUDE.md §8)."""
    assert _affected(path) == sorted([BRAIN, *LIMBS])


def test_a_push_of_several_files_is_the_union():
    assert _affected("shared/settings/vm_lifecycle.py", "docs/site/index.html", "README.md") == [
        "vm-lifecycle-worker", "workflows-docs",
    ]
    assert _affected() == [] and _affected("", "  ") == []


# --- the command line the workflow runs ---------------------------------------------------


def _run(*args: str, stdin: str = "") -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "scripts/image_changes.py", *args], cwd=REPO_ROOT,
                          input=stdin, capture_output=True, text=True, timeout=60)


def test_the_cli_answers_a_json_list_on_stdout_and_its_reasons_on_stderr():
    result = _run(stdin="activities/vm_lifecycle/placement.py\nREADME.md\n")
    assert result.returncode == 0
    assert json.loads(result.stdout) == ["vm-lifecycle-worker"]          # stdout is the list and nothing else
    assert "vm-lifecycle-worker: BUILD" in result.stderr and "placement.py (loaded)" in result.stderr
    assert "workflows-orchestrator: unaffected" in result.stderr


def test_the_cli_answers_an_empty_list_for_no_change():
    assert json.loads(_run(stdin="").stdout) == []


def test_the_cli_lists_every_image_and_the_table():
    assert sorted(json.loads(_run("--all").stdout)) == sorted(BY_NAME)
    table = _run("--table").stdout
    assert re.search(r"^shared/exceptions\.py\s+EVERY image$", table, re.M)
    assert re.search(r"^shared/settings/vm_lifecycle\.py\s+vm-lifecycle-worker$", table, re.M)
