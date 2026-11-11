"""day2_sigs against REAL local bare repos — fully offline.

One bare repo per sig stands in for the GitLab project (tree at the root) or,
with a path prefix, for the one dev GitHub repo with `sigs/<sig>/`. The
default branch is whatever the remote's HEAD says — `main` or `master` —
and the folder lands there with its .gitkeep; a second run finds it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities.cluster_lifecycle import day2_sigs
from shared.exceptions import ValuesRepoGitError
from shared.models.cluster_lifecycle import Day2SigRegistrationRequest

REQUEST = Day2SigRegistrationRequest(sig="redbull", site="site2", env="prep", mce="ocp4-prep-mce-site2-a", cluster="ocp4-prep-e2e-site2-a")
FOLDER = "sites/site2/prep/mces/ocp4-prep-mce-site2-a/ocp4-prep-e2e-site2-a"


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _bare(root: Path, name: str, branch: str, files: dict[str, str]) -> Path:
    bare = root / name
    _git("init", "-q", "--bare", f"--initial-branch={branch}", str(bare), cwd=root)
    seed = root / f"{name}-seed"
    _git("clone", "-q", str(bare), str(seed), cwd=root)
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _git("push", "-q", "origin", f"HEAD:{branch}", cwd=seed)
    return bare


def _tree(origin: Path, branch: str) -> list[str]:
    return _git("ls-tree", "-r", "--name-only", branch, cwd=origin).split()


async def _register(origin: Path, prefix: str = "", request: Day2SigRegistrationRequest = REQUEST):
    return await day2_sigs.register(repo_url=str(origin), token="", path_prefix=prefix, request=request)


async def test_creates_the_folder_on_the_default_branch(tmp_path):
    origin = _bare(tmp_path, "sig.git", "main", {"README.md": "sig\n"})
    registration = await _register(origin)
    assert (registration.sig, registration.path, registration.created) == ("redbull", FOLDER, True)
    assert registration.commit_sha == _git("rev-parse", "main", cwd=origin).strip()
    assert f"{FOLDER}/.gitkeep" in _tree(origin, "main")
    log = _git("log", "-1", "--format=%s|%an", "main", cwd=origin).strip()
    assert log == "chore: register ocp4-prep-e2e-site2-a in day2 (redbull) [provision-cluster-workflow]|provision-cluster-workflow"


async def test_a_master_default_branch_is_used_as_found(tmp_path):
    origin = _bare(tmp_path, "sig.git", "master", {"README.md": "sig\n"})
    assert await day2_sigs.default_branch(repo_url=str(origin), token="") == "master"
    registration = await _register(origin)
    assert registration.created and f"{FOLDER}/.gitkeep" in _tree(origin, "master")


async def test_a_folder_already_there_is_success_with_nothing_pushed(tmp_path):
    origin = _bare(tmp_path, "sig.git", "main", {f"{FOLDER}/.gitkeep": "", "README.md": "x\n"})
    before = _git("rev-parse", "main", cwd=origin).strip()
    registration = await _register(origin)
    assert (registration.created, registration.commit_sha, registration.path) == (False, None, FOLDER)
    assert _git("rev-parse", "main", cwd=origin).strip() == before


async def test_a_folder_holding_other_files_counts_as_registered(tmp_path):
    origin = _bare(tmp_path, "sig.git", "main", {f"{FOLDER}/argocd/values.yaml": "a: 1\n"})
    assert (await _register(origin)).created is False


async def test_a_second_run_is_a_no_op(tmp_path):
    origin = _bare(tmp_path, "sig.git", "main", {"README.md": "x\n"})
    first = await _register(origin)
    second = await _register(origin)
    assert first.created and not second.created
    assert _git("rev-parse", "main", cwd=origin).strip() == first.commit_sha


async def test_the_dev_layout_puts_the_tree_under_the_prefix(tmp_path):
    origin = _bare(tmp_path, "day2.git", "main", {"sigs/redbull/README.md": "x\n"})
    registration = await _register(origin, prefix="sigs/redbull")
    assert registration.path == f"sigs/redbull/{FOLDER}"
    assert f"sigs/redbull/{FOLDER}/.gitkeep" in _tree(origin, "main")


@pytest.mark.parametrize("prefix", ["/sigs/redbull", "sigs/redbull/", "/sigs/redbull/"])
async def test_a_prefix_is_taken_without_its_slashes(tmp_path, prefix):
    assert day2_sigs.cluster_folder(prefix, REQUEST) == f"sigs/redbull/{FOLDER}"


def test_a_prefix_escaping_the_repo_is_refused():
    with pytest.raises(ValuesRepoGitError, match="inside the repo"):
        day2_sigs.cluster_folder("../elsewhere", REQUEST)


@pytest.mark.parametrize("field", ["sig", "site", "env", "mce", "cluster"])
@pytest.mark.parametrize("value", ["../x", "a/b", "Up", "", "-x", "a b"])
def test_every_path_segment_is_a_plain_token(field, value):
    request = REQUEST.model_copy(update={field: value})
    with pytest.raises(ValuesRepoGitError, match="not a day2 path segment"):
        if field == "sig":
            day2_sigs._segment(request.sig, "sig")
        else:
            day2_sigs.cluster_folder("", request)


async def test_an_unreachable_sig_repo_stays_retryable(tmp_path):
    with pytest.raises(ValuesRepoGitError):
        await _register(tmp_path / "missing.git")


async def test_a_push_rejected_because_the_branch_moved_is_retryable_and_the_retry_converges(tmp_path, monkeypatch):
    origin = _bare(tmp_path, "sig.git", "main", {"README.md": "x\n"})
    real = day2_sigs.git_repo.commit_and_push
    raced = []

    async def racing(*args, **kwargs):
        if not raced:
            raced.append(True)
            other = tmp_path / "other"
            _git("clone", "-q", str(origin), str(other), cwd=tmp_path)
            (other / "README.md").write_text("changed by someone else\n")
            _git("add", "-A", cwd=other)
            _git("-c", "user.name=o", "-c", "user.email=o@t", "commit", "-q", "-m", "concurrent", cwd=other)
            _git("push", "-q", "origin", "main", cwd=other)
        return await real(*args, **kwargs)

    monkeypatch.setattr(day2_sigs.git_repo, "commit_and_push", racing)
    with pytest.raises(ValuesRepoGitError, match="push"):
        await _register(origin)
    registration = await _register(origin)
    assert registration.created
    log = _git("log", "--format=%s", "-2", "main", cwd=origin).splitlines()
    assert log[1] == "concurrent"
