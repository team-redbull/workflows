"""git_repo.snapshot: how a limb READS a repo — the remote asked every time, a clone only when its head moved.

Against local bare repos, fully offline (tests/test_git_repo.py's helpers; its
`https://git.test/` trick puts a real token into a real https URL). Every test
keeps its snapshots under its own `cache_root`, so none reads another's.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import sys
from pathlib import Path

import pytest

from activities import git_repo
from shared.exceptions import ValuesBranchNotFoundError, ValuesRepoGitError
from tests.test_git_repo import HOST, SECRET, _bare_repo, _git, files_holding, serve_over_https


@pytest.fixture
def remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    serve_over_https(tmp_path, monkeypatch)
    return _bare_repo(tmp_path, "repo.git", {"README.md": "seed\n", "dir/file.txt": "one\n"})


@pytest.fixture
def cache(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def git_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every git subcommand the client runs, in order."""
    calls: list[str] = []
    real = git_repo.exec_git

    async def recording(args, **kwargs):
        calls.append(args[0])
        return await real(args, **kwargs)

    monkeypatch.setattr(git_repo, "exec_git", recording)
    return calls


def _push(remote: Path, tmp_path: Path, name: str, content: str, branch: str = "main") -> str:
    """One more commit on the remote's branch; its sha."""
    checkout = tmp_path / name
    _git("clone", "-q", str(remote), str(checkout), cwd=tmp_path)
    (checkout / "README.md").write_text(content)
    _git("add", "-A", cwd=checkout)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", name, cwd=checkout)
    _git("push", "-q", "origin", f"HEAD:{branch}", cwd=checkout)
    return _git("rev-parse", "HEAD", cwd=checkout).strip()


def _head(remote: Path, branch: str = "main") -> str:
    return _git("rev-parse", branch, cwd=remote).strip()


def _open(remote: Path, cache: Path, **overrides):
    arguments = dict(repo_url=str(remote), token="", repo="day1", cache_root=cache)
    arguments.update(overrides)
    return git_repo.snapshot(**arguments)


def _directories(cache: Path) -> list[Path]:
    """Every snapshot directory under the cache root (each holds one clone, `repo/`)."""
    return sorted(path.parent for path in cache.glob("*/*/repo"))


class TestTheRemoteIsAskedEveryTimeAndClonedOnlyWhenItMoved:
    async def test_an_unchanged_head_clones_once_and_ls_remote_runs_each_time(self, remote, cache, git_calls):
        paths = []
        for _ in range(3):
            async with _open(remote, cache) as snapshot:
                assert (snapshot.path / "README.md").read_text() == "seed\n"
                assert snapshot.revision == _head(remote)
                paths.append(snapshot.path)
        assert git_calls.count("ls-remote") == 3 and git_calls.count("clone") == 1
        assert len(set(paths)) == 1 and len(_directories(cache)) == 1

    async def test_the_revision_is_the_head_the_remote_reported(self, remote, cache, tmp_path):
        async with _open(remote, cache) as snapshot:
            assert snapshot.revision == _head(remote)
            assert _git("rev-parse", "HEAD", cwd=snapshot.path).strip() == snapshot.revision
        pushed = _push(remote, tmp_path, "next", "changed\n")
        async with _open(remote, cache) as snapshot:
            assert snapshot.revision == pushed == _head(remote)

    async def test_a_new_commit_clones_again_and_replaces_the_snapshot(self, remote, cache, tmp_path, git_calls):
        async with _open(remote, cache) as first:
            old = first.path
        _push(remote, tmp_path, "next", "changed\n")
        async with _open(remote, cache) as second:
            assert (second.path / "README.md").read_text() == "changed\n"
            assert second.path != old
        assert git_calls.count("clone") == 2
        assert not old.exists() and len(_directories(cache)) == 1
        async with _open(remote, cache):
            pass
        assert git_calls.count("clone") == 2

    async def test_concurrent_reads_of_one_head_cause_one_clone(self, remote, cache, git_calls):
        async def read() -> tuple[Path, str]:
            async with _open(remote, cache) as snapshot:
                await asyncio.sleep(0)
                return snapshot.path, (snapshot.path / "README.md").read_text()

        results = await asyncio.gather(*(read() for _ in range(30)))
        assert git_calls.count("clone") == 1 and git_calls.count("ls-remote") == 30
        assert {path for path, _ in results} == {results[0][0]} and {text for _, text in results} == {"seed\n"}

    async def test_each_branch_has_its_own_snapshot(self, remote, cache, tmp_path, git_calls):
        _push(remote, tmp_path, "side", "on the side\n", branch="side")
        async with _open(remote, cache) as main, _open(remote, cache, branch="side") as side:
            assert (main.path / "README.md").read_text() == "seed\n"
            assert (side.path / "README.md").read_text() == "on the side\n"
            assert side.branch == "side" and main.revision != side.revision
        assert git_calls.count("clone") == 2

    async def test_a_head_that_moved_between_the_question_and_the_clone_is_read_as_what_was_cloned(
        self, remote, cache, tmp_path, monkeypatch
    ):
        """The clone takes the branch's head as it is then: `revision` is that commit, never the older answer."""
        real = git_repo.exec_git
        moved: list[str] = []

        async def racing(args, **kwargs):
            if args[0] == "clone" and not moved:
                moved.append(_push(remote, tmp_path, "racer", "moved\n"))
            return await real(args, **kwargs)

        monkeypatch.setattr(git_repo, "exec_git", racing)
        async with _open(remote, cache) as snapshot:
            assert snapshot.revision == moved[0] == _head(remote)
            assert (snapshot.path / "README.md").read_text() == "moved\n"


class TestAReplacedSnapshotOutlivesItsReaders:
    async def test_it_survives_while_a_read_holds_it_and_is_deleted_after(self, remote, cache, tmp_path):
        async with _open(remote, cache) as held:
            _push(remote, tmp_path, "next", "changed\n")
            async with _open(remote, cache) as newer:
                assert newer.path != held.path
                assert (newer.path / "README.md").read_text() == "changed\n"
            # Still the commit this read began on, every file of it.
            assert (held.path / "README.md").read_text() == "seed\n"
            assert (held.path / "dir" / "file.txt").read_text() == "one\n"
            assert len(_directories(cache)) == 2
            kept = held.path
        assert not kept.exists()
        assert [path / "repo" for path in _directories(cache)] == [newer.path]

    async def test_two_readers_of_the_replaced_one_both_finish_before_it_goes(self, remote, cache, tmp_path):
        first = _open(remote, cache)
        second = _open(remote, cache)
        one = await first.__aenter__()
        two = await second.__aenter__()
        assert one.path == two.path
        _push(remote, tmp_path, "next", "changed\n")
        async with _open(remote, cache):
            pass
        await first.__aexit__(None, None, None)
        assert (two.path / "README.md").read_text() == "seed\n"
        await second.__aexit__(None, None, None)
        assert not two.path.exists()

    async def test_a_reader_that_fails_still_lets_go(self, remote, cache, tmp_path):
        with pytest.raises(RuntimeError):
            async with _open(remote, cache) as held:
                kept = held.path
                _push(remote, tmp_path, "next", "changed\n")
                async with _open(remote, cache):
                    pass
                raise RuntimeError("the caller failed")
        assert not kept.exists()


class TestNothingCachedAnswersForTheRemote:
    async def test_a_missing_branch_raises_as_fresh_clone_does_and_serves_nothing(self, remote, cache, git_calls):
        async with _open(remote, cache):
            pass
        _git("branch", "-m", "main", "renamed", cwd=remote)
        with pytest.raises(ValuesBranchNotFoundError) as fresh:
            async with git_repo.fresh_clone(repo_url=str(remote), token="", repo="day1", prefix="t-"):
                pass
        with pytest.raises(ValuesBranchNotFoundError) as cached:
            async with _open(remote, cache):
                raise AssertionError("a cached snapshot answered for a branch the remote no longer has")
        assert str(cached.value) == str(fresh.value) == "the day1 repo has no `main` branch"
        with pytest.raises(ValuesBranchNotFoundError, match="gone, says the caller"):
            async with _open(remote, cache, missing_message="gone, says the caller"):
                pass

    async def test_an_unreachable_remote_raises_as_fresh_clone_does_and_serves_nothing(self, remote, cache, tmp_path):
        async with _open(remote, cache):
            pass
        remote.rename(tmp_path / "moved-away.git")
        with pytest.raises(ValuesRepoGitError, match="git ls-remote for branch 'main' failed") as fresh:
            async with git_repo.fresh_clone(repo_url=str(remote), token="", repo="day1", prefix="t-"):
                pass
        with pytest.raises(ValuesRepoGitError) as cached:
            async with _open(remote, cache):
                raise AssertionError("a cached snapshot answered for a remote that cannot be asked")
        assert str(cached.value) == str(fresh.value)

    async def test_a_clone_that_fails_leaves_no_directory_and_the_old_snapshot_in_place(
        self, remote, cache, tmp_path, monkeypatch
    ):
        async with _open(remote, cache) as first:
            old = first.path
        _push(remote, tmp_path, "next", "changed\n")
        real = git_repo.exec_git

        async def failing(args, **kwargs):
            if args[0] == "clone":
                await real(args, **kwargs)  # the directory exists, half the work is done
                return 128, "", "fatal: the remote hung up"
            return await real(args, **kwargs)

        monkeypatch.setattr(git_repo, "exec_git", failing)
        with pytest.raises(ValuesRepoGitError, match="the remote hung up"):
            async with _open(remote, cache):
                raise AssertionError("a failed clone was served")
        assert [path / "repo" for path in _directories(cache)] == [old]
        monkeypatch.setattr(git_repo, "exec_git", real)
        async with _open(remote, cache) as retried:
            assert (retried.path / "README.md").read_text() == "changed\n"
        assert not old.exists()

    async def test_a_cancelled_clone_leaves_no_directory(self, remote, cache, monkeypatch):
        real = git_repo.exec_git
        started = asyncio.Event()

        async def hanging(args, **kwargs):
            if args[0] == "clone":
                await real(args, **kwargs)
                started.set()
                await asyncio.sleep(60)
            return await real(args, **kwargs)

        monkeypatch.setattr(git_repo, "exec_git", hanging)

        async def read() -> None:
            async with _open(remote, cache):
                raise AssertionError("a cancelled clone was served")

        task = asyncio.create_task(read())
        await asyncio.wait_for(started.wait(), 30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert _directories(cache) == []
        monkeypatch.setattr(git_repo, "exec_git", real)
        async with _open(remote, cache) as snapshot:
            assert (snapshot.path / "README.md").read_text() == "seed\n"


class TestTheTokenStaysOutOfASnapshot:
    async def test_no_file_of_a_snapshot_holds_the_token(self, remote, cache):
        async with git_repo.snapshot(repo_url=HOST + "repo.git", token=SECRET, repo="day1", cache_root=cache) as snapshot:
            assert (snapshot.path / "README.md").read_text() == "seed\n"
            assert files_holding(cache, SECRET) == []
            assert _git("config", "--get", "remote.origin.url", cwd=snapshot.path).strip() == HOST + "repo.git"
            assert SECRET not in repr(snapshot)
            async with snapshot.scratch(prefix="t-") as scratch:
                assert files_holding(scratch.path, SECRET) == [] and SECRET not in repr(scratch)

    async def test_an_error_from_the_remote_does_not_carry_it(self, remote, cache):
        with pytest.raises(ValuesRepoGitError) as caught:
            async with git_repo.snapshot(repo_url=HOST + "nope.git", token=SECRET, repo="day1", cache_root=cache):
                pass
        assert SECRET not in str(caught.value)


# --- what a reader may ask of a snapshot ------------------------------------------------------


_DIRTY_THE_TREE = """\
import pathlib, sys
root = pathlib.Path(sys.argv[1])
(root / "README.md").write_text("rewritten by the script\\n")
(root / "report.txt").write_text("left behind\\n")
(root / "dir" / "__pycache__").mkdir()
print("ran in", root.name)
"""


class TestReadingASnapshot:
    async def test_the_blob_is_gits_own_id_of_the_file_at_that_commit(self, remote, cache):
        async with _open(remote, cache) as snapshot:
            assert await snapshot.blob("dir/file.txt") == _git("rev-parse", "main:dir/file.txt", cwd=remote).strip()
            with pytest.raises(ValuesRepoGitError):
                await snapshot.blob("no/such/file")

    async def test_a_repo_script_runs_in_a_copy_and_cannot_dirty_what_the_next_reader_sees(self, remote, cache):
        async with _open(remote, cache) as snapshot:
            async with snapshot.scratch(prefix="t-") as scratch:
                assert scratch.path != snapshot.path and (scratch.path / ".git").is_dir()
                code, output = await scratch.run_command(
                    [sys.executable, "-c", _DIRTY_THE_TREE, str(scratch.path)], timeout=30
                )
                assert (code, output) == (0, "ran in repo")
                assert (scratch.path / "report.txt").exists()
                copy = scratch.path
            assert not copy.exists()
        async with _open(remote, cache) as again:
            assert again.path == snapshot.path
            assert (again.path / "README.md").read_text() == "seed\n"
            assert not (again.path / "report.txt").exists() and not (again.path / "dir" / "__pycache__").exists()
            assert _git("status", "--porcelain", cwd=again.path) == ""


# --- the cache root: this process's, and what a dead one left -------------------------------------


class TestTheCacheRoot:
    def test_the_default_is_under_the_temporary_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr(git_repo.tempfile, "tempdir", str(tmp_path))
        assert git_repo.default_cache_root() == tmp_path / "git-snapshots"

    async def test_what_a_dead_process_left_is_removed_and_a_live_ones_is_kept(self, remote, cache):
        cache.mkdir()
        dead, live = cache / "1-dead", cache / "1-live"
        for directory in (dead, live):
            (directory / "day1-main-x" / "repo").mkdir(parents=True)
            directory.with_suffix(".lock").touch()
        holder = os.open(live.with_suffix(".lock"), os.O_RDWR)
        fcntl.flock(holder, fcntl.LOCK_EX)  # the "other process" is alive
        try:
            async with _open(remote, cache) as snapshot:
                assert not dead.exists() and not dead.with_suffix(".lock").exists()
                assert (live / "day1-main-x" / "repo").is_dir() and live.with_suffix(".lock").exists()
                assert snapshot.path.is_relative_to(cache) and live not in snapshot.path.parents
        finally:
            os.close(holder)

    async def test_a_cache_directory_removed_under_the_process_is_made_again(self, remote, cache, git_calls):
        async with _open(remote, cache) as first:
            old = first.path
        for directory in _directories(cache):
            _remove(directory)
        async with _open(remote, cache) as second:
            assert (second.path / "README.md").read_text() == "seed\n"
        assert git_calls.count("clone") == 2 and not old.exists()


def _remove(directory: Path) -> None:
    import shutil

    shutil.rmtree(directory)
