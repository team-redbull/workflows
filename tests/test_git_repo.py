"""The one git client (activities/git_repo.py): what it keeps out of a clone, and out of every error.

`https://git.test/<name>` is served from local bare repos through git's own
`url.<base>.insteadOf` (a GIT_CONFIG_GLOBAL file in the test's tmp dir), with
and without the token in the URL, so the token really is injected into an
https URL — the only kind authenticated_url() puts it in — and no request
leaves the machine.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from activities import git_repo
from shared.exceptions import ValuesRepoGitError

HOST = "https://git.test/"
# Not shaped like any real token on purpose; unique enough to search files for.
SECRET = "not-a-real-token-" + "7f3a9c" * 3


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def serve_over_https(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`https://git.test/<x>` and `https://x-access-token:<SECRET>@git.test/<x>` both mean `<root>/<x>`."""
    config = root / "gitconfig-https"
    config.write_text(
        f'[url "{root}/"]\n'
        f"\tinsteadOf = https://x-access-token:{SECRET}@git.test/\n"
        f"\tinsteadOf = {HOST}\n"
    )
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")


def files_holding(root: Path, secret: str) -> list[str]:
    """Every file under `root`, `.git/` included, whose bytes contain `secret`."""
    needle = secret.encode()
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file() and needle in path.read_bytes())


def _bare_repo(root: Path, name: str, files: dict[str, str]) -> Path:
    bare = root / name
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=root)
    seed = root / f"{name}-seed"
    _git("clone", "-q", str(bare), str(seed), cwd=root)
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return bare


@pytest.fixture
def remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    serve_over_https(tmp_path, monkeypatch)
    return _bare_repo(tmp_path, "repo.git", {"README.md": "seed\n"})


class TestTheTokenStaysOutOfTheClone:
    async def test_no_file_of_a_clone_holds_the_token(self, remote, tmp_path):
        dest = tmp_path / "clone"
        await git_repo.clone(repo_url=HOST + "repo.git", branch="main", token=SECRET, dest=dest, missing_message="-")
        assert (dest / "README.md").read_text() == "seed\n"
        assert files_holding(dest, SECRET) == []
        assert _git("config", "--get", "remote.origin.url", cwd=dest).strip() == HOST + "repo.git"

    async def test_a_push_to_the_token_url_still_lands(self, remote, tmp_path):
        dest = tmp_path / "clone"
        await git_repo.clone(repo_url=HOST + "repo.git", branch="main", token=SECRET, dest=dest, missing_message="-")
        (dest / "README.md").write_text("changed\n")
        sha = await git_repo.commit_and_push(
            dest, repo_url=HOST + "repo.git", token=SECRET, branch="main", paths=["README.md"],
            message="change", author=git_repo.UPGRADE_AUTHOR,
        )
        assert _git("log", "-1", "--format=%H|%an <%ae>|%s", "main", cwd=remote).strip() == (
            f"{sha}|cluster-upgrade-workflow <cluster-upgrade-workflow@redbull.local>|change"
        )
        assert files_holding(dest, SECRET) == []


# --- CLAUDE.md §5: the token is scrubbed BEFORE an exception exists ------------------------


def _reject_every_push(bare: Path, said: str) -> None:
    """A pre-receive hook that refuses the push, printing `said` back to the pusher."""
    hook = bare / "hooks" / "pre-receive"
    hook.write_text(f"#!/bin/sh\necho 'refused for {said}' >&2\nexit 1\n")
    hook.chmod(0o755)


class TestNoErrorCarriesTheToken:
    async def test_a_failing_git_command_with_the_token_in_its_argv_and_output(self, tmp_path):
        with pytest.raises(ValuesRepoGitError) as caught:
            await git_repo.run_git(
                ["clone", str(tmp_path / SECRET / "missing.git"), str(tmp_path / "dest")], cwd=None, secrets=[SECRET]
            )
        message = str(caught.value)
        assert SECRET not in message
        assert "***" in message and "missing.git" in message

    async def test_a_timed_out_git_command(self, monkeypatch):
        monkeypatch.setattr(git_repo, "GIT_COMMAND_TIMEOUT_SECONDS", 0.5)
        with pytest.raises(ValuesRepoGitError, match="timed out") as caught:
            await git_repo.run_git(["-c", "alias.wait=!f() { sleep 2; }; f", "wait", SECRET], cwd=None, secrets=[SECRET])
        assert SECRET not in str(caught.value)

    async def test_a_rejected_push_whose_url_and_remote_output_hold_the_token(self, remote, tmp_path):
        dest = tmp_path / "clone"
        await git_repo.clone(repo_url=HOST + "repo.git", branch="main", token=SECRET, dest=dest, missing_message="-")
        _reject_every_push(remote, SECRET)
        (dest / "README.md").write_text("changed\n")
        with pytest.raises(ValuesRepoGitError) as caught:
            await git_repo.commit_and_push(
                dest, repo_url=HOST + "repo.git", token=SECRET, branch="main", paths=["README.md"],
                message="change", author=git_repo.UPGRADE_AUTHOR,
            )
        message = str(caught.value)
        assert SECRET not in message
        assert "git push https://x-access-token:***@git.test/repo.git" in message and "refused for ***" in message

    async def test_a_repo_script_printing_the_token(self, tmp_path):
        code, output = await git_repo.run_command(
            [sys.executable, "-c", f"print('leaked: {SECRET}')"], cwd=tmp_path, secrets=[SECRET], timeout=30
        )
        assert code == 0 and output == "leaked: ***"


# --- run_command: a repo's own script in its clone --------------------------------------------


class TestRepoScripts:
    async def test_no_token_variable_reaches_the_script(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DAY1_GIT_TOKEN", SECRET)
        monkeypatch.setenv("BRAIN_GIT_TOKEN", SECRET)
        code, output = await git_repo.run_command(
            [sys.executable, "-c", "import os; print(sorted(k for k in os.environ if k.endswith('_TOKEN')))"],
            cwd=tmp_path, secrets=[], timeout=30,
        )
        assert (code, output) == (0, "[]")

    async def test_all_of_the_output_comes_back_and_tail_cuts_it_for_a_message(self, tmp_path):
        code, output = await git_repo.run_command(
            [sys.executable, "-c", "print('first'); print('x' * 5000); print('last')"],
            cwd=tmp_path, secrets=[], timeout=30,
        )
        assert code == 0 and output.startswith("first\n") and output.endswith("\nlast")
        assert len(git_repo.tail(output)) == git_repo.OUTPUT_TAIL and git_repo.tail(output).endswith("last")

    async def test_a_script_that_cannot_start_is_127(self, tmp_path):
        code, output = await git_repo.run_command([str(tmp_path / "no-such-script")], cwd=tmp_path, secrets=[], timeout=30)
        assert code == 127 and "no-such-script" in output

    async def test_a_timeout_is_124_and_kills_the_script(self, tmp_path):
        pid_file = tmp_path / "pid"
        code, output = await git_repo.run_command(
            [sys.executable, "-c", _WRITE_PID_AND_SLEEP, str(pid_file)], cwd=tmp_path, secrets=[], timeout=1
        )
        assert code == 124 and "timed out after 1s" in output
        assert not _alive(int(pid_file.read_text()))

    async def test_a_cancelled_call_kills_the_script(self, tmp_path):
        pid_file = tmp_path / "pid"
        task = asyncio.create_task(git_repo.run_command(
            [sys.executable, "-c", _WRITE_PID_AND_SLEEP, str(pid_file)], cwd=tmp_path, secrets=[], timeout=60
        ))
        pid = await _wait_for_pid(pid_file)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not _alive(pid)


_WRITE_PID_AND_SLEEP = "import os, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(60)"


async def _wait_for_pid(path: Path) -> int:
    for _ in range(200):
        if path.exists() and path.read_text():
            return int(path.read_text())
        await asyncio.sleep(0.05)
    raise AssertionError(f"{path} was never written")


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True
