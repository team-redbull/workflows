"""The one git client (activities/git_repo.py): what it keeps out of a clone, and out of every error.

`https://git.test/<name>` is served from local bare repos through git's own
`url.<base>.insteadOf` (a GIT_CONFIG_GLOBAL file in the test's tmp dir), with
and without the token in the URL, so the token really is injected into an
https URL — the only kind authenticated_url() puts it in — and no request
leaves the machine.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities import git_repo

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
        _git("-c", "user.name=t", "-c", "user.email=t@test", "commit", "-q", "-am", "change", cwd=dest)
        await git_repo.run_git(
            ["push", git_repo.authenticated_url(HOST + "repo.git", SECRET), "HEAD:main"], cwd=dest, secrets=[SECRET]
        )
        assert _git("log", "-1", "--format=%s", "main", cwd=remote).strip() == "change"
        assert files_holding(dest, SECRET) == []
