"""Git by subprocess, THE git client of every limb that clones a repo — kept out of each limb's activities.py.

Used by cluster-lifecycle (day1_writer.py, day2_sigs.py), upgrade-gitops
(day1, day2, Brain) and vm-lifecycle, the day1 reads through
activities/day1_repo.py. No other module runs git. What a limb does with a
clone — which file, and what "already done" means for it — stays the limb's;
everything that is just git lives here. First extracted from
segment-lifecycle's values_repo.py (DESIGN O15, since removed).

Plain functions taking explicit parameters (never settings), so the offline
tests drive them against a local bare repo with no network and no Temporal.
A caller opens ONE `fresh_clone` per activity attempt and works through the
`Clone` it yields (head, blob, commit_and_push, run_command), so the repo URL
and token are passed once.

Rules this module enforces (CLAUDE.md §5, "Git-subprocess activities"):
  * git runs via asyncio.create_subprocess_exec — no Python git dependency;
    each limb image installs the git binary. Every command runs with
    http.sslVerify=false: the airgapped environment's internal CA cannot be
    injected into the images (see segment_lifecycle/activities._TLS_VERIFY).
  * The token is injected into an https clone URL in memory only
    (https://x-access-token:<token>@...) and NEVER logged: every command echo
    and every error message passes through redact() BEFORE an exception is
    constructed — a git error lands verbatim in Temporal history and the UI.
  * No file of a clone holds the token: clone() points `origin` back at the
    plain URL the moment the clone exists, and commit_and_push() pushes to the
    token URL it builds in memory. The hub limb runs day1's and Brain's own
    scripts inside their clones (run_command), and those scripts must not find
    a credential in `.git/config`.
  * Every command runs under GIT_COMMAND_TIMEOUT_SECONDS, below the git
    activities' 180s start_to_close_timeout, and no child process outlives its
    call: a timeout or a cancellation kills and reaps it first.
  * A clone first checks its branch with `git ls-remote --exit-code --heads`
    on the FULL ref: exit 2 is ValuesBranchNotFoundError, anything else
    ValuesRepoGitError (unreachable is not missing). The CALLER classifies a
    missing branch: the workflows retry it (a repo without `main` is fixed,
    not failed on; tests/test_error_classification.py), upgrade-gitops's
    write_incident turns it into a note.
  * ONE clone per activity attempt: a rejected push raises the retryable
    ValuesRepoGitError and Temporal's retry re-clones.
    No retry loop runs inside an attempt — it could not fit the 180s.
"""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from shared.exceptions import ValuesBranchNotFoundError, ValuesRepoGitError

# Kept below the git activities' 180s start_to_close_timeout so a hung remote
# fails the command and frees the worker before Temporal reaps the activity
# (same budget rule as the HTTP clients'). The clone is the only long command;
# the ls-remote branch check that precedes it, the local add/commit/rev-parse
# and the push share the remainder comfortably.
GIT_COMMAND_TIMEOUT_SECONDS = 150.0

# `git ls-remote --exit-code` exits 2 when the remote answered and holds no
# matching ref — the one answer that means "this branch does not exist".
_LS_REMOTE_NO_MATCHING_REF = 2

# day1, day2 and Brain are read and written at `main` only: Argo CD renders
# day1's main and nothing else. A sig repo is worked on its default branch.
MAIN = "main"

# `git ls-remote --symref <url> HEAD` names the default branch on this line.
_SYMREF = re.compile(r"^ref: refs/heads/(?P<branch>\S+)\tHEAD$", re.M)

# What a repo's script prints is cut to its last characters in a MESSAGE (it
# lands in history) — never before the caller has read all of it.
OUTPUT_TAIL = 2000


@dataclass(frozen=True)
class GitIdentity:
    name: str
    email: str


# Who the cluster-upgrade hub limb's commits are by, in day1 and Brain.
# Hard-coded, not configured, like every writer's identity: it identifies the
# writer, so it must not vary per environment.
UPGRADE_AUTHOR = GitIdentity(name="cluster-upgrade-workflow", email="cluster-upgrade-workflow@redbull.local")


def redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def tail(text: str) -> str:
    """The last OUTPUT_TAIL characters of a script's output, for a message."""
    return text[-OUTPUT_TAIL:]


def authenticated_url(repo_url: str, token: str) -> str:
    """Inject the token into an https clone URL; anything else (a local path
    in tests, ssh) passes through untouched."""
    if token and repo_url.startswith("https://"):
        return "https://x-access-token:" + token + "@" + repo_url.removeprefix("https://")
    return repo_url


async def _stop(process: asyncio.subprocess.Process) -> None:
    """Kill the child if it still runs, and reap it."""
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    await process.wait()


async def _communicate(process: asyncio.subprocess.Process, timeout: float) -> tuple[bytes, bytes] | None:
    """(stdout, stderr), or None when `timeout` passed first.

    The child never outlives the call: on a timeout, or on a cancellation
    (a worker shutting down, a sibling task failing), it is killed and reaped
    before this returns or re-raises.
    """
    try:
        return await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        await _stop(process)
        return None
    except asyncio.CancelledError:
        await _stop(process)
        raise


async def exec_git(
    args: list[str], *, cwd: Path | None, secrets: list[str]
) -> tuple[int, str, str]:
    """Run one git command and return (returncode, stdout, stderr), stderr
    already scrubbed of every secret. A timeout raises the retryable
    ValuesRepoGitError, its message scrubbed BEFORE the exception exists (it
    will be recorded in Temporal history). Callers that need to tell one exit
    code from another (ls-remote) use this; everything else uses run_git."""
    process = await asyncio.create_subprocess_exec(
        "git",
        # TLS verification off, matching the httpx clients (activities._TLS_VERIFY):
        # git keeps its OWN trust store, so the airgapped environment's
        # un-injectable internal CA has to be waived here separately. Passed on the
        # exec line rather than inside `args` so it never appears in the redacted
        # command echo recorded in Temporal history.
        "-c",
        "http.sslVerify=false",
        *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output = await _communicate(process, GIT_COMMAND_TIMEOUT_SECONDS)
    if output is None:
        raise ValuesRepoGitError(
            f"git {redact(' '.join(args), secrets)} timed out after "
            f"{GIT_COMMAND_TIMEOUT_SECONDS:.0f}s"
        )
    stdout, stderr = output
    return (
        process.returncode,
        stdout.decode(errors="replace"),
        redact(stderr.decode(errors="replace").strip(), secrets),
    )


async def run_git(args: list[str], *, cwd: Path | None, secrets: list[str]) -> str:
    """Run one git command; raise the retryable ValuesRepoGitError on any
    failure, with every secret scrubbed from the message BEFORE the exception
    exists (it will be recorded in Temporal history)."""
    returncode, stdout, stderr = await exec_git(args, cwd=cwd, secrets=secrets)
    if returncode != 0:
        raise ValuesRepoGitError(
            f"git {redact(' '.join(args), secrets)} failed "
            f"(exit {returncode}): {stderr}"
        )
    return stdout


async def require_branch(
    *, repo_url: str, branch: str, token: str, missing_message: str
) -> None:
    """Fail deterministically when the remote has no such branch.

    Without this, `git clone --branch <missing>` exits non-zero like any other
    git failure and raises the RETRYABLE ValuesRepoGitError, so a mistyped or
    deleted branch would retry every minute forever. ls-remote's --exit-code
    separates the two: exit 2 means the remote answered and holds no such ref
    (ValuesBranchNotFoundError, carrying `missing_message`); anything else —
    auth, DNS, a missing repo, a timeout — is an unreachable remote
    (ValuesRepoGitError). The calling workflow classifies both.

    The branch is matched as the FULL ref refs/heads/<branch>: a bare pattern
    would tail-match, so `x` would be "found" as `feature/x`.
    """
    args = [
        "ls-remote",
        "--exit-code",
        "--heads",
        authenticated_url(repo_url, token),
        f"refs/heads/{branch}",
    ]
    returncode, _, stderr = await exec_git(args, cwd=None, secrets=[token])
    if returncode == _LS_REMOTE_NO_MATCHING_REF:
        raise ValuesBranchNotFoundError(missing_message)
    if returncode != 0:
        raise ValuesRepoGitError(
            f"git ls-remote for branch {branch!r} failed (exit {returncode}): "
            f"{stderr}"
        )


async def branch_exists(*, repo_url: str, branch: str, token: str) -> bool:
    """Whether the remote has the branch (require_branch's classification:
    absent is a False, unreachable stays the ValuesRepoGitError)."""
    try:
        await require_branch(repo_url=repo_url, branch=branch, token=token, missing_message="absent")
    except ValuesBranchNotFoundError:
        return False
    return True


async def default_branch(*, repo_url: str, token: str, repo: str) -> str:
    """The remote's default branch, from `git ls-remote --symref <url> HEAD`; `repo` names it in an error."""
    returncode, stdout, stderr = await exec_git(
        ["ls-remote", "--symref", authenticated_url(repo_url, token), "HEAD"], cwd=None, secrets=[token]
    )
    if returncode != 0:
        raise ValuesRepoGitError(f"git ls-remote --symref on the {repo} repo failed (exit {returncode}): {stderr}")
    match = _SYMREF.search(stdout)
    if match is None:
        raise ValuesRepoGitError(f"the {repo} repo reported no default branch (ls-remote --symref HEAD gave no symref)")
    return match.group("branch")


async def clone(
    *, repo_url: str, branch: str, token: str, dest: Path, missing_message: str
) -> None:
    """Shallow single-branch clone of a checked branch — retries always start from a clean tree.

    The branch is checked FIRST (require_branch), so a missing one fails as
    ValuesBranchNotFoundError and the clone never sees an unchecked branch.
    Then `origin` is set back to the plain URL: git stores the URL a clone was
    given in `.git/config`, and a repo's own script run in the clone afterwards
    must not read the token there. Pushes go through commit_and_push(), which
    builds the token URL in memory again."""
    await require_branch(
        repo_url=repo_url, branch=branch, token=token, missing_message=missing_message
    )
    await run_git(
        [
            "clone",
            "--depth",
            "1",
            "--branch",
            branch,
            "--single-branch",
            authenticated_url(repo_url, token),
            str(dest),
        ],
        cwd=None,
        secrets=[token],
    )
    await run_git(["remote", "set-url", "origin", repo_url], cwd=dest, secrets=[token])


@dataclass(frozen=True)
class Clone:
    """One fresh clone of `branch` (fresh_clone), for ONE activity attempt.

    Carries the remote and its token so a caller passes them once. Each method
    is the module function of the same name, looked up when called (tests
    replace `git_repo.commit_and_push` to race a push)."""

    path: Path
    repo_url: str
    token: str = field(repr=False)
    branch: str = MAIN

    async def head(self) -> str:
        """The commit the clone is at."""
        return (await run_git(["rev-parse", "HEAD"], cwd=self.path, secrets=[self.token])).strip()

    async def blob(self, relative: str) -> str:
        """The git blob id of `relative` at HEAD: what a compare-and-set compares."""
        return (await run_git(["rev-parse", f"HEAD:{relative}"], cwd=self.path, secrets=[self.token])).strip()

    async def commit_and_push(
        self, *, paths: list[str], message: str, author: GitIdentity, branch: str | None = None
    ) -> str:
        """commit_and_push() to `branch`, the cloned branch unless named; the new commit's sha."""
        return await commit_and_push(
            self.path,
            repo_url=self.repo_url,
            token=self.token,
            branch=branch or self.branch,
            paths=paths,
            message=message,
            author=author,
        )

    async def run_command(self, argv: list[str], *, timeout: float) -> tuple[int, str]:
        """run_command() in this clone, the token scrubbed from its output."""
        return await run_command(argv, cwd=self.path, secrets=[self.token], timeout=timeout)


@asynccontextmanager
async def fresh_clone(
    *,
    repo_url: str,
    token: str,
    repo: str,
    prefix: str,
    branch: str = MAIN,
    missing_message: str | None = None,
) -> AsyncIterator[Clone]:
    """clone() of `branch` into a new TemporaryDirectory, removed on exit.

    `repo` names the repo in the missing-branch error unless `missing_message`
    says more; `prefix` names the temporary directory."""
    with tempfile.TemporaryDirectory(prefix=prefix) as tmp:
        dest = Path(tmp) / "repo"
        await clone(
            repo_url=repo_url,
            branch=branch,
            token=token,
            dest=dest,
            missing_message=missing_message or f"the {repo} repo has no `{branch}` branch",
        )
        yield Clone(path=dest, repo_url=repo_url, token=token, branch=branch)


async def commit_and_push(
    clone_dir: Path,
    *,
    repo_url: str,
    token: str,
    branch: str,
    paths: list[str],
    message: str,
    author: GitIdentity,
) -> str:
    """Add `paths`, commit them as `author`, push HEAD to `branch`; the new commit's sha.

    Pushed to the token URL built here, in memory — never to `origin`, whose
    URL clone() left without the token — and never forced. A rejected push
    (the branch moved since the clone) raises the retryable ValuesRepoGitError,
    its message scrubbed like every other: Temporal's retry re-clones, and the
    caller's "already so" check, made before anything else, lets it converge.
    """
    secrets = [token]
    await run_git(["add", "--", *paths], cwd=clone_dir, secrets=secrets)
    await run_git(
        ["-c", f"user.name={author.name}", "-c", f"user.email={author.email}", "commit", "-m", message],
        cwd=clone_dir,
        secrets=secrets,
    )
    await run_git(
        ["push", authenticated_url(repo_url, token), f"HEAD:refs/heads/{branch}"],
        cwd=clone_dir,
        secrets=secrets,
    )
    return (await run_git(["rev-parse", "HEAD"], cwd=clone_dir, secrets=secrets)).strip()


async def run_command(argv: list[str], *, cwd: Path, secrets: list[str], timeout: float) -> tuple[int, str]:
    """Run a repo's own script in its clone: (exit code, ALL its output, scrubbed).

    No shell. 124 is a timeout and 127 a script that could not start, each
    with a line saying so. The whole output comes back, so a caller judging
    it reads every line; cut it with tail() only to put it in a message.

    The child gets no `*_TOKEN` variable from this worker's environment, and
    no file of the clone holds a token (clone()). What this does NOT close:
    the script runs as this pod's UID, so it can still read /proc/1/environ —
    the worker's own environment, every token in it — and the mounted
    registry auth file. That cannot be fixed inside this pod: day1's and
    Brain's validator scripts run with this pod's credentials, and moving them
    to a separate Job (no Secret mounted, the clone handed over as data) is
    the fix.
    """
    env = {key: value for key, value in os.environ.items() if not key.upper().endswith("_TOKEN")}
    try:
        process = await asyncio.create_subprocess_exec(
            *argv, cwd=cwd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
    except OSError as err:
        return 127, redact(f"{argv[0]}: {err}", secrets)
    output = await _communicate(process, timeout)
    if output is None:
        return 124, redact(f"{' '.join(argv)} timed out after {timeout:.0f}s", secrets)
    return process.returncode or 0, redact(output[0].decode(errors="replace").strip(), secrets)
