"""Git by subprocess, for every limb that clones a repo — kept out of each limb's activities.py.

Extracted from activities/segment_lifecycle/values_repo.py (DESIGN O15) when the
cluster-upgrade hub limb needed the same client for day1, day2 and Brain:
one git client, not two. values_repo.py keeps its own names for these, so the
segment-lifecycle behaviour and its tests did not change.

Plain functions taking explicit parameters (never settings), so the offline
tests drive them against a local bare repo with no network and no Temporal.

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
    on the FULL ref: exit 2 is the deterministic ValuesBranchNotFoundError,
    anything else the retryable ValuesRepoGitError (unreachable is not missing).
  * ONE clone per activity attempt: a rejected push raises the retryable
    ValuesRepoGitError and Temporal's retry re-clones (values_repo.py's rule).
    No retry loop runs inside an attempt — it could not fit the 180s.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
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

# The cluster-upgrade hub limb's repos (day1, day2, Brain) are read and written
# at `main` only: Argo CD renders day1's main and nothing else.
MAIN = "main"

# What a repo's script prints is cut to its last characters in a MESSAGE (it
# lands in history) — never before the caller has read all of it.
OUTPUT_TAIL = 2000


@dataclass(frozen=True)
class GitIdentity:
    name: str
    email: str


# Who the cluster-upgrade hub limb's commits are by, in day1 and Brain.
# Hard-coded, not configured, like values_repo.GIT_USER_NAME: it identifies the
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
    (ValuesBranchNotFoundError, non-retryable, carrying `missing_message`);
    anything else — auth, DNS, a missing repo, a timeout — is an unreachable
    remote and stays retryable.

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


async def clone_main(*, repo_url: str, token: str, dest: Path, repo: str) -> None:
    """clone() of `main`, the one branch the hub limb's repos are worked on; `repo` names the repo in its error."""
    await clone(
        repo_url=repo_url, branch=MAIN, token=token, dest=dest,
        missing_message=f"the {repo} repo has no `{MAIN}` branch",
    )


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
