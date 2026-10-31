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
    plain URL the moment the clone exists, and a push goes to the token URL
    built in memory again (authenticated_url), never to `origin`. The hub
    limb runs day1's and Brain's own scripts inside their clones, and those
    scripts must not find a credential in `.git/config`.
  * Every command runs under GIT_COMMAND_TIMEOUT_SECONDS, below the git
    activities' 180s start_to_close_timeout.
  * A clone first checks its branch with `git ls-remote --exit-code --heads`
    on the FULL ref: exit 2 is the deterministic ValuesBranchNotFoundError,
    anything else the retryable ValuesRepoGitError (unreachable is not missing).
"""

from __future__ import annotations

import asyncio
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


def redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def authenticated_url(repo_url: str, token: str) -> str:
    """Inject the token into an https clone URL; anything else (a local path
    in tests, ssh) passes through untouched."""
    if token and repo_url.startswith("https://"):
        return "https://x-access-token:" + token + "@" + repo_url.removeprefix("https://")
    return repo_url


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
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=GIT_COMMAND_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise ValuesRepoGitError(
            f"git {redact(' '.join(args), secrets)} timed out after "
            f"{GIT_COMMAND_TIMEOUT_SECONDS:.0f}s"
        ) from None
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
    must not read the token there. A push names authenticated_url() itself."""
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
