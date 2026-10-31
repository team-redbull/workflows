"""The day1 values repo for the upgrade: read the rules and a cluster file, commit one change — at `main`.

Plain parameters, no settings and no Temporal (CLAUDE.md §1); the git client
is activities/git_repo.py, the one segment-lifecycle uses, so the token is
injected in memory only, scrubbed from every error, and every clone is a fresh
TemporaryDirectory. `main` only: Argo CD renders main and nothing else
(hcAppset.yaml pins it), and this limb has no branch input to check.

What the YAML means is day1_values.py and upgrade_rules.py; this module only
moves files between git and them, and runs day1's own validators:
`scripts/validate_upgrade_rules.py` before the rules are used, and the
configured day1 validator (D3) before every push.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from activities import git_repo
from activities.upgrade_gitops import day1_values, upgrade_rules
from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    Day1ChangeRejectedError,
    Day1FileChangedError,
    UpgradeRulesInvalidError,
    ValuesRepoGitError,
)
from shared.models.cluster_upgrade import Day1ChangeRequest, Day1Cluster, Day1Commit, UpgradeRules

BRANCH = "main"
_MISSING_MAIN = "the day1 repo has no `main` branch, the one Argo CD renders"
# A validator gets this long, inside the git activities' 180s.
VALIDATOR_TIMEOUT_SECONDS = 90.0
# A rejected push is retried from a fresh clone this many times within one
# attempt; past that the activity fails retryably and Temporal tries again.
PUSH_ATTEMPTS = 3
# What a validator prints is cut to its last characters: it lands in history.
_OUTPUT_TAIL = 2000


@dataclass(frozen=True)
class GitIdentity:
    name: str
    email: str


async def clone_main(*, repo_url: str, token: str, dest: Path) -> None:
    await git_repo.clone(repo_url=repo_url, branch=BRANCH, token=token, dest=dest, missing_message=_MISSING_MAIN)


async def head(clone: Path, *, token: str) -> str:
    return (await git_repo.run_git(["rev-parse", "HEAD"], cwd=clone, secrets=[token])).strip()


async def blob(clone: Path, path: str, *, token: str) -> str:
    """The git blob id of `path` at HEAD: what compare-and-set compares."""
    return (await git_repo.run_git(["rev-parse", f"HEAD:{path}"], cwd=clone, secrets=[token])).strip()


async def run_command(argv: list[str], *, cwd: Path, secrets: list[str], timeout: float) -> tuple[int, str]:
    """Run a repo's own script in its clone; (exit code, its output's tail, scrubbed).

    No shell. The child gets no `*_TOKEN` variable from this worker's
    environment, and no file of the clone holds a token (git_repo.clone): it
    is the repo's code, and has no business with them. What this does NOT
    close: the script runs as this pod's UID, so it can still read
    /proc/1/environ — the worker's own environment, every token in it — and
    the mounted registry auth file. That cannot be fixed inside this pod:
    day1's and Brain's validator scripts run with this pod's credentials, and
    moving them to a separate Job (no Secret mounted, the clone handed over as
    data) is the fix.
    """
    env = {key: value for key, value in os.environ.items() if not key.upper().endswith("_TOKEN")}
    try:
        process = await asyncio.create_subprocess_exec(
            *argv, cwd=cwd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
    except OSError as err:
        return 127, git_repo.redact(f"{argv[0]}: {err}", secrets)
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        return 124, f"{' '.join(argv)} timed out after {timeout:.0f}s"
    text = git_repo.redact(output.decode(errors="replace").strip(), secrets)
    return process.returncode or 0, text[-_OUTPUT_TAIL:]


# --- read_upgrade_rules ---------------------------------------------------------------


async def read_rules(*, repo_url: str, token: str, mce_cluster: str) -> UpgradeRules:
    """`upgrade/ladder.yaml` + `upgrade/mces/<mce>.yaml` at main, after day1's validator passed them."""
    with tempfile.TemporaryDirectory(prefix="day1-rules-") as tmp:
        clone = Path(tmp) / "repo"
        await clone_main(repo_url=repo_url, token=token, dest=clone)
        ladder = clone / upgrade_rules.LADDER_FILE
        mce = clone / upgrade_rules.mce_file(mce_cluster)
        missing = [str(path.relative_to(clone)) for path in (ladder, mce) if not path.is_file()]
        if missing:
            raise UpgradeRulesInvalidError(f"day1 main has no {' and no '.join(missing)}")
        validator = clone / upgrade_rules.VALIDATOR
        if not validator.is_file():
            raise UpgradeRulesInvalidError(f"day1 main has no {upgrade_rules.VALIDATOR} to validate the rules with")
        code, output = await run_command(
            [sys.executable, upgrade_rules.VALIDATOR, "--repo-root", str(clone)],
            cwd=clone, secrets=[token], timeout=VALIDATOR_TIMEOUT_SECONDS,
        )
        if code != 0:
            raise UpgradeRulesInvalidError(f"{upgrade_rules.VALIDATOR} refused the rules:\n{output}")
        return upgrade_rules.parse_rules(
            revision=await head(clone, token=token),
            ladder_text=ladder.read_text(encoding="utf-8"),
            mce_text=mce.read_text(encoding="utf-8"),
            mce_cluster=mce_cluster,
        )


# --- read_day1_cluster ------------------------------------------------------------------


def find_cluster_file(clone: Path, cluster: str) -> str:
    """The one `sites/*/mces/*/hostedClusters/<cluster>.yaml`; names are unique in day1 (D18).

    Compared by name, never globbed with it: the name comes from an
    unauthenticated API, and a `*` in it must not match other clusters.
    """
    sites = clone / "sites"
    matches = sorted(
        str(path.relative_to(clone))
        for path in (sites.glob("*/mces/*/hostedClusters/*.yaml") if sites.is_dir() else [])
        if path.name == f"{cluster}.yaml" and path.is_file()
    )
    if not matches:
        raise ClusterFileNotFoundError(
            f"no {cluster}.yaml under sites/*/mces/*/hostedClusters/ in day1 main: "
            "the cluster has no day1 file to upgrade it through"
        )
    if len(matches) > 1:
        raise AmbiguousClusterFileError(
            f"{len(matches)} files named {cluster}.yaml in day1 main ({', '.join(matches)}): "
            "cluster names must be unique, a person has to resolve it"
        )
    return matches[0]


async def read_cluster(*, repo_url: str, token: str, cluster: str) -> Day1Cluster:
    with tempfile.TemporaryDirectory(prefix="day1-cluster-") as tmp:
        clone = Path(tmp) / "repo"
        await clone_main(repo_url=repo_url, token=token, dest=clone)
        path = find_cluster_file(clone, cluster)
        return day1_values.read_cluster(
            clone, path, revision=await head(clone, token=token), blob=await blob(clone, path, token=token)
        )


# --- commit_day1_change ---------------------------------------------------------------------


def commit_message(request: Day1ChangeRequest, summary: str) -> str:
    return f"upgrade({request.cluster}): {summary} [{request.workflow_id}]"


def _changed(request: Day1ChangeRequest, current_blob: str) -> Day1FileChangedError:
    return Day1FileChangedError(
        f"{request.path} is at blob {current_blob}, not {request.expected_blob} as this run "
        "last read or wrote it: someone else changed it"
    )


def _require_cluster_path(request: Day1ChangeRequest) -> None:
    match = day1_values.CLUSTER_FILE.fullmatch(request.path)
    if match is None or ".." in request.path.split("/") or match.group("name") != request.cluster:
        raise ClusterFileNotFoundError(f"{request.path} is not the day1 file of {request.cluster}")


async def commit_change(
    *,
    repo_url: str,
    token: str,
    request: Day1ChangeRequest,
    validate_command: str,
    author: GitIdentity,
) -> Day1Commit:
    """One change to one cluster file at main: compare-and-set, edit, validate, push — never forced.

    The file already saying so is success with `changed` False, checked
    BEFORE the compare-and-set: a retry after a push that landed must not
    read its own commit as someone else's edit. A push rejected because main
    moved is retried from a fresh clone, where the compare-and-set runs again.
    """
    _require_cluster_path(request)
    for attempt in range(1, PUSH_ATTEMPTS + 1):
        with tempfile.TemporaryDirectory(prefix="day1-commit-") as tmp:
            clone = Path(tmp) / "repo"
            await clone_main(repo_url=repo_url, token=token, dest=clone)
            file = clone / request.path
            if not file.is_file():
                raise ClusterFileNotFoundError(f"{request.path} is no longer in day1 main")
            current = file.read_text(encoding="utf-8")
            current_blob = await blob(clone, request.path, token=token)
            changed_by_someone = request.expected_blob is not None and current_blob != request.expected_blob
            try:
                edit = day1_values.plan_edit(clone, request, current)
            except Day1ChangeRejectedError:
                # A file this run cannot edit, because someone else rewrote it,
                # is their edit to look at first.
                if changed_by_someone:
                    raise _changed(request, current_blob) from None
                raise
            if not edit.changed:
                return Day1Commit(revision=await head(clone, token=token), blob=current_blob, changed=False)
            if changed_by_someone:
                raise _changed(request, current_blob)
            file.write_text(edit.text, encoding="utf-8")
            if validate_command:
                code, output = await run_command(
                    shlex.split(validate_command), cwd=clone, secrets=[token], timeout=VALIDATOR_TIMEOUT_SECONDS
                )
                if code != 0:
                    raise Day1ChangeRejectedError(f"{validate_command} refused {edit.summary}:\n{output}")
            secrets = [token]
            await git_repo.run_git(["add", request.path], cwd=clone, secrets=secrets)
            await git_repo.run_git(
                ["-c", f"user.name={author.name}", "-c", f"user.email={author.email}",
                 "commit", "-m", commit_message(request, edit.summary)],
                cwd=clone, secrets=secrets,
            )
            # To the token URL, never `origin`: the clone's config holds no token.
            code, _, stderr = await git_repo.exec_git(
                ["push", git_repo.authenticated_url(repo_url, token), f"HEAD:{BRANCH}"], cwd=clone, secrets=secrets
            )
            if code == 0:
                return Day1Commit(
                    revision=await head(clone, token=token),
                    blob=await blob(clone, request.path, token=token),
                    changed=True,
                )
            if attempt == PUSH_ATTEMPTS:
                raise ValuesRepoGitError(
                    f"git push to day1 main was rejected {PUSH_ATTEMPTS} times (exit {code}): {stderr}"
                )
    raise AssertionError("unreachable")  # pragma: no cover
