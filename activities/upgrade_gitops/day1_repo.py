"""The day1 values repo for the upgrade: read the rules and a cluster file, commit one change — at `main`.

Plain parameters, no settings and no Temporal (CLAUDE.md §1); the git client
is activities/git_repo.py, the one segment-lifecycle uses, so the token is
injected in memory only, scrubbed from every error, kept out of every file of
the clone, and every clone is a fresh TemporaryDirectory — ONE per activity
attempt. `main` only: Argo CD renders main and nothing else (hcAppset.yaml
pins it), and this limb has no branch input to check.

What the YAML means is day1_values.py and upgrade_rules.py; this module only
moves files between git and them, and runs day1's own validators
(git_repo.run_command, which says what they can still reach):
`scripts/validate_upgrade_rules.py` before the rules are used, and the
configured day1 validator (D3) before every push.
"""

from __future__ import annotations

import shlex
import sys
import tempfile
from pathlib import Path

from activities import git_repo
from activities.upgrade_gitops import day1_values, upgrade_rules
from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    Day1ChangeRejectedError,
    Day1FileChangedError,
    UpgradeRulesInvalidError,
)
from shared.models.cluster_upgrade import Day1ChangeRequest, Day1Cluster, Day1Commit, UpgradeRules

# A validator gets this long, inside the git activities' 180s.
VALIDATOR_TIMEOUT_SECONDS = 90.0


async def clone_main(*, repo_url: str, token: str, dest: Path) -> None:
    await git_repo.clone_main(repo_url=repo_url, token=token, dest=dest, repo="day1")


async def head(clone: Path, *, token: str) -> str:
    return (await git_repo.run_git(["rev-parse", "HEAD"], cwd=clone, secrets=[token])).strip()


async def blob(clone: Path, path: str, *, token: str) -> str:
    """The git blob id of `path` at HEAD: what compare-and-set compares."""
    return (await git_repo.run_git(["rev-parse", f"HEAD:{path}"], cwd=clone, secrets=[token])).strip()


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
        code, output = await git_repo.run_command(
            [sys.executable, upgrade_rules.VALIDATOR, "--repo-root", str(clone)],
            cwd=clone, secrets=[token], timeout=VALIDATOR_TIMEOUT_SECONDS,
        )
        if code != 0:
            raise UpgradeRulesInvalidError(f"{upgrade_rules.VALIDATOR} refused the rules:\n{git_repo.tail(output)}")
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
) -> Day1Commit:
    """One change to one cluster file at main, in ONE clone: compare-and-set, edit, validate, push — never forced.

    The file already saying so is success with `changed` False, checked
    BEFORE the compare-and-set: Temporal's retry after a push that landed must
    not read its own commit as someone else's edit. A push rejected because
    main moved raises the retryable ValuesRepoGitError (git_repo.commit_and_push)
    and the retry re-clones, where the compare-and-set runs again. No retry
    loop here: one could not fit the activity's 180s (values_repo.py's rule).
    Committed as git_repo.UPGRADE_AUTHOR.
    """
    _require_cluster_path(request)
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
            code, output = await git_repo.run_command(
                shlex.split(validate_command), cwd=clone, secrets=[token], timeout=VALIDATOR_TIMEOUT_SECONDS
            )
            if code != 0:
                raise Day1ChangeRejectedError(f"{validate_command} refused {edit.summary}:\n{git_repo.tail(output)}")
        revision = await git_repo.commit_and_push(
            clone,
            repo_url=repo_url,
            token=token,
            branch=git_repo.MAIN,
            paths=[request.path],
            message=commit_message(request, edit.summary),
            author=git_repo.UPGRADE_AUTHOR,
        )
        return Day1Commit(revision=revision, blob=await blob(clone, request.path, token=token), changed=True)
