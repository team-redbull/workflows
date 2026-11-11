"""The day2 sig repos for provision-cluster: register a cluster by creating its folder.

day2 has no registration file — a cluster exists for a sig when
`sites/<site>/<env>/mces/<mce>/<cluster>/` exists in that sig's repo
(gitops-day2-prod ARCHITECTURE.md R6; the semantics of the retired chart Job's
files/add-cluster-to-day2.sh). So "registering" is one folder with a
`.gitkeep`, pushed to the sig repo's DEFAULT branch — bypassing the sig's
merge-request lint by design, as the script did.

Plain parameters, no settings and no Temporal (CLAUDE.md §1); the git client
is activities/git_repo.py, so the token is injected in memory only, scrubbed
from every error and kept out of every file of the clone, and every clone is
a fresh TemporaryDirectory — ONE per activity attempt. A rejected push (the
default branch moved) is the retryable ValuesRepoGitError and Temporal's
retry re-clones: the convergence loop the script ran itself.

Runs only AFTER the cluster file is merged to day1 `main` (the workflow waits
for the merge): day2 renders a cluster's `mastertag` from day1 main, and a
folder created before the merge breaks that sig's whole render
(APPLY-DAY2-COPY §4b).
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from activities import git_repo
from activities.cluster_lifecycle.day1_writer import PROVISION_AUTHOR
from shared.exceptions import ValuesRepoGitError
from shared.models.cluster_lifecycle import Day2SigRegistration, Day2SigRegistrationRequest

GITKEEP = ".gitkeep"
# One path segment each, as add-cluster-to-day2.sh accepts them: a value from
# the API or the value layers must never reach the path as `..` or `a/b`.
_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SYMREF = re.compile(r"^ref: refs/heads/(?P<branch>\S+)\tHEAD$", re.M)


def _segment(value: str, what: str) -> str:
    if not _SEGMENT.fullmatch(value):
        raise ValuesRepoGitError(f"{what} {value!r} is not a day2 path segment ({_SEGMENT.pattern})")
    return value


def cluster_folder(path_prefix: str, request: Day2SigRegistrationRequest) -> str:
    """`<prefix>/sites/<site>/<env>/mces/<mce>/<cluster>` (no prefix when the tree is at the root)."""
    parts = [
        "sites", _segment(request.site, "site"), _segment(request.env, "env"),
        "mces", _segment(request.mce, "MCE"), _segment(request.cluster, "cluster"),
    ]
    prefix = path_prefix.strip("/")
    if prefix:
        if ".." in prefix.split("/"):
            raise ValuesRepoGitError(f"day2 path prefix {prefix!r} must stay inside the repo")
        parts = prefix.split("/") + parts
    return "/".join(parts)


async def default_branch(*, repo_url: str, token: str) -> str:
    """The sig repo's default branch, from `git ls-remote --symref <url> HEAD`."""
    returncode, stdout, stderr = await git_repo.exec_git(
        ["ls-remote", "--symref", git_repo.authenticated_url(repo_url, token), "HEAD"], cwd=None, secrets=[token]
    )
    if returncode != 0:
        raise ValuesRepoGitError(f"git ls-remote --symref on the sig repo failed (exit {returncode}): {stderr}")
    match = _SYMREF.search(stdout)
    if match is None:
        raise ValuesRepoGitError("the sig repo reported no default branch (ls-remote --symref HEAD gave no symref)")
    return match.group("branch")


async def register(
    *, repo_url: str, token: str, path_prefix: str, request: Day2SigRegistrationRequest
) -> Day2SigRegistration:
    """The cluster's folder in the sig repo: created on the default branch, or already there."""
    _segment(request.sig, "sig")
    folder = cluster_folder(path_prefix, request)
    branch = await default_branch(repo_url=repo_url, token=token)
    with tempfile.TemporaryDirectory(prefix="day2-sig-") as tmp:
        clone = Path(tmp) / "repo"
        await git_repo.clone(
            repo_url=repo_url, branch=branch, token=token, dest=clone,
            missing_message=f"the sig repo's default branch {branch!r} vanished between ls-remote and the clone",
        )
        target = clone / folder
        if target.is_dir():
            return Day2SigRegistration(sig=request.sig, path=folder, created=False, commit_sha=None)
        target.mkdir(parents=True)
        (target / GITKEEP).write_text("", encoding="utf-8")
        sha = await git_repo.commit_and_push(
            clone,
            repo_url=repo_url,
            token=token,
            branch=branch,
            paths=[f"{folder}/{GITKEEP}"],
            message=f"chore: register {request.cluster} in day2 ({request.sig}) [provision-cluster-workflow]",
            author=PROVISION_AUTHOR,
        )
        return Day2SigRegistration(sig=request.sig, path=folder, created=True, commit_sha=sha)
