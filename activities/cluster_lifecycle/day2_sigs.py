"""The day2 sig repos for provision-cluster: register a cluster by creating its folder.

day2 has no registration file — a cluster exists for a sig when
`sites/<site>/<env>/mces/<mce>/<cluster>/` exists in that sig's repo, ONE
GitLab project per sig (`redbull/gitops-day2-prod/sigs/<sig>`), the tree at its root
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

from activities import git_repo
from activities.cluster_lifecycle.day1_writer import PROVISION_AUTHOR
from shared.exceptions import ValuesRepoGitError
from shared.models.cluster_lifecycle import Day2SigRegistration, Day2SigRegistrationRequest

GITKEEP = ".gitkeep"
# One path segment each, as add-cluster-to-day2.sh accepts them: a value from
# the API or the value layers must never reach the path as `..` or `a/b`.
_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def _segment(value: str, what: str) -> str:
    if not _SEGMENT.fullmatch(value):
        raise ValuesRepoGitError(f"{what} {value!r} is not a day2 path segment ({_SEGMENT.pattern})")
    return value


def cluster_folder(request: Day2SigRegistrationRequest) -> str:
    """`sites/<site>/<env>/mces/<mce>/<cluster>`, from the root of the sig's own project."""
    return "/".join([
        "sites", _segment(request.site, "site"), _segment(request.env, "env"),
        "mces", _segment(request.mce, "MCE"), _segment(request.cluster, "cluster"),
    ])


async def register(*, repo_url: str, token: str, request: Day2SigRegistrationRequest) -> Day2SigRegistration:
    """The cluster's folder in the sig repo: created on the default branch, or already there."""
    _segment(request.sig, "sig")
    folder = cluster_folder(request)
    branch = await git_repo.default_branch(repo_url=repo_url, token=token, repo="sig")
    async with git_repo.fresh_clone(
        repo_url=repo_url, token=token, repo="sig", prefix="day2-sig-", branch=branch,
        missing_message=f"the sig repo's default branch {branch!r} vanished between ls-remote and the clone",
    ) as sig:
        target = sig.path / folder
        if target.is_dir():
            return Day2SigRegistration(sig=request.sig, path=folder, created=False, commit_sha=None)
        target.mkdir(parents=True)
        (target / GITKEEP).write_text("", encoding="utf-8")
        sha = await sig.commit_and_push(
            paths=[f"{folder}/{GITKEEP}"],
            message=f"chore: register {request.cluster} in day2 ({request.sig}) [provision-cluster-workflow]",
            author=PROVISION_AUTHOR,
        )
        return Day2SigRegistration(sig=request.sig, path=folder, created=True, commit_sha=sha)
