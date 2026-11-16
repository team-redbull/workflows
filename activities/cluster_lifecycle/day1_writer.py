"""The day1 values repo for provision-cluster: where a cluster goes, and its file on its branch.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). The git client
is activities/git_repo.py and the reader activities/day1_repo.py — the
monorepo's one day1 reader, shared with vm-lifecycle and upgrade-gitops and
owned by none of them: the token injected in memory only, scrubbed from every
error, kept out of every file of the clone, every clone a fresh
TemporaryDirectory — ONE per activity attempt.

What is provision-cluster's:

  * inspect — `main` is read: the MCE folder is found by NAME among
    `sites/*/mces/` (iterated, never globbed with input), the cluster must not
    have a file there yet, and the text is judged against the merged value
    layers (cluster_values.validate_cluster_values).
  * publish — the cluster file goes on `provision-cluster/<cluster>`, created
    from `main` by the push itself (git_repo.commit_and_push pushes
    `HEAD:refs/heads/<branch>`). The commit carries NO `[skip ci]`: the day1
    CI chain must run on the branch — it skips allocation (the file carries
    `vlanId`), renders the MachineConfigs and pushes its own `[skip ci]`
    commit with the `mcConfigMap` section on top. A branch already there is a
    re-run: its file must mean the same as this run's (same_cluster_file),
    and then nothing is pushed.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from activities import git_repo
from activities.cluster_lifecycle import cluster_values
from activities.day1_repo import clone_main, head
from shared.cluster_names import provision_cluster_branch
from shared.exceptions import (
    AmbiguousMceFolderError,
    ClusterAlreadyProvisionedError,
    ClusterFileConflictError,
    MceFolderNotFoundError,
    ValuesBranchNotFoundError,
)
from shared.models.cluster_lifecycle import ClusterFilePublishRequest, ClusterFileRef, ClusterTarget
from shared.models.segment_lifecycle import DhcpValues

SITES_ROOT = "sites"
MCES = "mces"
HOSTED_CLUSTERS = "hostedClusters"

# Who the cluster file's commit is by. Hard-coded, not configured, like every
# other writer's identity here: it names THIS workflow in day1's history.
PROVISION_AUTHOR = git_repo.GitIdentity(
    name="provision-cluster-workflow", email="provision-cluster-workflow@redbull.local"
)

# A folder or file name as the edge validated it: one path segment, nothing git
# or the filesystem could read as more. Re-checked here because the limb must
# not trust a request to have come through the router.
_NAME = re.compile(r"^[a-z0-9]+(?:[-.a-z0-9]*[a-z0-9])?$")
_MCE_DIR = re.compile(rf"^{SITES_ROOT}/(?P<site>[^/]+)/{MCES}/(?P<mce>[^/]+)$")


def _require_name(value: str, what: str) -> str:
    if not _NAME.fullmatch(value) or ".." in value:
        raise MceFolderNotFoundError(f"{what} {value!r} is not a plain day1 folder or file name")
    return value


def cluster_file_path(mce_dir: str, cluster: str) -> str:
    return f"{mce_dir}/{HOSTED_CLUSTERS}/{cluster}.yaml"


def find_mce_dir(clone: Path, mce: str) -> tuple[str, str]:
    """(site, `sites/<site>/mces/<mce>`) for the ONE site holding that MCE folder.

    The sites are iterated, and `mce` compared as a name: it comes from an
    unauthenticated API, so it is never a glob and never a path.
    """
    _require_name(mce, "MCE folder")
    sites_root = clone / SITES_ROOT
    sites = sorted(path.name for path in sites_root.iterdir() if path.is_dir()) if sites_root.is_dir() else []
    matches = [site for site in sites if (sites_root / site / MCES / mce).is_dir()]
    if not matches:
        raise MceFolderNotFoundError(
            f"no {SITES_ROOT}/<site>/{MCES}/{mce} folder at day1 main (sites: {', '.join(sites) or 'none'}): "
            "the MCE is named by its folder, exactly"
        )
    if len(matches) > 1:
        raise AmbiguousMceFolderError(
            f"{MCES}/{mce} exists under {len(matches)} sites at day1 main ({', '.join(matches)}): "
            "an MCE lives at one site, a human must resolve which"
        )
    return matches[0], f"{SITES_ROOT}/{matches[0]}/{MCES}/{mce}"


async def inspect(*, repo_url: str, token: str, cluster: str, mce: str, values: str) -> ClusterTarget:
    """Where the cluster goes at day1 `main`, and whether its text would render there."""
    _require_name(cluster, "cluster")
    with tempfile.TemporaryDirectory(prefix="day1-inspect-") as tmp:
        clone = Path(tmp) / "repo"
        await clone_main(repo_url=repo_url, token=token, dest=clone)
        site, mce_dir = find_mce_dir(clone, mce)
        path = cluster_file_path(mce_dir, cluster)
        if (clone / path).exists():
            raise ClusterAlreadyProvisionedError(
                f"{path} already exists at day1 main: the cluster is provisioned (or a file of its "
                "name is). This workflow creates cluster files and never rewrites one; changing a "
                "cluster is an edit on day1"
            )
        validated = cluster_values.validate_cluster_values(clone, path, values, mce=mce)
        return ClusterTarget(
            site=site,
            mce_dir=mce_dir,
            cluster_name=validated.cluster_name,
            mastertag=validated.mastertag,
            config_names=validated.config_names,
            day2_env=validated.day2_env,
            day2_sigs=validated.day2_sigs,
            revision=await head(clone, token=token),
        )


async def branch_exists(*, repo_url: str, branch: str, token: str) -> bool:
    """Whether the remote has the branch (git_repo.require_branch's classification:
    absent is deterministic, unreachable stays the retryable ValuesRepoGitError)."""
    try:
        await git_repo.require_branch(repo_url=repo_url, branch=branch, token=token, missing_message="absent")
    except ValuesBranchNotFoundError:
        return False
    return True


async def publish(
    *, repo_url: str, token: str, request: ClusterFilePublishRequest, dhcp_values: DhcpValues
) -> ClusterFileRef:
    """The cluster file on `provision-cluster/<cluster>`: pushed, or already there."""
    _require_name(request.cluster, "cluster")
    if not _MCE_DIR.fullmatch(request.mce_dir) or ".." in request.mce_dir.split("/"):
        raise MceFolderNotFoundError(f"{request.mce_dir!r} is not a day1 MCE folder path (sites/<site>/mces/<mce>)")
    branch = request.branch or provision_cluster_branch(request.cluster)
    path = cluster_file_path(request.mce_dir, request.cluster)
    text = cluster_values.assemble_cluster_file(request.values, request.vlan_id, dhcp_values)

    if not await branch_exists(repo_url=repo_url, branch=branch, token=token):
        with tempfile.TemporaryDirectory(prefix="day1-publish-") as tmp:
            clone = Path(tmp) / "repo"
            await clone_main(repo_url=repo_url, token=token, dest=clone)
            if not (clone / request.mce_dir).is_dir():
                raise MceFolderNotFoundError(f"{request.mce_dir} is no longer at day1 main — it was there when this run inspected it")
            file = clone / path
            if file.exists():
                raise ClusterAlreadyProvisionedError(
                    f"{path} appeared at day1 main since this run inspected it: the cluster is provisioned"
                )
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(text, encoding="utf-8")
            sha = await git_repo.commit_and_push(
                clone,
                repo_url=repo_url,
                token=token,
                branch=branch,
                paths=[path],
                message=f"feat: provision hosted cluster {request.cluster} [provision-cluster-workflow]",
                author=PROVISION_AUTHOR,
            )
            return ClusterFileRef(relative_path=path, branch=branch, commit_sha=sha, changed=True, dhcp_values=dhcp_values)

    with tempfile.TemporaryDirectory(prefix="day1-rerun-") as tmp:
        clone = Path(tmp) / "repo"
        await git_repo.clone(
            repo_url=repo_url, branch=branch, token=token, dest=clone,
            missing_message=f"branch {branch} vanished between the check and the clone",
        )
        file = clone / path
        if not file.is_file():
            raise ClusterFileConflictError(
                f"branch {branch} exists at day1 but has no {path}: not this workflow's branch, or one "
                "a person emptied — delete or rename it, then re-run"
            )
        reason = cluster_values.same_cluster_file(file.read_text(encoding="utf-8"), text)
        if reason is not None:
            raise ClusterFileConflictError(
                f"branch {branch} already carries {path} and it is not this run's file ({reason}); "
                "a human must decide which is right — this workflow never overwrites"
            )
        return ClusterFileRef(relative_path=path, branch=branch, commit_sha=None, changed=False, dhcp_values=dhcp_values)
