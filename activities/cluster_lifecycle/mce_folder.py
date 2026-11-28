"""A new MCE's folder in the day1 values repo, for create-upi-cluster: on its branch, or already merged.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). git through
activities/git_repo.py, the day1 layout through activities/day1_repo.py (AC009),
exactly as day1_writer.py does for a hosted cluster's file.

The folder is what adds an MCE to the hub's Argo CD: the `<site>-mces`
ApplicationSet makes one Application per `sites/<site>/mces/*` folder at `main`,
with destination name = the folder name = the Argo CD cluster Secret's `name`
(register_upi_cluster_in_argocd). Two files:

  * `values.yaml` — the MCE's own value layer (the operator's text, verbatim,
    or a header comment only: the site's and the global layers then apply alone);
  * `version.yaml` — `mastertag: <the installed cluster's version>`, the MCE's
    version as day2 reads it.

No `hostedClusters/`: an EMPTY file there fails the MCE's hosted-cluster
generator, and a missing folder is simply an MCE with no hosted clusters yet.

Re-runs compare MEANING, not bytes (YAML-equal), as day1_writer does: the same
content already at `main` (a re-run past the merge) or on the branch is success;
anything else is MceFolderConflictError — this module never overwrites day1.
"""

from __future__ import annotations

import re

import yaml

from activities import git_repo
from activities.day1_repo import fresh_main, main_snapshot, mce_dir, mce_values_path, mce_version_path, site_dir
from shared.exceptions import Day1SiteNotFoundError, MceFolderConflictError, MceFolderNotFoundError
from shared.models.upi_lifecycle import MceFolderPublishRequest, MceFolderRef

UPI_AUTHOR = git_repo.GitIdentity(name="create-upi-cluster-workflow", email="create-upi-cluster-workflow@redbull.local")

_NAME = re.compile(r"^[a-z0-9]+(?:[-a-z0-9]*[a-z0-9])?$")
_VALUES_HEADER = (
    "# The MCE's own value layer, added by create-upi-cluster for site {site}.\n"
    "# Empty: the site's and the global value layers apply alone.\n"
)


def _require_name(value: str, what: str) -> str:
    if not _NAME.fullmatch(value):
        raise MceFolderNotFoundError(f"{what} {value!r} is not a plain day1 folder name")
    return value


def folder_files(request: MceFolderPublishRequest) -> dict[str, str]:
    """{relative path: text} of the folder this request writes."""
    folder = mce_dir(request.site, request.mce)
    values = request.values if request.values and request.values.strip() else _VALUES_HEADER.format(site=request.site)
    if not values.endswith("\n"):
        values += "\n"
    return {
        mce_values_path(folder): values,
        mce_version_path(folder): f"mastertag: {request.mastertag}\n",
    }


def _meaning(text: str) -> object:
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as err:
        return ("unparsable", str(err)[:200])


def differences(present: dict[str, str | None], wanted: dict[str, str]) -> list[str]:
    """Which files of the folder do not MEAN what this run writes (missing included)."""
    problems = []
    for path, text in wanted.items():
        found = present.get(path)
        if found is None:
            problems.append(f"{path} is missing")
        elif _meaning(found) != _meaning(text):
            problems.append(f"{path} differs")
    return problems


def check_values_text(values: str | None) -> None:
    """The operator's values text must be ONE YAML mapping (or nothing)."""
    if values is None or not values.strip():
        return
    try:
        loaded = yaml.safe_load(values)
    except yaml.YAMLError as err:
        raise MceFolderConflictError(f"mce_values is not YAML: {str(err)[:300]}") from None
    if loaded is not None and not isinstance(loaded, dict):
        raise MceFolderConflictError(f"mce_values must be one YAML mapping, not {type(loaded).__name__}")


async def publish(*, repo_url: str, token: str, request: MceFolderPublishRequest) -> MceFolderRef:
    """The MCE folder on `request.branch`: pushed, already there, or already merged."""
    _require_name(request.mce, "MCE")
    _require_name(request.site, "site")
    check_values_text(request.values)
    wanted = folder_files(request)
    folder = mce_dir(request.site, request.mce)
    files = sorted(wanted)

    async with main_snapshot(repo_url=repo_url, token=token) as day1:
        if request.site not in day1.sites():
            raise Day1SiteNotFoundError(
                f"no {site_dir(request.site)}/ at day1 main (sites: {', '.join(day1.sites()) or 'none'}): "
                "the hub's ApplicationSets add MCEs per site folder"
            )
        elsewhere = [site for site, _ in day1.mce_folders(request.mce) if site != request.site]
        if elsewhere:
            raise MceFolderConflictError(
                f"an MCE folder named {request.mce} already exists at day1 main under site(s) "
                f"{', '.join(elsewhere)}: MCE names are unique across day1"
            )
        if day1.mce_folders(request.mce):
            present = {path: _read(day1.root / path) for path in wanted}
            problems = differences(present, wanted)
            if problems:
                raise MceFolderConflictError(
                    f"{folder} already exists at day1 main and is not this run's ({'; '.join(problems)}); "
                    "a person decides which is right — this workflow never overwrites day1"
                )
            return MceFolderRef(folder=folder, files=files, branch=request.branch, changed=False, on_main=True)

    if not await git_repo.branch_exists(repo_url=repo_url, branch=request.branch, token=token):
        async with fresh_main(repo_url=repo_url, token=token, prefix="day1-mce-") as clone:
            if (clone.path / folder).exists():
                raise MceFolderConflictError(f"{folder} appeared at day1 main since this run read it")
            for path, text in wanted.items():
                target = clone.path / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding="utf-8")
            sha = await clone.commit_and_push(
                paths=files,
                message=f"feat: add MCE {request.mce} at {request.site} [create-upi-cluster-workflow]",
                author=UPI_AUTHOR,
                branch=request.branch,
            )
            return MceFolderRef(folder=folder, files=files, branch=request.branch, commit_sha=sha, changed=True)

    # conventions: allow AC008 — the writer's own "already there" half: a re-run reads the
    # branch this workflow pushed, once (day1_writer.publish's precedent).
    async with git_repo.fresh_clone(
        repo_url=repo_url, token=token, repo="day1", prefix="day1-mce-rerun-", branch=request.branch,
        missing_message=f"branch {request.branch} vanished between the check and the clone",
    ) as clone:
        present = {path: _read(clone.path / path) for path in wanted}
        problems = differences(present, wanted)
        if problems:
            raise MceFolderConflictError(
                f"branch {request.branch} already exists at day1 and its {folder} is not this run's "
                f"({'; '.join(problems)}); delete or rename the branch, then re-run"
            )
        return MceFolderRef(folder=folder, files=files, branch=request.branch, changed=False)


def _read(path) -> str | None:
    return path.read_text(encoding="utf-8") if path.is_file() else None
