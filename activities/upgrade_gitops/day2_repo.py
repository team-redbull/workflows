"""The day2 repo: does every version-pinned chart of a cluster have a layer for every step (check 4)?

Read against the mock of gitops-day2-prod (D12), whose sig repos sit side by
side under `sigs/<sig>/` (day2 `ARCHITECTURE.md` §3):
  * a hosted cluster's charts are the folders of `sigs/<sig>/sites/<site>/<env>/mces/<mce>/<cluster>/`
    — "folder basename == Argo cluster name" — plus `sigs/<sig>/defaults/hosted-clusters/`,
    which every hosted cluster gets;
  * a chart is version-pinned in a sig when `sigs/<sig>/operators/<chart>/ocp-versions/` exists,
    and a version's layer is `ocp-versions/<full version>/` (`ARCHITECTURE.md:189`).
Without the layer the chart falls back to the team default when mastertag
moves (R1, `ARCHITECTURE.md:857-883`), so every step's version needs one.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). A READ, so it
takes activities/git_repo.py's snapshot: the remote is asked for its head
every time and day2 is cloned only when it moved.
"""

from __future__ import annotations

from pathlib import Path

from activities import git_repo
from shared.models.cluster_upgrade import Day2LayerGap, Day2Layers


def cluster_charts(root: Path, cluster: str) -> list[tuple[str, str]]:
    """(sig, chart) for every chart the cluster's day2 Applications render, sorted."""
    sigs = root / "sigs"
    found: set[tuple[str, str]] = set()
    if not sigs.is_dir():
        return []
    for sig in sorted(path for path in sigs.iterdir() if path.is_dir()):
        # conventions: allow AC009 — day2's tree (`sites/<site>/<env>/mces/<mce>/<cluster>/`
        # under each sig), not day1's: `Day1` does not walk it.
        day2_cluster_folders = "sites/*/*/mces/*/*"
        folders = [
            path for path in sig.glob(day2_cluster_folders)
            if path.is_dir() and path.name == cluster
        ]
        folders.append(sig / "defaults" / "hosted-clusters")
        for folder in folders:
            if folder.is_dir():
                found.update((sig.name, chart.name) for chart in folder.iterdir() if chart.is_dir())
    return sorted(found)


def layer_gaps(root: Path, cluster: str, versions: list[str]) -> list[Day2LayerGap]:
    gaps = []
    for sig, chart in cluster_charts(root, cluster):
        layers = root / "sigs" / sig / "operators" / chart / "ocp-versions"
        if not layers.is_dir():
            continue  # not version-pinned: the chart renders the same for every version
        gaps += [
            Day2LayerGap(sig=sig, chart=chart, version=version)
            for version in versions
            if not (layers / version).is_dir()
        ]
    return gaps


async def check_layers(*, repo_url: str, token: str, cluster: str, versions: list[str]) -> Day2Layers:
    async with git_repo.snapshot(repo_url=repo_url, token=token, repo="day2") as day2:
        return Day2Layers(missing=layer_gaps(day2.path, cluster, versions))
