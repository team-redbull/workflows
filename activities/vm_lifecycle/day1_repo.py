"""The day1 values repo for create-vm: a zone's KubeVirt clusters, listed from the seed folder.

The clone and the cluster-file reader are activities/day1_repo.py's — the
monorepo's one day1 reader, shared with upgrade-gitops and owned by neither
limb (the operator, 2026-11-05) — on activities/git_repo.py: the token
injected in memory only, scrubbed from every error, kept out of every file of
the clone, every clone a fresh TemporaryDirectory — ONE per activity attempt.
`main` only: Argo CD renders main and nothing else.

What is create-vm's: the folder `sites/<site>/mces/ocp4-seed-<site>-<zone>/
hostedClusters/`, built from VALIDATED tokens (shared/cluster_names.py) and
read with `iterdir()`, never globbed with input; a missing folder is an empty
list. Day1 `platform: kubevirt` is NOT the filter — that key means a cluster
whose NODES are VMs; the folder is.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from activities.day1_repo import clone_main, head, list_cluster_files, load_cluster_values
from shared.cluster_names import name_token, seed_cluster
from shared.models.vm_lifecycle import KvClusterEntry, KvClusterList


async def list_kv_clusters(*, repo_url: str, token: str, site: str, zone: str) -> KvClusterList:
    """The clusters in the seed folder of (site, zone) at day1 `main`, one entry per file.

    Observe-only: one fresh clone, the folder listed, each file read with its
    layers for its clusterName. A missing folder is an EMPTY list. Clone
    failures are git_repo's retryable ValuesRepoGitError (and
    ValuesBranchNotFoundError for a repo without `main`).
    """
    site = name_token(site, "site")
    zone = name_token(zone, "zone")
    seed = seed_cluster(site, zone)
    with tempfile.TemporaryDirectory(prefix="day1-kv-") as tmp:
        clone = Path(tmp) / "repo"
        await clone_main(repo_url=repo_url, token=token, dest=clone)
        entries: list[KvClusterEntry] = []
        for path in list_cluster_files(clone, site, seed):
            values = load_cluster_values(clone, path)
            entries.append(
                KvClusterEntry(
                    name=Path(path).stem,
                    cluster_name=values.cluster_name,
                    hosted_cluster=values.hosted_cluster,
                    path=path,
                    problems=list(values.problems),
                )
            )
        return KvClusterList(clusters=entries, revision=await head(clone, token=token))
