"""day1's hosted clusters as a typed answer, for workflows whose own limb holds no day1 token.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). Nothing about
day1 is decided here: the reading, the kinds, the zone check and the order are
activities/day1_repo.py's `Day1` (the monorepo's one day1 reader), on its
snapshot of `main` — the remote asked for its head every time, a clone only
when it moved. This module only turns its records into the boundary models and
attaches the commit they were read at.
"""

from __future__ import annotations

from activities.day1_repo import ClusterRecord, main_snapshot
from shared.models.cluster_lifecycle import Day1ClusterEntry, Day1ClusterList, Day1ClusterQuery


def entry(record: ClusterRecord) -> Day1ClusterEntry:
    return Day1ClusterEntry(
        name=record.name,
        site=record.site,
        folder=record.folder,
        kind=record.kind,
        env=record.env,
        zone=record.zone,
        id=record.id,
        cluster_name=record.cluster_name,
        hosted_cluster=record.hosted_cluster,
        hosted_cluster_namespace=record.hosted_cluster_namespace,
        path=record.path,
        problems=list(record.problems),
    )


async def list_clusters(*, repo_url: str, token: str, query: Day1ClusterQuery) -> Day1ClusterList:
    """The clusters day1 `main` holds for the query, and the commit they were read at.

    Observe-only. A filter matching nothing is an empty list; one that is not
    a token of the naming convention is Day1's ValueError (the activity checks
    the query first, so that never costs a call to the remote)."""
    async with main_snapshot(repo_url=repo_url, token=token) as day1:
        records = day1.clusters(site=query.site, env=query.env, kind=query.kind, zone=query.zone)
        return Day1ClusterList(clusters=[entry(record) for record in records], revision=day1.revision)
