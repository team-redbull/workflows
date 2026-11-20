"""THE day1 values repo reader of the monorepo: day1 `main` at one commit, as a set of
questions (`Day1`), plus a cluster file read the way the hostedclusters-setup chart renders it.

Shared by every limb that reads day1 — cluster-lifecycle (provision-cluster's
inspect, and `list_day1_clusters` for workflows whose own limb holds no day1
token), upgrade-gitops (the upgrade's rules, cluster file and edits) and
server-lifecycle (update-nodepool's read) — so it lives HERE, next to
activities/git_repo.py, in no domain: a limb never imports another domain's
code to read day1 (the operator, 2026-11-05). It cannot live in shared/ —
that is the brain↔limb contract, temporalio + pydantic only, imported inside
the workflow sandbox (SH001), and this needs PyYAML and git. Every limb image
copies all of activities/, so each limb that needs it carries it.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). The git client
is activities/git_repo.py. `main` only: Argo CD renders main and nothing else.
  * a READ opens `main_snapshot()`: git_repo.snapshot of `main` — the remote is
    asked for its head every time, a clone is made only when it moved — as a
    `Day1`, whose `revision` every read result carries.
  * a WRITE opens `fresh_main()`: one fresh clone per activity attempt.

THE REPO (inspected 2026-11-20, gitops-day1/platform-config): every cluster
file is `sites/<site>/mces/<folder>/hostedClusters/<cluster>.yaml`. A
`<folder>` is an MCE (`ocp4-<env>-mce-<site>-<letter>`, or a legacy name such
as `ocp4-mce-site2`) or a SEED (`ocp4-seed-<site>-<zone number>`, the one
cluster hosting a zone's KV clusters). There is no env folder. This module is
the ONE place that layout is written down (checker AC009): a domain module
asks `Day1`, or builds the path it writes with `mce_dir` / `cluster_file_path`.

`Day1` — the definitions, each in one place:
  * kind `kv`: a hosted cluster under a SEED folder. Never by name: day1 has
    `ocp4-test-kubevirt-site4` under an MCE, and it is an `hc`.
  * kind `hc`: a hosted cluster under any other folder. KV clusters are hosted
    clusters too, so `hc` means "not under a seed", not "not KubeVirt".
  * zone (kv only): the seed folder's (`seed_zone`), cross-checked against
    EVERY NodePool's `nodeLabels["topology.kubernetes.io/zone"]` in the merged
    values; a NodePool naming another zone or none, or no NodePool at all, is
    a `problems` line — the record is still returned, with it.
  * id (kv only): the sequence letter the convention ends the name with
    (`ocp4-<lifecycle>-kv-<site>-<id>`), given in creation order; a name off
    the convention has none, and a `problems` line.
  * env: `day2.env` from the merged values, else the cluster name's first
    field after `ocp4-` (`cluster_env`, provision-cluster's rule, moved here).
    Never the folder's name: legacy folders carry none.
  * site: the `sites/<site>` folder.
  `Day1` answers FACTS and raises none of shared/exceptions.py: what a missing
  or a duplicated name means is the calling limb's to classify. Folders are
  walked with `iterdir()` and compared by name, never globbed and never joined
  into a path from caller input; `site`, `zone` and `env` filters are
  validated tokens.

READING a cluster file (`load_cluster_values`) follows the production
derivation in stack-export:
  * the value layers, in Argo CD's order (`hcAppset.yaml` valueFiles):
    `sites/configValues.yaml` → `sites/<site>/values.yaml` →
    `sites/<site>/mces/<mce>/values.yaml` → the cluster file; a missing layer
    is skipped (`ignoreMissingValueFiles: true`). Helm merges them: maps deep,
    lists and scalars replaced, a null deletes the key.
  * clusterName: the cluster FILE's own `clusterName`, else the file name
    without `.yaml` and `clusterFilePrefix` (`hcAppset.yaml:35`, a parameter,
    so it outranks every layer). The prefix is argocd-platform's
    `hostedClusters/values.yaml` (`ocp4-`), which configValues.yaml may
    override because the `<site>-mces` ApplicationSet passes it as a value file.
  * the HostedCluster is `openshift-<clusterName>` in `hcp-<clusterName>`
    (`templates/hc.yaml:34-35`).
  Whatever the chart would refuse to render, and every duplicate key, is a
  line in `problems`, never an exception.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from activities import git_repo
from shared.cluster_names import (
    ClusterNameError,
    env_of_hosted_cluster,
    name_token,
    parse_kv_cluster,
    seed_site,
    seed_zone,
    zone_number,
)
from shared.upgrade_layout import DAY1_CLUSTER_FILE as CLUSTER_FILE
from shared.upgrade_layout import ZONE_LABEL

SITES = "sites"
MCES = "mces"
HOSTED_CLUSTERS = "hostedClusters"
CONFIG_VALUES = f"{SITES}/configValues.yaml"
# argocd-platform hostedClusters/values.yaml `clusterFilePrefix` (stack-export).
DEFAULT_CLUSTER_FILE_PREFIX = "ocp4-"

KIND_ALL = "all"
KIND_HC = "hc"
KIND_KV = "kv"
KINDS = (KIND_ALL, KIND_HC, KIND_KV)

_MCE_DIR = re.compile(rf"{SITES}/(?P<site>[^/]+)/{MCES}/(?P<folder>[^/]+)")


# --- git -------------------------------------------------------------------------------


def fresh_main(*, repo_url: str, token: str, prefix: str) -> AbstractAsyncContextManager[git_repo.Clone]:
    """A fresh clone of day1 `main` (git_repo.fresh_clone), for one activity attempt of a WRITER."""
    return git_repo.fresh_clone(repo_url=repo_url, token=token, repo="day1", prefix=prefix)


@asynccontextmanager
async def main_snapshot(*, repo_url: str, token: str, cache_root: Path | None = None) -> AsyncIterator[Day1]:
    """day1 `main` at the remote's head, for a READ (git_repo.snapshot), as a `Day1`."""
    async with git_repo.snapshot(repo_url=repo_url, token=token, repo="day1", cache_root=cache_root) as snapshot:
        yield Day1(root=snapshot.path, revision=snapshot.revision, snapshot=snapshot)


# --- the layout: the paths a writer builds ----------------------------------------------------


def mce_dir(site: str, folder: str) -> str:
    """`sites/<site>/mces/<folder>`: an MCE's (or a seed's) folder, relative to the repo root."""
    return f"{SITES}/{site}/{MCES}/{folder}"


def parse_mce_dir(path: str) -> tuple[str, str] | None:
    """(site, folder) of a `sites/<site>/mces/<folder>` path; None for anything else, `..` segments included."""
    match = _MCE_DIR.fullmatch(path)
    if match is None or ".." in path.split("/"):
        return None
    return match.group("site"), match.group("folder")


def cluster_file_path(folder_dir: str, cluster: str) -> str:
    """`<folder_dir>/hostedClusters/<cluster>.yaml`: where a cluster's file is, or goes."""
    return f"{folder_dir}/{HOSTED_CLUSTERS}/{cluster}.yaml"


# --- YAML ---------------------------------------------------------------------


def load_yaml(text: str, where: str) -> tuple[Any, list[str]]:
    """The document, last key winning as Helm's YAML parser does, and every key written twice.

    A document that does not parse is (None, [the reason]).
    """
    problems: list[str] = []

    class _Loader(yaml.SafeLoader):
        pass

    def construct_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
        seen: dict[Any, int] = {}
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = loader.construct_object(key_node, deep=True)
            line = key_node.start_mark.line + 1
            try:
                first = seen.get(key)
            except TypeError:  # an unhashable key: the constructor below reports it
                continue
            if first is not None:
                problems.append(
                    f"{where}:{line}: key {key!r} is written twice (first at line {first}); "
                    "only the last one counts"
                )
            else:
                seen[key] = line
        return loader.construct_mapping(node, deep=deep)

    _Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping)
    try:
        return yaml.load(text, Loader=_Loader), problems  # noqa: S506 - a SafeLoader subclass
    except yaml.YAMLError as err:
        return None, [f"{where}: not valid YAML: {err}"]


def merge_values(layers: list[dict]) -> dict:
    """Helm's merge of value files: maps deep, everything else replaced, a null deletes."""
    merged: dict = {}
    for layer in layers:
        merged = _coalesce(merged, layer)
    return merged


def _coalesce(base: dict, over: dict) -> dict:
    result = dict(base)
    for key, value in over.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _coalesce(result[key], value)
        else:
            result[key] = value
    return result


def unset(value: Any) -> bool:
    """Sprig's `default`: nil, "", 0, false and empty collections count as unset."""
    return value is None or value is False or value == "" or value == 0 or value == [] or value == {}


# --- reading a cluster file -----------------------------------------------------------


@dataclass
class ClusterValues:
    """Everything read about one cluster file: each layer, the merge, and what is wrong."""

    path: str
    own: dict  # the cluster file alone
    merged: dict
    cluster_name: str
    problems: list[str] = field(default_factory=list)

    @property
    def hosted_cluster(self) -> str:
        """The HostedCluster the chart renders (`templates/hc.yaml:34`)."""
        return f"openshift-{self.cluster_name}"

    @property
    def hosted_cluster_namespace(self) -> str:
        return f"hcp-{self.cluster_name}"


def layer_paths(path: str) -> list[str]:
    """The value files Argo CD renders this cluster file with, in order, the file itself last."""
    match = CLUSTER_FILE.fullmatch(path)
    if match is None:
        raise ValueError(f"{path} is not a day1 cluster file (sites/<site>/mces/<mce>/hostedClusters/<name>.yaml)")
    site, mce = match.group("site"), match.group("mce")
    return [CONFIG_VALUES, f"{SITES}/{site}/values.yaml", f"{mce_dir(site, mce)}/values.yaml", path]


def load_cluster_values(root: Path, path: str, *, text: str | None = None) -> ClusterValues:
    """The cluster file at `path` under `root` with its layers; `text` stands in for the file's content."""
    problems: list[str] = []
    layers: list[dict] = []
    own: dict = {}
    config_values: dict = {}
    for layer in layer_paths(path):
        is_cluster_file = layer == path
        if is_cluster_file and text is not None:
            content = text
        else:
            file = root / layer
            if not file.is_file():
                continue
            content = file.read_text(encoding="utf-8")
        data, found = load_yaml(content, layer)
        problems += found
        if data is None:
            data = {}
        if not isinstance(data, dict):
            problems.append(f"{layer}: must be a mapping of values, not {type(data).__name__}")
            data = {}
        layers.append(data)
        if is_cluster_file:
            own = data
        if layer == CONFIG_VALUES:
            config_values = data
    prefix = config_values.get("clusterFilePrefix")
    prefix = DEFAULT_CLUSTER_FILE_PREFIX if prefix is None else str(prefix)
    name = CLUSTER_FILE.fullmatch(path).group("name")  # type: ignore[union-attr]
    cluster_name = str(own["clusterName"]) if not unset(own.get("clusterName")) else name.removeprefix(prefix)
    return ClusterValues(path=path, own=own, merged=merge_values(layers), cluster_name=cluster_name, problems=problems)


def cluster_env(merged: dict, stem: str) -> str | None:
    """The environment a cluster belongs to: `day2.env` when the merged values name one,
    else the first field of its name after `ocp4-` (`ocp4-prod-iac-site2-a` -> `prod`).

    THE rule, for provision-cluster's day2 folder and for every `Day1` query.
    Never the MCE folder's name: legacy folders (`ocp4-mce-site2`) carry none."""
    day2 = merged.get("day2")
    explicit = day2.get("env") if isinstance(day2, dict) else None
    if isinstance(explicit, str) and explicit:
        return explicit
    return env_of_hosted_cluster(stem)


# --- listing a folder's cluster files -----------------------------------------------------


def list_cluster_files(clone: Path, site: str, mce_folder: str) -> list[str]:
    """Every `sites/<site>/mces/<mce_folder>/hostedClusters/*.yaml`, relative, sorted; [] without the folder.

    `iterdir()`, never a glob with input in it: the tokens are validated by the
    caller, and this never interprets them.
    """
    folder = clone / SITES / site / MCES / mce_folder / HOSTED_CLUSTERS
    if not folder.is_dir():
        return []
    return sorted(
        str(path.relative_to(clone)) for path in folder.iterdir() if path.is_file() and path.suffix == ".yaml"
    )


# --- day1 at one commit, as questions ---------------------------------------------------------


@dataclass(frozen=True)
class ClusterRecord:
    """One hosted cluster as day1 describes it at `revision` (the module docstring defines each field)."""

    name: str  # the file stem: the cluster's name everywhere (Argo CD Application, queue names)
    site: str
    folder: str  # the MCE or seed folder the file is under
    kind: str  # KIND_HC | KIND_KV
    env: str | None
    zone: str | None  # kv only
    id: str | None  # kv only; None when the name is off the convention
    cluster_name: str
    hosted_cluster: str
    hosted_cluster_namespace: str
    path: str
    problems: list[str]
    revision: str


def _zone_problems(merged: dict, zone: str, path: str) -> list[str]:
    """Why the NodePools do not all say the cluster's nodes are in `zone`; [] when they do."""
    nodepools = merged.get("nodepool")
    if not isinstance(nodepools, list) or not nodepools:
        return [f"{path}: no NodePool, so no `{ZONE_LABEL}` nodeLabel says its nodes are in zone {zone}"]
    problems: list[str] = []
    for index, entry in enumerate(nodepools):
        labels = entry.get("nodeLabels") if isinstance(entry, dict) else None
        label = labels.get(ZONE_LABEL) if isinstance(labels, dict) else None
        if unset(label):
            problems.append(
                f"{path}: nodepool[{index}] has no `{ZONE_LABEL}` nodeLabel; under the seed of zone {zone} it must name it"
            )
        elif str(label) != zone:
            problems.append(
                f"{path}: nodepool[{index}] labels its nodes zone {str(label)!r}, but the file is under the seed of zone {zone}"
            )
    return problems


def _order(record: ClusterRecord) -> tuple:
    """Site, folder, then oldest first: the id letter, a name without one last, ties by name."""
    return (record.site, record.folder, record.id is None, record.id or "", record.name)


@dataclass(frozen=True)
class Day1:
    """day1 `main` at ONE commit, read-only: the questions workflows ask about its clusters.

    Pure over a directory — `Day1(root, revision="…")` on a fixture tree needs
    no git. `main_snapshot()` opens it on the cached snapshot of the remote's
    head and adds what only git answers (`blob`, `scratch`)."""

    root: Path
    revision: str
    snapshot: git_repo.Snapshot | None = field(default=None, repr=False, compare=False)

    # --- the walk -------------------------------------------------------------------

    def sites(self) -> list[str]:
        """Every `sites/<site>` folder, sorted."""
        return self._folders_of(self.root / SITES)

    def folders(self, site: str) -> list[str]:
        """Every MCE and seed folder of a site (one of `sites()`), sorted."""
        return self._folders_of(self.root / SITES / site / MCES)

    @staticmethod
    def _folders_of(parent: Path) -> list[str]:
        return sorted(path.name for path in parent.iterdir() if path.is_dir()) if parent.is_dir() else []

    def _site_folders(self, site: str | None) -> list[tuple[str, str]]:
        """(site, folder) for every folder of `site`, or of every site; a site compared by name."""
        return [(found, folder) for found in self.sites() if site in (None, found) for folder in self.folders(found)]

    # --- the questions -----------------------------------------------------------------

    def clusters(
        self, site: str | None = None, env: str | None = None, kind: str = KIND_ALL, zone: str | None = None
    ) -> list[ClusterRecord]:
        """Every hosted cluster the filters keep. Every other query is this one, filtered.

        Sorted by site, folder, then oldest first (the id letter, a name
        without one last, ties by name). A filter naming nothing in day1 is an
        empty list; a `site`, `env` or `zone` that is not a token of the naming
        convention, or an unknown `kind`, is refused (ValueError) before any
        folder is read. `zone` keeps the clusters under that zone's seed, so
        with it only `kv` records remain."""
        if kind not in KINDS:
            raise ValueError(f"kind {kind!r} is not one of {', '.join(KINDS)}")
        if site is not None:
            name_token(site, "site")
        if env is not None:
            name_token(env, "env")
        if zone is not None:
            zone_number(zone)
        records: list[ClusterRecord] = []
        for found, folder in self._site_folders(site):
            folder_zone = seed_zone(folder)
            folder_kind = KIND_KV if folder_zone is not None else KIND_HC
            if kind not in (KIND_ALL, folder_kind) or (zone is not None and folder_zone != zone):
                continue
            for path in list_cluster_files(self.root, found, folder):
                record = self._record(path, found, folder)
                if env is None or record.env == env:
                    records.append(record)
        return sorted(records, key=_order)

    def kv_clusters(self, site: str | None = None, zone: str | None = None) -> list[ClusterRecord]:
        """The KV clusters (hosted clusters under a seed folder) of a site and zone, oldest first.

        A record with `problems` is listed, not hidden: whether it may be
        chosen is the caller's rule."""
        return self.clusters(site=site, kind=KIND_KV, zone=zone)

    def hosted_clusters(
        self, site: str | None = None, env: str | None = None, include_kv: bool = False
    ) -> list[ClusterRecord]:
        """The hosted clusters under MCE folders; with `include_kv`, the KV clusters under seeds too."""
        return self.clusters(site=site, env=env, kind=KIND_ALL if include_kv else KIND_HC)

    def cluster_paths(self, name: str) -> list[str]:
        """Every `<name>.yaml` cluster file, relative, sorted. Names are unique in day1, so
        more than one is a repo mistake the caller reports. Compared by name: a
        `*` or a `/` in it matches nothing."""
        wanted = f"{name}.yaml"
        return sorted(
            path
            for site, folder in self._site_folders(None)
            for path in list_cluster_files(self.root, site, folder)
            if Path(path).name == wanted
        )

    def cluster(self, name: str) -> ClusterRecord | None:
        """The one cluster of that name, its values merged; None when day1 has no such file.

        ValueError when several files carry the name (`cluster_paths` lists
        them): a limb that must classify that asks `cluster_paths` first."""
        paths = self.cluster_paths(name)
        if not paths:
            return None
        if len(paths) > 1:
            raise ValueError(f"{len(paths)} files named {name}.yaml in day1 ({', '.join(paths)}): names must be unique")
        match = CLUSTER_FILE.fullmatch(paths[0])
        return self._record(paths[0], match.group("site"), match.group("mce"))  # type: ignore[union-attr]

    def values(self, path: str) -> ClusterValues:
        """The cluster file at `path` with its value layers merged (load_cluster_values)."""
        return load_cluster_values(self.root, path)

    def mce_folders(self, name: str) -> list[tuple[str, str]]:
        """(site, `sites/<site>/mces/<name>`) for every site holding a folder of exactly that name."""
        return [(site, mce_dir(site, folder)) for site, folder in self._site_folders(None) if folder == name]

    def has_file(self, path: str) -> bool:
        """Whether the commit holds anything at `path` (a relative path the caller built from validated names)."""
        return (self.root / path).exists()

    def _record(self, path: str, site: str, folder: str) -> ClusterRecord:
        values = self.values(path)
        name = Path(path).stem
        problems = list(values.problems)
        zone = seed_zone(folder)
        identifier: str | None = None
        if zone is not None:
            if seed_site(folder) != site:
                problems.append(
                    f"{path}: seed folder {folder} is named for site {seed_site(folder)!r}, but is under {SITES}/{site}"
                )
            try:
                parsed = parse_kv_cluster(name)
            except ClusterNameError:
                problems.append(
                    f"{path}: a KV cluster is named `ocp4-<lifecycle>-kv-<site>-<id>`; {name!r} is not, "
                    "so its place in the zone's order is unknown"
                )
            else:
                identifier = parsed.id
                if parsed.site != site:
                    problems.append(f"{path}: the name says site {parsed.site!r}, but the file is under {SITES}/{site}")
            problems += _zone_problems(values.merged, zone, path)
        return ClusterRecord(
            name=name,
            site=site,
            folder=folder,
            kind=KIND_KV if zone is not None else KIND_HC,
            env=cluster_env(values.merged, name),
            zone=zone,
            id=identifier,
            cluster_name=values.cluster_name,
            hosted_cluster=values.hosted_cluster,
            hosted_cluster_namespace=values.hosted_cluster_namespace,
            path=path,
            problems=problems,
            revision=self.revision,
        )

    # --- what only git answers ------------------------------------------------------------

    def _git(self) -> git_repo.Snapshot:
        if self.snapshot is None:
            raise RuntimeError("this Day1 is over a plain directory: only main_snapshot() opens one git can answer for")
        return self.snapshot

    async def blob(self, path: str) -> str:
        """The git blob id of `path` at `revision`: what a later commit's compare-and-set compares."""
        return await self._git().blob(path)

    def scratch(self, *, prefix: str) -> AbstractAsyncContextManager[git_repo.Scratch]:
        """A throwaway copy of the tree for one of day1's own scripts (git_repo.Snapshot.scratch)."""
        return self._git().scratch(prefix=prefix)
