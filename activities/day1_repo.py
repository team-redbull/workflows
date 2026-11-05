"""THE day1 values repo reader of the monorepo: clone `main`, read a cluster file the way
the hostedclusters-setup chart renders it, list an MCE folder's cluster files.

Shared by every limb that reads day1 — vm-lifecycle (a zone's KubeVirt
clusters, activities/vm_lifecycle/day1_repo.py) and upgrade-gitops (the
upgrade's rules, cluster file and edits, activities/upgrade_gitops/day1_repo.py)
— so it lives HERE, next to activities/git_repo.py, in no domain: a limb
never imports another domain's code to read day1 (the operator, 2026-11-05).
It cannot live in shared/ — that is the brain↔limb contract, temporalio +
pydantic only, imported inside the workflow sandbox (SH001), and this needs
PyYAML and git. Every limb image copies all of activities/, so each limb that
needs it carries it.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). The git client
is activities/git_repo.py: the token is injected in memory only, scrubbed
from every error, kept out of every file of the clone, and every clone is a
fresh TemporaryDirectory — ONE per activity attempt, owned by the caller.
`main` only: Argo CD renders main and nothing else.

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

LISTING an MCE folder's cluster files (`list_cluster_files`): the folder
`sites/<site>/mces/<mce>/hostedClusters/` is read with `iterdir()`, never
globbed with input — the caller validates the tokens, this never interprets
them; a missing folder is an empty list.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from activities import git_repo
from shared.upgrade_layout import DAY1_CLUSTER_FILE as CLUSTER_FILE

CONFIG_VALUES = "sites/configValues.yaml"
# argocd-platform hostedClusters/values.yaml `clusterFilePrefix` (stack-export).
DEFAULT_CLUSTER_FILE_PREFIX = "ocp4-"


# --- git -------------------------------------------------------------------------------


async def clone_main(*, repo_url: str, token: str, dest: Path) -> None:
    await git_repo.clone_main(repo_url=repo_url, token=token, dest=dest, repo="day1")


async def head(clone: Path, *, token: str) -> str:
    return (await git_repo.run_git(["rev-parse", "HEAD"], cwd=clone, secrets=[token])).strip()


async def blob(clone: Path, path: str, *, token: str) -> str:
    """The git blob id of `path` at HEAD: what compare-and-set compares."""
    return (await git_repo.run_git(["rev-parse", f"HEAD:{path}"], cwd=clone, secrets=[token])).strip()


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
    return [CONFIG_VALUES, f"sites/{site}/values.yaml", f"sites/{site}/mces/{mce}/values.yaml", path]


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


# --- listing a folder's cluster files -----------------------------------------------------


def list_cluster_files(clone: Path, site: str, mce_folder: str) -> list[str]:
    """Every `sites/<site>/mces/<mce_folder>/hostedClusters/*.yaml`, relative, sorted; [] without the folder.

    `iterdir()`, never a glob with input in it: the tokens are validated by the
    caller, and this never interprets them.
    """
    folder = clone / "sites" / site / "mces" / mce_folder / "hostedClusters"
    if not folder.is_dir():
        return []
    return sorted(
        str(path.relative_to(clone)) for path in folder.iterdir() if path.is_file() and path.suffix == ".yaml"
    )
