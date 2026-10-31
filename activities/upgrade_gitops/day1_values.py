"""A day1 cluster file as the hostedclusters-setup chart sees it, and the upgrade's edits to it — pure.

No git, no settings, no Temporal: day1_repo.py clones and pushes, this module
reads text and returns text, so every rule here is a plain unit test.

READING (`read_cluster`) follows the production derivation in stack-export:
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
  * a NodePool is `.name`, else `<clusterName>-<infraEnv>` with infraEnv =
    `.infraEnvName`, else `.agentLabelSelector.nodeLabelValue`
    (`templates/nodepool.yaml:14-22`); its release is its own `workertag`, else
    the top-level `workertag`, else `mastertag` (`nodepool.yaml:8,101`).
  Whatever the chart would refuse to render, and every duplicate (a key
  written twice, a NodePool named twice), is a line in `problems`, never an
  exception: planning check 3 shows them all at once.

EDITING (`plan_edit`) changes only the lines of the keys a change owns, as
text, so the rest of the file stays byte-identical (DESIGN §6.1), then
re-parses the result and refuses it unless exactly those keys changed and the
file now renders what was asked. A file it cannot edit that way (flow style, a
key written twice, a NodePool list inherited from another layer) is refused
with Day1ChangeRejectedError: a person edits it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from shared.exceptions import Day1ChangeRejectedError
from shared.models.cluster_upgrade import (
    ClusterProfile,
    ClusterUpgradeSettings,
    Day1ChangeKind,
    Day1ChangeRequest,
    Day1Cluster,
    Day1NodePool,
)
from shared.ocp_version import release_tag, release_version

# Where day1 keeps a hosted cluster's file; the folders name its site and MCE.
CLUSTER_FILE = re.compile(r"sites/(?P<site>[^/]+)/mces/(?P<mce>[^/]+)/hostedClusters/(?P<name>[^/]+)\.yaml")
CONFIG_VALUES = "sites/configValues.yaml"
# argocd-platform hostedClusters/values.yaml `clusterFilePrefix` (stack-export).
DEFAULT_CLUSTER_FILE_PREFIX = "ocp4-"
# The chart's repositories under imagePath when a file names none: production's
# one full mirror (stack-export hc.yaml:30,46; the port's hc.releaseRepo, hc.yaml:37).
DEFAULT_RELEASE_REPO = "openshift/release-images"
DEFAULT_CPO_REPO = "openshift/release"
_ZONE_LABEL = "topology.kubernetes.io/zone"
_IN_PLACE = "InPlace"
_PAUSED = "true"


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


def _unset(value: Any) -> bool:
    """Sprig's `default`: nil, "", 0, false and empty collections count as unset."""
    return value is None or value is False or value == "" or value == 0 or value == [] or value == {}


def _bare(value: Any, what: str, problems: list[str]) -> str | None:
    """A release as the chart takes it (`X.Y.Z` or `X.Y.Z-x86_64`), bare; None when unset or unusable."""
    if _unset(value):
        return None
    try:
        return release_version(str(value))
    except ValueError as err:
        problems.append(f"{what}: {err}")
        return None


# --- reading --------------------------------------------------------------------


@dataclass
class ClusterValues:
    """Everything read about one cluster file: each layer, the merge, and what is wrong."""

    path: str
    own: dict  # the cluster file alone
    merged: dict
    cluster_name: str
    problems: list[str] = field(default_factory=list)


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
    cluster_name = str(own["clusterName"]) if not _unset(own.get("clusterName")) else name.removeprefix(prefix)
    return ClusterValues(path=path, own=own, merged=merge_values(layers), cluster_name=cluster_name, problems=problems)


@dataclass
class NodePoolEntry:
    """One `nodepool[]` entry: its index in the list, the chart's name for it, the entry as written."""

    index: int
    name: str
    entry: dict


def nodepool_entries(values: ClusterValues, problems: list[str]) -> list[NodePoolEntry]:
    """The merged `nodepool` list under the chart's names; what the chart would refuse goes to `problems`."""
    raw = values.merged.get("nodepool")
    if raw is None:
        return []
    where = f"{values.path}: nodepool"
    if not isinstance(raw, list):
        problems.append(f"{where} must be a list")
        return []
    platform = str(values.merged.get("platform") or "")
    entries: list[NodePoolEntry] = []
    first_index: dict[str, int] = {}
    for index, entry in enumerate(raw):
        at = f"{where}[{index}]"
        if not isinstance(entry, dict):
            problems.append(f"{at} must be a mapping")
            continue
        selector = entry.get("agentLabelSelector") or {}
        selector = selector if isinstance(selector, dict) else {}
        infra_env_name, label_value = entry.get("infraEnvName"), selector.get("nodeLabelValue")
        infra_env = infra_env_name if not _unset(infra_env_name) else label_value
        if not _unset(infra_env_name) and not _unset(label_value) and infra_env_name != label_value:
            problems.append(
                f"{at}: infraEnvName {infra_env_name!r} and agentLabelSelector.nodeLabelValue "
                f"{label_value!r} disagree; the chart refuses to render it"
            )
        if _unset(entry.get("name")) and _unset(infra_env):
            problems.append(f"{at}: neither name nor infraEnvName; the chart refuses to render it")
            continue
        name = str(entry["name"]) if not _unset(entry.get("name")) else f"{values.cluster_name}-{infra_env}"
        if platform != "kubevirt" and _unset(infra_env):
            problems.append(f"{at} ({name}): no infraEnvName on the agent platform; the chart refuses to render it")
        if name in first_index:
            problems.append(f"{at}: NodePool {name} is named twice (nodepool[{first_index[name]}] and [{index}])")
        else:
            first_index[name] = index
        entries.append(NodePoolEntry(index=index, name=name, entry=entry))
    return entries


def _nodepool(entry: NodePoolEntry, where: str, problems: list[str]) -> Day1NodePool:
    raw = entry.entry
    at = f"{where}: nodepool[{entry.index}] ({entry.name})"
    replicas: int | None = 1  # the chart's default
    if "replicas" in raw:
        try:
            replicas = int(raw["replicas"])
        except (TypeError, ValueError):
            problems.append(f"{at}: replicas {raw['replicas']!r} is not a number")
            replicas = None
    labels = raw.get("nodeLabels") if isinstance(raw.get("nodeLabels"), dict) else {}
    zone = labels.get(_ZONE_LABEL)
    in_place = raw.get("inPlace") if isinstance(raw.get("inPlace"), dict) else {}
    max_unavailable = in_place.get("maxUnavailable")
    return Day1NodePool(
        name=entry.name,
        workertag=_bare(raw.get("workertag"), f"{at}: workertag", problems),
        upgrade_type=str(raw.get("upgradeType") or _IN_PLACE),
        zone=None if _unset(zone) else str(zone),
        replicas=replicas,
        max_unavailable=None if max_unavailable is None else str(max_unavailable),
    )


_UPGRADE_KEYS = {"profile", "nodePoolOrder", "soakMinutes", "wave"}


def upgrade_settings(own: dict, where: str, problems: list[str]) -> ClusterUpgradeSettings:
    """The cluster file's own `upgrade:` block (DESIGN §6.3); a value it cannot use is a problem."""
    block = own.get("upgrade")
    if block is None:
        return ClusterUpgradeSettings()
    at = f"{where}: upgrade"
    if not isinstance(block, dict):
        problems.append(f"{at} must be a mapping")
        return ClusterUpgradeSettings()
    for key in sorted(set(block) - _UPGRADE_KEYS):
        problems.append(f"{at}: unknown key {key!r} (known: {', '.join(sorted(_UPGRADE_KEYS))})")
    settings = ClusterUpgradeSettings()
    profile = block.get("profile")
    if profile is not None:
        try:
            settings.profile = ClusterProfile(profile)
        except ValueError:
            problems.append(f"{at}.profile must be one of {[p.value for p in ClusterProfile]}, got {profile!r}")
    order = block.get("nodePoolOrder")
    if order is not None:
        if isinstance(order, list) and all(isinstance(name, str) for name in order):
            settings.nodepool_order = list(order)
        else:
            problems.append(f"{at}.nodePoolOrder must be a list of NodePool names")
    for key, attribute in (("soakMinutes", "soak_minutes"), ("wave", "wave")):
        value = block.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            problems.append(f"{at}.{key} must be a whole number of at least 0, got {value!r}")
        else:
            setattr(settings, attribute, value)
    return settings


def read_cluster(root: Path, path: str, *, revision: str, blob: str) -> Day1Cluster:
    """The cluster file at `path` in the clone at `root`, as the upgrade sees it."""
    values = load_cluster_values(root, path)
    problems = list(values.problems)
    merged = values.merged
    mastertag = _bare(merged.get("mastertag"), f"{path}: mastertag", problems)
    if _unset(merged.get("mastertag")):
        problems.append(f"{path}: no mastertag in the file or any layer above it")
    entries = nodepool_entries(values, problems)
    nodepools = [_nodepool(entry, path, problems) for entry in entries]
    image_path, image_hc = merged.get("imagePath"), merged.get("imageHc")
    # hc.releaseRepo: `releaseRepo | default`; cpoRepo: hc.orDefault, so an
    # explicit "" means no CPO annotation (helm-charts-hostedclusters-setup
    # templates/_hostedcluster-helpers.tpl, hc.yaml:37).
    release_repo = merged.get("releaseRepo")
    cpo_repo = merged.get("cpoRepo", DEFAULT_CPO_REPO)
    return Day1Cluster(
        path=path,
        revision=revision,
        blob=blob,
        mastertag=mastertag,
        top_level_workertag=_bare(merged.get("workertag"), f"{path}: workertag", problems),
        image_path=None if _unset(image_path) else str(image_path),
        image_hc=None if _unset(image_hc) else str(image_hc),
        hosted_cluster=f"openshift-{values.cluster_name}",
        hosted_cluster_namespace=f"hcp-{values.cluster_name}",
        release_repo=DEFAULT_RELEASE_REPO if _unset(release_repo) else str(release_repo),
        cpo_repo="" if cpo_repo is None or cpo_repo == "" else str(cpo_repo),
        nodepools=nodepools,
        upgrade=upgrade_settings(values.own, path, problems),
        problems=problems,
    )


def effective_versions(values: ClusterValues) -> dict[str, str | None]:
    """Each NodePool's release as the chart renders it: own workertag, top-level workertag, mastertag."""
    ignored: list[str] = []
    fallback = _bare(values.merged.get("workertag"), "", ignored) or _bare(values.merged.get("mastertag"), "", ignored)
    return {
        entry.name: _bare(entry.entry.get("workertag"), "", ignored) or fallback
        for entry in nodepool_entries(values, ignored)
    }


# --- editing: where the lines are ------------------------------------------------

_TOP_LEVEL_KEY = re.compile(r"^(?P<key>[A-Za-z_][\w.-]*)\s*:(?:\s|$)")
_SCALAR_LINE = re.compile(
    r"^(?P<lead>[ \t]*(?:-[ \t]+)?)(?P<key>[A-Za-z_][\w.-]*)(?P<colon>[ \t]*:[ \t]*)"
    r"(?P<value>\"[^\"]*\"|'[^']*'|[^\s#][^#]*?)?(?P<rest>[ \t]*(?:#.*)?)$"
)
_DASH = re.compile(r"^(?P<indent>[ ]*)-(?:[ ]+(?P<after>\S.*)?)?$")


def _strip_eol(line: str) -> tuple[str, str]:
    body = line.rstrip("\r\n")
    return body, line[len(body):]


def top_level_lines(lines: list[str], key: str) -> list[int]:
    """Indices of the column-0 `key:` lines (more than one is a key written twice)."""
    return [
        index for index, line in enumerate(lines)
        if (match := _TOP_LEVEL_KEY.match(line)) is not None and match.group("key") == key
    ]


# A value starting like this is a block scalar, an anchor, an alias, a tag or a
# flow collection: not a plain value this module may rewrite.
_NOT_PLAIN = ("|", ">", "&", "*", "!", "{", "[")


def _plain_scalar(line: str, key: str) -> re.Match | None:
    match = _SCALAR_LINE.match(_strip_eol(line)[0])
    if match is None or match.group("key") != key or match.group("value") is None:
        return None
    return None if match.group("value").startswith(_NOT_PLAIN) else match


def _is_scalar_line(line: str, key: str) -> bool:
    return _plain_scalar(line, key) is not None


def set_scalar(line: str, key: str, value: str, *, quoted: bool = False) -> str:
    """The same line with `key`'s value replaced, its quoting, comment and line end kept."""
    body, eol = _strip_eol(line)
    match = _plain_scalar(line, key)
    if match is None:
        raise Day1ChangeRejectedError(f"cannot edit {key!r} on the line {body.strip()!r}: not a plain `{key}: value`")
    old = match.group("value")
    if old.startswith('"') or quoted:
        new = f'"{value}"'
    elif old.startswith("'"):
        new = f"'{value}'"
    else:
        new = value
    return f"{match.group('lead')}{key}{match.group('colon')}{new}{match.group('rest')}{eol or chr(10)}"


@dataclass
class EntrySpan:
    """Where one `nodepool[]` entry sits in the file: [start, end) lines, its keys' indent."""

    start: int
    end: int
    key_indent: int


def nodepool_spans(lines: list[str]) -> list[EntrySpan]:
    """The text span of every entry of the cluster file's own top-level `nodepool:` list.

    Block style only, as every production file writes it; anything else is
    refused rather than guessed at.
    """
    found = top_level_lines(lines, "nodepool")
    if len(found) != 1:
        raise Day1ChangeRejectedError(
            "the cluster file has no nodepool list of its own" if not found
            else f"nodepool is written {len(found)} times in the cluster file"
        )
    head = found[0]
    if _strip_eol(lines[head])[0].split(":", 1)[1].split("#", 1)[0].strip():
        raise Day1ChangeRejectedError("nodepool is written in flow style ([...]): only a block list can be edited")
    end = len(lines)
    for index in range(head + 1, len(lines)):
        body = _strip_eol(lines[index])[0]
        if body.startswith(("---", "...")) or (body[:1] not in ("", " ", "\t", "#", "-")):
            end = index
            break
    dash_lines = [
        (index, match) for index in range(head + 1, end)
        if (match := _DASH.match(_strip_eol(lines[index])[0])) is not None
    ]
    if not dash_lines:
        return []
    indent = min(len(match.group("indent")) for _, match in dash_lines)
    starts = [(index, match) for index, match in dash_lines if len(match.group("indent")) == indent]
    spans = []
    for position, (start, match) in enumerate(starts):
        stop = starts[position + 1][0] if position + 1 < len(starts) else end
        after = match.group("after")
        if after is not None and after.startswith(("{", "[")):
            raise Day1ChangeRejectedError(f"nodepool entry at line {start + 1} is in flow style: only block style can be edited")
        if after is not None:
            key_indent = indent + (len(_strip_eol(lines[start])[0]) - indent - len(after))
        else:
            following = next(
                (len(body) - len(body.lstrip(" ")) for body in (_strip_eol(line)[0] for line in lines[start + 1:stop])
                 if body.strip() and not body.lstrip().startswith("#")),
                indent + 2,
            )
            key_indent = following
        spans.append(EntrySpan(start=start, end=stop, key_indent=key_indent))
    return spans


def _entry_key_lines(lines: list[str], span: EntrySpan, key: str) -> list[int]:
    found = []
    for index in range(span.start, span.end):
        body = _strip_eol(lines[index])[0]
        if index == span.start:
            match = _DASH.match(body)
            after = (match.group("after") or "") if match else ""
            if re.match(rf"{re.escape(key)}\s*:(?:\s|$)", after):
                found.append(index)
            continue
        if re.match(rf"^ {{{span.key_indent}}}{re.escape(key)}\s*:(?:\s|$)", body):
            found.append(index)
    return found


def _entry_insert_at(lines: list[str], span: EntrySpan) -> int:
    """Right after the entry's last line of its own (trailing blanks and outer comments stay below)."""
    last = span.start
    for index in range(span.start, span.end):
        body = _strip_eol(lines[index])[0]
        stripped = body.lstrip(" ")
        if not stripped:
            continue
        if stripped.startswith("#") and len(body) - len(stripped) < span.key_indent:
            continue
        last = index
    return last + 1


# --- editing: what each change does ---------------------------------------------


@dataclass
class Edit:
    """The planned result of one change: the new text, whether it differs, and why it is right."""

    text: str
    changed: bool
    summary: str


class _Text:
    """The cluster file's lines, edited in place."""

    def __init__(self, text: str) -> None:
        self.lines = text.splitlines(keepends=True)
        if self.lines and not self.lines[-1].endswith(("\n", "\r")):
            self.lines[-1] += "\n"

    def render(self) -> str:
        return "".join(self.lines)

    def set_top(self, key: str, value: str, *, quoted: bool = False, after: str | None = None) -> None:
        found = top_level_lines(self.lines, key)
        if len(found) > 1:
            raise Day1ChangeRejectedError(f"{key} is written {len(found)} times in the cluster file")
        if found:
            self.lines[found[0]] = set_scalar(self.lines[found[0]], key, value, quoted=quoted)
            return
        rendered = f'{key}: "{value}"\n' if quoted else f"{key}: {value}\n"
        anchor = top_level_lines(self.lines, after) if after else []
        if len(anchor) == 1 and _is_scalar_line(self.lines[anchor[0]], after or ""):
            self.lines.insert(anchor[0] + 1, rendered)
        else:
            self.lines.append(rendered)

    def drop_top(self, key: str) -> bool:
        found = top_level_lines(self.lines, key)
        if len(found) > 1:
            raise Day1ChangeRejectedError(f"{key} is written {len(found)} times in the cluster file")
        if not found:
            return False
        if not _is_scalar_line(self.lines[found[0]], key):
            raise Day1ChangeRejectedError(f"cannot drop {key!r}: it is not a plain `{key}: value` line")
        del self.lines[found[0]]
        return True

    def set_entry(self, index: int, key: str, value: str) -> None:
        spans = nodepool_spans(self.lines)
        span = spans[index]
        found = _entry_key_lines(self.lines, span, key)
        if len(found) > 1:
            raise Day1ChangeRejectedError(f"nodepool[{index}] writes {key} {len(found)} times")
        if found:
            self.lines[found[0]] = set_scalar(self.lines[found[0]], key, value)
            return
        self.lines.insert(_entry_insert_at(self.lines, span), f"{' ' * span.key_indent}{key}: {value}\n")


def _require_editable_entries(text: _Text, values: ClusterValues, entries: list[NodePoolEntry]) -> None:
    """The cluster file's own nodepool list, block style, one text span per parsed entry."""
    own = values.own.get("nodepool")
    if not isinstance(own, list):
        raise Day1ChangeRejectedError(
            f"{values.path} has no nodepool list of its own (it inherits one): a NodePool's workertag "
            "cannot be written into it"
        )
    spans = nodepool_spans(text.lines)
    if len(spans) != len(own):
        raise Day1ChangeRejectedError(
            f"{values.path}: found {len(spans)} nodepool entries in the text but {len(own)} in the YAML; "
            "edit it by hand"
        )
    if len(entries) != len(own):
        raise Day1ChangeRejectedError(f"{values.path}: a nodepool entry the chart cannot name; fix the file first")


def plan_edit(root: Path, request: Day1ChangeRequest, current: str) -> Edit:
    """The cluster file after `request`, verified; `changed` False when it already says so.

    Raises Day1ChangeRejectedError when the file cannot be edited by its key
    lines alone, or the edit would not come out as asked.
    """
    path = request.path
    before = load_cluster_values(root, path, text=current)
    entries = nodepool_entries(before, [])
    versions_before = effective_versions(before)
    text = _Text(current)

    if request.change is Day1ChangeKind.CONTROL_PLANE:
        version = _require_version(request)
        wanted = _wanted_versions(request, versions_before)
        unknown = sorted(set(request.nodepool_versions) - set(versions_before))
        if unknown:
            raise Day1ChangeRejectedError(f"{path} lists no NodePool {', '.join(unknown)}")
        summary = f"control plane {_bare(before.merged.get('mastertag'), '', []) or '?'} → {version}"
        if _done_control_plane(before, version, wanted):
            return Edit(text=current, changed=False, summary=summary)
        text.set_top("mastertag", release_tag(version))
        _pin_nodepools(text, root, path, entries, wanted)
    elif request.change is Day1ChangeKind.NODEPOOL:
        version = _require_version(request)
        name = request.nodepool or ""
        summary = f"NodePool {name} → {version}"
        if name not in versions_before:
            raise Day1ChangeRejectedError(f"{path} lists no NodePool {name}")
        if versions_before[name] == version:
            return Edit(text=current, changed=False, summary=summary)
        _require_editable_entries(text, before, entries)
        text.set_entry(next(entry.index for entry in entries if entry.name == name), "workertag", release_tag(version))
    elif request.change is Day1ChangeKind.PAUSE:
        summary = 'pausedUntil "true" (abort)'
        if before.own.get("pausedUntil") == _PAUSED:
            return Edit(text=current, changed=False, summary=summary)
        text.set_top("pausedUntil", _PAUSED, quoted=True, after="mastertag")
    elif request.change is Day1ChangeKind.FINISH:
        summary = "finish: every NodePool pinned on its own, the top-level workertag dropped"
        if "workertag" not in before.own:
            return Edit(text=current, changed=False, summary=summary)
        _finish(text, root, path, entries, versions_before)
    else:  # pragma: no cover - a kind added to the model without an edit here
        raise Day1ChangeRejectedError(f"no edit for change {request.change!r}")

    edited = text.render()
    _verify(root, request, before, current, edited, versions_before)
    return Edit(text=edited, changed=edited != current, summary=summary)


def _require_version(request: Day1ChangeRequest) -> str:
    if not request.version:
        raise Day1ChangeRejectedError(f"a {request.change.value} change needs a version")
    return release_version(request.version)


def _wanted_versions(request: Day1ChangeRequest, versions_before: dict[str, str | None]) -> dict[str, str | None]:
    """Where a control-plane step leaves each NodePool: as the request pins it, else where it is now."""
    return {
        name: release_version(request.nodepool_versions[name]) if name in request.nodepool_versions else now
        for name, now in versions_before.items()
    }


def _done_control_plane(values: ClusterValues, version: str, wanted: dict[str, str | None]) -> bool:
    return (
        _bare(values.merged.get("mastertag"), "", []) == version
        and effective_versions(values) == wanted
    )


def _pin_nodepools(
    text: _Text,
    root: Path,
    path: str,
    entries: list[NodePoolEntry],
    wanted: dict[str, str | None],
) -> None:
    """Keep every NodePool where `wanted` says, with mastertag already moved ("masters first", CONTEXT §6.6).

    A NodePool with its own workertag gets that line set. Those that inherit
    get one top-level workertag when they all stay on one release (the slim
    file's way, DESIGN §6.1), else a workertag line each.
    """
    moved = load_cluster_values(root, path, text=text.render())
    now = effective_versions(moved)
    by_name = {entry.name: entry for entry in entries}
    pinned = [name for name in wanted if not _unset(by_name[name].entry.get("workertag"))]
    inheritors = [name for name in wanted if name not in pinned]
    for name in pinned:
        if now[name] != wanted[name]:
            _set_own_workertag(text, moved, entries, by_name[name], wanted[name])
    if all(now[name] == wanted[name] for name in inheritors):
        return
    targets = {wanted[name] for name in inheritors}
    if len(targets) == 1 and None not in targets:
        text.set_top("workertag", release_tag(targets.pop() or ""), after="mastertag")
        return
    for name in inheritors:
        if now[name] != wanted[name]:
            _set_own_workertag(text, moved, entries, by_name[name], wanted[name])


def _set_own_workertag(
    text: _Text, values: ClusterValues, entries: list[NodePoolEntry], entry: NodePoolEntry, version: str | None
) -> None:
    if version is None:
        raise Day1ChangeRejectedError(f"NodePool {entry.name} has no release to stay on")
    _require_editable_entries(text, values, entries)
    text.set_entry(entry.index, "workertag", release_tag(version))


def _finish(
    text: _Text, root: Path, path: str, entries: list[NodePoolEntry], versions: dict[str, str | None]
) -> None:
    """Drop the top-level workertag, first pinning on its own every NodePool it still holds back.

    Rendering-neutral by construction (the sync after it expects nothing out
    of sync): with workers_to_target false a NodePool may still sit below
    mastertag, and dropping the line would move it.
    """
    trial = _Text(text.render())
    trial.drop_top("workertag")
    without = effective_versions(load_cluster_values(root, path, text=trial.render()))
    by_name = {entry.name: entry for entry in entries}
    values = load_cluster_values(root, path, text=text.render())
    for name, version in versions.items():
        if without.get(name) != version:
            _set_own_workertag(text, values, entries, by_name[name], version)
    text.drop_top("workertag")


def _verify(
    root: Path,
    request: Day1ChangeRequest,
    before: ClusterValues,
    current: str,
    edited: str,
    versions_before: dict[str, str | None],
) -> None:
    """Re-parse the edit: only the change's keys moved, nothing got duplicated, and it says what was asked."""
    after = load_cluster_values(root, request.path, text=edited)
    duplicated_before = {_duplicated_key(problem) for problem in before.problems}
    new_duplicates = sorted({_duplicated_key(problem) for problem in after.problems} - duplicated_before)
    if new_duplicates or (after.own == {} and edited.strip()):
        raise Day1ChangeRejectedError(
            f"the edited {request.path} does not parse cleanly: {'; '.join(after.problems) or 'empty'}"
        )
    allowed = _allowed_paths(request.change)
    moved = sorted(path for path in _changed_paths(before.own, after.own) if not allowed(path))
    if moved:
        raise Day1ChangeRejectedError(
            f"the edit of {request.path} would also change {', '.join(_dotted(path) for path in moved)}; "
            "nothing was pushed"
        )
    versions_after = effective_versions(after)
    wrong: list[str] = []
    if request.change is Day1ChangeKind.CONTROL_PLANE:
        version = release_version(request.version or "")
        if _bare(after.merged.get("mastertag"), "", []) != version:
            wrong.append(f"mastertag is not {version}")
        for name, want in _wanted_versions(request, versions_before).items():
            if versions_after.get(name) != want:
                wrong.append(f"NodePool {name} would run {versions_after.get(name)}, not {want}")
    elif request.change is Day1ChangeKind.NODEPOOL:
        version = release_version(request.version or "")
        for name, was in versions_before.items():
            want = version if name == request.nodepool else was
            if versions_after.get(name) != want:
                wrong.append(f"NodePool {name} would run {versions_after.get(name)}, not {want}")
    elif request.change is Day1ChangeKind.PAUSE:
        if after.own.get("pausedUntil") != _PAUSED:
            wrong.append('pausedUntil is not "true"')
    elif request.change is Day1ChangeKind.FINISH:
        if "workertag" in after.own:
            wrong.append("the top-level workertag is still there")
        if versions_after != versions_before:
            wrong.append("a NodePool's release would change")
    if wrong:
        raise Day1ChangeRejectedError(f"the edit of {request.path} did not come out as asked: {'; '.join(wrong)}")


def _duplicated_key(problem: str) -> str:
    match = re.search(r"key (.+?) is written twice", problem)
    return match.group(1) if match else problem


def _allowed_paths(change: Day1ChangeKind):
    def nodepool_workertag(path: tuple) -> bool:
        return len(path) == 3 and path[0] == "nodepool" and isinstance(path[1], int) and path[2] == "workertag"

    rules = {
        Day1ChangeKind.CONTROL_PLANE: lambda p: p in (("mastertag",), ("workertag",)) or nodepool_workertag(p),
        Day1ChangeKind.NODEPOOL: nodepool_workertag,
        Day1ChangeKind.PAUSE: lambda p: p == ("pausedUntil",),
        Day1ChangeKind.FINISH: lambda p: p == ("workertag",) or nodepool_workertag(p),
    }
    return rules[change]


def _changed_paths(before: Any, after: Any, path: tuple = ()) -> list[tuple]:
    """Every key path whose value differs between two parsed documents."""
    if isinstance(before, dict) and isinstance(after, dict):
        found: list[tuple] = []
        for key in sorted(set(before) | set(after), key=str):
            if key not in before or key not in after:
                found.append((*path, key))
            else:
                found += _changed_paths(before[key], after[key], (*path, key))
        return found
    if isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        found = []
        for index, (old, new) in enumerate(zip(before, after)):
            found += _changed_paths(old, new, (*path, index))
        return found
    return [] if before == after and type(before) is type(after) else [path]


def _dotted(path: tuple) -> str:
    return ".".join(f"[{part}]" if isinstance(part, int) else str(part) for part in path) or "the whole file"
