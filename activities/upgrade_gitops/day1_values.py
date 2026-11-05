"""A day1 cluster file as the upgrade sees it, and the upgrade's edits to it — pure.

No git, no settings, no Temporal: day1_repo.py clones and pushes, this module
reads text and returns text, so every rule here is a plain unit test.

READING (`read_cluster`) starts from activities/day1_repo.py's
`load_cluster_values` — the monorepo's one reader of a cluster file with its
value layers and its clusterName, shared with vm-lifecycle — and adds what
only the upgrade reads:
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The monorepo's day1 reader (shared with vm-lifecycle). The names not used
# below are re-exported on purpose: upgrade_rules.py reads load_yaml from here.
from activities.day1_repo import (  # noqa: F401
    CONFIG_VALUES,
    DEFAULT_CLUSTER_FILE_PREFIX,
    ClusterValues,
    layer_paths,
    load_cluster_values,
    load_yaml,
    merge_values,
)
from activities.day1_repo import unset as _unset
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
from shared.upgrade_layout import DAY1_CLUSTER_FILE as CLUSTER_FILE  # noqa: F401 — day1_repo.py reads it here
from shared.upgrade_layout import IN_PLACE as _IN_PLACE
from shared.upgrade_layout import ZONE_LABEL as _ZONE_LABEL

# The chart's repositories under imagePath when a file names none: production's
# one full mirror (stack-export hc.yaml:30,46; the port's hc.releaseRepo, hc.yaml:37).
DEFAULT_RELEASE_REPO = "openshift/release-images"
DEFAULT_CPO_REPO = "openshift/release"
_PAUSED = "true"


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

    def set_entry_child(self, index: int, key: str, child: str, value: str, *, quoted: bool = False) -> None:
        """`nodepool[index].key.child: value`, `key` a block mapping, written or added by its lines.

        A `key:` written in flow style (`inPlace: {maxUnavailable: 1}`) is
        refused: only a block mapping's child line can be set or added.
        """
        rendered = f'"{value}"' if quoted else value
        span = nodepool_spans(self.lines)[index]
        found = _entry_key_lines(self.lines, span, key)
        if len(found) > 1:
            raise Day1ChangeRejectedError(f"nodepool[{index}] writes {key} {len(found)} times")
        if not found:
            pad = " " * span.key_indent
            self.lines.insert(_entry_insert_at(self.lines, span), f"{pad}{key}:\n{pad}  {child}: {rendered}\n")
            return
        head = found[0]
        after_colon = _strip_eol(self.lines[head])[0].split(":", 1)[1].split("#", 1)[0].strip()
        if after_colon:
            raise Day1ChangeRejectedError(
                f"nodepool[{index}].{key} is not written as a block mapping ({after_colon}): edit it by hand"
            )
        child_indent: int | None = None
        children: list[int] = []
        last = head
        for line_index in range(head + 1, span.end):
            body = _strip_eol(self.lines[line_index])[0]
            stripped = body.lstrip(" ")
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(body) - len(stripped)
            if indent <= span.key_indent:
                break
            child_indent = indent if child_indent is None else child_indent
            if indent == child_indent and re.match(rf"{re.escape(child)}\s*:(?:\s|$)", stripped):
                children.append(line_index)
            last = line_index
        if len(children) > 1:
            raise Day1ChangeRejectedError(f"nodepool[{index}].{key} writes {child} {len(children)} times")
        if children:
            self.lines[children[0]] = set_scalar(self.lines[children[0]], child, value, quoted=quoted)
            return
        pad = " " * (child_indent if child_indent is not None else span.key_indent + 2)
        self.lines.insert(last + 1, f"{pad}{child}: {rendered}\n")


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
        entry = next(entry for entry in entries if entry.name == name)
        limit_before = max_unavailable_of(entry)
        new_limit = request.max_unavailable is not None and limit_before != request.max_unavailable
        if new_limit:
            summary += f", maxUnavailable {limit_before or 'unset'} → {request.max_unavailable}"
        if versions_before[name] == version and not new_limit:
            return Edit(text=current, changed=False, summary=summary)
        _require_editable_entries(text, before, entries)
        if versions_before[name] != version:
            text.set_entry(entry.index, "workertag", release_tag(version))
        if new_limit:
            value = request.max_unavailable or ""
            text.set_entry_child(entry.index, "inPlace", "maxUnavailable", value, quoted=value.endswith("%"))
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


def max_unavailable_of(entry: NodePoolEntry) -> str | None:
    """A NodePool entry's own `inPlace.maxUnavailable`, as text ("1", "33%"); None when unset."""
    in_place = entry.entry.get("inPlace")
    value = in_place.get("maxUnavailable") if isinstance(in_place, dict) else None
    return None if value is None else str(value)


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
    # D28: a NodePool commit may also set that one NodePool's inPlace.maxUnavailable.
    limited = (
        next((entry.index for entry in nodepool_entries(before, []) if entry.name == request.nodepool), None)
        if request.change is Day1ChangeKind.NODEPOOL and request.max_unavailable is not None else None
    )

    def allowed_here(path: tuple) -> bool:
        if limited is not None and path[:3] == ("nodepool", limited, "inPlace") and len(path) <= 4:
            return len(path) == 3 or path[3] == "maxUnavailable"
        return allowed(path)

    moved = sorted(path for path in _changed_paths(before.own, after.own) if not allowed_here(path))
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
        if request.max_unavailable is not None:
            entry = next((e for e in nodepool_entries(after, []) if e.name == request.nodepool), None)
            limit = max_unavailable_of(entry) if entry else None
            if limit != request.max_unavailable:
                wrong.append(f"NodePool {request.nodepool}'s maxUnavailable would be {limit}, not {request.max_unavailable}")
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
