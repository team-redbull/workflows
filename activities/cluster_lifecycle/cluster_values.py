"""The hosted-cluster file as provision-cluster reads and writes it — pure functions.

Three jobs, no I/O, no settings, no Temporal (CLAUDE.md §1), so every rule is
tested with a string:

  * validate_cluster_values — what the chart would refuse, judged on the text
    a person wrote merged with the day1 value layers the way Argo CD merges
    them (activities/day1_repo.load_cluster_values). Every reason is
    collected; the ONE exception raised lists them all.
  * assemble_cluster_file — the text that goes to day1: the person's text,
    byte for byte, then ONE blank line, then the allocation block (`vlanId`,
    `dhcp_values`). No YAML round-trip ever: comments and layout survive.
  * same_cluster_file — the re-run check: the file on the branch means the
    same as the text this run would write, with the `mcConfigMap` key the
    day1 CI chain appends (in its own commit, after our push) set aside.
    Semantic, not textual: the chain's injector also re-spaces blank lines.

The allocation block's layout lives in ONE place (render_allocation_block,
moved from the retired activities/segment_lifecycle/values_repo.py). It carries
NO marker comment (the operator, 2026-11-11): the marker existed because
several writers committed to one file in turn (a person, allocate-segment,
the day1 chain), and since provision-cluster writes the whole file in one push
there is nothing to separate. day1's own scripts key on the `vlanId:` /
`dhcp_values:` lines, never on a comment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from activities.day1_repo import load_cluster_values, load_yaml
from shared.cluster_names import env_of_hosted_cluster, env_of_mce_folder
from shared.exceptions import ClusterValuesRejectedError
from shared.models.segment_lifecycle import DhcpValues

# What the chart cannot render without (templates/owner-configmap.yaml is not
# nil-safe; nodepool.yaml ranges over `nodepool`; hc.yaml needs `mastertag`).
REQUIRED_OWNER_FIELDS = ("merkaz", "anaf", "mador")
# Keys this workflow writes or the day1 chain appends: a file that already
# carries one would end up with a duplicate top-level key, whose first
# occurrence YAML silently discards.
FORBIDDEN_KEYS = ("vlanId", "dhcp_values", "mcConfigMap", "AutomateAllocation")
# The environment folders day2 files a cluster under (add-cluster-to-day2.sh).
DAY2_ENVIRONMENTS = ("prod", "prep", "test")
# The key the day1 CI chain's injector appends, set aside by same_cluster_file.
CHAIN_APPENDED_KEY = "mcConfigMap"

# X.Y.Z, optionally -<arch> (the chart completes a bare X.Y.Z with -x86_64).
_MASTERTAG = re.compile(r"^\d+\.\d+\.\d+(?:-[A-Za-z0-9_]+)?$")
# A config name becomes a ConfigMap name in hcp-<clusterName>: a DNS-1123 label.
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
# A sig repo name, as add-cluster-to-day2.sh accepts it.
_SIG_NAME = re.compile(r"^[a-z0-9][a-z0-9-]*$")


@dataclass
class ValidatedCluster:
    """What validate_cluster_values derived from a file the chart would render."""

    cluster_name: str
    mastertag: str
    config_names: list[str] = field(default_factory=list)
    day2_env: str | None = None
    day2_sigs: list[str] = field(default_factory=list)


def config_names_of(own: dict) -> list[str]:
    """The MachineConfig names the file references: `mcFiles` when it lists them,
    else every nodepool's `configs[].name`, first-seen, each once — the rule of
    day1's generate_from_template.resolve_mc_files. Empty is legal."""
    mc_files = own.get("mcFiles")
    if isinstance(mc_files, list):
        return [str(name) for name in mc_files if name]
    names: list[str] = []
    for nodepool in own.get("nodepool") or []:
        if not isinstance(nodepool, dict):
            continue
        for config in nodepool.get("configs") or []:
            name = config.get("name") if isinstance(config, dict) else None
            if name and str(name) not in names:
                names.append(str(name))
    return names


def _nodepool_reasons(own: dict) -> list[str]:
    reasons: list[str] = []
    nodepools = own.get("nodepool")
    if not isinstance(nodepools, list) or not nodepools:
        return ["`nodepool` must be a non-empty list (the chart renders one NodePool per entry)"]
    for index, nodepool in enumerate(nodepools):
        where = f"nodepool[{index}]"
        if not isinstance(nodepool, dict):
            reasons.append(f"{where} must be a mapping")
            continue
        configs = nodepool.get("configs")
        if configs is None:
            continue  # a KubeVirt pool lists no MachineConfigs; the chart renders without
        if not isinstance(configs, list):
            reasons.append(f"{where}.configs must be a list of `- name: <config>` entries")
            continue
        for position, config in enumerate(configs):
            name = config.get("name") if isinstance(config, dict) else None
            if not isinstance(name, str) or not name:
                reasons.append(f"{where}.configs[{position}] must be `- name: <config>`")
            elif not _DNS_LABEL.fullmatch(name):
                reasons.append(
                    f"{where}.configs[{position}].name {name!r} must be a DNS-1123 label: "
                    "it becomes a ConfigMap name in hcp-<clusterName>"
                )
    return reasons


def _day2(merged: dict, stem: str, mce: str) -> tuple[str | None, list[str], list[str]]:
    """(env, sigs, reasons): the sig repos the merged layers register the cluster in,
    and the environment folder they get — derived only when there are sigs."""
    reasons: list[str] = []
    day2 = merged.get("day2")
    if day2 is None:
        return None, [], reasons
    if not isinstance(day2, dict):
        return None, [], ["`day2` must be a mapping (copy_to_sigs, env)"]
    raw_sigs = day2.get("copy_to_sigs") or []
    if not isinstance(raw_sigs, list):
        return None, [], ["`day2.copy_to_sigs` must be a list of sig repo names"]
    sigs: list[str] = []
    for sig in raw_sigs:
        if not isinstance(sig, str) or not _SIG_NAME.fullmatch(sig):
            reasons.append(f"`day2.copy_to_sigs` entry {sig!r} is not a sig repo name ({_SIG_NAME.pattern})")
        elif sig not in sigs:
            sigs.append(sig)
    if not sigs:
        return None, [], reasons
    explicit = day2.get("env")
    if explicit is not None and not isinstance(explicit, str):
        reasons.append("`day2.env` must be a string")
        explicit = None
    env = explicit or env_of_hosted_cluster(stem)
    mce_env = env_of_mce_folder(mce)
    if explicit is None and mce_env is not None and env != mce_env:
        reasons.append(
            f"the cluster's environment ({env!r}, from its name) and its MCE folder's "
            f"({mce_env!r}, from {mce!r}) disagree; set `day2.env` to say which folder day2 "
            "files it under"
        )
    if env not in DAY2_ENVIRONMENTS:
        reasons.append(
            f"day2 environment {env!r} is not one of {list(DAY2_ENVIRONMENTS)}; set `day2.env`"
            if env else "no day2 environment could be derived from the cluster name; set `day2.env`"
        )
    return env, sigs, reasons


def validate_cluster_values(root: Path, path: str, text: str, *, mce: str) -> ValidatedCluster:
    """Judge the text the way the chart would render it at `path`, with the day1
    value layers under `root`; raise ClusterValuesRejectedError listing EVERY
    reason, or return what was derived."""
    values = load_cluster_values(root, path, text=text)
    reasons = list(values.problems)
    own = values.own
    if not isinstance(own, dict) or not own:
        raise ClusterValuesRejectedError(
            f"{path}: the hosted-cluster file must be a YAML mapping of values"
            + (": " + "; ".join(reasons) if reasons else "")
        )
    merged = values.merged

    mastertag = own.get("mastertag")
    if mastertag is None or str(mastertag) in ("", "null"):
        reasons.append("`mastertag` is required (the OpenShift release, X.Y.Z or X.Y.Z-<arch>)")
        mastertag = ""
    elif not _MASTERTAG.fullmatch(str(mastertag)):
        reasons.append(f"`mastertag` {mastertag!r} must be X.Y.Z or X.Y.Z-<arch> (4.22.8, 4.22.8-x86_64)")

    reasons += _nodepool_reasons(own)

    owner = merged.get("owner")
    if not isinstance(owner, dict):
        reasons.append(f"`owner` with {', '.join(REQUIRED_OWNER_FIELDS)} is required (the chart's owner ConfigMap is not nil-safe)")
    else:
        for field_name in REQUIRED_OWNER_FIELDS:
            if owner.get(field_name) in (None, ""):
                reasons.append(f"`owner.{field_name}` is required")

    for key in FORBIDDEN_KEYS:
        if key in own:
            reasons.append(
                f"`{key}` must not be in the file: this workflow writes the allocation "
                "(vlanId, dhcp_values) and the day1 CI chain appends mcConfigMap"
            )

    stem = Path(path).stem
    explicit_name = own.get("clusterName")
    if explicit_name not in (None, ""):
        # load_cluster_values takes the file's clusterName when set, so the
        # name the stem derives is read from an empty file at the same path.
        derived = load_cluster_values(root, path, text="{}").cluster_name
        if str(explicit_name) != derived:
            reasons.append(
                f"`clusterName` {explicit_name!r} disagrees with the file name ({stem}.yaml derives "
                f"{derived!r}); leave it out, or name the cluster after it"
            )

    env, sigs, day2_reasons = _day2(merged, stem, mce)
    reasons += day2_reasons

    if reasons:
        raise ClusterValuesRejectedError(f"{path} would not render as a hosted cluster: " + "; ".join(reasons))
    return ValidatedCluster(
        cluster_name=values.cluster_name,
        mastertag=str(mastertag),
        config_names=config_names_of(own),
        day2_env=env,
        day2_sigs=sigs,
    )


# --- the allocation block ----------------------------------------------------------

# The one definition of the appended block. Everything the DHCP stack needs
# beyond this (leaseDurationDays, dns, subnetMask, failover) is inherited from
# the upstream values layers; scopeName derives from the file name; gateway is
# deliberately omitted so the chart derives the /24's .254, and startRange/
# endRange are omitted for the same reason — absent, dhcp_scope_manager derives
# .1-.253, which the exclusions then carve the ends out of. vlanId is what the
# day1 chain renders the nm-conf MachineConfig from.
_ALLOCATION_BLOCK_TEMPLATE = """\
vlanId: {vlan_id}

dhcp_values:
  network: "{network}"{exclusions}"""

# Appended only when the type's policy defines exclusions. A type with none
# leaves the key out entirely rather than writing an empty list: the block then
# reads exactly like a hand-written minimal cluster file, and any exclusions a
# site layer defines keep applying. No blank line before "exclusions:" — it
# stays a sibling of "network:" under dhcp_values, not a new section.
_EXCLUSIONS_SECTION_TEMPLATE = """
  exclusions:
{exclusions}"""

_EXCLUSION_TEMPLATE = """\
    - startAddress: "{start_address}"
      endAddress: "{end_address}\""""


def render_allocation_block(vlan_id: int, dhcp_values: DhcpValues) -> str:
    """The exact text appended to a cluster file (no trailing newline)."""
    exclusions_section = ""
    if dhcp_values.exclusions:
        exclusions_section = _EXCLUSIONS_SECTION_TEMPLATE.format(
            exclusions="\n".join(
                _EXCLUSION_TEMPLATE.format(
                    start_address=exclusion.start_address,
                    end_address=exclusion.end_address,
                )
                for exclusion in dhcp_values.exclusions
            )
        )
    return _ALLOCATION_BLOCK_TEMPLATE.format(
        vlan_id=vlan_id,
        network=dhcp_values.network,
        exclusions=exclusions_section,
    )


def append_block(content: str, block: str) -> str:
    """Append the block per the blank-line rule: one blank line always
    separates the file's last content line from the block; an empty or
    whitespace-only file gets the block as its whole content."""
    if not content.strip():
        return block + "\n"
    return content.rstrip("\n") + "\n\n" + block + "\n"


def assemble_cluster_file(text: str, vlan_id: int, dhcp_values: DhcpValues) -> str:
    """The file provision-cluster publishes: the person's text, then the allocation block."""
    return append_block(text, render_allocation_block(vlan_id, dhcp_values))


# --- the re-run check ---------------------------------------------------------------


def _mapping(text: str, where: str) -> tuple[dict[str, Any] | None, str | None]:
    document, problems = load_yaml(text, where)
    if problems and document is None:
        return None, "; ".join(problems)
    if not isinstance(document, dict):
        return None, f"{where} is not a YAML mapping"
    return document, None


def same_cluster_file(published_text: str, assembled_text: str) -> str | None:
    """None when the file on the branch means what this run would write; else why not.

    The branch file's mapping, with CHAIN_APPENDED_KEY (what the day1 chain's
    injector appends in its own commit) dropped, must equal the assembled
    text's mapping. Whitespace, comments and the chain's blank-line
    re-spacing are not differences; a different spec, vlan or network is.
    """
    published, problem = _mapping(published_text, "the branch's cluster file")
    if problem:
        return problem
    assembled, problem = _mapping(assembled_text, "the assembled cluster file")
    if problem:
        return problem
    published = {key: value for key, value in published.items() if key != CHAIN_APPENDED_KEY}
    if published == assembled:
        return None
    differing = sorted(
        key for key in set(published) | set(assembled) if published.get(key) != assembled.get(key)
    )
    return f"the branch's cluster file differs from this run's at {', '.join(differing)}"
