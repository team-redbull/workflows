"""Git plumbing for the day1 values repo — kept out of activities.py.

Everything here is a plain function taking explicit parameters (never reading
settings), so the offline tests drive it against a local bare repo with no
network and no Temporal. The @activity.defn wrappers in activities.py pass
the real configuration in.

The generic git client — command timeout, token injection and scrubbing, the
ls-remote branch check, the shallow clone — lives in activities/git_repo.py,
shared with the cluster-upgrade hub limb; this module keeps its own names for
it, so nothing that calls or tests it changed.

Rules this module enforces:
  * git runs via asyncio.create_subprocess_exec — no new Python dependency,
    but the limb image must install the git binary (see the Dockerfile). Every command
    runs with http.sslVerify=false — the airgapped environment's internal CA
    cannot be injected into this image (see activities._TLS_VERIFY).
  * The push token is injected into the clone URL in memory only
    (https://x-access-token:<token>@...) and NEVER logged: every command echo
    and every error message passes through _redact() BEFORE an exception is
    constructed — a git error lands verbatim in Temporal history and the UI,
    so scrubbing at the logging layer alone would be too late.
  * Every clone goes into a fresh TemporaryDirectory and is always discarded,
    so a Temporal retry starts clean; a rejected non-fast-forward push raises
    the retryable ValuesRepoGitError and the retry re-clones and re-applies,
    converging with no merge special-casing.
  * The branch is a PARAMETER — the one the run names (the day1 pipeline's own
    branch), never configuration. It comes from an unauthenticated API, so no
    git command sees it before _require_branch has accepted it: ls-remote gets
    it only as the full ref refs/heads/<branch>, and clone/push get it only
    after that check passed. A branch the remote does not have is the
    deterministic ValuesBranchNotFoundError; an unreachable remote stays the
    retryable ValuesRepoGitError.

The allocation block layout lives in ONE place (_ALLOCATION_BLOCK_TEMPLATE +
render_allocation_block), and split_marker_block is the parsing counterpart —
written so a future remove_allocation_from_cluster_values can reuse both
unchanged.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from activities import git_repo
from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    ClusterValuesConflictError,
)
from shared.models.segment_lifecycle import ClusterFileLocation, DhcpValues

# The shared git client (activities/git_repo.py), under the names this module
# has always used. GIT_COMMAND_TIMEOUT_SECONDS stays below the git activities'
# 180s start_to_close_timeout.
_GIT_COMMAND_TIMEOUT_SECONDS = git_repo.GIT_COMMAND_TIMEOUT_SECONDS
_redact = git_repo.redact
authenticated_url = git_repo.authenticated_url
_exec_git = git_repo.exec_git
_run_git = git_repo.run_git

# What a branch the remote lacks means HERE: the run named the wrong one.
_MISSING_BRANCH = (
    "Branch {branch!r} does not exist in the values repo — the caller "
    "must pass the branch its cluster file lives on (the day1 "
    "pipeline's own branch)"
)

MARKER = "# === Added By Segment-Allocation Workflow ==="

# The day1 repo's layout and this workflow's committer identity. Hardcoded, not
# configured: the clusters root is the repo's own structure (a wrong value finds
# no cluster file at all, so there is nothing an operator would usefully tune),
# and the commit identity names THIS workflow — it identifies the writer, so it
# must not vary per environment.
CLUSTERS_ROOT = "sites"
GIT_USER_NAME = "segment-allocation-workflow"
GIT_USER_EMAIL = "segment-allocation-workflow@redbull.local"

# The one definition of the appended block. Everything the DHCP stack needs
# beyond this (leaseDurationDays, dns, subnetMask, failover) is inherited from
# the upstream values layers; scopeName derives from the file name; gateway is
# deliberately omitted so the chart derives the /24's .254, and startRange/
# endRange are omitted for the same reason — absent, dhcp_scope_manager derives
# .1-.253, which the exclusions then carve the ends out of. vlanId has no
# consumer in any chart — it is an audit annotation, as the old CI script
# wrote it.
_ALLOCATION_BLOCK_TEMPLATE = """\
{marker}
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

# A dhcp_values or vlanId key at column 0 that is NOT under our marker means
# somebody else wrote an allocation into this file. Appending a second
# top-level dhcp_values would be a duplicate YAML key that silently discards
# the first (taking scopeName/pxe/gateway/failover with it) — refuse instead.
_FOREIGN_ALLOCATION_KEY_RE = re.compile(r"^(vlanId|dhcp_values)\s*:", re.MULTILINE)

# The two values that IDENTIFY the allocation inside an existing marker block.
# A re-run compares these — not the block's text — so an operator may tune the
# scope in place (a hand-picked startRange/endRange for one cluster, an extra
# exclusion) without the next run calling their edit a conflicting allocation.
# What this workflow guarantees is that the file records the vlan and network
# the Segments Manager confirmed; the DHCP detail around them is day1's, and
# the operator's.
_RECORDED_VLAN_RE = re.compile(r"^vlanId:\s*(\d+)\s*$", re.MULTILINE)
_RECORDED_NETWORK_RE = re.compile(r'^\s+network:\s*"([^"]+)"\s*$', re.MULTILINE)


def render_allocation_block(vlan_id: int, dhcp_values: DhcpValues) -> str:
    """The exact text appended to a cluster values file (no trailing newline)."""
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
        marker=MARKER,
        vlan_id=vlan_id,
        network=dhcp_values.network,
        exclusions=exclusions_section,
    )


def split_marker_block(content: str) -> tuple[str, str | None]:
    """Split file content into (everything before the marker, the marker block).

    The block is None when no marker is present. The counterpart of
    render_allocation_block — and the helper a future
    remove_allocation_from_cluster_values reuses to strip the block.
    """
    index = content.find(MARKER)
    if index == -1:
        return content, None
    return content[:index], content[index:]


def read_allocation_identity(block: str) -> tuple[int, str] | None:
    """The (vlanId, network) an existing marker block records, or None when the
    block does not carry both — a mangled block a human has to look at."""
    vlan_match = _RECORDED_VLAN_RE.search(block)
    network_match = _RECORDED_NETWORK_RE.search(block)
    if vlan_match is None or network_match is None:
        return None
    return int(vlan_match.group(1)), network_match.group(1)


def append_block(content: str, block: str) -> str:
    """Append the block per the blank-line rule: one blank line always
    separates the file's last content line from the marker; an empty or
    whitespace-only file gets the block with the marker as its first line."""
    if not content.strip():
        return block + "\n"
    return content.rstrip("\n") + "\n\n" + block + "\n"


async def _require_branch(*, repo_url: str, branch: str, token: str) -> None:
    """Fail deterministically when the remote has no such branch (git_repo.require_branch).

    Without this, `git clone --branch <missing>` exits non-zero like any other
    git failure and raises the RETRYABLE ValuesRepoGitError, so a mistyped or
    deleted branch would retry every minute forever: ls-remote exit 2 is the
    non-retryable ValuesBranchNotFoundError, anything else stays retryable.
    """
    await git_repo.require_branch(
        repo_url=repo_url, branch=branch, token=token,
        missing_message=_MISSING_BRANCH.format(branch=branch),
    )


async def _clone(
    *, repo_url: str, branch: str, token: str, dest: Path
) -> None:
    """Shallow single-branch clone — retries always start from a clean tree.

    The branch is checked FIRST (_require_branch), so a missing one fails as
    ValuesBranchNotFoundError and the clone never sees an unchecked branch.
    Both activities clone through here, so both inherit the check."""
    await _require_branch(repo_url=repo_url, branch=branch, token=token)
    await _run_git(
        [
            "clone",
            "--depth",
            "1",
            "--branch",
            branch,
            "--single-branch",
            authenticated_url(repo_url, token),
            str(dest),
        ],
        cwd=None,
        secrets=[token],
    )


def _find_cluster_file(clone_dir: Path, cluster: str) -> ClusterFileLocation:
    """Exactly one CLUSTERS_ROOT/<site>/**/<cluster>.yaml, or a loud,
    deterministic failure. The site is the path segment directly beneath the
    clusters root — the repo layout is the source of truth for it."""
    root = clone_dir / CLUSTERS_ROOT
    matches = sorted(root.glob(f"*/**/{cluster}.yaml")) if root.is_dir() else []
    if not matches:
        raise ClusterFileNotFoundError(
            f"No {cluster}.yaml found under {CLUSTERS_ROOT}/ in the values "
            "repo — the cluster has no values file to record an allocation in"
        )
    if len(matches) > 1:
        relative = [str(path.relative_to(clone_dir)) for path in matches]
        raise AmbiguousClusterFileError(
            f"{len(matches)} files named {cluster}.yaml under {CLUSTERS_ROOT}/ "
            f"in the values repo ({relative}) — cluster file names are the "
            "identity the day1 stack keys on, so a human must resolve this"
        )
    match = matches[0]
    return ClusterFileLocation(
        site=match.relative_to(root).parts[0],
        relative_path=str(match.relative_to(clone_dir)),
    )


async def locate_cluster_file(
    *, repo_url: str, branch: str, token: str, cluster: str
) -> ClusterFileLocation:
    with tempfile.TemporaryDirectory(prefix="day1-locate-") as tmp:
        clone_dir = Path(tmp) / "repo"
        await _clone(repo_url=repo_url, branch=branch, token=token, dest=clone_dir)
        return _find_cluster_file(clone_dir, cluster)


async def append_allocation(
    *,
    repo_url: str,
    branch: str,
    token: str,
    relative_path: str,
    cluster: str,
    vlan_id: int,
    dhcp_values: DhcpValues,
) -> tuple[str | None, bool]:
    """Append the allocation block to the cluster's file, commit and push.

    Returns (commit_sha, changed): (None, False) when the file already records
    THIS allocation — a re-run, nothing to push. "This allocation" means the
    marker block's vlanId and network match; the rest of the block is left
    exactly as it stands, so a cluster whose scope an operator tuned by hand
    (its own startRange/endRange, an extra exclusion) survives every later
    re-run untouched. Raises the non-retryable ClusterValuesConflictError when
    the file carries a DIFFERENT allocation (another vlan or network, a marker
    block too mangled to read one out of, or an unmarked dhcp_values/vlanId
    key), and the retryable ValuesRepoGitError for any git failure — including
    a rejected non-fast-forward push, which the retry resolves by re-cloning.
    """
    block = render_allocation_block(vlan_id, dhcp_values)
    with tempfile.TemporaryDirectory(prefix="day1-append-") as tmp:
        clone_dir = Path(tmp) / "repo"
        await _clone(repo_url=repo_url, branch=branch, token=token, dest=clone_dir)

        values_file = clone_dir / relative_path
        if not values_file.is_file():
            # The file existed when locate_cluster_file ran; its disappearance
            # between then and now is a repo change a human must look at.
            raise ClusterFileNotFoundError(
                f"{relative_path} no longer exists in the values repo — it was "
                "present when this run located it"
            )
        content = values_file.read_text()

        head, existing_block = split_marker_block(content)
        if existing_block is not None:
            # Compare the allocation, not the text. The vlan and the network
            # are what this workflow put on record and what a second, clashing
            # allocation would change; everything else in the block is DHCP
            # detail an operator is allowed to have adjusted for this cluster.
            recorded = read_allocation_identity(existing_block)
            if recorded == (vlan_id, dhcp_values.network):
                return None, False
            found = (
                f"vlan {recorded[0]} on {recorded[1]}"
                if recorded is not None
                else "a block with no readable vlanId/network"
            )
            raise ClusterValuesConflictError(
                f"{relative_path} already carries a segment-allocation block "
                f"for {found}, not vlan {vlan_id} on {dhcp_values.network} — "
                "refusing to overwrite; a human must decide which allocation "
                "is right"
            )
        if _FOREIGN_ALLOCATION_KEY_RE.search(head):
            raise ClusterValuesConflictError(
                f"{relative_path} already contains a top-level dhcp_values or "
                "vlanId key not written by this workflow — appending would "
                "create a duplicate YAML key that silently discards the "
                "existing block"
            )

        values_file.write_text(append_block(content, block))
        secrets = [token]
        # `[skip ci]`: the push lands on the day1 pipeline's OWN branch, and
        # that pipeline runs on every non-main push unless the commit message
        # says otherwise — without the marker, our push would start a SECOND
        # pipeline that re-runs the allocator that triggered this run.
        await _run_git(["add", relative_path], cwd=clone_dir, secrets=secrets)
        await _run_git(
            [
                "-c",
                f"user.name={GIT_USER_NAME}",
                "-c",
                f"user.email={GIT_USER_EMAIL}",
                "commit",
                "-m",
                f"chore: allocate segment for {cluster} "
                "[segment-allocation-workflow] [skip ci]",
            ],
            cwd=clone_dir,
            secrets=secrets,
        )
        commit_sha = (
            await _run_git(["rev-parse", "HEAD"], cwd=clone_dir, secrets=secrets)
        ).strip()
        await _run_git(["push", "origin", branch], cwd=clone_dir, secrets=secrets)
        return commit_sha, True
