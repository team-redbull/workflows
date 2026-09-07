"""Git plumbing for the day1 values repo — kept out of activities.py.

Everything here is a plain function taking explicit parameters (never reading
settings), so the offline tests drive it against a local bare repo with no
network and no Temporal. The @activity.defn wrappers in activities.py pass
the real configuration in.

Rules this module enforces:
  * git runs via asyncio.create_subprocess_exec — no new Python dependency,
    but the limb image must install the git binary (see the Dockerfile).
  * The push token is injected into the clone URL in memory only
    (https://x-access-token:<token>@...) and NEVER logged: every command echo
    and every error message passes through _redact() BEFORE an exception is
    constructed — a git error lands verbatim in Temporal history and the UI,
    so scrubbing at the logging layer alone would be too late.
  * Every clone goes into a fresh TemporaryDirectory and is always discarded,
    so a Temporal retry starts clean; a rejected non-fast-forward push raises
    the retryable ValuesRepoGitError and the retry re-clones and re-applies,
    converging with no merge special-casing.

The allocation block layout lives in ONE place (_ALLOCATION_BLOCK_TEMPLATE +
render_allocation_block), and split_marker_block is the parsing counterpart —
written so a future remove_allocation_from_cluster_values can reuse both
unchanged.
"""

from __future__ import annotations

import asyncio
import re
import tempfile
from pathlib import Path

from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    ClusterValuesConflictError,
    ValuesRepoGitError,
)
from shared.models.segment_lifecycle import ClusterFileLocation, DhcpValues

# Kept below the git activities' 180s start_to_close_timeout so a hung remote
# fails the command and frees the worker before Temporal reaps the activity
# (same budget rule as the HTTP clients'). The clone is the only long command;
# the local add/commit/rev-parse and the push share the remainder comfortably.
_GIT_COMMAND_TIMEOUT_SECONDS = 150.0

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
# deliberately omitted so the chart derives the /24's .254. vlanId has no
# consumer in any chart — it is an audit annotation, as the old CI script
# wrote it.
_ALLOCATION_BLOCK_TEMPLATE = """\
{marker}
vlanId: {vlan_id}

dhcp_values:
  network: "{network}"
  startRange: "{start_range}"
  endRange: "{end_range}"

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


def render_allocation_block(vlan_id: int, dhcp_values: DhcpValues) -> str:
    """The exact text appended to a cluster values file (no trailing newline)."""
    exclusions = "\n".join(
        _EXCLUSION_TEMPLATE.format(
            start_address=exclusion.start_address,
            end_address=exclusion.end_address,
        )
        for exclusion in dhcp_values.exclusions
    )
    return _ALLOCATION_BLOCK_TEMPLATE.format(
        marker=MARKER,
        vlan_id=vlan_id,
        network=dhcp_values.network,
        start_range=dhcp_values.start_range,
        end_range=dhcp_values.end_range,
        exclusions=exclusions,
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


def append_block(content: str, block: str) -> str:
    """Append the block per the blank-line rule: one blank line always
    separates the file's last content line from the marker; an empty or
    whitespace-only file gets the block with the marker as its first line."""
    if not content.strip():
        return block + "\n"
    return content.rstrip("\n") + "\n\n" + block + "\n"


def _redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def authenticated_url(repo_url: str, token: str) -> str:
    """Inject the token into an https clone URL; anything else (a local path
    in tests, ssh) passes through untouched."""
    if token and repo_url.startswith("https://"):
        return "https://x-access-token:" + token + "@" + repo_url.removeprefix("https://")
    return repo_url


async def _run_git(args: list[str], *, cwd: Path | None, secrets: list[str]) -> str:
    """Run one git command; raise the retryable ValuesRepoGitError on any
    failure, with every secret scrubbed from the message BEFORE the exception
    exists (it will be recorded in Temporal history)."""
    process = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=_GIT_COMMAND_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise ValuesRepoGitError(
            f"git {_redact(' '.join(args), secrets)} timed out after "
            f"{_GIT_COMMAND_TIMEOUT_SECONDS:.0f}s"
        ) from None
    if process.returncode != 0:
        raise ValuesRepoGitError(
            f"git {_redact(' '.join(args), secrets)} failed "
            f"(exit {process.returncode}): "
            f"{_redact(stderr.decode(errors='replace').strip(), secrets)}"
        )
    return stdout.decode(errors="replace")


async def _clone(
    *, repo_url: str, branch: str, token: str, dest: Path
) -> None:
    """Shallow single-branch clone — retries always start from a clean tree."""
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

    Returns (commit_sha, changed): (None, False) when the file already ends in
    this exact block — a re-run, nothing to push. Raises the non-retryable
    ClusterValuesConflictError when the file carries a DIFFERENT allocation
    (marker with other values, or an unmarked dhcp_values/vlanId key), and the
    retryable ValuesRepoGitError for any git failure — including a rejected
    non-fast-forward push, which the retry resolves by re-cloning.
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
            if existing_block.strip() == block.strip():
                return None, False
            raise ClusterValuesConflictError(
                f"{relative_path} already carries a segment-allocation block "
                "with DIFFERENT values — refusing to overwrite; a human must "
                "decide which allocation is right"
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
        await _run_git(["add", relative_path], cwd=clone_dir, secrets=secrets)
        await _run_git(
            [
                "-c",
                f"user.name={GIT_USER_NAME}",
                "-c",
                f"user.email={GIT_USER_EMAIL}",
                "commit",
                "-m",
                f"chore: allocate segment for {cluster} [segment-allocation-workflow]",
            ],
            cwd=clone_dir,
            secrets=secrets,
        )
        commit_sha = (
            await _run_git(["rev-parse", "HEAD"], cwd=clone_dir, secrets=secrets)
        ).strip()
        await _run_git(["push", "origin", branch], cwd=clone_dir, secrets=secrets)
        return commit_sha, True
