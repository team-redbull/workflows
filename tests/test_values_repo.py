"""values_repo git plumbing against a REAL local bare repo — fully offline.

A bare repo in tmp_path stands in as `origin`, so clone → append → commit →
push is exercised end to end with no network, no token and no mocking of git
itself. The blank-line rule, the marker no-op and both conflict refusals are
pinned here; the Temporal wiring around these functions is covered by
tests/test_allocate_segment_workflow.py with mock activities.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities.segment_lifecycle import values_repo
from activities.segment_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    ClusterValuesConflictError,
)

CLUSTER = "ocp4-test-cluster-site1-a"
CLUSTER_FILE = f"sites/site1/mces/mce-a/hostedClusters/{CLUSTER}.yaml"
DHCP_VALUES = build_dhcp_values("10.20.90.0/24", [(1, 10), (241, 254)])

EXPECTED_BLOCK = """\
# === Added By Segment-Allocation Workflow ===
vlanId: 23

dhcp_values:
  network: "10.20.90.0"

  exclusions:
    - startAddress: "10.20.90.1"
      endAddress: "10.20.90.10"
    - startAddress: "10.20.90.241"
      endAddress: "10.20.90.254\""""

# A type whose policy excludes nothing: network only, no exclusions key. The
# DHCP API distributes its derived .1-.253 whole.
EXPECTED_BLOCK_NO_EXCLUSIONS = """\
# === Added By Segment-Allocation Workflow ===
vlanId: 23

dhcp_values:
  network: "10.20.90.0\""""


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """A bare `origin` seeded with one cluster values file on branch main."""
    bare = tmp_path / "origin.git"
    _git("init", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / "seed"
    _git("clone", str(bare), str(seed), cwd=tmp_path)
    file_path = seed / CLUSTER_FILE
    file_path.parent.mkdir(parents=True)
    file_path.write_text('description: "prep workers"\n')
    _git("add", ".", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-m", "seed", cwd=seed)
    _git("push", "origin", "main", cwd=seed)
    return bare


def _origin_file(origin: Path, tmp_path: Path, name: str) -> str:
    checkout = tmp_path / name
    _git("clone", str(origin), str(checkout), cwd=tmp_path)
    return (checkout / CLUSTER_FILE).read_text()


async def _append(origin: Path, **overrides):
    kwargs = dict(
        repo_url=str(origin),
        branch="main",
        token="",
        relative_path=CLUSTER_FILE,
        cluster=CLUSTER,
        vlan_id=23,
        dhcp_values=DHCP_VALUES,
    )
    kwargs.update(overrides)
    return await values_repo.append_allocation(**kwargs)


async def test_clone_append_commit_push_roundtrip(origin, tmp_path):
    commit_sha, changed = await _append(origin)
    assert changed
    assert commit_sha

    content = _origin_file(origin, tmp_path, "verify")
    # Exactly one blank line between the last content line and the marker.
    assert content == 'description: "prep workers"\n\n' + EXPECTED_BLOCK + "\n"
    # The pushed sha is origin's HEAD.
    assert commit_sha == _git("rev-parse", "main", cwd=origin).strip()
    log = _git("log", "-1", "--format=%s %an", "main", cwd=origin).strip()
    assert log == (
        f"chore: allocate segment for {CLUSTER} [segment-allocation-workflow] "
        f"{values_repo.GIT_USER_NAME}"
    )


async def test_empty_file_gets_the_marker_first(origin, tmp_path):
    seed = tmp_path / "empty-seed"
    _git("clone", str(origin), str(seed), cwd=tmp_path)
    (seed / CLUSTER_FILE).write_text("\n   \n")  # whitespace-only counts as empty
    _git("add", ".", cwd=seed)
    _git("-c", "user.name=s", "-c", "user.email=s@t", "commit", "-m", "blank", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    _, changed = await _append(origin)
    assert changed
    content = _origin_file(origin, tmp_path, "verify-empty")
    assert content == EXPECTED_BLOCK + "\n"  # marker is the first line, no leading blank


async def test_identical_block_is_a_no_op(origin):
    await _append(origin)
    before = _git("rev-parse", "main", cwd=origin).strip()

    commit_sha, changed = await _append(origin)
    assert not changed
    assert commit_sha is None
    assert _git("rev-parse", "main", cwd=origin).strip() == before  # nothing pushed


async def test_marker_with_different_values_is_refused(origin):
    await _append(origin)
    with pytest.raises(ClusterValuesConflictError, match="DIFFERENT values"):
        await _append(origin, vlan_id=99)


async def test_preexisting_unmarked_dhcp_values_is_refused(origin, tmp_path):
    seed = tmp_path / "fixture-seed"
    _git("clone", str(origin), str(seed), cwd=tmp_path)
    (seed / CLUSTER_FILE).write_text(
        'dhcp_values:\n  network: "10.20.40.0"\n'
    )
    _git("add", ".", cwd=seed)
    _git("-c", "user.name=s", "-c", "user.email=s@t", "commit", "-m", "fixture", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    with pytest.raises(ClusterValuesConflictError, match="not written by this workflow"):
        await _append(origin)


async def test_preexisting_bare_vlan_id_is_refused(origin, tmp_path):
    seed = tmp_path / "vlan-seed"
    _git("clone", str(origin), str(seed), cwd=tmp_path)
    (seed / CLUSTER_FILE).write_text('description: "x"\nvlanId: 7\n')
    _git("add", ".", cwd=seed)
    _git("-c", "user.name=s", "-c", "user.email=s@t", "commit", "-m", "vlan", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    with pytest.raises(ClusterValuesConflictError, match="dhcp_values or vlanId"):
        await _append(origin)


async def test_locate_finds_the_single_cluster_file(origin):
    location = await values_repo.locate_cluster_file(
        repo_url=str(origin), branch="main", token="", cluster=CLUSTER,
    )
    assert location.site == "site1"  # the path segment beneath the clusters root
    assert location.relative_path == CLUSTER_FILE


async def test_locate_unknown_cluster_raises(origin):
    with pytest.raises(ClusterFileNotFoundError):
        await values_repo.locate_cluster_file(
            repo_url=str(origin), branch="main", token="",
            cluster="no-such-cluster",
        )


async def test_locate_duplicate_cluster_file_raises(origin, tmp_path):
    seed = tmp_path / "dup-seed"
    _git("clone", str(origin), str(seed), cwd=tmp_path)
    duplicate = seed / "sites/site2/mces/mce-b/hostedClusters" / f"{CLUSTER}.yaml"
    duplicate.parent.mkdir(parents=True)
    duplicate.write_text('description: "impostor"\n')
    _git("add", ".", cwd=seed)
    _git("-c", "user.name=s", "-c", "user.email=s@t", "commit", "-m", "dup", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    with pytest.raises(AmbiguousClusterFileError):
        await values_repo.locate_cluster_file(
            repo_url=str(origin), branch="main", token="", cluster=CLUSTER,
        )


def test_token_is_injected_only_into_https_urls():
    assert values_repo.authenticated_url(
        "https://github.com/org/repo.git", "s3cret"
    ) == "https://x-access-token:s3cret@github.com/org/repo.git"
    # Local paths (these tests) and tokenless URLs pass through untouched.
    assert values_repo.authenticated_url("/tmp/origin.git", "s3cret") == "/tmp/origin.git"
    assert values_repo.authenticated_url(
        "https://github.com/org/repo.git", ""
    ) == "https://github.com/org/repo.git"


def test_a_type_without_exclusions_renders_network_only():
    # No exclusions key at all rather than an empty list: the block reads like
    # a hand-written minimal cluster file, and a site layer's exclusions (if
    # one ever defines any) keep applying.
    values = build_dhcp_values("10.20.90.0/24", [])
    assert values_repo.render_allocation_block(23, values) == EXPECTED_BLOCK_NO_EXCLUSIONS


def test_split_marker_block_roundtrips_for_the_future_remover():
    block = values_repo.render_allocation_block(23, DHCP_VALUES)
    content = values_repo.append_block('description: "x"\n', block)
    head, found = values_repo.split_marker_block(content)
    assert head == 'description: "x"\n\n'
    assert found.strip() == block.strip()
    # No marker -> the whole content is the head; the remover can rely on it.
    assert values_repo.split_marker_block("plain: file\n") == ("plain: file\n", None)
