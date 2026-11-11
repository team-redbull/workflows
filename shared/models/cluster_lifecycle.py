"""Typed state for the cluster-lifecycle domain: provision-cluster's input, the state it
carries across continue-as-new, its progress and result, and the activity requests and
answers of its one hub limb (`cluster-lifecycle-worker`).

No `extra="forbid"` on any model here: these cross the workflow/activity
boundary and are decoded from history (CLAUDE.md §5). The API edge
(workflow_domains/cluster_lifecycle/router.py) is the strict one — and its
body is not JSON at all but the hosted-cluster YAML text itself, which arrives
here VERBATIM in `ProvisionClusterInput.values`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from shared.models.segment_lifecycle import AllocateSegmentResult, DhcpValues, SegmentType

# --- the run ----------------------------------------------------------------------------


class ProvisionClusterInput(BaseModel):
    """Input to ProvisionClusterWorkflow: which cluster, in which MCE folder, from which text.

    `cluster` is the day1 file STEM, `ocp4-<clusterName>` (canonicalised at the
    edge, shared/cluster_names.hosted_cluster_stem): the Argo CD Application's
    name, the Segments Manager's `cluster_name`, the allocate-segment child's id
    suffix and this run's. `mce` is the MCE FOLDER name under `sites/<site>/mces/`,
    verbatim — real folders include `ocp4-mce-site2` and `ocp4-mce-site4-prep`,
    which no naming convention parses, so the folder is found by name in the
    clone and never derived. `values` is the hosted-cluster YAML a person wrote,
    byte for byte: the run appends the allocation block to it and pushes it; it
    never re-serialises it, so comments and layout reach day1 unchanged.
    """

    cluster: str = Field(min_length=1)
    mce: str = Field(min_length=1)
    values: str = Field(min_length=1)


class ClusterTarget(BaseModel):
    """What inspect_cluster_target read at day1 `main` about where the cluster goes.

    `mce_dir` is `sites/<site>/mces/<mce>` as found in the clone; `cluster_name`
    the chart's clusterName (the file's own `clusterName` else the stem minus
    `ocp4-`); `config_names` the MachineConfig names the nodepools reference
    (`mcFiles` when set, else the union of every nodepool's `configs[].name`,
    first-seen order — empty is legal, a KubeVirt pool lists none);
    `day2_sigs` the sig repos the merged value layers list under
    `day2.copy_to_sigs` (empty = no day2 registration) and `day2_env` the
    environment folder they get, derived only when there are sigs;
    `revision` the day1 `main` commit all of it was read at.
    """

    site: str = Field(min_length=1)
    mce_dir: str = Field(min_length=1)
    cluster_name: str = Field(min_length=1)
    mastertag: str = Field(min_length=1)
    config_names: list[str] = Field(default_factory=list)
    day2_env: str | None = None
    day2_sigs: list[str] = Field(default_factory=list)
    revision: str = Field(min_length=1)


class ClusterFileRef(BaseModel):
    """Outcome of publish_cluster_file. `changed=False` (commit_sha None) means the
    branch already carried this file — a re-run — and nothing was pushed.
    `dhcp_values` is what the file records either way, so the run can report it."""

    relative_path: str = Field(min_length=1)
    branch: str = Field(min_length=1)
    commit_sha: str | None
    changed: bool
    dhcp_values: DhcpValues


class MergeRequestRef(BaseModel):
    """The merge request (GitLab) or pull request (GitHub) for the branch: its web
    URL, the provider's identifier (a GitLab iid or a GitHub number, as text) and
    whether ensure_merge_request created it or found it already open."""

    url: str = Field(min_length=1)
    identifier: str = Field(min_length=1)
    created: bool


class MergeRequestState(BaseModel):
    """What the provider says about the merge request: `open` while a person has not
    acted, `merged` with the merge commit, `closed` without merging. GitLab's
    `locked` reads as open (a merge in progress)."""

    state: Literal["open", "merged", "closed"]
    merged_sha: str | None = None


class Day2SigRegistration(BaseModel):
    """One sig repo the cluster was registered in: the folder created (with its
    `.gitkeep`), or `created=False` when it was already there — success either way."""

    sig: str = Field(min_length=1)
    path: str = Field(min_length=1)
    created: bool
    commit_sha: str | None


class ProvisionClusterState(BaseModel):
    """Everything a run has done so far, carried across continue-as-new (CLAUDE.md
    §5: a HUMAN wait backs off and continues as new so history stays bounded).
    Every field optional: a fresh run starts with none of it, and a continued run
    skips each phase whose outcome it finds here. `waited_since` and `polls` are
    the merge wait's clock and count, for progress."""

    target: ClusterTarget | None = None
    allocation: AllocateSegmentResult | None = None
    published: ClusterFileRef | None = None
    merge_request: MergeRequestRef | None = None
    waited_since: datetime | None = None
    polls: int = 0


class ProvisionClusterRunArgs(BaseModel):
    """The workflow's single argument (see InitializeSegmentRunArgs for why a wrapper):
    the public input plus the state a continued run restores."""

    input: ProvisionClusterInput
    state: ProvisionClusterState | None = None


class ProvisionClusterProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str
    allocate_segment_workflow_id: str | None = None
    branch: str | None = None
    merge_request_url: str | None = None
    merge_request_state: str | None = None
    waited_since: datetime | None = None
    polls: int = 0


class ProvisionClusterResult(BaseModel):
    """What the run did: where the file went, which segment it recorded, the merge
    request a person merged, and the day2 sig repos the cluster is registered in
    (`[]` when the value layers list none). `merged_sha` is the merge commit on
    `main` when the provider reports one."""

    cluster: str
    cluster_name: str
    site: str
    mce: str
    mastertag: str
    config_names: list[str]
    vlan_id: int
    segment: str
    epg_name: str
    network: str
    branch: str
    cluster_file: str
    commit_sha: str | None
    file_changed: bool
    merge_request_url: str
    merge_request_created: bool
    merged_sha: str | None
    day2: list[Day2SigRegistration]


# --- activity requests ------------------------------------------------------------------


class ClusterTargetRequest(BaseModel):
    """Input to inspect_cluster_target: the run's input, read against day1 `main`."""

    cluster: str = Field(min_length=1)
    mce: str = Field(min_length=1)
    values: str = Field(min_length=1)


class ClusterFilePublishRequest(BaseModel):
    """Input to publish_cluster_file: the text to publish and the allocation to append
    to it. `type` travels with it because the DHCP exclusion policy is PER TYPE
    (the limb selects that type's ranges out of DHCP_EXCLUSION_OCTET_RANGES)."""

    cluster: str = Field(min_length=1)
    mce_dir: str = Field(min_length=1)
    branch: str = Field(min_length=1)
    values: str = Field(min_length=1)
    vlan_id: int
    segment: str = Field(min_length=1)  # CIDR
    type: SegmentType


class MergeRequestRequest(BaseModel):
    """Input to ensure_merge_request: the branch to merge into `main`, and what the
    merge request says."""

    branch: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = ""


class Day2SigRegistrationRequest(BaseModel):
    """Input to register_cluster_in_day2_sig: which sig repo, and the folder
    `sites/<site>/<env>/mces/<mce>/<cluster>/` to create in it."""

    sig: str = Field(min_length=1)
    site: str = Field(min_length=1)
    env: str = Field(min_length=1)
    mce: str = Field(min_length=1)
    cluster: str = Field(min_length=1)
