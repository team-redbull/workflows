"""Cluster-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/cluster_lifecycle/activities.py and
are registered against these names on the cluster-lifecycle activity queue, on
the hub. Workflows import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.cluster_lifecycle import (
    ClusterFilePublishRequest,
    ClusterFileRef,
    ClusterShortcut,
    ClusterShortcutRequest,
    ClusterTarget,
    ClusterTargetRequest,
    Day1ClusterList,
    Day1ClusterQuery,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
)
from shared.models.vm_lifecycle import KvClusterList, KvClusterListRequest


@activity.defn
async def inspect_cluster_target(request: ClusterTargetRequest) -> ClusterTarget:
    """Read day1 `main` for where the cluster goes, and refuse what the chart would.

    Reads `main` at its head (cloned only when it moved since this worker last
    read it), finds the ONE `sites/<site>/mces/<mce>` folder named exactly
    `request.mce` (iterated, never globbed with input), refuses a cluster whose
    `hostedClusters/<cluster>.yaml` already exists there, and validates the
    YAML text against the merged value layers the way the hostedclusters-setup
    chart renders them: `mastertag`, a non-empty `nodepool`, the three `owner`
    fields, no `vlanId`/`dhcp_values`/`mcConfigMap`/`tuningConfigmap`/`AutomateAllocation`, a
    `clusterName` only if it equals the derived one, config names that are
    DNS-1123 labels, no duplicate keys. Derives the config names, the day2 sig
    list and its environment.

    Observe-only, so trivially idempotent. Raises MceFolderNotFoundError,
    AmbiguousMceFolderError, ClusterAlreadyProvisionedError and
    ClusterValuesRejectedError (the message lists EVERY reason) — all
    deterministic, non-retryable; a clone failure is the retryable
    ValuesRepoGitError.
    """
    ...


@activity.defn
async def publish_cluster_file(request: ClusterFilePublishRequest) -> ClusterFileRef:
    """Put the cluster file — the text plus the allocation block — on its own branch.

    Branch `provision-cluster/<cluster>`. Absent on the remote: cloned from
    `main`, the file written, committed WITHOUT `[skip ci]` (the day1 chain must
    run on the branch and render the MachineConfigs onto it) and pushed, which
    creates the branch. Present: cloned, and the file compared SEMANTICALLY —
    its YAML mapping, with the `mcConfigMap` and `tuningConfigmap` keys the
    day1 chain appends dropped, must equal the assembled text's — then `changed=False` and nothing
    pushed; anything else is ClusterFileConflictError (deterministic): a
    different spec on the branch, a branch without the file, a file that does
    not parse. A rejected push (the branch moved) is the retryable
    ValuesRepoGitError and the retry re-clones.
    """
    ...


@activity.defn
async def ensure_merge_request(request: MergeRequestRequest) -> MergeRequestRef:
    """The open GitLab merge request from the branch to `main`: found by source
    branch when one is open, else created with `remove_source_branch`.

    Idempotent by find-before-create. Raises GitProviderAuthError (401/403) and
    GitProviderRequestRejectedError (400/404/422: the provider refused the
    request as sent — a missing target branch, a project the token cannot see),
    both deterministic; a 409 (a merge request born between the find and the
    create), a 5xx or an unreachable provider is the retryable GitProviderError,
    and the retry finds what was created.
    """
    ...


@activity.defn
async def get_merge_request_state(merge_request: MergeRequestRef) -> MergeRequestState:
    """Read the merge request back: open, merged (with the merge commit) or closed.

    Read-only, so trivially idempotent. Raises GitProviderAuthError and
    GitProviderRequestRejectedError (404: the merge request is gone), both
    deterministic; anything else is the retryable GitProviderError.
    """
    ...


@activity.defn
async def register_cluster_in_day2_sig(request: Day2SigRegistrationRequest) -> Day2SigRegistration:
    """Create `sites/<site>/<env>/mces/<mce>/<cluster>/.gitkeep` in the sig's GitLab
    project, on its default branch — how day2 learns a cluster exists (there is
    no registration file, only the folder; APPLY-DAY2-COPY).

    The repo is DAY2_SIG_REPO_URL_TEMPLATE with `{sig}` filled in (one project per
    sig, the tree at its root); the default branch is read from the remote
    (`ls-remote --symref HEAD`). Idempotent: a folder already there is success
    with `created=False` and nothing pushed. The push goes straight to the
    default branch, bypassing the sig's merge-request lint by design. A rejected
    push — the branch moved, or a token whose role may not push the protected
    branch — is the retryable ValuesRepoGitError and the retry re-clones — the
    convergence loop add-cluster-to-day2.sh ran itself.
    """
    ...


@activity.defn
async def ensure_cluster_shortcut(request: ClusterShortcutRequest) -> ClusterShortcut:
    """The cluster's console shortcut (go-link) in the shortcuts service: `request.shortcut`
    pointing at CLUSTER_CONSOLE_URL_TEMPLATE with `{cluster}` = `request.cluster`,
    created with `username` SHORTCUT_OWNER.

    Find-before-create (`GET /check/<name>`), then the create (`POST /create`) and
    a read-back check: success only once the service reports the name taken by
    SHORTCUT_OWNER. Already taken by SHORTCUT_OWNER is success, `created=False`,
    nothing posted. Raises ShortcutTakenError (another owner holds the name —
    deterministic, non-retryable); EVERY other failure, refusals included, is the
    retryable ShortcutsApiError (the run is past its merge and cannot be re-run).
    """
    ...


# --- shared with other domains ----------------------------------------------------------


@activity.defn
async def list_day1_clusters(query: Day1ClusterQuery) -> Day1ClusterList:
    """The hosted clusters day1 `main` describes, filtered by site, env, kind and zone.

    Lives in THIS limb because it reads the day1 repo — whose URL and token
    already sit on the cluster-lifecycle limb. A workflow of another domain
    calls it with task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE rather than a
    second deployment holding a copy of that credential. A new question about
    day1 is a new filter on the query (and a method of activities/day1_repo.py's
    `Day1`), never a new activity. Not a generic git activity: no clone crosses
    it and it writes nothing; it answers typed facts about one commit, and the
    answer carries that commit (`revision`).

    `kind` `kv` is a cluster under a SEED folder (`ocp4-<lifecycle>-seed-<site>-<zone>-<id>`, zone
    `az<N>`), never one named kubevirt; `hc` is one under any other folder. A
    kv entry whose NodePools do not all label their nodes with the seed's zone,
    or whose name is off the convention or disagrees with its seed's site, zone
    or lifecycle, is listed WITH its `problems`.

    Observe-only, so trivially idempotent. The remote is asked for `main`'s
    head on every call and cloned only when it moved (git_repo.snapshot), so
    calls arriving together cause one clone. A filter that matches nothing is
    an empty list. Raises a NON-RETRYABLE ApplicationError of type
    Day1QueryInvalidError for a filter that is not a token of the naming
    convention (refused before any git call); a clone failure is the
    retryable ValuesRepoGitError, a day1 without `main` ValuesBranchNotFoundError.
    """
    ...


# --- shared with the vm-lifecycle domain ------------------------------------------------
#
# create-vm (workflow_domains/vm_lifecycle/create_vm.py) asks THIS limb which KV
# clusters a zone has: the limb holds the day1 token, and the worker inside a
# KV cluster holds none (the get_inventory_segment precedent — an activity lives
# where its credential is, CLAUDE.md §2).


@activity.defn
async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
    """The KV clusters of (lifecycle, site, zone) as day1 `main` lists them, OLDEST FIRST, with
    the placement buffer every run of the zone must use.

    The zone's clusters are the files of its seeds' folders of that lifecycle (the
    MCE's — a prod MCE's VMs go on prod KV clusters only; a zone may have several
    seeds, `-a`, `-b`, whose clusters are ordered together):
    `sites/<site>/mces/ocp4-<lifecycle>-seed-<site>-<zone>-<id>/hostedClusters/`
    (`Day1.kv_clusters`, activities/day1_repo.py), read on the commit-keyed
    snapshot of `main`: the remote is asked for its head every time, and a
    clone happens only when the head moved. Each file is one entry, sorted by
    the sequence letter its name ends with. An entry's `zone` is set only when
    every one of its NodePools labels its nodes with the seed's zone and its name
    — `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`, which also gives its `id` — has
    the seed's site, zone and lifecycle; otherwise `problems` says why and the run
    never uses it. A zone with no such seed is an EMPTY list, never an error.
    `folder` names the seed folders as a pattern. `buffer_percent` is this
    worker's HEADROOM_BUFFER_PERCENT. Observe-only. Raises a NON-RETRYABLE
    ApplicationError of type Day1QueryInvalidError for a request without a
    lifecycle (a run recorded before the field: refused, never guessed), before
    any git call; the retryable ValuesRepoGitError / ValuesBranchNotFoundError
    (unreachable, or no `main`: waited out).
    """
    ...
