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
    ClusterTarget,
    ClusterTargetRequest,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
)


@activity.defn
async def inspect_cluster_target(request: ClusterTargetRequest) -> ClusterTarget:
    """Read day1 `main` for where the cluster goes, and refuse what the chart would.

    Clones `main`, finds the ONE `sites/<site>/mces/<mce>` folder named exactly
    `request.mce` (iterated, never globbed with input), refuses a cluster whose
    `hostedClusters/<cluster>.yaml` already exists there, and validates the
    YAML text against the merged value layers the way the hostedclusters-setup
    chart renders them: `mastertag`, a non-empty `nodepool`, the three `owner`
    fields, no `vlanId`/`dhcp_values`/`mcConfigMap`/`AutomateAllocation`, a
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
    its YAML mapping, with the `mcConfigMap` key the day1 chain appends
    dropped, must equal the assembled text's — then `changed=False` and nothing
    pushed; anything else is ClusterFileConflictError (deterministic): a
    different spec on the branch, a branch without the file, a file that does
    not parse. A rejected push (the branch moved) is the retryable
    ValuesRepoGitError and the retry re-clones.
    """
    ...


@activity.defn
async def ensure_merge_request(request: MergeRequestRequest) -> MergeRequestRef:
    """The open merge request (GitLab) / pull request (GitHub) from the branch to
    `main`: found by source branch when one is open, else created — GitLab with
    `remove_source_branch`, GitHub relying on the repository's delete-on-merge
    setting — and read back.

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
    """Create `<path prefix>/sites/<site>/<env>/mces/<mce>/<cluster>/.gitkeep` in the
    sig's repo, on its default branch — how day2 learns a cluster exists (there is
    no registration file, only the folder; APPLY-DAY2-COPY).

    The repo is DAY2_SIG_REPO_URL_TEMPLATE with `{sig}` filled in, the prefix
    DAY2_SIG_PATH_TEMPLATE likewise; the default branch is read from the remote
    (`ls-remote --symref HEAD`). Idempotent: a folder already there is success
    with `created=False` and nothing pushed. The push goes straight to the
    default branch, bypassing the sig's merge-request lint by design. A rejected
    push is the retryable ValuesRepoGitError and the retry re-clones — the
    convergence loop add-cluster-to-day2.sh ran itself.
    """
    ...
