# provision-cluster — the branch, the CI chain and day2

What the code does not say by itself: who commits what on a cluster's day1
branch and in which order, why a re-run compares the file semantically, and why
day2 registration waits for the merge. The run's shape is the module docstring
(`workflow_domains/cluster_lifecycle/provision_cluster.py`) and the docs page
(`docs/site/cluster-lifecycle/provision-cluster.html`); the decisions are in
CLAUDE.md §4.

## The branch, commit by commit

```
day1 main ──┐
            │  1. provision-cluster: feat: provision hosted cluster <stem> [provision-cluster-workflow]
            │     sites/<site>/mces/<mce>/hostedClusters/<stem>.yaml
            │       = the person's text, verbatim
            │       + one blank line
            │       + vlanId: <vlan>          (no marker comment)
            │         dhcp_values: {network, exclusions}
            │     NO [skip ci]
            │
            │  2. the day1 CI chain (GitLab, non-main branches only):
            │       allocate_segments   → "already carries allocation state — skipping"
            │       stage_mc_sources    → mc-files/<clusterName>/ from the names + vlanId + mastertag
            │       render_mc_configs   → butane
            │       inject_configs      → appends mcConfigMap: [...] (no CI header since the
            │                             APPLY-PROVISION-CLUSTER.md §4.5 patch; a legacy one is removed)
            │       commit_and_push     → CI: Update mcConfigMap from mc-files [skip ci]
            │
            │  3. provision-cluster: the merge request from provision-cluster/<stem> to main
            │     (opened right after 1; it follows the branch, so it shows 2 when 2 lands)
            │
            │  4. a person merges (after the pipeline is green and commit 2 is on the branch)
            ▼
day1 main   5. Argo CD renders the Application (synced by hand in production)
            6. provision-cluster registers the cluster in the day2 sig repos
```

Commit 1 must not carry `[skip ci]`: the chain's `workflow.rules` skip a commit
whose message says so, and the MachineConfigs would never be rendered. Commit
2 carries it, as it always has, so the chain does not run on its own commit.

The workflow never waits for commit 2. The merge request's description tells
the reviewer to merge only once the pipeline is green and that commit is on the
branch — the same rule a human-pushed branch had.

## Why the re-run compares semantically

A re-run of a COMPLETED cluster (same workflow id, new run) or a retry after a
rejected push reaches `publish_cluster_file` with the branch already there. By
then the branch's file is commit 2's: the person's text, the allocation block,
`inject_configs.py`'s blank-line normalisation (it collapses runs of blank
lines and adds one before every top-level key) and the appended `mcConfigMap`
section. A byte comparison would call every re-run a conflict.

So `cluster_values.same_cluster_file` parses both texts (`day1_repo.load_yaml`,
duplicate keys reported) and compares the branch file's mapping, with the
`mcConfigMap` key dropped, against the assembled text's. Whitespace, comments
and the chain's section are not differences; a different `vlanId`, `network`,
`mastertag` or nodepool is `ClusterFileConflictError`, naming the key. Once the
chart renders MachineConfigs at sync time (the deferred
`mc-sources-rendered-at-sync` plan) and the chain's injector goes, the compare
can become byte-for-byte again.

## Why day2 waits for the merge

A sig repo registers a cluster by the folder
`sites/<site>/<env>/mces/<mce>/<cluster>/` (gitops-day2-prod ARCHITECTURE.md
R6). day2's render of that sig reads the cluster's `mastertag` from day1 `main`
(the `ocp-versions/<v>/` layers), so a folder whose cluster is not on `main`
yet breaks the sig's WHOLE render (APPLY-DAY2-COPY §4b, seen live). The chart
Job this replaces avoided it by running post-sync; the workflow gets the same
guarantee by observing the merge: `registering-day2` runs only after
`get_merge_request_state` read `merged`.

The environment folder is `day2.env` from the merged value layers, else the
cluster name's first field (`ocp4-prod-iac-site2-a` → `prod`), cross-checked
against the MCE folder's when it is `ocp4-<env>-mce-…`; one of `prod`, `prep`,
`test`; derived only when `day2.copy_to_sigs` lists a sig. The push goes to the
sig repo's default branch (read from `ls-remote --symref`), bypassing the sig's
merge-request lint by design, as `add-cluster-to-day2.sh` did; a folder already
there is success.

One GitLab project per sig, `DAY2_SIG_REPO_URL_TEMPLATE =
https://<gitlab>/redbull/gitops-day2-prod/sigs/{sig}.git`, the tree at the root.

## What the run deliberately does not do

- Render a MachineConfig or write `mcConfigMap`: the day1 chain's, on the
  branch, until the chart renders them at sync time.
- Push `main`, merge, or sync the Application: a person's, in production.
- The go-links console shortcut and the `*.apps` DNS record: still the
  hostedclusters-setup chart's Crossplane `DisposableRequest`s, until their
  contracts are reported from the air-gap and mocked for dev.
- Verify the DHCP scope after the merge (a bounded read-back with
  `get_dhcp_scope`): a follow-up.
- Undo anything on failure: the allocation is idempotent per (cluster, site,
  type), the branch and the merge request converge on a re-run, day2 folders
  are "already there = success". A merge request closed without merging fails
  the run and leaves the branch and the allocation for a person to decide on;
  release-segment gives the segment back if the cluster is abandoned.

## One git provider: GitLab

GitLab is the only provider the limb knows (the operator, 2026-11-14: one
convention to maintain, and the air-gapped service must not carry a second one).
`DAY1_GIT_PROVIDER`, the GitHub pull-request path and `DAY2_SIG_PATH_TEMPLATE`
(sigs as folders of one repo) were removed. The merge request is opened through
`<host of DAY1_REPO_URL>/api/v4/projects/<the URL's path, URL-encoded>` — the
path with every subgroup, `redbull/gitops-day1/platform-config` — and each sig is
its own project, `redbull/gitops-day2-prod/sigs/<sig>`.

Proven on 2026-11-14 against the GitLab on the dev cluster (CE 19.5.3, the
`redbull` group mocking the air-gap's subgroups), with a GROUP access token
(Maintainer, `api` + `write_repository`), the limb's modules alone and then a
whole run from source:

| checked | result |
| --- | --- |
| clone / `ls-remote` / push with the token in the URL | works (`x-access-token:<token>@`; GitLab takes any username) |
| the project id as one encoded segment through the Route | works |
| merge request: created, found on a re-run, a second create | 201, found, 409 → retried and found |
| `remove_source_branch` | the branch is gone after the merge |
| state: open, closed, merged | read; the merged commit for a merge commit, a squash and a fast-forward (GitLab reports no merge commit there, so the merge request's head is taken) |
| the day1 chain's `mcConfigMap` commit on the branch, then a re-run | `changed=False`; another vlan → `ClusterFileConflictError` |
| day2: the folder on each sig project's protected `main` | pushed with Maintainer; again → `created=False` |
| a wrong token / one without `api` / an unknown project | 401, 403 → `GitProviderAuthError`; 404 → `GitProviderRequestRejectedError` |
| a Developer-role token on a sig's protected branch, or a sig with no project | the push or `ls-remote` is refused → `ValuesRepoGitError`, RETRIED: the run waits at `registering-day2` with GitLab's words on the activity, and goes on once the role or the project is fixed (failing it would strand a merged cluster — a re-run stops at `ClusterAlreadyProvisionedError`) |

Not on that instance, so not proven there: a pipeline on the branch (it has no
runner — the day1 chain's commit was made by hand), push rules, merge-request
approvals, and a GitLab served under a relative URL root (`https://host/gitlab/…`
is not supported: the URL's host is taken as GitLab's root).
