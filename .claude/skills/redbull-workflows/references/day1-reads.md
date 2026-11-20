# A workflow needs something from day1

day1 (`gitops-day1/platform-config`, `main` only) is where every OpenShift cluster is
described, so many workflows ask it questions. They all ask through ONE library,
`activities/day1_repo.py`'s `Day1`: a read-only view of day1 at one commit. No module
walks day1's folders or parses a day1 path itself (checker AC009).

## The procedure

1. **Find the `Day1` query that answers it.**

   | question | query |
   | --- | --- |
   | the hosted clusters of a site / an environment | `day1.hosted_clusters(site=, env=, include_kv=False)` |
   | a zone's KV clusters, oldest first | `day1.kv_clusters(site=, zone=)` |
   | any combination of the filters | `day1.clusters(site=, env=, kind="all"\|"hc"\|"kv", zone=)` |
   | one cluster by name, or whether it exists | `day1.cluster(name)` (None when absent), `day1.cluster_paths(name)` |
   | a cluster file's merged values | `day1.values(record.path)` |
   | which site holds an MCE (or seed) folder | `day1.mce_folders(name)` |
   | a file's blob id for a later compare-and-set | `await day1.blob(path)` |

   Each cluster query returns `ClusterRecord`s: `name` (the file stem), `site`, `folder`,
   `kind`, `env`, `zone` and `id` (kv only), `cluster_name`, `hosted_cluster` and its
   namespace, `path`, `problems`, `revision`. The definitions (what `kv` means, where a zone
   and an env come from) are in the module's docstring and nowhere else.

2. **None does: add a `Day1` method.** Pure over the directory (`self.root`), folders walked
   with `iterdir()` and compared by name, caller input validated as a token and never
   joined into a path or a glob. It returns facts — records, paths, None for "not there" —
   and raises nothing from `shared/exceptions.py`: what a missing or duplicated name means
   is the calling limb's to classify. Test it on the fixture tree in
   `tests/test_day1_repo.py` (no git needed: `Day1(tmp_path, revision="…")`). If a workflow
   must reach it across limbs, add a FILTER to `Day1ClusterQuery`
   (`shared/models/cluster_lifecycle.py`, optional, defaulting to "no filter") — never a
   new activity for a new question.

3. **Call it from the right place.**
   - The workflow's limb holds NO day1 token: call the `list_day1_clusters` activity
     (`shared/interfaces/cluster_lifecycle.py`) with
     `task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE`. A filter it refuses is a non-retryable
     `Day1QueryInvalidError` already; `ValuesRepoGitError` is retried;
     `ValuesBranchNotFoundError` (day1 without `main`) is the workflow's to classify.
   - The limb holds the token and reads inside its own step (upgrade-gitops before an
     edit, provision-cluster's inspect): `async with day1_repo.main_snapshot(repo_url=...,
     token=...) as day1:` in its technology module, no cross-limb call.

4. **Never parse a day1 path or its YAML in a domain module** (AC009). A WRITER that must
   name the file it creates builds the path with `day1_repo.mce_dir(site, folder)` and
   `day1_repo.cluster_file_path(folder_dir, cluster)`, and checks a path it was handed
   with `day1_repo.parse_mce_dir`.

5. **Carry `revision` in the result**, so the run's history says which commit it decided
   on. `Day1ClusterList.revision`, every `ClusterRecord.revision` and `Day1.revision` are
   that commit.

## Reads and writes are opened differently

A read opens `main_snapshot()` (any other repo: `git_repo.snapshot(...)`): the remote is
asked for `main`'s head every time and cloned only when it moved, so thirty runs arriving
together cause one clone, and a run on an unchanged `main` none. The tree is SHARED by
every reader of that commit: write nothing under it and run nothing in it. One of day1's
own scripts runs in `async with day1.scratch(prefix=...) as scratch:` — a throwaway copy —
through `scratch.run_command`.

A write keeps `fresh_main()` / `git_repo.fresh_clone(...)`, one per attempt, and its block
reaches `commit_and_push` (AC008). A read that decides a later write in ANOTHER activity
is fine on a snapshot: that write re-checks in its own fresh clone (the blob
compare-and-set).

## Tests

- the `Day1` method on the fixture tree (`tests/test_day1_repo.py`): the match, the empty
  answer, a record with `problems`, a path-shaping input refused;
- the limb's reader against a local bare repo (`tests/test_day1_writer.py`,
  `tests/test_upgrade_gitops_day1.py` are the shape), every snapshot under the test's own
  `cache_root` when the test counts clones;
- a new `Day1ClusterQuery` filter through the real activity
  (`tests/test_cluster_lifecycle_limb.py`).
