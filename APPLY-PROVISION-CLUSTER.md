# APPLY-PROVISION-CLUSTER — the provision-cluster workflow in the air-gap

Standalone apply guide for one change across three air-gapped repos. Today a
hosted cluster is born by a person pushing `<cluster>.yaml` to a day1 branch;
the day1 CI chain calls allocate-segment (which pushes the `vlanId` +
`dhcp_values` block back onto the branch), renders the MachineConfigs and
injects them into the file; a person opens and merges the MR. This change makes
the orchestrator the entry point: **provision-cluster** takes the cluster YAML
as its request body, allocates the segment (allocate-segment as a child),
pushes the file with the allocation block to `provision-cluster/<cluster>`,
opens the merge request, waits for a person to merge it, and then registers
the cluster in the day2 sig repos.

**What stays exactly as it is.** The day1 CI chain keeps rendering the
MachineConfigs on the branch (it skips allocation when `vlanId` is present —
it already does); it only stops writing its `# === Added by CI ===` header
(§4.5). The hostedclusters-setup chart, the argocd-platform appsets,
the day2 platform and the sig repos' structure do not change. A person still
merges, and still syncs the Application by hand.

**What changes.**

| repo | change | section |
|---|---|---|
| `gitops-day1/platform-config` (day1 values) | the `allocate_segments` CI job stops calling allocate-segment (two lines); `scripts/allocate_segments.py` and its test go; the chain stops writing the `# === Added by CI ===` header; README | §4 |
| `redbull-platform` (Argo CD) | a new chart `gitops/charts/cluster-lifecycle-worker/` + `gitops/services/cluster-lifecycle-worker/app.yaml`; new image tags for the brain and `segment-lifecycle-worker` | §5 |
| the image registry | three images to mirror: `workflows-orchestrator`, `segment-lifecycle-worker`, `cluster-lifecycle-worker` | §6 |

**Two API changes to know about.** `POST /workflows/segment-lifecycle/allocate-segment`
now takes `{"cluster", "site"}` and answers **422** to `values_branch` (the run no
longer writes day1). `POST /workflows/cluster-lifecycle/provision-cluster?cluster=&mce=`
is new; its body is the cluster YAML (`Content-Type: application/yaml`).

> `<day1>` below is a `gitops-day1/platform-config` checkout, `<platform>` the
> air-gap's `redbull-platform` mirror checkout, `<GITLAB>` your GitLab host,
> `<api>` the `workflows-orchestrator-api` Route. `main` is the day1 branch Argo
> CD renders. Every command runs from the repo root it names.

---

## 1. Before you start — checks to run in the air-gap

Run every check before you change anything. Each one says what to run, what a
good result looks like, and what to do otherwise. **Do not start §3 until all
eight are green or have their fix applied.** Checks 2, 4 and 5 are the ones this
repo cannot answer for you.

### 1.1 Repos

**Check 1 — the day1 CI is the chain this guide edits.**

```bash
cd <day1>
grep -c 'python scripts/allocate_segments.py' .gitlab-ci.yml        # -> 1
grep -c 'git pull --ff-only origin "$CI_COMMIT_BRANCH"' .gitlab-ci.yml   # -> 1
grep -c 'python scripts/filter_frozen.py --mr-comment' .gitlab-ci.yml    # -> 1
ls scripts/allocate_segments.py tests/test_allocate_segments.py     # -> both exist
grep -c 'skip ci' scripts/commit_and_push.sh                        # -> 2 (the chain's own commit is [skip ci])
grep -n 'vlanId' scripts/allocate_segments.py | head -3             # -> the "already carries allocation state" skip
```

| Output | What it means | What to do |
|---|---|---|
| all as shown | the chain this guide was written against | continue |
| the first two are `0` | the allocate job was already rewritten, or this is an older chain | compare §4.1 by hand; apply only what is missing |
| `skip ci` is `0` | the chain's commit would start a second pipeline on the branch | not this guide's problem, but fix it before the first run: the workflow's push MUST start the chain, and the chain's own commit must not |

**Check 2 — the Segments Manager spells sites the way day1 names its folders.**
provision-cluster passes the day1 folder name (`sites/<site>/`) to
allocate-segment as the site, which cross-checks it against `GET /api/sites`.
Nothing verified this before: a human typed both.

```bash
curl -s https://<segments-manager>/api/sites | python3 -c 'import json,sys; print(sorted(json.load(sys.stdin)["sites"]))'
ls <day1>/sites/ | grep -v configValues.yaml
```

| Output | What it means | What to do |
|---|---|---|
| every day1 site folder appears in the Segments Manager's list, spelled the same | good | continue |
| a folder is spelled differently (`site2` vs `59`, `bat-yam` vs `batyam`) | every provision-cluster run at that site would fail `AllocateSegmentFailedError: UnknownSite` | rename one side so they match (the Segments Manager's site is a string field; the folder name is what Argo CD's `<site>-mces` appset sweeps). Do NOT add a mapping to the workflow |

**Check 3 — no allocate-segment run is in flight.** The refactored workflow
replays a run started by the old code differently (it dropped two activities);
runs last minutes and CI was the only caller.

```bash
temporal workflow list --query 'WorkflowType="AllocateSegmentWorkflow" AND ExecutionStatus="Running"'   # -> empty
```

If any is listed, wait for it (or, if it is stuck on a day1 push that can no
longer succeed, terminate it in the Temporal UI with a note) before §3 step (e).

### 1.2 GitLab

**Check 4 — the redbull git token can do what provision-cluster needs.** The
one token (`redbull-git-token` Secret, key `REDBULL_GIT_TOKEN`, deployed in
`redbull-workflows`) must: clone and push `gitops-day1/platform-config`, open a
merge request there through the API (`api` scope), and push every sig project's
default branch.

```bash
TOKEN=$(oc get secret redbull-git-token -n redbull-workflows -o jsonpath='{.data.REDBULL_GIT_TOKEN}' | base64 -d)
# the API: the project is visible and the token has `api`
curl -sk -H "PRIVATE-TOKEN: $TOKEN" https://<GITLAB>/api/v4/projects/platform%2Fgitops-day1%2Fplatform-config | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["path_with_namespace"], d["default_branch"])'
# -> platform/gitops-day1/platform-config main
curl -sk -o /dev/null -w '%{http_code}\n' -H "PRIVATE-TOKEN: $TOKEN" 'https://<GITLAB>/api/v4/projects/platform%2Fgitops-day1%2Fplatform-config/merge_requests?state=opened'
# -> 200
# push rights on day1 and on each sig project (dry-run push of nothing)
git -c http.sslVerify=false ls-remote --heads "https://x-access-token:$TOKEN@<GITLAB>/platform/gitops-day1/platform-config.git" main | cut -f2   # -> refs/heads/main
for sig in $(cat <day1>/sites/configValues.yaml | python3 -c 'import yaml,sys; print(" ".join((yaml.safe_load(sys.stdin).get("day2") or {}).get("copy_to_sigs") or []))'); do
  git -c http.sslVerify=false push --dry-run "https://x-access-token:$TOKEN@<GITLAB>/redbull/gitops-day2-prod/sigs/$sig.git" HEAD:refs/heads/main 2>&1 | tail -1
done
```

| Output | What it means | What to do |
|---|---|---|
| the project line, `200`, `refs/heads/main`, and `Everything up-to-date` (or a would-push line) per sig | the token is enough | continue |
| `401` / `403` on the API, or `{"message":"403 Forbidden"}` | the token lacks `api` (a `write_repository`-only token clones and pushes but cannot open a merge request) | re-issue the project/group access token with `api` + `write_repository`; rotate the Secret (gitops/SECRETS.md) |
| the sig push dry-run says `protected branch` / `You are not allowed to push` | the sig's default branch is protected against this token | allow the token's role to push to the default branch of every sig project (the chart Job this replaces pushed the same way — APPLY-DAY2-COPY §3); there is no second token for the sigs, the group token does both |

Which sigs exist is whatever the merged day1 value layers list under
`day2.copy_to_sigs` — the cluster file, the MCE's and the site's `values.yaml`
can add to `configValues.yaml`'s. Check every project those name.

**Check 5 — the hub pods can reach the GitLab API.** The limb runs in
`redbull-workflows`; git already works from there (segment-lifecycle-worker
cloned day1), the API is the same host over HTTPS.

```bash
oc run -n redbull-workflows gl-check --rm -it --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal -- \
  curl -sk -o /dev/null -w '%{http_code}\n' https://<GITLAB>/api/v4/version
# -> 401 (reachable, token required) — anything else is a network problem to fix first
```

TLS: the limb's HTTP client and its git both run with verification OFF against
the internal CA (CLAUDE.md §5, the Segments Manager precedent); nothing to mount.

### 1.3 The platform mirror and the images

**Check 6 — the redbull-platform mirror has the shape §5 extends.**

```bash
cd <platform>
ls gitops/charts/segment-lifecycle-worker/templates/config.yaml gitops/services/segment-lifecycle-worker/app.yaml   # -> both
ls gitops/charts/cluster-lifecycle-worker 2>&1                                   # -> No such file (not applied yet)
grep -n 'name: redbull-git-token' -r gitops/charts/ | head -3                     # -> the Secret every git limb names
oc get secret redbull-git-token -n redbull-workflows -o name                      # -> secret/redbull-git-token
```

**Check 7 — the three images are built and mirrored at ONE version.** The brain
registers `ProvisionClusterWorkflow` and the refactored `AllocateSegmentWorkflow`;
the segment-lifecycle limb stopped registering two activities; the new limb is
new. All three come from the same `redbull-workflows` commit (the tag CI gave
it on GitHub, `vX.Y.Z` per image). Mirror them:

```bash
for image in workflows-orchestrator segment-lifecycle-worker cluster-lifecycle-worker; do
  skopeo inspect docker://<mirror-registry>/team-redbull/$image:<tag> >/dev/null && echo "$image:<tag> ok"
done
```

**Check 8 — the day2 sig project URL template fits the air-gap.** One GitLab
project per sig, the tree at the project root:

```bash
git -c http.sslVerify=false ls-remote --symref "https://x-access-token:$TOKEN@<GITLAB>/redbull/gitops-day2-prod/sigs/<a sig>.git" HEAD | head -1
# -> ref: refs/heads/<default>	HEAD
```

If the sigs live elsewhere, adjust `config.day2SigRepoUrlTemplate` in §5.2
before you merge; `{sig}` must stay in it (or in `day2SigPathTemplate`).

---

## 2. Why this is safe

- **Nothing on `main` changes without a person.** The workflow pushes a NEW
  branch per cluster and opens a merge request; the merge is a person's, as it
  was. It never pushes `main`, never rewrites an existing cluster file (a file
  already on `main` fails the run at once), never touches another cluster.
- **The day1 chain is unchanged except for one call and one comment line.**
  §4.5 only stops it writing a comment; every key it renders is the same, and
  a legacy header it finds is removed, never stacked. It already skips a file
  that carries `vlanId` ("already carries allocation state — skipping"); the
  workflow's file does. The two lines removed in §4.1 only stop the chain from
  calling an allocate-segment that would now answer 422 — for a file a person
  pushes by hand WITHOUT `vlanId`, which nothing should do any more.
- **A hand-pushed file still works** for everything but the allocation: the
  chain renders its MachineConfigs as before. If someone does push one without
  `vlanId`, `generate_from_template` renders nm-conf with a literal `None` VLAN
  — wrong, but visible in the MR diff and never merged by accident. The README
  says new clusters come from the API.
- **allocate-segment's old runs are closed** (check 3); the new result shape
  drops three fields and decodes every old payload.
- **The chart keeps the keys the old segment-lifecycle image needs.** This
  guide does not remove `DAY1_REPO_URL`, `DAY1_GIT_TOKEN` or
  `DHCP_EXCLUSION_OCTET_RANGES` from `segment-lifecycle-worker`'s chart: the new
  image ignores them (`extra="ignore"`), and the old one still reads them. They
  go in a later change, once the new tag is the deployed one.
- **Rollback is a tag bump.** Put the previous `image.tag` back in
  `workflows-orchestrator` and `segment-lifecycle-worker`, delete the
  `cluster-lifecycle-worker` service folder, and revert §4.1. A provision-cluster
  run in flight is then terminated in the Temporal UI; its branch and merge
  request stay for a person to finish or delete; its segment is released with
  release-segment if the cluster is abandoned.

---

## 3. Order of work and gates

| step | where | gate before the next |
|---|---|---|
| (a) day1 CI: §4 as one MR to `main` | `<day1>` | the MR's pipeline is green (its `run_tests` job runs the suite without the deleted test) |
| (b) the chart and the app entry: §5 as one MR | `<platform>` | `helm lint` + `helm template` + the server dry-run (§5.4) pass; the MR is NOT merged yet |
| (c) the images: §6 — mirror all three, bump the three tags in the same `<platform>` MR | the registry, `<platform>` | check 7 green |
| (d) merge the `<platform>` MR | Argo CD | `workflows-orchestrator` rolls (the brain first — it owns the global ConfigMap), then `segment-lifecycle-worker`, then the new `cluster-lifecycle-worker` app appears and its pod reaches `Running` (§7.1) |
| (e) first run: §7.2 | the API | COMPLETED, the cluster on `main`, its day2 folder present |

Step (a) before (d): once the new brain is up, the OLD chain's allocate job
would fail on the 422 for any hand-pushed branch; with (a) merged first, the
chain simply no longer calls.

---

## 4. Day1 values repo — `gitops-day1/platform-config`

### 4.1 `.gitlab-ci.yml` — the `allocate_segments` job stops calling

The job keeps its name (it is wired into `needs:` of `stage_mc_sources`) and
keeps computing `targets.env` (`get_changed_clusters.py`, `filter_frozen.py`).
Only the call and the pull after it go:

```diff
@@ allocate_segments: script
     - |
       if [ -z "$TARGET_YAML_FILES" ]; then
         echo "No target clusters after frozen filter — nothing to do."
         exit 0
       fi
-    - python scripts/allocate_segments.py
-    # Bring the workflow's pushed vlanId/dhcp_values block into the workspace
-    - git pull --ff-only origin "$CI_COMMIT_BRANCH"
+    # Allocation is provision-cluster's now (redbull-workflows, 2026-11-11): the
+    # cluster file it pushes already carries vlanId + dhcp_values, so this job
+    # only computes the chain's targets. A file pushed by hand without vlanId
+    # gets no allocation here — new clusters come from the API (README).
```

Also in the `allocate_segments` job's comment block (`# Job 0: Allocate
network segments for new clusters (workflows-orchestrator)`), rename to
`# Job 0: Compute the chain's targets (allocation is provision-cluster's)`.
The `WORKFLOWS_ORCHESTRATOR_URL` variable was read only by the deleted script:
remove it from `variables:` too (keep it if anything else in your pipeline uses it).

### 4.2 Files to delete

```bash
cd <day1>
git rm scripts/allocate_segments.py tests/test_allocate_segments.py
grep -rn 'allocate_segments' scripts/ tests/ SCRIPTS.md README.md   # -> only prose, if anything; fix the prose
```

`scripts/SCRIPTS.md`: drop the `allocate_segments.py` row (or mark it "moved to
provision-cluster"). `tests/TESTS.md`: drop its `test_allocate_segments.py`
paragraph.

### 4.3 `README.md` — "Adding a new cluster"

Replace the "push a branch with the new file" instructions with:

```
POST /workflows/cluster-lifecycle/provision-cluster?cluster=<name>&mce=<mce folder>
Content-Type: application/yaml
<the slim cluster file: owner, mastertag, nodepools with their config names —
 no vlanId, no dhcp_values, no mcConfigMap>
```

and say: the run allocates the segment, pushes `provision-cluster/<name>` with
the `vlanId` + `dhcp_values` block, opens the MR; this pipeline then renders
the MachineConfigs onto the branch as before; merge once it is green and the
`CI: Update mcConfigMap` commit is on the branch; sync the Application by hand;
the run then registers the cluster in day2 and completes.

### 4.4 Untouched

`stage_mc_sources`, `render_mc_configs`, `commit_and_push_changes`,
`run_tests`, `validate_all_clusters` (one docstring line in §4.5), every
script they run except the two §4.5 changes, `mc-files/`, and the
`workflow.rules` that keep the chain off `[skip ci]` commits and off `main`. The upgrade-gitops limb's
`DAY1_VALIDATE_COMMAND` (`python3 scripts/validate_all_clusters.py`) keeps
working unchanged.

### 4.5 No more marker comments — the CI header goes

The cluster file used to carry two marker comments: `# === Added By
Segment-Allocation Workflow ===` (allocate-segment's block) and `# === Added by
CI ===` (the chain's `mcConfigMap`). They separated several writers' commits to
one file. provision-cluster writes the whole file in ONE push and adds no
marker, so the chain's header has nothing left to separate (decided
2026-11-11). After this patch:

- `generate_from_template.py` no longer appends the header for a
  manual-allocation (`AutomateAllocation: false`) cluster;
- `inject_configs.py` writes `mcConfigMap:` after one blank line with no
  header, and REMOVES a legacy header it finds directly above `mcConfigMap` or
  alone at the end of the file, so an existing cluster loses its header the
  next time the chain rewrites it (a one-comment diff in that MR) and never
  gets a second one;
- a legacy header above anything else (an old `vlanId` block) is left alone;
- `CI_HEADER` stays in `generate_from_template.py`, marked LEGACY, only for
  that removal.

Tested against the mirror: the suite was 159 passed, 1 skipped before and is
159 passed, 1 skipped after (two header tests replaced by three that pin the
new behaviour), and `git apply --check` is clean. Save the block below as
`no-ci-header.patch` at the repo root and run `git apply no-ci-header.patch`
(on a sanitised-mirror mismatch, apply the hunks by hand: they are small).

```diff
--- a/scripts/generate_from_template.py
+++ b/scripts/generate_from_template.py
@@ -38,6 +38,10 @@
 
 TEMPLATES_DIR = Path('mc-files/templates')
 MC_FILES_DIR = Path('mc-files')
+# LEGACY: the header CI used to write above mcConfigMap (and, for manual-allocation
+# clusters, at the end of the file). Nothing writes it any more — provision-cluster
+# pushes the whole file in one commit, so there is nothing to separate. Kept only so
+# inject_configs can remove the old header from a file it rewrites.
 CI_HEADER = "# === Added by CI ==="
 
 # Hosted-cluster files are named <CLUSTER_FILE_PREFIX><clusterName>.yaml. Must
@@ -148,23 +152,6 @@
             if name and name not in names:
                 names.append(name)
     return names
-
-
-def add_ci_header(yaml_file):
-    """Append the CI header to a YAML file if it is not already present."""
-    with open(yaml_file, 'r', encoding='utf-8') as f:
-        content = f.read()
-
-    if CI_HEADER in content:
-        return
-
-    with open(yaml_file, 'a', encoding='utf-8') as f:
-        # Match the old shell wrapper: always add a blank line before the header
-        if content and not content.endswith('\n'):
-            f.write('\n')
-        f.write('\n' + CI_HEADER + '\n')
-
-    logger.info(f"Added '{CI_HEADER}' to {yaml_file}")
 
 
 def validate_mastertag(mastertag):
@@ -215,7 +202,6 @@
             raise ValueError(
                 f"vlanId missing in {yaml_file} - required when AutomateAllocation=false"
             )
-        add_ci_header(yaml_file)
 
     return cluster_name, vlan_id, openshift_version
 
--- a/scripts/inject_configs.py
+++ b/scripts/inject_configs.py
@@ -14,8 +14,8 @@
 import verify_missing_configs
 # Same-directory import (scripts/ is on sys.path when run as a script).
 # generate_from_template.py owns the shared YAML-error formatter and the
-# CI-header constant (it appends the same header for manual-allocation
-# clusters).
+# LEGACY CI-header constant (removed from files this script rewrites, never
+# written).
 from generate_from_template import (format_yaml_error, CI_HEADER,
                                     resolve_cluster_name, resolve_mc_files)
 
@@ -222,10 +222,9 @@
 def write_yaml_preserving_original_formatting(target_file, original_content, mc_config_map):
     """Write YAML preserving original formatting and only adding/updating mcConfigMap"""
 
-    # Remove any existing mcConfigMap section, together with the CI header
-    # that marks it — a re-run must never stack another header. The header
-    # is only eaten when directly adjacent to the section; a header marking
-    # something else (e.g. a vlanId block) is left alone.
+    # Remove any existing mcConfigMap section, together with a LEGACY CI
+    # header directly above it (nothing writes the header any more). A
+    # header marking something else (e.g. a vlanId block) is left alone.
     # This regex handles mcConfigMap at any indentation level and removes the entire section
     content_without_mc = re.sub(
         rf'^({re.escape(CI_HEADER)}\n)?mcConfigMap:.*?(?=^\w|\Z)',
@@ -233,6 +232,12 @@
 
     # Clean up any trailing whitespace but preserve the structure
     lines = content_without_mc.rstrip().split('\n')
+    # A LEGACY header left alone at the end of the file (the old
+    # add_ci_header for manual-allocation clusters) marks nothing: drop it.
+    while lines and lines[-1] == CI_HEADER:
+        lines.pop()
+        while lines and lines[-1].strip() == '':
+            lines.pop()
 
     # Add consistent spacing between TOP-LEVEL sections only
     formatted_lines = []
@@ -265,20 +270,12 @@
         # file's final newline.
         f.write('\n'.join(formatted_lines) + '\n')
 
-        # Add mcConfigMap section under the CI header
+        # Add the mcConfigMap section after one blank line, with no header
         if mc_config_map:
-            # A header already trailing the content (generate's
-            # add_ci_header appends one for manual-allocation clusters) is
-            # reused, not duplicated.
-            trailing_header = bool(formatted_lines) and formatted_lines[-1] == CI_HEADER
-            if not trailing_header:
-                # Blank line between content and the header — the content
-                # above already ends with a newline, so this adds just the
-                # blank line (no extra newline when content already ends
-                # with a trailing blank line).
-                if formatted_lines and formatted_lines[-1].strip() != '':
-                    f.write('\n')
-                f.write(CI_HEADER + '\n')
+            # The content above already ends with a newline, so this adds
+            # just the blank line.
+            if formatted_lines and formatted_lines[-1].strip() != '':
+                f.write('\n')
             f.write('mcConfigMap:\n')
             for i, item in enumerate(mc_config_map):
                 f.write(f'- name: {item["name"]}\n')
--- a/scripts/validate_all_clusters.py
+++ b/scripts/validate_all_clusters.py
@@ -11,8 +11,7 @@
 
 Non-mutating by construction: each cluster is rendered inside a throwaway
 per-cluster copy of mc-files/ in a temp directory. generate_from_template.py
-appends its '# === Added by CI ===' header to the cluster YAML it processes
-and writes generated files under mc-files/<cluster>/ — both land in the temp
+writes generated files under mc-files/<cluster>/, which land in the temp
 copy. The checkout is never touched; nothing is committed or pushed.
 
 Every cluster in the repo must render — there is no skip list. A deferred
--- a/tests/test_generate_from_template.py
+++ b/tests/test_generate_from_template.py
@@ -227,25 +227,19 @@
         assert not (run_in_repo_tmp / "mc-files/trap-cluster/trapped.bu").exists()
 
 
-class TestCiHeader:
-    """The '# === Added by CI ===' marker added by the old shell wrapper."""
+class TestNoCiHeader:
+    """The '# === Added by CI ===' header is no longer written: provision-cluster
+    pushes the whole file in one commit, so nothing needs separating."""
 
-    def test_ci_header_added_when_automate_false(self, run_in_repo_tmp):
+    def test_no_ci_header_is_added_when_automate_false(self, run_in_repo_tmp):
         yaml_path = run_in_repo_tmp / "sites/site4/mces/ocp4-mce-site4/hostedClusters/test-cluster.yaml"
+        before = yaml_path.read_text()
 
         assert generator.process_yaml_file(str(yaml_path)) is True
-
-        content = yaml_path.read_text()
-        assert "# === Added by CI ===" in content
 
-    def test_ci_header_not_duplicated(self, run_in_repo_tmp):
-        yaml_path = run_in_repo_tmp / "sites/site4/mces/ocp4-mce-site4/hostedClusters/test-cluster.yaml"
-
-        generator.process_yaml_file(str(yaml_path))
-        generator.process_yaml_file(str(yaml_path))
-
         content = yaml_path.read_text()
-        assert content.count("# === Added by CI ===") == 1
+        assert "# === Added by CI ===" not in content
+        assert content == before  # generate never edits the cluster file
 
 
 class TestOutputVerification:
--- a/tests/test_inject_configs.py
+++ b/tests/test_inject_configs.py
@@ -124,14 +124,14 @@
             [{"name": "cfg", "config": "key: value"}])
         lines = target.read_text().split("\n")
         idx = lines.index("mcConfigMap:")
-        assert lines[idx - 1] == "# === Added by CI ==="  # the CI header
-        assert lines[idx - 2] == ""   # the separator blank line
-        assert lines[idx - 3] != ""   # and only one of them
+        assert lines[idx - 1] == ""   # the separator blank line, no header
+        assert lines[idx - 2] != ""   # and only one of them
+        assert "# === Added by CI ===" not in lines
 
-    def test_trailing_ci_header_is_reused_not_duplicated(self, tmp_path):
-        # generate_from_template appends the CI header at EOF for
-        # manual-allocation clusters; inject must write its mcConfigMap
-        # under that header, not stack a second one.
+    def test_a_legacy_trailing_ci_header_is_dropped(self, tmp_path):
+        # The old generate_from_template appended the CI header at EOF for
+        # manual-allocation clusters; nothing writes it now, and inject removes
+        # one it finds there.
         original = ("clusterName: c\n"
                     "mastertag: 4.16.27-x86_64\n"
                     "\n"
@@ -139,9 +139,24 @@
         target = self._write(
             tmp_path, original, [{"name": "cfg", "config": "key: value"}])
         content = target.read_text()
-        assert content.count("# === Added by CI ===") == 1
-        assert "mcConfigMap:" in content
+        assert "# === Added by CI ===" not in content
+        # (top-level keys get their usual blank-line spacing)
+        assert content.startswith("clusterName: c\n\nmastertag: 4.16.27-x86_64\n\nmcConfigMap:\n")
 
+    def test_a_legacy_ci_header_above_mcconfigmap_is_dropped(self, tmp_path):
+        original = ("clusterName: c\n"
+                    "\n"
+                    "# === Added by CI ===\n"
+                    "mcConfigMap:\n"
+                    "- name: old\n"
+                    "  config: |\n"
+                    "    a: b\n")
+        target = self._write(
+            tmp_path, original, [{"name": "cfg", "config": "key: value"}])
+        content = target.read_text()
+        assert "# === Added by CI ===" not in content
+        assert content == "clusterName: c\n\nmcConfigMap:\n- name: cfg\n  config: |\n    key: value\n"
+
     def test_idempotent_over_own_output(self, tmp_path):
         # Re-serializing over the first write's output (parse → re-serialize)
         # must be byte-identical.
```

```bash
cd <day1>
git apply no-ci-header.patch && rm no-ci-header.patch
python -m pytest tests/ -q                                                         # -> all passed (the butane skips as before)
grep -rn 'add_ci_header\|f.write(CI_HEADER' scripts/                               # -> nothing
```

---

## 5. Platform mirror — `redbull-platform`

### 5.1 `gitops/charts/cluster-lifecycle-worker/Chart.yaml` — new

```yaml
apiVersion: v2
name: cluster-lifecycle-worker
description: >-
  The cluster-lifecycle domain's activity worker — provision-cluster's limb: reads day1 main for where a hosted cluster goes, pushes the cluster file (with the segment allocate-segment reserved) to its own branch, opens the merge request on the day1 repo's git provider, reads it back until a person merges, and registers the cluster in the day2 sig repos. Named for the process, not the domain: it polls the domain-scoped cluster-lifecycle-activity queue. One release on the hub — it holds no Kubernetes credential, only the day1 git token (push + the provider's API) and the day2 token. Deploys alongside the shared workflows-orchestrator chart, which runs the workflow brain.
type: application
version: 0.1.0
appVersion: "v0.1.0"
```

### 5.2 `gitops/charts/cluster-lifecycle-worker/values.yaml` — new (air-gap values)

```yaml
# cluster-lifecycle-worker — provision-cluster's hub limb (redbull-workflows: activities/cluster_lifecycle/).
config:
  # The day1 values repo, as the segment-lifecycle, upgrade-gitops and vm-lifecycle charts name it.
  day1RepoUrl: "https://<GITLAB>/platform/gitops-day1/platform-config.git"
  # GitLab: a merge request with remove_source_branch.
  day1GitProvider: "gitlab"
  # The DHCP exclusion policy written into every cluster file's dhcp_values block,
  # per segment type: last-octet ranges NOT distributed. COPY the value from
  # gitops/charts/segment-lifecycle-worker/values.yaml (config.dhcpExclusionOctetRanges).
  dhcpExclusionOctetRanges:
    HC:
      - [1, 10]
      - [241, 254]
  # One GitLab project per sig, the tree at the project root.
  day2SigRepoUrlTemplate: "https://<GITLAB>/redbull/gitops-day2-prod/sigs/{sig}.git"
  day2SigPathTemplate: ""
secrets:
  # The one Secret every git limb reads (gitops/SECRETS.md): the redbull GROUP token, ONE token
  # for day1 and every sig project — the chart has no second Secret. The pod gets it as
  # REDBULL_GIT_TOKEN. For provision-cluster it must also reach the GitLab API
  # (`api` + `write_repository`) and push each sig project's default branch.
  gitTokenSecret:
    name: redbull-git-token
    key: REDBULL_GIT_TOKEN
activityWorker:
  image:
    repository: <mirror-registry>/team-redbull/cluster-lifecycle-worker
    tag: <the mirrored tag, §6>
    pullPolicy: IfNotPresent
  replicas: 2
```

### 5.3 `templates/config.yaml`, `templates/activity-worker.yaml`, `gitops/services/cluster-lifecycle-worker/app.yaml` — new

Copy the three files verbatim from the dev redbull-platform's `main`
(`gitops/charts/cluster-lifecycle-worker/templates/` and
`gitops/services/cluster-lifecycle-worker/app.yaml`). Only `values.yaml` differs
between the two environments (§5.2).
Nothing in them is environment-specific: the ConfigMap renders the five keys
above, the Deployment mounts `workflows-orchestrator-config` +
`cluster-lifecycle-config`, maps the Secret's key onto `REDBULL_GIT_TOKEN` (the one token for day1 and the
sigs), `automountServiceAccountToken:
false`, no Namespace; `app.yaml` is `namespace: redbull-workflows`.

### 5.4 Verify before merging

```bash
cd <platform>/gitops/charts
helm lint cluster-lifecycle-worker
helm template cluster-lifecycle-worker cluster-lifecycle-worker -n redbull-workflows | oc apply --dry-run=server -n redbull-workflows -f -
# -> serviceaccount, configmap and deployment "created (server dry run)"
```

### 5.5 Untouched

`gitops/charts/segment-lifecycle-worker/` keeps every key it has (§2); only
its `activityWorker.image.tag` changes (§6). `workflows-orchestrator` only its
`image.tag`. No change to `hostedclusters-setup`, `argocd-platform`, the day2
platform or any sig repo's structure.

---

## 6. Images

Three images, ONE source commit. On GitHub, CI tagged each image `vX.Y.Z` and
wrote the tag into the dev redbull-platform's `values.yaml`; the air-gap has
no CI, so the tags are set by hand in the same `<platform>` MR as §5:

| image | values file | key |
|---|---|---|
| `workflows-orchestrator` (the brain + the API) | `gitops/charts/workflows-orchestrator/values.yaml` | `image.tag` |
| `segment-lifecycle-worker` | `gitops/charts/segment-lifecycle-worker/values.yaml` | `activityWorker.image.tag` |
| `cluster-lifecycle-worker` | `gitops/charts/cluster-lifecycle-worker/values.yaml` | `activityWorker.image.tag` |

Mirror all three before merging (check 7). The brain MUST be the new one when
the limbs are: the new brain registers `ProvisionClusterWorkflow` and dispatches
the refactored allocate-segment, whose activities the new segment-lifecycle
image registers (the old limb would still register the two removed ones, which
is harmless; the old brain would not know the new workflow at all).

---

## 7. Verify

### 7.1 After the platform MR (step d)

```bash
oc get pods -n redbull-workflows | grep -E 'workflows-orchestrator|segment-lifecycle-worker|cluster-lifecycle-worker'   # -> all Running, new ReplicaSets
oc logs -n redbull-workflows deploy/cluster-lifecycle-worker | grep 'polling queue'   # -> Activity worker polling queue=cluster-lifecycle-activity
oc logs -n redbull-workflows deploy/workflows-orchestrator | grep 'polling queues' | grep -o 'provision-cluster-workflow'   # -> provision-cluster-workflow
curl -sk https://<api>/openapi.json | python3 -c 'import json,sys; d=json.load(sys.stdin); p=d["paths"]["/workflows/cluster-lifecycle/provision-cluster"]["post"]; print(list(p["requestBody"]["content"]))'
# -> ['application/yaml']
# the retired field answers 422, naming provision-cluster
curl -sk -o /dev/null -w '%{http_code}\n' -X POST -H 'content-type: application/json' -d '{"cluster":"x","values_branch":"y"}' https://<api>/workflows/segment-lifecycle/allocate-segment   # -> 422
```

A crash-looping `cluster-lifecycle-worker` names the missing or invalid key in
its log (fail-fast settings): `day1_git_provider`, `day2_sig_repo_url_template`
(`{sig}` missing), `dhcp_exclusion_octet_ranges`.

### 7.2 The first run (step e)

Use a prep MCE and a throwaway name. In Swagger (`https://<api>/docs`), open
`POST /workflows/cluster-lifecycle/provision-cluster`, "Try it out": the body
shows the slim example; set `cluster=prep-e2e-<site>-a` and `mce=<a prep MCE
folder of that site>`, edit `mastertag` to a release the mirror has and
`infraEnvName` to a real InfraEnv, Execute → **202** with
`provision-cluster-ocp4-prep-e2e-<site>-a`. The same from a shell:

```bash
curl -sk -X POST -H 'Content-Type: application/yaml' --data-binary @cluster.yaml \
  'https://<api>/workflows/cluster-lifecycle/provision-cluster?cluster=prep-e2e-<site>-a&mce=<mce folder>'
curl -sk https://<api>/workflows/runs/provision-cluster-ocp4-prep-e2e-<site>-a
```

Then, in order:

| what to look at | expected |
|---|---|
| the status endpoint | `inspecting-target` → `allocating-segment` → `publishing-cluster-file` → `opening-merge-request` → `awaiting-merge` within a minute; `progress.merge_request_url` set |
| the Segments Manager | an Allocated HC segment for `ocp4-prep-e2e-<site>-a` at the site |
| GitLab, branch `provision-cluster/ocp4-prep-e2e-<site>-a` | one commit by `provision-cluster-workflow`: the file = your YAML + one blank line + `vlanId` and `dhcp_values`, NO marker comment, NO `mcConfigMap` |
| the branch's pipeline | `allocate_segments` says "already carries allocation state — skipping"; the chain renders and pushes `CI: Update mcConfigMap from mc-files [skip ci]`, which appends `mcConfigMap:` with NO `# === Added by CI ===` header; green |
| the merge request | open, from the branch to `main`, "delete source branch" ticked, description with the site/vlan/segment and the merge instruction |
| POST again (same cluster) | **409** naming the run id |
| POST `allocate-segment` `{"cluster": "ocp4-prep-e2e-<site>-a", "site": "<site>"}` by hand while the run waits | **409** (same id as the child); after the child finished, a new run that COMPLETES with the same segment |
| merge the MR (after the pipeline, with the CI commit on the branch) | within 5 minutes the run leaves `awaiting-merge`; `registering-day2` if the layers list sigs; **COMPLETED** with `merged_sha` and `day2[]` |
| each sig project | `sites/<site>/<env>/mces/<mce>/ocp4-prep-e2e-<site>-a/.gitkeep` on the default branch, committed by `provision-cluster-workflow` |
| Argo CD | the cluster's Application appears (sync by hand); the Crossplane `Request` renders the DHCP scope from the merged block |
| POST again after completion | a run that fails `ClusterAlreadyProvisionedError` at once |

Clean up afterwards: delete the test cluster's file from `main` (its
Application and scope go with it — the PostDelete hook runs release-segment),
delete the sig folders, and let the branch deletion the MR did stand.

### 7.3 What a wait looks like

A run in `awaiting-merge` polls the merge request every 5 minutes and
continues as new every 48 polls (4 hours): in the Temporal UI the workflow id
stays, the run id changes, and the new run's input carries the state (the
target, the allocation, the published file, the merge request). That is normal.
Closing the MR without merging fails the run with `MergeRequestClosedError`;
the branch and the allocation stay for a person to decide on.

---

## 8. Rollback

1. `<platform>`: previous `image.tag` for `workflows-orchestrator` and
   `segment-lifecycle-worker`; delete `gitops/services/cluster-lifecycle-worker/`
   (the chart folder may stay). Merge; Argo CD rolls back and removes the limb.
2. `<day1>`: revert §4 (restore the two lines and `allocate_segments.py`; §4.5
   can stay — no header is harmless — or be reverted with `git apply -R`).
3. Any `provision-cluster-*` run in flight: terminate in the Temporal UI, then
   finish its merge request by hand or close it; release its segment with
   `POST /workflows/segment-lifecycle/release-segment {"cluster": …}` if the
   cluster is abandoned.

---

## 9. Docs

In redbull-workflows: the docs site's `cluster-lifecycle/provision-cluster.html`
and the rewritten `segment-lifecycle/allocate-segment.html` (shipped in the
`workflows-docs` image — mirror it with the other three if the site is
deployed in the air-gap), `docs/design/provision-cluster.md` (the branch, the
chain and day2), README (workflows, API and configuration tables), CLAUDE.md §4
(the decisions, dated 2026-11-11). The deferred chart-side MachineConfig
rendering has its own plan and is not part of this change.

---

## 10. Final gate

- [ ] checks 1–8 green
- [ ] (a) the day1 MR merged (§4.1–4.5), its pipeline green
- [ ] (b)+(c) the platform MR: chart + app entry + three tags, lint/template/dry-run green, images mirrored
- [ ] (d) merged; the three pods Running on the new tags; the API lists `application/yaml` for provision-cluster; `values_branch` answers 422
- [ ] (e) the first run COMPLETED end to end (§7.2), the test cluster cleaned up
- [ ] the previous tags written down next to this file for §8
