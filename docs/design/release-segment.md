# release-segment — deferred design

Status: **deferred, not scheduled.** Written down so picking it up later is a
read, not a re-derivation. It sits under `docs/` (non-code material) but
outside `docs/site/` on purpose: it is a design note, not published
documentation — the catalogue lists release-segment as a planned row
(`href: null` in `docs/site/assets/nav.js`) until it exists.

## Why deferred

release-segment is going to sit inside a larger **delete-cluster** workflow
whose step ownership is not settled — which steps belong to a segment-release
workflow versus the surrounding cluster teardown is exactly the open question.
Building the release half now risks building the wrong half. Nothing in the
allocate path forecloses it:

- the Segments Manager already has `POST /api/segments/release` (keyed by the
  CIDR alone, idempotent for an already-Available segment, 409 for Locked);
- the block allocate-segment appends to a cluster's values file is designed to
  be strippable — it starts at a fixed marker line and runs to end of file,
  and `values_repo.split_marker_block` /
  `values_repo.render_allocation_block` are the shared parse/render pair a
  `remove_allocation_from_cluster_values` reuses unchanged.

## The flow, when it is built

```
run(ReleaseSegmentRunArgs{input: {cluster, type=HC}})

  1. locate the allocation      GET /api/segments?…  (or by-segment, once the
                                caller's contract is settled: cluster vs CIDR)
  2. release it                 POST /api/segments/release {segment}
  3. verify by read-back        GET /api/segments/by-segment
                                → status Available ∧ cluster_name None
  4. clean the values file      strip the marker block (split_marker_block),
                                commit, push — same clone/push plumbing as
                                append_allocation_to_cluster_values
```

Verification sits before the git write, mirroring allocate-segment's
verify-after-mutate rule. Steps 1–3 are straightforward. Step 4 and what
happens *after* it are where the real problems live — the two below are the
genuinely hard parts, captured here so they are not rediscovered the hard way.

## Teardown problem 1 — pruning cannot be narrowed to the DHCP Request in prod

Removing the `dhcp_values` block from the cluster file makes the
`helm-charts-hostedclusters-setup` chart stop rendering the Crossplane
`Request` (its template gates on `and .Values.dhcp_values (hasKey … "network")`).
But the Argo Application for the hostedClusters tier runs with **`prune:
false`**, so the already-created `Request` — and therefore the live DHCP scope
— stays behind, orphaned but running.

Why not just flip pruning on?

- In **this test environment** the leaf tier renders exactly one resource
  (`helm-charts-hostedclusters-setup` has a single template,
  `dhcp-scope-request.yaml`), so `prune: true` there would have touched only
  the Crossplane `Request`. Tempting.
- In the **air-gapped production environment** that same tier carries every
  resource that deploys a cluster. `prune: true` there means a values-file
  mistake can delete cluster workloads — dangerous, and not an option.
- Per-resource opt-in does not exist: Argo's
  `argocd.argoproj.io/sync-options: Prune=false` only opts a resource **out**
  of pruning; there is no `Prune=true` to opt a single resource in.

Conclusion: with `prune: false`, deleting the block leaves the live scope in
place. **Tearing the scope down needs a deliberate mechanism, not a side
effect** — candidates when this is picked up: the workflow deleting the scope
via the DHCP API (`DELETE /api/v1/scopes/{scope}` — but that makes the
workflow a second writer, the exact thing allocate-segment avoids), deleting
the Crossplane `Request` CR directly (needs RBAC into the cluster), or a
narrowly-scoped prune-enabled Application owning only DHCP Requests.

## Teardown problem 2 — a cluster values file must never be left empty

From `hcAppset.yaml`'s own comment: *"A `files` generator has nothing to
template from an empty file… a placeholder left empty fails the generator
rather than being skipped."* A release that restores the file byte-for-byte to
0 bytes (plausible when the file was empty before allocate-segment wrote the
block, since the marker is then the first line) **breaks Application
generation for that whole MCE**.

So release must either:

- **delete the file** — which is also how a cluster is decommissioned, but a
  `git mv`/delete tears down the Argo Application named after the file, and
  with it everything that Application manages; or
- **leave a valid non-empty stub** (e.g. `description: "released"`), keeping
  the Application alive with no DHCP scope.

Which one is right depends on whether release-segment runs as part of full
cluster deletion or as a standalone "give the VLAN back" operation — a
decision that belongs to whoever owns the delete-cluster flow.

## What Part 1 already settled

The shared-segment guard this design once needed is gone: the Segments
Manager no longer supports one segment shared by many clusters
(`cluster_name` holds exactly one name; the comma-list form and its regex
matching were removed, existing data migrated by
`segments-manager/scripts/migrate_drop_shared_clusters.py`). Release therefore
never has to answer "which cluster am I releasing this segment *from*?" — the
segment's one `cluster_name` is the whole answer, and
`POST /api/segments/release` frees exactly one cluster's segment.

## Open questions for the delete-cluster owner

1. Who deletes the live DHCP scope, and by which mechanism (problem 1)?
2. File deletion vs stub on release (problem 2) — tied to whether the cluster
   itself is being deleted.
3. The trigger contract: does release-segment take a cluster (symmetric with
   allocate-segment) or a CIDR (symmetric with the Segments Manager API)?
4. Ordering inside delete-cluster: the scope must stop serving leases before
   the segment returns to the pool, or a re-allocated segment could collide
   with still-live leases.
