# release-segment — design note

Status: **implemented 2026-10-10** (`workflow_domains/segment_lifecycle/release_segment.py`,
published at `docs/site/segment-lifecycle/release-segment.html`). Everything below the
"Resolution" section is the deferred design as it was written, kept because its two
teardown problems are still the reason for the shape that shipped.

## Resolution — how the open questions were answered

1. **Who deletes the live DHCP scope?** The Argo CD cascade, as before (file deleted →
   Application deleted → resources finalizer → `Request` deleted → provider-http
   `DELETE`). release-segment is the safety net for a scope that survived it: it GETs
   the scope and DELETEs it only if it is still there, then reads back its absence. It
   never POSTs or PUTs, and it only runs once the Application and its `Request` are gone,
   so it is never a second writer racing Crossplane. It carries a copy of the DHCP API
   token for that one DELETE (CLAUDE.md §8).
2. **File deletion vs stub?** Neither, inside this workflow: the trigger IS the file
   deletion. release-segment makes no git change, and problem 2 (an empty file breaking
   the generator) cannot arise.
3. **Cluster or CIDR?** Cluster + type (default HC), symmetric with allocate-segment.
   The CIDR is looked up (`GET /api/segments?status=Allocated&type=…&fresh=true`,
   exact `cluster_name` match client-side). Nothing allocated completes as a no-op.
4. **Ordering?** Scope before pool, enforced by phase order (`removing-dhcp-scope` →
   `releasing-segment`). A DHCP API outage therefore holds the release back.

**Trigger:** a `PostDelete` hook Job in the hostedcluster-setup chart, which Argo CD runs
only after every resource of the Application is gone. Argo CD Notifications were rejected
(`on-deleted` fires when deletion starts, fire-and-forget). The hook is a bridge until a
deprovision-cluster workflow starts release-segment as a child.

---

*The deferred design, as written:*

## Why deferred

release-segment is going to sit inside a larger **delete-cluster** workflow
whose step ownership is not settled — which steps belong to a segment-release
workflow versus the surrounding cluster teardown is exactly the open question.
Building the release half now risks building the wrong half. Nothing in the
allocate path forecloses it:

- the Segments Manager already has `POST /api/segments/release` (keyed by the
  CIDR alone, idempotent for an already-Available segment);
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
                                → status Available ∧ cluster_name None ∧ type None
                                  (release clears the type allocation stamped on)
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
