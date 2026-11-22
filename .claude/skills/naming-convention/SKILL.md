---
name: naming-convention
description: The platform naming convention for clusters (tenant, MCE, seed, KV), InfraEnvs, NodePools, nodes and servers (bare metal and VM), zones and the redbull.io/* and topology labels. Use whenever a change BUILDS, PARSES, VALIDATES, MATCHES or DOCUMENTS such a name or label — any workflow, activity, router, model, test fixture, chart value or docs page that names a cluster, an InfraEnv, a NodePool, a server/VM, a day1 folder or file, or a label. Fetches the convention's single source of truth, kubevirt-workflows' docs/naming-convention.md, before anything is named.
paths:
  - "shared/cluster_names.py"
  - "shared/infraenv_names.py"
  - "shared/bmc_address.py"
  - "shared/upgrade_layout.py"
  - "activities/day1_repo.py"
  - "activities/vm_lifecycle/**"
  - "activities/server_provisioning/**"
  - "workflow_domains/server_provisioning/**"
  - "workflow_domains/vm_lifecycle/**"
  - "workflow_domains/server_lifecycle/**"
  - "workflow_domains/cluster_lifecycle/**"
---

# naming-convention — name things the way the platform does

The platform's naming convention is NOT written down in this repo. Its single source of
truth is:

  **https://github.com/Idan-Maimon/kubevirt-workflows/blob/main/docs/naming-convention.md**

It defines the names of clusters (tenant, MCE, seed, KV), nodes (bare metal and VM),
InfraEnvs, the zone and where it comes from, the labels on InfraEnvs/Agents, NodePools,
Nodes and VirtualMachines, and the claimed-Agent mark. It changes; this skill does not copy
it, so it can never go stale against it.

## Step 1 — read the convention, fresh, every time

Never name anything from memory, from CLAUDE.md, or from what the code does today — those are
exactly what may be behind the convention. Fetch it (the repo is public):

```bash
gh api repos/Idan-Maimon/kubevirt-workflows/contents/docs/naming-convention.md \
  -H "Accept: application/vnd.github.raw"
# which revision you read, and when it last changed:
gh api "repos/Idan-Maimon/kubevirt-workflows/commits?path=docs/naming-convention.md&per_page=3" \
  --jq '.[] | .sha[0:7] + " " + .commit.committer.date + " " + (.commit.message | split("\n")[0])'
```

Without `gh`, fetch
`https://raw.githubusercontent.com/Idan-Maimon/kubevirt-workflows/main/docs/naming-convention.md`.
If it cannot be read at all, STOP and say so: do not fall back to a remembered version.

Read the WHOLE file — the names (§1), zones (§2), how a server reaches its zone (§3), the labels
on each layer (§4) and the claimed-Agent mark (§5) depend on each other — and its open items,
which say what is not settled yet.

## Step 2 — the convention wins; a disagreement is reported, never silently kept

- Where this repo's code, tests, docs or CLAUDE.md spell a name or a label differently from the
  convention, the convention is right. Name the mismatch to the user (file:line, what the code
  does, which section of the convention says otherwise) before or with the change.
- A decision recorded in CLAUDE.md that the convention now contradicts (it was written against
  an earlier draft) is NOT a reason to keep the old spelling — but it is a reason to say so
  explicitly, because the operator made it on purpose.
- Something the convention leaves OPEN (its open-items table) or does not cover is the
  operator's to decide: ask, do not invent.
- Keep the operator's vocabulary: "day1" is the repo; a seed is a "seed", never an "mce"; a
  KubeVirt cluster is a "KV cluster"; in day1 the two kinds are `hc` and `kv`.

## Step 3 — apply it the way this repo applies names

- **A name is parsed ONCE, at the API edge**, and what it says (flavor, size, site, zone, id…) is
  carried in the run's input, never re-derived on replay (CLAUDE.md §4, the provision-dell-server
  region precedent). Changing a parser therefore never changes a recorded run — but changing
  what a WORKFLOW computes from carried values is a replay question (§5: `workflow.patched`,
  goldens).
- **One place per rule.** The convention's grammar lives in `shared/` (pure, stdlib only, read
  by both sides): `shared/cluster_names.py` (cluster, MCE, seed, KV names; zones),
  `shared/infraenv_names.py` (InfraEnv and VM names, the vCPU→memory ratio). Do not write a
  second regex for a name anywhere else — import the parser. Label keys used on both sides go
  in `shared/` too (`ZONE_LABEL` in `shared/upgrade_layout.py`); a limb's own label keys live in
  its technology module (`activities/vm_lifecycle/vm_spec.py`).
- **Where names are used today** (check each that your change touches):

  | name / label | built or checked in |
  | --- | --- |
  | MCE name, its site | `shared/cluster_names.py` (`canonical_mce_cluster`, `site_of_mce_cluster`), every server-lifecycle and vm-lifecycle route |
  | seed / KV cluster, zone | `shared/cluster_names.py`, `activities/day1_repo.py` (which day1 folder is a seed, which file a KV cluster, the zone check), `activities/cluster_lifecycle/` (`list_kv_clusters`, `list_day1_clusters`), the vm-lifecycle worker's `KV_CLUSTER` setting |
  | InfraEnv | `shared/infraenv_names.py`, `workflow_domains/vm_lifecycle/router.py`, add-server-to-mce's pool draw (`server_pool_prefix`) |
  | VM name and labels | `shared/infraenv_names.py` (`vm_name`), `activities/vm_lifecycle/vm_spec.py` |
  | bare-metal server name | add-server-to-mce's pool draw (`server_pool_prefix`) and name checks; provision-dell-server's check of the name OME carries (`workflow_domains/server_provisioning/server_name.py`, on the `feat/provision-dell-server` branch, not `main`) |
  | hosted cluster / day1 file stem | `shared/cluster_names.py` (`hosted_cluster_stem`), provision-cluster |
  | NodePool name, selector, nodeLabels | `activities/day1_repo.py` and the upgrade-gitops day1 modules (rendered as the hostedclusters-setup chart does), update-nodepool |
  | Agent labels (`redbull.io/claimed`…) | uninstall-server-from-hc, update-nodepool's scale-down |

  Grep for the old spelling as well as the parser: fixtures, goldens' INPUTS, docs pages
  (`docs/site/`), chart values, README tables and CLAUDE.md all repeat example names.
- **A name change touches the docs in the same change** (`/redbull-workflows` step 3): the
  workflow's docs page, the examples in Swagger (router `Field(examples=…)`), CLAUDE.md's
  bullet for the workflow, and the lab README if it lists names.
- **Lengths are part of the name.** Check what the convention bounds (a bare-metal name for
  Cisco UCS, a DNS-1123 label for anything Kubernetes names, a derived name such as a VM's
  `<name>-root` DataVolume) where the name is built, with a named error, not where it fails.
