# Charts, config keys and secrets

Two homes for charts, decided by where the limb runs:

| limb | chart lives | deployed by |
| --- | --- | --- |
| the brain (`workflows-orchestrator`) and HUB limbs (`segment-lifecycle-worker`, `server-provisioning-worker`, `upgrade-gitops-worker`, ...) | redbull-platform `gitops/charts/<service>/` | Argo CD: one ApplicationSet over `gitops/services/<service>/app.yaml`; pushing redbull-platform `main` DEPLOYS |
| PER-MCE limbs (`server-lifecycle-worker`, `cluster-upgrade-worker`, `upgrade-agent-worker`) | this repo, `deploy/charts/<limb>-worker/` | copied into redbull-platform as `gitops/charts/<limb>-worker/` (where CI bumps the tag) by CI's `sync-upgrade-charts` on each `main` push that changes them — a new per-MCE limb joins `scripts/sync_platform_charts.sh`, the job's render step and the `charts` filter; `server-lifecycle-worker`'s redbull-platform chart is its own, not synced and deployed per MCE from there (example ApplicationSet in `deploy/argocd/`); `deploy/` triggers no image build |

A chart in redbull-platform with no `gitops/services/` entry deploys nothing; CI still bumps
its tag. `server-provisioning-worker` and cluster-upgrade's three are like that today.

redbull-platform is assumed checked out at `../redbull-platform`; verify the path before
editing, and if it is absent hand the chart change over as a diff in the reply. Its
CLAUDE.md sections "workflows charts: no chart ever creates its own Namespace",
"Charts live in this repo" and "Verifying a chart before pushing" are required
reading. The archived `helm-charts-<service>` repos are dead: read nothing, push nothing.

## What every limb chart has

- ServiceAccount + Deployment named `<limb>-worker`; a `checksum/config` annotation so a
  key change rolls the pods; `envFrom` the global `workflows-orchestrator-config` (hub
  limbs) or its own Temporal endpoint values (per-MCE limbs, where no brain ConfigMap
  exists), the `<limb>-config` ConfigMap and each Secret.
- `values.yaml` holding every operator knob as structured YAML (`config.<camelKey>`); the
  ConfigMap template only renders it (`| toJson | quote` for structured values).
- Secrets behind `secrets.existing<X>Secret` (the ESO hook), never a `lookup`-generated
  value (Argo re-mints it every sync), never a real secret committed unless CLAUDE.md
  §8 says that copy is rendered from git on purpose (`DHCP_API_TOKEN`).
- No `templates/namespace.yaml`, no `createNamespace` value.
- Per-MCE limbs: `mceCluster` (names the queue), `temporal.host/namespace/tls.*`, and RBAC
  derived call by call from `activities.py`, listed in the chart README's table. Never
  a verb the activities do not use.

## Adding or removing a config key

1. `values.yaml` + the ConfigMap template (or the Secret).
2. Ship the chart BEFORE (or with) the image whose settings require the key, or the pod
   crash-loops on fail-fast settings. Removing is the reverse: the chart drops the key
   only after the image that stopped needing it is the deployed tag.
3. `shared/settings.py`, `.env.example`, `tests/conftest.py`, README's configuration
   table, the chart README's configuration table.

## Verify before pushing

```bash
helm lint <chart-dir>
helm template <limb>-worker <chart-dir> -n <namespace> | oc apply --dry-run=server -f -
```

`helm template` renders a missing value as an empty string without a warning; only the
server dry-run runs the admission webhooks the sync will.

## Image tags and order

- Brain release first (it owns the global ConfigMap), then Secrets, then limb charts.
- A new chart references a tag that exists: merge it after CI pushed the first image.
- Deploy and document images by VERSION TAG, never by commit sha (the operator,
  2026-11-02). Every limb's job uses the reusable workflow: a `main` build publishes
  `vX.Y.Z`, bumps `values.yaml` on redbull-platform `main` (that commit deploys, when
  the chart has a `gitops/services/` entry) and tags `<chart>/vX.Y.Z` there. A branch
  build is `<branch-slug>-<short sha>`, written nowhere; set it by hand only to try a branch.
- So a new limb's chart goes into redbull-platform BEFORE its build job: the bump fails
  on a missing `values.yaml`. Image-only jobs were the stopgap for that and are gone.
