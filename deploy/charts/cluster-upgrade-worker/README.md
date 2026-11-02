# cluster-upgrade-worker

The cluster-upgrade domain's per-MCE activity worker. **One release per MCE
cluster, installed inside that MCE.** It dials out to the hub's Temporal, polls
`cluster-upgrade-activity-<mceCluster>`, and works on its own MCE as its own
ServiceAccount — the workflow identity of DESIGN §2. No kubeconfig of any
cluster is stored anywhere, and the chart holds no credential at all.

What it does there, all from `activities/cluster_upgrade/activities.py`:

- reads the MCE's HostedClusters, NodePools, NodePool MachineSets, ImageDigestMirrorSets
  and ConfigMap `hypershift/supported-versions` (planning, pre-flight, the watches);
- **syncs** the hosted cluster's Argo CD Application to the run's day1 commit, by
  patching its `operation` — the one routine write on the MCE;
- reads each hosted cluster through that HostedCluster's admin kubeconfig, and writes
  there only an admin ack (D16) and the tier-2 actions a person approved;
- deletes a control-plane pod when a person approves a `delete_hcp_pod`.

It **never writes a HostedCluster, NodePool or IDMS** (DESIGN D1): an upgrade changes
them only through a day1 commit that Argo CD applies.

The domain's other two limbs are separate deployments with other credentials:
`upgrade-gitops-worker` on the hub (day1 git, the mirror, Brain) and
`upgrade-agent-worker` per MCE (no Kubernetes credential).

## Install

The brain must run first (it owns the workflow queues); this worker only polls its
activity queue. On the sandbox the MCE is the hub itself (local-cluster hosts the
hosted clusters), so Temporal is the in-cluster Service, plain gRPC:

```bash
helm upgrade --install cluster-upgrade-worker <chart dir> \
  -n redbull-workflows \
  --set mceCluster=ocp4-mce-workflow-test \
  --set temporal.host=temporal-frontend.temporal.svc.cluster.local:7233 \
  --set image.tag=<vX.Y.Z> \
  --set replicaCount=1
```

`<chart dir>`: this chart, in the workflows repo at `deploy/charts/cluster-upgrade-worker/` or
its copy in redbull-platform at `gitops/charts/cluster-upgrade-worker/`.

`<vX.Y.Z>`: a version tag. Every push to `main` that touches `activities/` or `shared/`
publishes `ghcr.io/team-redbull/cluster-upgrade-worker:vX.Y.Z` (`build-cluster-upgrade-worker`,
the shared build workflow), writes that tag into redbull-platform's copy
(`gitops/charts/cluster-upgrade-worker/values.yaml`, `image.tag`) and tags redbull-platform
`cluster-upgrade-worker/vX.Y.Z`. Installed from redbull-platform's copy, leave `image.tag`
unset to get the newest. A branch build is tagged `<branch-slug>-<short sha>` and written
nowhere; deploy those only to try a branch.

On a remote MCE, Temporal is reached through the hub's TLS Route: set
`temporal.host` to `<route-host>:443`, `temporal.tls.enabled: true` and the CA, exactly
as the workflows repo's `deploy/charts/server-lifecycle-worker/README.md` describes (steps
1–6 there).

`mceCluster` must be the MCE's folder in day1, `sites/<site>/mces/<mceCluster>/`:
an upgrade run takes its MCE from where the cluster's file is (D18) and dispatches to
`cluster-upgrade-activity-<that name>`. With or without the `ocp4-` prefix; the
worker always uses it with.

Check the worker polls its queue:

```bash
temporal task-queue describe --task-queue cluster-upgrade-activity-<mceCluster> --task-queue-type activity
```

The chart is copied into redbull-platform as `gitops/charts/cluster-upgrade-worker/`
(2026-11-02), where CI bumps its tag; it has no `gitops/services/` entry there yet, so Argo CD
deploys nothing from it. Change the chart here and copy it there in the same change. To deploy
it with Argo CD, add `gitops/services/cluster-upgrade-worker-<mce>/app.yaml` per MCE, as
`gitops/services/server-lifecycle-worker/app.yaml` there describes. For many MCEs, the workflows
repo's `deploy/argocd/server-lifecycle-worker-applicationset.yaml` is the shape, with this
chart's name.

## Configuration

| Value | Env | Default | Meaning |
|---|---|---|---|
| `mceCluster` | `MCE_CLUSTER` | — (required) | The MCE this worker serves; names its queue |
| `temporal.host` | `TEMPORAL_HOST` | — (required) | Temporal as reachable from this MCE |
| `temporal.namespace` | `TEMPORAL_NAMESPACE` | `default` | The Temporal namespace; must match the hub |
| `temporal.tls.*` | `TEMPORAL_TLS_CA`, `TEMPORAL_TLS_SERVER_NAME` | off | TLS to Temporal (a Route) |
| `argo.namespace` | `ARGO_NAMESPACE` | `openshift-gitops` | Where the hosted clusters' Applications are; the Argo Role is rendered there |
| `argo.valuesRef` | `ARGO_VALUES_REF` | `values` | The `ref` of a multi-source Application's day1 values source |
| `rbac.create` | | `true` | Render the RBAC below |
| `replicaCount` | | `2` | Pods polling the queue (load sharing and failover) |
| `image.repository` / `image.tag` | | ghcr / appVersion | Mirror path and tag in a disconnected environment |

The worker fails to start on a missing `MCE_CLUSTER` or `TEMPORAL_HOST`, a namespace
that is not a Kubernetes name, or a ref Argo CD would refuse.

Every key above is in the ConfigMap `cluster-upgrade-config` (`<domain>-config`, CLAUDE.md
§8). Before this chart named it `cluster-upgrade-worker-config`; an upgrade of the release
creates the new one, rolls the pods onto it and deletes the old one, with nothing to do by
hand unless something outside the chart mounted the old name.

The pod runs as a non-root UID (`USER 1001` in the image; any UID OpenShift assigns works)
with a read-only root filesystem, no privilege escalation and no capabilities. The one path
it writes is the `/tmp` emptyDir: each hosted cluster's admin CA, certificate and key, in a
temporary directory deleted when the call ends.

### Multi-source Applications

The production hcAppset renders each hosted cluster's Application with two sources: the
hostedclusters-setup chart, and the day1 repo as `ref: values` whose files the chart's
`valueFiles` read as `$values/...`. The worker syncs the day1 commit **in the values
source** and keeps the chart source at the revision Argo CD last compared it at. The
patch carries `sync.sources` and one `sync.revisions` entry per source, as
`argocd app sync --revisions <sha> --source-positions <n>` does: without `sources`, Argo
CD 3.1 reads `$values` at day1's HEAD, not at the commit (`GetRefSources`,
`util/argo/argo.go`). An Application with several sources and none with
`ref: <argo.valuesRef>` is refused (`ArgoSyncRefusedError`), never guessed. Single-source
Applications sync with `sync.revision`.

## RBAC

Every grant comes from a call in `activities/cluster_upgrade/activities.py`; nothing else
is granted. Cluster-wide where HyperShift puts the object in a per-cluster namespace no
request names ahead of time: the HostedCluster's own (`hcp-<clusterName>` from the
hostedclusters-setup chart) and its control-plane namespace (`<that namespace>-<name>`).

| Where | Group / resource | Verbs | The call |
|---|---|---|---|
| cluster-wide | `hypershift.openshift.io` hostedclusters | get, list | `_hosted_cluster`: get by name in the namespace day1 names; list by name when a run planned before it knew the namespace |
| cluster-wide | `hypershift.openshift.io` nodepools | get, list | `read_hosted_cluster`, `read_hosted_health`, `collect_evidence` list a HostedCluster's; `watch_nodepool` gets one |
| cluster-wide | `cluster.x-k8s.io` machinesets | get | `watch_nodepool`: the NodePool's MachineSet's target/current config annotations |
| cluster-wide | `config.openshift.io` imagedigestmirrorsets | list | `read_mce_state`: the MCE's mirrors (pre-flight check 11) |
| cluster-wide | `""` secrets | get | the HostedCluster's admin kubeconfig (`status.kubeconfig`). **The widest grant: every Secret of the MCE**, as server-lifecycle-worker's `rbac.hostedClusters` |
| cluster-wide | `""` pods | delete | `execute_proposed_action`'s `delete_hcp_pod`, a tier-2 action a person approved, in the cluster's own control-plane namespace; the catalog refuses `etcd-*` and `kube-apiserver-*` |
| `argo.namespace` | `argoproj.io` applications | get, list, patch | `read_argo_application`, `collect_evidence`; `sync_argo_application` patches `operation`; `list_day2_applications` lists by the `day2.gitops/cluster` label |
| `hypershift` | `""` configmaps, `resourceNames: [supported-versions]` | get | `read_mce_state` (D10: git is intent, the cluster is truth) |

Not granted, on purpose: any write to HostedClusters, NodePools or IDMS (D1), Agents,
`pods/exec`, `impersonate`, `escalate`, `bind`. Narrowing the Application patch to the
`operation` field and the pod delete to control-plane namespaces is the
ValidatingAdmissionPolicies' job (DESIGN §9, deliverable 7), not this chart's.

## Network

The worker reaches a hosted cluster's API at its control plane's in-cluster Service,
`https://kube-apiserver.<control-plane namespace>.svc:6443` (the kubeconfig's external
endpoint may not be reachable from the pod network; TLS is still verified against the
kubeconfig's CA). Its pods therefore need, **from the worker's namespace**:

- egress to the `kube-apiserver` pods of every control-plane namespace on **TCP 6443**,
  plus DNS;
- egress to the MCE's own API server (`kubernetes.default.svc`) and to Temporal.

Nothing is needed on the control-plane side. HyperShift reconciles a NetworkPolicy
`kas` in every control-plane namespace that admits ingress to `app: kube-apiserver` pods
on TCP 6443 **from any source** (`reconcileKASNetworkPolicy`,
`hypershift-operator/controllers/hostedcluster/network_policies.go`, release-4.20; the
port is `config.KASSVCPort`, 6443). Only a NetworkPolicy of your own on the worker's
namespace that restricts egress must allow the above. On the sandbox `redbull-workflows`
has no NetworkPolicy. A hosted API the worker cannot reach shows as
`HostedClusterUnreachableError` on the activity, and as `reachable: false` in the
pre-flight.
