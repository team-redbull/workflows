# create-vm lab on `cluster-qcl8n`: the base services, then the workers

The services a create-vm end-to-end test needs before any workflow worker is deployed, on
a disposable Red Hat workshop cluster (`https://api.cluster-qcl8n.dyn.redhatworkshops.io:6443`,
OpenShift 4.22, three nodes, ODF external ceph, OpenShift GitOps 1.18). Everything is an
Argo CD Application in `openshift-gitops`, in the lab's own project `lab-create-vm`, and
everything the lab creates carries the label `redbull.io/lab=create-vm`.

Deployed 2026-11-20, in two layers: `lab.sh` is the base (git, Temporal, OpenShift
Virtualization) and `workers.sh` the workflow's own services on top of it (the brain, the
day1 reader, a vm-lifecycle worker inside this cluster, and the fake KV clusters). The
second half of this page, [The workers and what the runs showed](#the-workers-and-what-the-runs-showed-2026-11-20),
is about `workers.sh`.

```sh
./lab.sh up                    # deploy or converge (safe to re-run)
./lab.sh status                # Applications, pods, every address
./lab.sh verify                # Temporal, the workers' git clone, one Halted VM; leaves no VM
LAB_CONFIRM=down ./lab.sh down # remove everything

./workers.sh up                    # images, charts, day1, Applications (safe to re-run)
./workers.sh status                # pods, queues, how to start a run
LAB_CONFIRM=down ./workers.sh down # the workers and the VMs their runs made; run it BEFORE lab.sh down
```

`lab.sh` refuses any cluster but the one above (`LAB_API_SERVER` to change it).

What of it has run: `status` and `verify` as written; `up` three times against the lab
already standing — once pushing a change to the lab repo and adding an Application, once
changing nothing, once after deleting `lab-git/lab-git-tokens` (it re-issued both tokens
and Argo CD's credential). The first
install was made by the same commands typed one at a time, so `up`'s create branches for
the `lab-git` namespace, the admin Secret and the `redbull` organisation have not run from
the script. **`down` was never run** (it would have removed the lab this folder describes):
its steps follow Red Hat's CNV uninstall order and the platform repo's rule about deleting
an Application before the repo it renders from.

## What runs where

| Application (`openshift-gitops`) | Source | Namespace | What |
|---|---|---|---|
| `lab-create-vm-git` | public chart `gitea` 12.7.0, `https://dl.gitea.com/charts/` | `lab-git` | Gitea 1.27.0 rootless, one pod, sqlite on a 2Gi PVC, Route |
| `lab-create-vm-temporal` | lab repo, `gitops/charts/temporal` | `temporal` | Temporal 1.29.7 (frontend, history, matching, worker, one pod each), UI, PostgreSQL 17 on a 10Gi PVC |
| `lab-create-vm-openshift-virtualization` | lab repo, `gitops/charts/openshift-virtualization` | `openshift-cnv` | OperatorGroup `openshift-cnv-og`, Subscription `kubevirt-hyperconverged` (`stable`, `redhat-operators`) → CSV `kubevirt-hyperconverged-operator.v4.22.16`, HyperConverged `kubevirt-hyperconverged` |
| `lab-create-vm-extras` | lab repo, `lab/extras` | — | Namespace `inventory`; Deployment `temporal/temporal-grpc-relay` |

The **lab repo** is `redbull/platform` in the in-cluster Gitea. `lab.sh` builds it from:

- `gitops/charts/temporal` and `gitops/charts/openshift-virtualization` of the platform
  repo **as committed at `origin/main` `203bbf3`** (`git archive`, so the checkout is only
  read and its working tree never matters). No other chart is copied.
- `platform-extra/` of this folder (`lab/extras/`).
- one patch (below).

### Overrides, all in the Applications' `valuesObject`

| Application | Override | Why |
|---|---|---|
| temporal | `ui.auth.enabled: false`, `ui.route.enabled: false` | The chart allowlists the platform's `ocpadmin` and re-randomises its cookie Secret on every Argo CD render. With no login, a Route on a public workshop domain would let anyone start workflows: the UI is reached by port-forward. |
| temporal | `postgresql.global.compatibility.openshift.adaptSecurityContext: force` | See "Argo CD's cluster cache" below. |
| temporal | `temporal.services.*.replicaCount: 1`, `postgresql.primary.extendedConfiguration: max_connections = 200` | As shipped (two pods per role, PostgreSQL's default 100 connections) the eight server pods held all 100 within fifteen minutes and PostgreSQL refused 321 connections in the next thirty (`sorry, too many clients already`). After: 42 in use of 200, none refused. The platform chart sets neither a pool size nor `max_connections`, so its defaults would meet the same ceiling elsewhere. |
| openshift-virtualization | `operator-olm.subscription.source: redhat-operators` | Connected cluster: no mirrored `generic-cs` catalog. |
| openshift-virtualization | `operator-olm.operatorGroup.name: openshift-cnv-og` | The workshop's own `kubevirt` Application would create that OperatorGroup. See "The workshop's `kubevirt` Application". |
| openshift-virtualization | `vmBridge.enabled: false` | The NNCP enslaves `bond0` and needs kubernetes-nmstate; the cluster has neither. |
| openshift-virtualization | `ignoreDifferences` on `/spec/featureGates/disableMdevConfiguration` | HCO 4.22.16 no longer has that gate and prunes it, so the Application stayed OutOfSync and selfHeal re-applied it every few seconds (96 syncs in ten minutes). |

### The one patch to a platform chart

`gitops/charts/openshift-virtualization/templates/hyperconverged.yaml` asks for
`uninstallStrategy: BlockUninstallIfWorkloadExist`. HCO 4.22.16 refuses the object:

```
spec.uninstallStrategy: Unsupported value: "BlockUninstallIfWorkloadExist":
supported values: "RemoveWorkloads", "BlockUninstallIfWorkloadsExist"
```

The value is hard-coded in the template, so `lab.sh` rewrites that one word
(`…WorkloadsExist`) in the lab repo's copy. The platform chart itself has the same problem
against this HCO version; it and the `disableMdevConfiguration` gate are worth fixing there.

## Addresses

| What | Address |
|---|---|
| git, in-cluster, plain http | `http://gitea-http.lab-git.svc.cluster.local:3000/redbull/<repo>.git` |
| git, https (edge Route, publicly trusted wildcard certificate) | `https://gitea-lab-git.apps.cluster-qcl8n.dyn.redhatworkshops.io/redbull/<repo>.git` |
| Gitea web UI and API | `https://gitea-lab-git.apps.cluster-qcl8n.dyn.redhatworkshops.io/` (`/api/v1`) |
| Temporal frontend, in-cluster gRPC | `temporal-frontend.temporal.svc.cluster.local:7233`, namespace `default` (retention 168h) |
| Temporal frontend from the laptop | `oc -n temporal port-forward deploy/temporal-grpc-relay 7233:7233`, then `--address 127.0.0.1:7233` |
| Temporal UI | `oc -n temporal port-forward svc/temporal-ui 8080:8080`, then `http://localhost:8080` |
| Temporal's PostgreSQL | `temporal-postgresql.temporal.svc.cluster.local:5432` |
| VM namespace | `inventory` |
| Argo CD | `oc -n openshift-gitops get route openshift-gitops-server` |

**`oc port-forward svc/temporal-frontend` does not work**, on this chart anywhere: the
frontend binds gRPC on its pod IP only (`BIND_ON_IP`), and a port-forward dials localhost
inside the pod (`dial tcp [::1]:7233: connect: connection refused`). The relay is a
`socat` pod (the cluster's own `openshift/tools` image) that listens on every address and
forwards to the frontend Service. Workers use the Service, never the relay.

## Credentials

All generated by `lab.sh up` on first run, kept only in cluster Secrets, never on disk:

| Secret | Keys | Used for |
|---|---|---|
| `lab-git/gitea-admin` | `username` (`lab-admin`), `password` | Gitea's admin: web UI, API, git over http(s) |
| `lab-git/lab-git-tokens` | `username`, `rw-token`, `ro-token` | Gitea access tokens of the admin: `rw-token` pushes (`write:repository`), `ro-token` only reads |
| `openshift-gitops/lab-create-vm-git-creds` | Argo CD credential template (`repo-creds`) for `http://gitea-http.lab-git.svc.cluster.local:3000/` | Argo CD reads every repo of this Gitea with `ro-token` |

Nothing in Gitea is readable anonymously (sign-in required, registration off, repos
private): an anonymous `git ls-remote` gets a credential prompt, the read token a 403 on
push. Gitea takes a token as the basic-auth password under any username, so the workers'
`https://x-access-token:<token>@host/...` works as it is.

Temporal's PostgreSQL password is the platform chart's committed test default (Secret
`temporal/temporal-postgresql`). Not a lab credential; nothing outside the namespace uses it.

## Using the git server

```sh
HOST=$(oc -n lab-git get route gitea -o jsonpath='{.spec.host}')
RW=$(oc -n lab-git get secret lab-git-tokens -o jsonpath='{.data.rw-token}' | base64 -d)

# Push from the laptop. A repo that does not exist is CREATED by the push (private).
git -c credential.helper= push "https://x-access-token:${RW}@${HOST}/redbull/day1.git" main
```

`-c credential.helper=` keeps the lab token out of the macOS keychain.

For a worker's `DAY1_REPO_URL`, use the **https Route URL**
(`https://gitea-lab-git.apps.cluster-qcl8n.dyn.redhatworkshops.io/redbull/day1.git`) with
`DAY1_GIT_TOKEN` = `ro-token` (or `rw-token` for a limb that pushes): the workers' git
client adds a token to https URLs only, and the plain-http Service URL refuses anonymous
reads. `lab.sh verify` clones exactly that way from a pod.

Gitea has organisations, not GitLab subgroups: a repo is `<owner>/<repo>`, two segments.
The air-gap's `redbull/gitops-day1/platform-config` shape cannot be mirrored here, and
Gitea's API is not GitLab's (`/api/v4`), so provision-cluster's merge requests cannot run
against it. create-vm only clones.

Adding an Application for a worker: push its chart into `redbull/platform` (`lab.sh up`
replaces only `gitops/charts/temporal`, `gitops/charts/openshift-virtualization` and
`lab/extras`, and leaves everything else in the repo alone), then apply an Application in
project `lab-create-vm` with
`repoURL: http://gitea-http.lab-git.svc.cluster.local:3000/redbull/platform.git`.

## What was verified (2026-11-20)

`./lab.sh verify` repeats all of it.

- **Temporal**: `temporal operator namespace describe -n default` → `Registered`,
  retention 168h; `cluster health` → `SERVING`. Asked both from inside the cluster (the
  frontend pod's own CLI against the Service) and from the laptop through the relay.
- **git**: `git -c http.sslVerify=false clone https://x-access-token:<ro-token>@<route>/redbull/platform.git`
  from a pod in the cluster; push from the laptop with `rw-token`; push-to-create.
- **A Halted VM** in `inventory` (`test/vm-halted.yaml`): `runStrategy: Halted`, blank
  `dataVolumeTemplates` root disk 2Gi, `ReadWriteMany` + `Filesystem`,
  `ocs-external-storagecluster-cephfs`, one pod-network interface with
  `macAddress: 0e:00:63:00:00:01`, label `redfish-enabled: "true"`.
  - The DataVolume reached `Succeeded` and the PVC `Bound` (RWX, Filesystem, 2Gi) with the
    VM `Stopped` and no VMI. VM creation → DataVolume Ready: **117 s the first time** (CDI
    pulled its importer image), then **18 s, 29 s, 20 s and 11 s** — and **133 s and 222 s**
    for two created while the control plane was stalling (below).
  - The disk is `spec.pvc` (a literal 2Gi claim), not `spec.storage`.
  - A blank Filesystem disk is not free: CDI runs an **importer pod** in the VM's namespace
    to create `disk.img` (through a `prime-…` PVC), so "bound" needs a schedulable pod
    there even though the VM never starts.
  - The cephfs StorageProfile lists `ReadWriteMany` + `Filesystem`.
- **Interface binding**: both `masquerade: {}` and `bridge: {}` on the pod network are
  admitted (a real create each, `201`); the cluster's `defaultNetworkInterface` is
  `masquerade`.
- **KubeMacPool** is on (`kubemacpool-mac-controller-manager`, range
  `02:b0:96:00:00:00`–`02:b0:96:ff:ff:ff`) and `inventory` is not opted out (no
  `mutatevirtualmachines.kubemacpool.io=ignore` label); a MAC outside its range is accepted.
  **A duplicate MAC is NOT refused**, in the case tried: a second Halted VM with
  `0e:00:63:00:00:01` in the same namespace (and, as a server dry run, in another) answers
  `201 Created` — no error message at all; KubeMacPool's log shows it handling both. CNV
  ships an alert for collisions (`KubemacpoolMACCollisionDetected`, on `kmp_mac_collisions`),
  but that metric stayed empty for four minutes with the two Halted VMs side by side. Not
  tried: two RUNNING VMs, or a MAC inside KubeMacPool's own range. So for Halted VMs nothing
  on this cluster refuses or flags a duplicate MAC: whoever creates the VM has to look for
  the MAC itself.
- All test VMs, DataVolumes and PVCs were deleted; `inventory` is empty and kept.

## The workers and what the runs showed (2026-11-20)

`./workers.sh up` builds four images ON the cluster (binary Docker builds into its internal
registry, tag `lab`), pushes the charts and `./day1` into the lab's git, and applies five
more Applications in `openshift-gitops`; the workloads run in `redbull-workflows`.

| Application | what it is |
| --- | --- |
| `lab-create-vm-workflows-orchestrator` | the brain and the trigger API, redbull-platform's chart (the worktree of its `feat/vm-lifecycle-per-kv-cluster` branch) |
| `lab-create-vm-cluster-lifecycle-worker` | the day1 reader (`list_kv_clusters`), reading the lab's day1 repo with the read-only token |
| `lab-create-vm-vm-lifecycle-worker-a` | THE REAL KV CLUSTER'S WORKER: this cluster as `ocp4-lab-kv-five-a`, zone az1, MAC prefix `0e:00:63`, cephfs, vm.yaml's network (`openshift-cnv/vm-external-network`, here an OVN layer2 overlay), 2Gi root disks |
| `lab-create-vm-vm-lifecycle-worker-i` | the same cluster under a second name, `ocp4-lab-kv-five-i`, zone az3, on `ceph-rbd` (WaitForFirstConsumer): the rollback scenario |
| `lab-create-vm-kv-fleet` | `tests/kv_fleet.py` as one pod: seven fake KV clusters, each with its own worker on its own queue, running the real worker code over an in-memory cluster |

The lab's day1 (`./day1`, pushed as `redbull/day1`) is what makes the zones:

| seed folder | files | what a run sees |
| --- | --- | --- |
| `sites/five/mces/ocp4-seed-five-1` (zone az1) | `…-five-a`, `…-five-b`, `…-five-d` (its NodePools say az2), `ocp4-test-kubevirt-five` (not a KV name) | `a` real, `b` has no worker (`no-worker`, 30 s), the other two `day1-problem` rows, never read |
| `sites/five/mces/ocp4-seed-five-2` (zone az2) | `…-five-e` … `…-five-h` | four fake clusters of one 100-core node |
| `sites/five/mces/ocp4-seed-five-3` (zone az3) | `…-five-i` | the real cluster on the storage class that never binds |
| `sites/bat-yam/mces/ocp4-seed-bat-yam-1` (zone az1) | `…-bat-yam-a` … `-c` | three fake clusters of three 64-core nodes |
| `sites/five/mces/ocp4-prod-mce-five-a` | `ocp4-test-kubevirt-site4` | an ordinary hosted cluster under an MCE: not a KV cluster, whatever its name |

### What of `workers.sh` has run

`up` end to end once on the empty namespace, then `images`, `charts`, `day1` and `apps`
several times each as the code changed; `status` and `fleet` as written. **`down` was never
run** (the lab is still standing), so "run it before `lab.sh down`" is the intended order,
not a tested one. `api` was used from the laptop for the first runs; the later ones were
started from inside the API pod.

### What ran, and how it ended

Every run was started through the trigger API and read back from `GET /workflows/runs/<id>`
and from the cluster.

- **One VM on the real cluster** (`c200-kv2-five-az1`): `ocp-c200-kv2-five-0e0063b6b5ec` on
  `ocp4-lab-kv-five-a`, `runStrategy: Halted`, labels `redfish-enabled=true` and
  `redbull.io/created-by=<run id>`, annotation `redbull.io/workflow-id`, its DataVolume
  `Succeeded` and PVC `Bound` 27 s after the create. The FIRST attempt of this run failed, and
  that was a real finding: see "What the lab changed" below.
- **The worker's figures are the cluster's own.** Its allocatable and requested totals equal
  what the three nodes report (`oc describe node`, allocated resources) to the millicore and
  the byte: 18 840 m and 58 533 609 472 bytes requested at the time of the read.
- **Ten runs at once on az1** (`c200-kv4-five-az1`, `count: 10`): two VMs placed on `a`, then
  eight `NoKvClusterFitsError`, each naming `a` as `fit-buffer`, `b` as `no-worker`, and the
  two day1 problems. 248 s for all ten, because every run waits 30 s for `b`'s worker that
  does not exist, one run at a time. The zone's lock granted twelve times, no lease expired.
- **How long a run holds its zone** (from the committed histories): 0.3 s when the first
  cluster it reads fits (figures 0.1 s, create 0.1 s), and 30 s MORE for every cluster ahead
  of the fitting one that is in day1 with no worker. So a burst is cheap on a healthy zone
  and slow behind a dead one: at 30 s a run, about sixty runs get through one zone inside
  the 30-minute lock wait and the rest fail `KvZoneLockTimeoutError`. A cluster in day1
  without a running worker is worth fixing, or taking out of day1, before a large `count`.
- **The worker pod killed during the disk wait**: the run waited for the new pod and
  completed. **The brain pod killed mid-run**: completed. **Temporal's frontend and history
  pods killed mid-run**: completed.
- **A cancel during the disk wait** (az3): the run ended `CANCELED`, `cancel_rollback`
  "cancelled; removed inventory/ocp-c200-kv1-five-0e006445d4a6 on ocp4-lab-kv-five-i", and
  no VM, DataVolume or PVC of it was left.
- **The ten-minute rollback** (az3, the WaitForFirstConsumer class): `FAILED` in phase
  `removing-vm` with `VmRootDiskNotBoundError` — "root disk …-root phase PendingPopulation,
  bound False, admitted True" — and nothing left behind.
- **The fake fleet, through the real brain, Temporal and day1**: ten 32-core VMs on az2 went
  `e, e, f, f, g, g, h, h` and two `NoKvClusterFitsError`, in 17 s; sixteen on bat-yam az1
  went five, five, five and one no capacity, in 24 s. Runs of an earlier attempt were terminated
  by hand while they stood in az2's line: the lock passed the seven it still held over in one
  step.
- **The control-plane node rebooted** (a `Rebooted` event at 13:57 UTC; cause not
  established — no MachineConfig changed, both pools are on the config of 19 hours before)
  while no create-vm run was in flight. Every pod of the lab restarted, some after failing
  to reach the internal registry for a few minutes; the four lock workflows stayed RUNNING,
  and afterwards a create on the real cluster completed in 41 s and ten on az2 ended eight
  Completed (`e`–`h`, two each, as the simulator's log shows) and two Failed. Not a test of
  a reboot UNDER a run: that is the pod kills above and the suite's dead-worker cases.
- **Seven of these histories are committed as replay goldens**:
  `tests/histories/create_vm/lab-cluster-qcl8n-{create,waited-in-line,no-capacity,cancel,rollback}.json`
  and `tests/histories/kv_zone_lock/lab-cluster-qcl8n-lock-{az1,az2}.json`, worker identities
  scrubbed like every golden.

### What the lab changed

- **The VM's network is vm.yaml's, always multus.** The chart first took the whole
  `networks[0]` body as a map with a multus default; the lab's `{pod: {}}` was MERGED with it
  by Helm into a network of two types, and KubeVirt refused every create (422, "should have
  only one network type"). On 2026-11-21 the operator set the platform's `vm.yaml` as the
  target, which never uses the pod network: the worker now always builds
  `{multus: {networkName: …}}`, and only the name is a value (`vm.networkName`, default
  `openshift-cnv/vm-external-network`). The lab got a NetworkAttachmentDefinition of that
  name (an OVN layer2 overlay, `lab/extras/vm-external-network.yaml`), and the VMs created
  before that day are on the pod network.
- **The simulator runs in the cluster, not on the laptop**: through `oc port-forward` its
  workers' long polls never got a task, so its clusters showed as `no-worker`.
- `temporal workflow cancel` through the relay failed (RST_STREAM); it works from inside the
  frontend pod (`oc -n temporal exec deploy/temporal-frontend -c temporal -- temporal …`).

### What it did not cover

Two real KV clusters (the second "cluster" is this one under another name, so its figures
count the first one's VMs — correct for one cluster, and not a test of two); a multus VM
network; a VM actually booted by Ironic through kubevirt-redfish; a worker reaching Temporal
from ANOTHER cluster through the Route with TLS; add-server-to-mce taking the VM. Lease
expiry and the dead-worker cases were run in the test suite (`tests/test_create_vm_workflow.py`,
on the production waits), not here.

## The workshop's `kubevirt` Application

The cluster came with workshop Applications in project `default`, from
`https://github.com/rhpds/ocp-virt-admin-rosetta-gitops.git` (`dev`). None was edited,
synced or deleted. Its `kubevirt` Application once installed CNV (2026-11-19 20:00Z),
which was gone again when the lab started: no `openshift-cnv`, no KubeVirt CRD.

It names the same objects the platform chart must: Subscription `kubevirt-hyperconverged`
(the OLM package name) and HyperConverged `kubevirt-hyperconverged` (the only name HCO
accepts), both in `openshift-cnv`. Its OperatorGroup is `openshift-cnv-og`, and the lab's
is given that name too, because a second OperatorGroup in a namespace
(`TooManyOperatorGroups`) breaks every CSV there, while one shared object does not.

Residual risk: that Application has automated sync without selfHeal and last synced
revision `049a1db`. It did not start an operation when the lab's objects appeared (checked:
last operation still 2026-11-19T20:00:34Z), and will not unless `dev` gets a new commit or
someone syncs it by hand. If it does, it applies its own Subscription (`startingCSV`
v4.20.8, ignored by an installed operator) and its HyperConverged spec
(`defaultCPUModel: Icelake-Server-v2`), takes over the tracking label, and its
`vm-datastore-job` sets `enableCommonBootImageImport: true`; the lab's Application
(selfHeal) then applies its own spec back. Annoying, not destructive.

## Argo CD's cluster cache (why `adaptSecurityContext: force`)

Argo CD answers Helm's `.Capabilities.APIVersions` from its cluster cache. This cluster's
application controller last started at 2026-11-20T10:12Z, while the aggregated APIs were
still stale after the cluster resumed:

```
Partial success when performing preferred resource discovery … stale GroupVersion discovery:
apps.openshift.io/v1, authorization.openshift.io/v1, build.openshift.io/v1, image.openshift.io/v1, …
```

Since then its `helm template` gets no `security.openshift.io/v1`, `route.openshift.io/v1`
or any other openshift-apiserver group. The temporal chart's PostgreSQL decides "am I on
OpenShift" from exactly that, kept `runAsUser`/`fsGroup` 1001, and restricted-v2 refused
the pod. The override makes the render independent of the cache.

Still so at 12:31Z (the newest render of the temporal chart). **A worker chart that tests
`.Capabilities.APIVersions.Has "route.openshift.io/v1"` (or the SCC API) renders as if
this were not OpenShift** until the controller rebuilds its cache, which a restart of
`openshift-gitops-application-controller-0` would do. That was not done: it is the
workshop's Argo CD, and the lab does not depend on it.

## The control plane stalls (found, not fixed)

From about 11:46Z the root disk of all three nodes became slow, and it had not recovered
by 12:34Z:

| | before the lab (10:36–11:26Z) | since (11:56–12:34Z) |
|---|---|---|
| etcd WAL fsync p99 | 8–11 ms | 0.7–2.2 s (single fsyncs up to 16 s) |
| control-plane `vda` average write latency | 1.4–1.6 ms | 16–28 ms |
| worker-1 `vda` average write latency | 1.1–1.5 ms | 12–54 ms |

The API server then misses 5 s deadlines and controllers lose their leader election and
restart: at 12:18–12:22Z kube-controller-manager, cert-manager, several ODF operators and,
in `openshift-cnv`, hco-operator, cdi-deployment, cdi-operator, virt-controller,
virt-operator and others (all back to Running by 12:34Z, HyperConverged Available again).
While CDI's controller was down, two test DataVolumes took 133 s and 222 s instead of
11–29 s, and deleting the VMs took minutes to remove their DataVolumes.

What it is not: the ceph volumes (`rbd*` devices, 1.6–3.6 ms) or a heavy writer (the
control plane wrote 59 ops/s before and 62–95 after; worker-2 shows 31 ms at 6 writes/s).
The onset coincides with the CNV install, which pulled images at about 7 MB/s for half an
hour on a node that also runs etcd — about 240 pods now sit on that one node — but the latency
stayed after the pulls ended, on all three nodes at once, which points at the storage
under the node VMs. Not established which.

For a create-vm run this means: a root disk can take minutes to bind, and an API call can
time out, for reasons that are the cluster's. Check before trusting a failure:

```sh
oc -n openshift-cnv get pods | grep -vE 'Running|Completed'
oc -n openshift-etcd logs etcd-control-plane-cluster-qcl8n-1 -c etcd --since=5m | grep -c 'slow fdatasync'
```

## What was granted or changed on the cluster

- **No RBAC was granted.** The `openshift-gitops` application controller already holds
  `cluster-admin` (ClusterRoleBinding `gitops-cluster-admin`), cluster-scoped and in every
  namespace. Argo CD's own configuration was not touched.
- In `openshift-gitops`: AppProject `lab-create-vm`, the four base Applications and the
  workers' five, Secret `lab-create-vm-git-creds`.
- By `workers.sh`: namespace `redbull-workflows` (BuildConfigs, ImageStreams, Secret
  `redbull-git-token` = the lab git's own read-only token, read from `lab-git/lab-git-tokens`,
  and a placeholder `shortcuts-api-token` the worker's settings require),
  the `redbull/day1` repo in the lab's git, and the VMs its runs created in `inventory`.
  The vm-lifecycle chart's ClusterRole and binding, once per release. No laptop credential
  was put on the cluster.
- Namespaces `lab-git`, `temporal`, `openshift-cnv`, `inventory`.
- Cluster-scoped, through OLM and the CNV operators: the CNV CSV's CRDs, webhooks,
  ClusterRoles and its node components (`virt-handler` and friends on every node; no
  MachineConfig, no reboot).
- On the lab's own Applications only: two sync operations that could never finish (the
  refused PostgreSQL pod, the refused HyperConverged) were terminated by patching the
  Application's `status.operationState.phase` to `Terminating`. An interim Application
  `lab-create-vm-namespaces` was replaced by `lab-create-vm-extras` (removed without
  cascade; `inventory` stayed).
- Created outside Argo CD, by `lab.sh`, because a generated credential cannot come from
  git: namespace `lab-git` (so the Secret can exist before the chart), Secrets
  `lab-git/gitea-admin`, `lab-git/lab-git-tokens`, `openshift-gitops/lab-create-vm-git-creds`,
  the Gitea organisation `redbull` and the two access tokens (Gitea's API).
- Images came from `docker.io` (temporalio, busybox, bitnamilegacy/postgresql),
  `docker.gitea.com`, `registry.redhat.io` and the internal registry. No Docker Hub rate
  limit was met.

## Files

```
lab.sh                                    up / status / verify / down
argocd/project.yaml                       AppProject lab-create-vm
argocd/app-git.yaml                       Gitea (its chart values inline)
argocd/app-temporal.yaml                  Temporal (overrides inline)
argocd/app-openshift-virtualization.yaml  CNV (overrides inline)
argocd/app-extras.yaml                    inventory namespace + Temporal relay
platform-extra/lab/extras/                what the lab adds to the lab repo (inventory, the VM network, the relay)
test/vm-halted.yaml                       the verification VM
workers.sh                                up / images / charts / day1 / status / api / fleet / down
argocd/app-workflows-orchestrator.yaml    the brain and the API
argocd/app-cluster-lifecycle-worker.yaml  the day1 reader
argocd/app-vm-lifecycle-worker-{a,i}.yaml the real cluster's worker, under two names
argocd/app-kv-fleet.yaml                  the fake KV clusters
day1/                                     the lab's day1 repo (only what create-vm reads)
kv-fleet/Dockerfile                       tests/kv_fleet.py as an image
platform-extra/lab/kv-fleet/              its Deployment
```

The Gitea Route's host (`gitea-lab-git.apps.cluster-qcl8n.dyn.redhatworkshops.io`) is
written in `argocd/app-git.yaml` three times; it is the only cluster-specific value
besides `LAB_API_SERVER`.
