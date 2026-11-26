# vm-lifecycle-worker

The vm-lifecycle domain's activity worker. **One release per KV cluster, installed inside
that KubeVirt cluster.** It dials out to the hub's Temporal, polls
`vm-lifecycle-activity-<kvCluster>`, and works on its own cluster as its own ServiceAccount.
The KV cluster exposes nothing, and nothing on the hub holds a credential for it or its MAC
prefix. The chart holds **no Secret at all**.

What it does there, all from `activities/vm_lifecycle/activities.py`, for
[create-vm](../../../docs/site/vm-lifecycle/create-vm.html) and
[delete-vm](../../../docs/site/vm-lifecycle/delete-vm.html):

- **measures** its cluster when a create-vm run asks (`read_kv_cluster_figures`): the usable
  nodes' allocatable, what pods request, what the stopped `redfish-enabled` VMs will request,
  each usable node's headroom as a CPU and memory pair. It only measures; the run on the hub
  decides, with one placement buffer for every cluster;
- **creates** one Halted VirtualMachine per request (`create_vm_on_cluster`): it draws an
  8-character serial, derives the name `ocp-<flavor>-kv<cpu>-<site>-<serial>` (the serial also
  the VM's SMBIOS serial), builds the VirtualMachine with NO MAC and this cluster's own storage
  class, network and node selector, and creates it, drawing again by itself when the name is
  taken (3 draws); it reads back the MAC KubeMacPool assigned;
- **waits** for the VM's root disk to bind (`await_vm_ready`, one heartbeating activity);
- **deletes** a run's own VM and its root DataVolume when that run will not complete
  (`delete_vm_on_cluster`): only the VM labelled with the run's id, only by the UID it read;
- for **delete-vm** — one installed VM removed for good — **inspects** the VM named with its
  DataVolumes, claims, PersistentVolumes, snapshots and the pods mounting them
  (`inspect_vm_for_deletion`), **deletes** the VM, its DataVolumes, each claim's CDI
  VolumeImportSource and its claims (`delete_vm_and_volumes`: its root disk's, and
  kubevirt-redfish's boot ISO claim when the media was never ejected), and **waits** until
  nothing of it is left, its PersistentVolumes included (`await_vm_gone`). Only a VM create-vm
  made for the caller's MCE (its labels), only by the UIDs it read; it NEVER deletes a
  PersistentVolume, a VolumeSnapshot or a pod.

It never starts, stops or patches a VM (kubevirt-redfish owns the power state), never
creates a namespace, and never chooses a MAC: **KubeMacPool assigns every VM's MAC, unique on
the cluster** (the operator, 2026-11-25). The worker requests none, because KubeMacPool does
not refuse a duplicate REQUESTED MAC (tested on cluster-qcl8n). So the cluster MUST run
KubeMacPool (OpenShift Virtualization does, by default) and the VM namespace must not be
labelled `mutatevirtualmachines.kubemacpool.io=ignore`: a VM that comes back without a MAC is
deleted and the create fails (`VmMacNotAssignedError`). The range is the HyperConverged
object's (`spec.networking.kubeMacPoolConfiguration`), set apart from this chart — until it is,
each cluster keeps the random `02:xx:xx` range KubeMacPool picked, which two clusters can share.

The zone's list of KV clusters is not this worker's: it is day1's, read on the hub by the
`cluster-lifecycle-worker`, which also holds the placement buffer
(`HEADROOM_BUFFER_PERCENT`).

## `kvCluster` is the day1 file name

A create-vm run reads its zone's KV clusters from day1,
`sites/<site>/mces/ocp4-<lifecycle>-seed-<site>-<zone>-<id>/hostedClusters/<kv cluster>.yaml`, and sends
each cluster's work to `vm-lifecycle-activity-<that file's name>`. This worker polls
`vm-lifecycle-activity-<kvCluster>`. **The two must be the same string**, so `kvCluster` is
exactly the day1 file's name without `.yaml` (`ocp4-prod-kv-five-az1-a`), never abbreviated and
never canonicalised. It must be `ocp4-<lifecycle>-kv-<site>-<zone>-<id>` (the platform naming
convention): the worker reads its site and zone from it and refuses a VM of another site or zone.

A wrong value is not silent. The render fails on a name of another shape; a name of the right
shape that is not the day1 file's leaves the cluster a `no-worker` row in every run of its
zone, and a worker reached under another cluster's queue answers `readable: false` naming
the cluster it is for.

## Install

The brain must run first (it owns the workflow queues); this worker only polls its activity
queue. `kvCluster` is the cluster's own; `kubemacpool` and `vm` are the same on every KV
cluster unless one differs. All are rendered through day2, never typed in by hand:

```bash
helm upgrade --install vm-lifecycle-worker <chart dir> \
  -n redbull-workflows \
  --set kvCluster=ocp4-prod-kv-five-az1-a \
  --set vm.storageClassName=<an Immediate, RWX-Filesystem class> \
  --set temporal.host=<the hub's Temporal Route>:443 \
  --set temporal.tls.enabled=true \
  --set-file temporal.tls.caCert=<the Route's CA> \
  --set image.tag=<vX.Y.Z>
```

`<chart dir>`: this chart, in the workflows repo at `deploy/charts/vm-lifecycle-worker/` or its
copy in redbull-platform at `gitops/charts/vm-lifecycle-worker/`.

No value of the chart is a MAC: KubeMacPool on the cluster assigns every VM's.

`<vX.Y.Z>`: a version tag. Every push to `main` that changes a file this limb loads publishes
`ghcr.io/team-redbull/vm-lifecycle-worker:vX.Y.Z` (its job in CI's `build` matrix), writes that
tag into redbull-platform's copy (`gitops/charts/vm-lifecycle-worker/values.yaml`, `image.tag`)
and tags redbull-platform `vm-lifecycle-worker/vX.Y.Z`. Installed from redbull-platform's copy,
leave `image.tag` unset to get the newest.

Temporal is reached through the hub's TLS Route: set `temporal.host` to `<route-host>:443`,
`temporal.tls.enabled: true` and the CA, exactly as
`deploy/charts/server-lifecycle-worker/README.md` describes for an MCE (steps 1–6 there; the
Route and its certificate are the same ones). Only a worker on the hub itself (a lab) uses the
in-cluster Service with TLS off.

Check the worker polls its queue:

```bash
temporal task-queue describe --task-queue vm-lifecycle-activity-<kvCluster> --task-queue-type activity
```

CI's `sync-upgrade-charts` copies this chart into redbull-platform as
`gitops/charts/vm-lifecycle-worker/` on every push to `main` that changes it
(`scripts/sync_platform_charts.sh`, keeping the tag CI wrote), so edit it here, never there.
Until 2026-11-20 redbull-platform held a hub chart of this name (one release for every zone,
reaching each KV cluster through two HTTP agents); the first sync replaces it. There is no
`gitops/services/` entry for this chart: each KV cluster gets it through day2.

## The storage class

`vm.storageClassName` must **bind immediately** and allow a **`ReadWriteMany` + `Filesystem`**
volume. The VM is created Halted, so nothing ever consumes its disk: with a
`WaitForFirstConsumer` class the root disk never binds, and every create-vm run ends, ten
minutes later, by deleting its VM again (`VmRootDiskNotBoundError`). ceph-rbd refuses
`ReadWriteMany` + `Filesystem` (multi-node access is for block volumes only); cephfs allows it.

## Configuration

| Value | Env | Default | Meaning |
|---|---|---|---|
| `kvCluster` | `KV_CLUSTER` | — (required) | The KV cluster this worker runs in: exactly its day1 file name; names its queue |
| `vm.storageClassName` | `VM_STORAGE_CLASS_NAME` | — (required) | The root disk's storage class (Immediate binding, RWX + Filesystem) |
| `vm.networkName` | `VM_NETWORK_NAME` | `openshift-cnv/vm-external-network` | `<namespace>/<name>` of the NetworkAttachmentDefinition every VM boots on. The VM's network is ALWAYS multus, as in the platform's `vm.yaml`; only its name is a value, for a cluster whose NAD has another |
| `vm.nodeSelector` | `VM_NODE_SELECTOR` (JSON) | `{}` | The VM template's nodeSelector. Capacity is measured on the nodes matching it only |
| `vm.redfishLabel` | `VM_REDFISH_LABEL` | `redfish-enabled=true` | The label kubevirt-redfish exposes a VM by, and the one stopped VMs are counted by |
| `vm.rootDiskSize` | `VM_ROOT_DISK_SIZE` | `120Gi` | The root disk's size |
| `temporal.host` | `TEMPORAL_HOST` | — (required) | Temporal as reachable from this KV cluster |
| `temporal.namespace` | `TEMPORAL_NAMESPACE` | `default` | The Temporal namespace; must match the hub |
| `temporal.tls.*` | `TEMPORAL_TLS_CA`, `TEMPORAL_TLS_SERVER_NAME` | off | TLS to Temporal (a Route) |
| `rbac.create` | | `true` | Render the RBAC below |
| `replicaCount` | | `2` | Pods polling the queue (load sharing and failover) |
| `image.repository` / `image.tag` | | ghcr / appVersion | Mirror path and tag in a disconnected environment |

The render fails (and so does the worker's start, on the same rules) without `kvCluster`,
`vm.storageClassName` or `temporal.host`; on a `vm.networkName` that is not
`<namespace>/<name>`; on a `kvCluster`
that is not `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`. (A chart rendered before 2026-11-25 still
sets `VM_MAC_PREFIX`; the worker ignores it.) A KV cluster whose worker does not start is skipped as `no-worker` by
every run of its zone, 30 seconds each.

There is no placement buffer here. It is one value for every KV cluster,
`HEADROOM_BUFFER_PERCENT` of the hub's `cluster-lifecycle-worker` (15 by default).

Every key above is in the ConfigMap `vm-lifecycle-config` (`<domain>-config`, CLAUDE.md §8).
The pod runs as a non-root UID (`USER 1001` in the image; any UID OpenShift assigns works)
with a read-only root filesystem, no privilege escalation and no capabilities. It writes
nothing to disk; `/tmp` is an emptyDir for Python and the Temporal core.

Two pods share the queue. Every activity is idempotent across them: a create first looks for
the VM its run already made (by the `redbull.io/created-by` label), and the disk wait resumes
from its last heartbeat.

## RBAC

Every grant comes from a call in `activities/vm_lifecycle/kubevirt_api.py`; nothing else is
granted. All of it is cluster-wide, on this KV cluster only — the capacity figures count pods
and stopped VMs in every namespace, a run's VM and a VM's name are looked for in every
namespace, and the VM namespace is the request's.

| Group / resource | Verbs | The call |
|---|---|---|
| `""` nodes | list | `list_nodes`: allocatable, Ready, cordoned, taints, labels (the figures) |
| `""` pods | list | `list_unfinished_pods`: every unfinished pod's requests and node (the figures); which pods mount a claim of a VM being deleted (delete-vm) |
| `""` namespaces | get | `namespace_exists`: the VM namespace must exist; the worker never creates one |
| `""` persistentvolumeclaims | get, list, delete | `pvc_phase`: is the root disk's claim Bound (the ready wait); `list_pvcs`, `delete_pvc` (delete-vm: the claims of the VM being deleted, each by UID) |
| `""` persistentvolumes | get, list | `list_pvs`, `get_pv` (delete-vm: the volumes behind those claims, found by `claimRef` and watched until gone; never deleted) |
| `kubevirt.io` virtualmachines | get, list, create, delete | `list_vms` (the figures; the run-id and name lookups of a create; the delete's lookup), `get_vm` (the ready wait; the delete's identity check), `create_vm`, `delete_vm` (a run's own VM, by UID) |
| `kubevirt.io` virtualmachineinstances | list | `list_vmis`: which VMs have a launcher pod on a node (the figures) |
| `cdi.kubevirt.io` datavolumes | get, delete | `get_datavolume` (the root disk's phase), `delete_datavolume` (with its VM) |
| `cdi.kubevirt.io` volumeimportsources | delete | `delete_volume_import_source` (delete-vm: what a claim was filled from, named by the claim's `dataSourceRef`) |
| `snapshot.storage.k8s.io` volumesnapshots | list | `list_volume_snapshots` (delete-vm: a snapshot of a deleted VM's disk keeps its volume in the array; reported, never deleted) |

Not granted, on purpose: any update or patch of a VirtualMachine, the `start`/`stop`
subresources, any write to a node, pod or namespace, any delete of a PersistentVolume or a
VolumeSnapshot, Secrets, `pods/exec`. **A claim's `delete` IS granted since 2026-11-26**
(it was on this list until then): delete-vm removes a deleted VM's own claims, which is the
only way its volumes leave the storage array.

A missing verb is `KvClusterAccessError` (403, permanent) on the create, the wait or a
delete, and an `unreachable` row on the figures read. **So this chart ships BEFORE an image
with delete-vm**: a new image on the old ClusterRole fails every delete-vm run at its first
call, where an old image only answers "not registered" and the run waits.

## Network

The worker only makes **outbound** connections; nothing connects to it and the chart has no
Service and no Route. Its pods need, from the worker's namespace:

| To | Port | Why |
|---|---|---|
| this cluster's own API server (`kubernetes.default.svc`) | TCP 443 | every call in the RBAC table |
| the hub's Temporal frontend, through its Route | TCP 443 (gRPC over TLS, HTTP/2) | long-polling `vm-lifecycle-activity-<kvCluster>`, heartbeats, results |
| cluster DNS | UDP/TCP 53 | resolving both |

Only a NetworkPolicy of your own on the worker's namespace that restricts egress must allow
the above. A worker that cannot reach Temporal exits and restarts; one that cannot read its
own API answers `readable: false` on the figures read and retries its other calls.

The hub and the KV cluster must keep their clocks in step (NTP): the worker creates nothing
after the `create_before` the run sends, which is 45 seconds before the zone's lock moves on;
25 of them cover one API call in flight, 20 a difference between the two clocks.
