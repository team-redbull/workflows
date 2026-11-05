# The KubeVirt clusters' agents — kv-capacity and kv-vm-api

Status: **contract fixed 2026-11-05, with create-vm; the agents' repo
(`team-redbull/kv-agents`) is to be created.** This page is what the
vm-lifecycle limb (`activities/vm_lifecycle/kv_capacity_api.py`,
`kv_vm_api.py`) sends and expects; the models are vendored in
`shared/models/vm_lifecycle.py` (the "wire contract" section) and
`tests/test_kv_agents_contract.py` keeps the agents' copy equal to them
(`tests/fixtures/kv_agents_contract.schema.json`; regenerate with
`UPDATE_KV_AGENTS_CONTRACT=1`, compare the agents' checkout with
`KV_AGENTS_SERVICE_DIR`).

## Why agents

create-vm has to read every KubeVirt cluster of a zone and write one VM on one
of them, from a limb on the hub. A kubeconfig per cluster on that limb was
rejected (admin-equivalent credentials for the whole fleet on one pod; `secrets
get` cluster-wide on the MCE); Thanos was rejected (lag, and the allowlist
cannot compute the active-pod join). So each cluster runs two small services
of ours, deployed by the platform (day2 sigs) behind Routes, each with ONE
fleet bearer token, each with exactly the RBAC its verbs need:

| agent | does | RBAC |
| --- | --- | --- |
| **kv-capacity** | read-only facts the placement rule needs | get/list nodes, pods, virtualmachines, virtualmachineinstances |
| **kv-vm-api** | the idempotent VM create / read / delete, rendering the platform's `vm.yaml` | create/get/delete virtualmachines, get/delete datavolumes, get namespaces |

Two services, not one: different RBAC and blast radius ⇒ different
deployments (CLAUDE.md §2). Both are DUMB: the fit test, the buffer and the
best-fit ranking live in the limb (`activities/vm_lifecycle/placement.py`,
`docs/design/kubevirt-vm-placement.md`), so the policy has one home.

Routes: `https://kv-capacity.apps.<cluster>.<domain>` and
`https://kv-vm-api.apps.<cluster>.<domain>`, rendered by the limb from
`KV_CAPACITY_URL_TEMPLATE` / `KV_VM_API_URL_TEMPLATE` (`{cluster}` = day1
`clusterName`, `{name}`, `{hosted_cluster}`, `{site}`, `{zone}`). The
certificates are the ingress wildcard's; the limb does NOT verify them unless
`KV_AGENT_CA` is set (the operator's decision, 2026-11-04). Tokens travel in
`Authorization: Bearer` only.

## kv-capacity

`GET /v1/capacity` → `KvCapacityReport` (200). Integers only: millicores and
bytes, never Kubernetes quantities.

```json
{
  "cluster": "kubevirt-bat-yam-a1",
  "observed_at": "2026-11-05T10:00:00Z",
  "nodes": [
    {"name": "worker-0", "schedulable": true,
     "allocatable_cpu_millicores": 64000, "allocatable_memory_bytes": 549755813888,
     "requested_cpu_millicores": 30000, "requested_memory_bytes": 138512695296}
  ],
  "halted_reservations": [
    {"namespace": "inventory", "name": "ocp-x201-zone-a-32-3f9c1a",
     "cpu_millicores": 30000, "memory_bytes": 138512695296}
  ],
  "vms": [
    {"namespace": "inventory", "name": "ocp-x201-zone-a-32-3f9c1a", "mac": "02:a1:b2:3f:9c:1a"}
  ]
}
```

- `nodes`: every node, with `schedulable` = not `spec.unschedulable` AND
  `Ready`; `requested_*` = Σ container requests of Pending|Running pods on
  the node — the kube-scheduler's own view (one
  `list pods --all-namespaces` with `status.phase!=Succeeded,status.phase!=Failed`).
  An optional `NODE_SELECTOR` label set restricts the nodes (and is what
  simulates several clusters on one, in the lab).
- `halted_reservations`: VMs in the chassis namespaces (`CHASSIS_NAMESPACES`)
  carrying the label `redfish-enabled=true`, with `runStrategy: Halted` and NO
  virt-launcher pod (once the pod exists the pod counts instead — no double
  count), sized by the template's `resources.requests` when set, else as
  KubeVirt itself would size the launcher (memory = guest memory, CPU = cores /
  the default `cpuAllocationRatio` 10). When `NODE_SELECTOR` is set, only VMs
  whose template `nodeSelector` matches it.
- `vms`: EVERY VM the agent can list, running or not, with its fixed MAC
  (`null` when none): the zone-wide set a new suffix must stay out of.

Status: 401/403 a wrong token; anything else the limb reads as `unreachable`
(5xx, timeout) or `no-agent` (404, the router's 503) — a row in the table,
never a failed run on its own.

## kv-vm-api

`PUT /v1/vms/{name}` body `KvVmCreateRequest`
`{"namespace", "cpu", "memory_gib", "mac", "labels"}`:

- **201** `KvVmCreateResponse` — created: the chart's `vm.yaml.j2` rendered
  with `name`, `cpu`, `memory="<n>Gi"`, `mac`, `labels` and the per-cluster
  values (storage class, network, node selector), `runStrategy: Halted`,
  label `redfish-enabled: "true"`, the `macAddress` on the interface and the
  CPU request floored at one core — the two differences from
  redbull-platform's `vm.yaml`, which stays as it is;
- **200** `KvVmCreateResponse` with `created: false` — a VM of this name with
  the SAME identity exists (namespace, mac, cpu, memory_gib, our labels;
  `runStrategy` is NEVER compared): the caller's own earlier PUT;
- **409** `KvVmConflict` `{"reason", "existing": KvVmState}` — the name holds
  ANOTHER identity;
- **404** `KvApiError` — the namespace does not exist (the agent never creates one);
- **422** `KvApiError` — a request the template cannot render, or a VirtualMachine the API
  server / an admission webhook refused (400/422 from the cluster, its message carried: a
  deterministic answer, so never a 500 that create-vm would retry for ever);
- **401** — a wrong token.

`GET /v1/vms/{name}?namespace=` → **200** `KvVmState`
`{"name", "namespace", "mac", "cpu", "memory_gib", "run_strategy", "admitted",
"root_disk": {"name", "phase", "bound"}}` (`admitted`: KubeVirt accepted the
spec; `root_disk`: the DataVolume's phase and whether its PVC is Bound) or
**404**.

`DELETE /v1/vms/{name}?namespace=&mac=` → **204** deleted (the VM and its
DataVolume), **404** already gone (success for the caller), **409**
`KvVmConflict` when the VM's MAC is not the one given — the agent only ever
deletes the caller's own VM — or **401**.

Every non-2xx body is a `KvApiError` `{"error", "detail"}` unless stated;
`detail` is the sentence the limb carries into the run's failure, so write it
for the operator.

## The Redfish side

kubevirt-redfish runs `systemIdConvention: legacy` (decided 2026-11-05): the
Redfish system id is the bare VM name, so `ocp-<infraenv>-<suffix>` is the
VM, the Redfish system and the server server-scan lists — one string, and the
one add-server-to-mce's `^ocp-<infraEnv>` pool match needs. Under `legacy` the
server resolves a name across every chassis the Redfish user may see, so VM
names must be unique across them; create-vm's are (the suffix is the MAC's,
unique zone-wide).

## Implementation notes (the agents' repo)

Python/FastAPI, two images on `ghcr.io/team-redbull` (`kv-capacity`,
`kv-vm-api`), charts `gitops/charts/kv-capacity` and `gitops/charts/kv-vm-api`
in redbull-platform (the shape of `gitops/charts/kubevirt-redfish/`: SA,
ClusterRole/Binding or Role, Deployment, Service, Route edge, the token in a
Secret created out of band); `vm.yaml` ported to Jinja2 as a ConfigMap of the
kv-vm-api chart, with storage class, network and node selector as chart values
(vm.yaml's as defaults). The contract models are copied verbatim into
`kv_agents/contract.py` with the snapshot beside them.
