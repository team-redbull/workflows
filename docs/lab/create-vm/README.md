# Lab: create-vm end to end on one OpenShift Virtualization cluster

## Purpose

Prove the create-vm workflow, the vm-lifecycle limb, the two KubeVirt agents (`kv-capacity`,
`kv-vm-api`, repo `kv-agents`) and their redbull-platform charts TOGETHER, on a real cluster,
without a redbull platform there: Temporal, the brain and the limb run from source on a laptop,
the agents run on the cluster, and SEVERAL KubeVirt "clusters" are simulated by agent release
pairs pinned to nodes (the charts' `config.nodeSelector` / `vm.nodeSelector`).

Built and verified on **2026-11-05** on `cluster-64spr.dyn.redhatworkshops.io` (RHDP, OCP 4.22.13,
CNV 4.22, ODF external; nodes `control-plane-cluster-64spr-1` 31.5 cores / 124.7 GiB,
`worker-cluster-64spr-{1,2}` 7.5 cores / 22.4 GiB each; kubevirt-redfish in `kubevirt-redfish`,
chassis namespace `redfish-vms`).

## The simulated zone

`day1/` is the mock day1 tree (its README says what each file exercises). Site `lab`, from
MCE `ocp4-lab-mce-lab-a`; seed `ocp4-seed-lab-a` lists eight clusters, `ocp4-seed-lab-b` one.

| day1 cluster | agents | what it proves |
|---|---|---|
| `ocp4-kubevirt-lab-a-1` | pinned to the control-plane node | fits small VMs |
| `-2` | worker-1 (4 GiB headroom) | `fit-node` for every size |
| `-3` | worker-2 (11 GiB headroom) | fits a 1-vCPU VM at a small buffer, `fit-node`/`fit-buffer` otherwise |
| `-4` | NO node selector (all three nodes) | the fullest cluster; counts every labelled Halted VM, `agent-vm-1` included |
| `-5` | selector matching no node | `no-nodes` |
| `-6` | deployed, scaled to 0 | `no-agent` (the router's 503) |
| `-7` | in day1 only, no release | `no-agent` (503 for an unknown host) |
| `-8` | its kv-capacity holds another token | `refused-token` |
| `ocp4-kubevirt-lab-b-1` | control-plane node, `vm.storageClassName` = a WaitForFirstConsumer class | the 10 min rollback |

Storage: `ocs-external-storagecluster-cephfs` for the clusters that must succeed. **The plan's
`ocs-external-storagecluster-ceph-rbd-virtualization` refuses vm.yaml's root disk** (`ReadWriteMany`
+ `Filesystem`: `multi node access modes are only supported on rbd block type volumes`, found by a
one-minute pre-flight DataVolume) — a cluster's class must have that pair in its StorageProfile.
Network: vm.yaml's multus default, untouched — a Halted VM never resolves it.

## Rebuild it

```bash
oc login …                                   # cluster-admin on the lab cluster
./lab.sh images                              # BuildConfigs in ns kv-agents → the two images in the internal registry
export KV_CAPACITY_TOKEN=$(openssl rand -hex 16) KV_VM_API_TOKEN=$(openssl rand -hex 16)
./lab.sh up                                  # Secrets + 8 release pairs; prints pods, routes
eval "$(./lab.sh seed-day1)"                 # the mock day1 → a local bare repo; DAY1_* + GIT_CONFIG_GLOBAL env
eval "$(./lab.sh env)"                       # KV_*_URL_TEMPLATE, tokens, HEADROOM_BUFFER_PERCENT, VM_MAC_PREFIX
temporal server start-dev --headless --port 7234 --db-filename /tmp/create-vm-lab.db &
TEMPORAL_HOST=localhost:7234 .venv/bin/python -m workflow_domains.main_worker_init &
TEMPORAL_HOST=localhost:7234 .venv/bin/python -m activities.vm_lifecycle.worker_init &
TEMPORAL_HOST=localhost:7234 .venv/bin/uvicorn workflow_domains.api:app --port 8081 &
curl -s -X POST localhost:8081/workflows/vm-lifecycle/create-vm -H 'content-type: application/json' \
  -d '{"mce_cluster":"ocp4-lab-mce-lab-a","infra_env":"x201-zone-a-2","namespace":"redfish-vms"}'
curl -s localhost:8081/workflows/runs/create-vm-ocp4-lab-mce-lab-a_x201-zone-a-2_1
./lab.sh down                                # ONLY our VMs (ocp-x201-*), the releases, ns kv-agents
```

The day1 trick: the limb injects its token into `https://` URLs only, so `seed-day1` writes a
gitconfig mapping `https://git.test/` (with and without the token) onto the bare repo and hands the
limb `GIT_CONFIG_GLOBAL` — the same `insteadOf` trick `tests/test_git_repo.py` uses. Nothing is
pushed to the real day1 repo: its `main` is what Argo CD renders.

## What ran, and what it showed

Pod sizes under the convention: CPU `max(1, cpu × 15 // 16)` cores, memory `cpu × 8 + 1` GiB.
Live requests when it ran: control-plane 15.7 cores / 40.8 GiB, worker-1 5.8 / 18.2, worker-2
4.1 / 11.0.

| run | request | buffer | outcome |
|---|---|---|---|
| 1 | `x201-zone-a-2` | 30 | COMPLETED on `-4` (cpu 59% after, vs `-1` 53%): `ocp-x201-zone-a-2-68ef15`, MAC `02:a1:b2:68:ef:15`, Halted, `redfish-enabled=true`, requests cpu 1 / 16Gi, DataVolume Succeeded on cephfs in 20 s; the table shows all eight clusters, `-2`/`-3` `fit-node`, `-5` `no-nodes`, `-6`/`-7` `no-agent`, `-8` `refused-token` |
| 2 | `x201-zone-a-2` | 30 | COMPLETED on `-1`: `-4` now `fit-buffer` with `halted=2` (run 1's VM + `agent-vm-1`), memory 70% after > the ceiling — **a Halted reservation tipped the next create elsewhere** |
| 3 | `x201-zone-a-8` (7 cores / 65 GiB) | 30 | FAILED `NoKvClusterFitsError`: `-1`, `-4` fit-buffer; `-2`, `-3` fit-node; the rest as above — every cluster named under its reason, nothing created |
| 4 | `x201-zone-a-16` (15 cores / 129 GiB) | 30 | FAILED `NoKvClusterFitsError`: `fit-node` on all four readable clusters (the largest node holds 83 GiB) |
| 5 | `x201-zone-c-2` | 30 | FAILED `NoKvClusterError` before any cluster was read (no `ocp4-seed-lab-c/` folder) |
| 6 | `x201-zone-a-2`, `count: 2` | 30 | two slots started at once (`_1`, `_2`), BOTH FAILED `NoKvClusterFitsError`: with three Halted VMs reserved the control-plane clusters are past the buffer |
| 7 | `x201-zone-b-2` | 30 | FAILED `VmRootDiskNotBoundError` after 10 min: the DataVolume sat in `PendingPopulation` (WFFC), the run deleted the VM and the DataVolume (`removing-vm`), nothing left on the cluster. The limb was RESTARTED during the wait: attempt 2 resumed the clock from the heartbeat and ended at 10:01 total |
| 8 | `x201-zone-a-1` (1 core / 9 GiB) | 5 | COMPLETED on `-3` (worker-2, memory 89% after) over `-4` (84%) and `-1` (66%) — **best-fit on real numbers**, the fullest fitting cluster wins |
| 9 | `x201-zone-a-1`, `count: 2` | 5 | COMPLETED twice, BOTH on `-4` with distinct names and MACs: the two runs ranked before either created, so `-3` (now `fit-buffer`, `halted=1` from run 8) was skipped by both and `-4` taken twice — **reservations protect sequential creates only** |
| 10 | `x201-zone-b-2`, wrong `KV_CAPACITY_TOKEN` | 30 | FAILED `KvAgentAuthError` after one attempt: every cluster of the zone refused |
| 11 | `x201-zone-a-2`, wrong `KV_CAPACITY_TOKEN` | 30 | COMPLETED on `-8` — whose "wrong" token was exactly the one the limb now sent; the other seven are `refused-token` / `no-agent` rows. One cluster holding a different fleet token is a skip, not a failure |

API edges: `-metal` InfraEnv, `-0`, an MCE not named `ocp4-<env>-mce-<site>-<letter>` and
`count: 21` are 422s with the convention in the message.

Measured (one lab VM switched to the pod network and started once): a 16 GiB guest's
virt-launcher requests 17,502,830,593 bytes of memory, **308 MiB over the guest**, cpu `1` as
the template says — the 1 GiB pad in `placement.py` is conservative.

The first run's history is `tests/histories/create_vm/lab-cluster-64spr-create.json`, replayed on
every `pytest` (`tests/test_create_vm_replay.py`).

## What the lab cannot show, and the simulator does

`tests/kv_agents_simulator.py` + `tests/test_create_vm_e2e.py` run the real workflow and the real
limb (its httpx clients, its git clone, its heartbeating wait) against a fleet of simulated
clusters in every state: a twelve-cluster ranking with a tie, `malformed` and `unreachable`
agents (every bad host on one apps domain answers the router's 503, so the lab only shows
`no-agent`), a name born in between (409 → redraw), an answer lost after the create (the retry
finds its own VM), a VM replaced behind the run (the rollback leaves it), the namespace / template
/ token failures. Placement's boundaries are `tests/test_vm_lifecycle_placement.py::TestEdges`.
