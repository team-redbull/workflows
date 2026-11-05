# Mock day1 for the create-vm lab

A day1-SHAPED tree, never the real day1 repo: create-vm lists a zone's KubeVirt
clusters from `sites/<site>/mces/ocp4-seed-<site>-<zone>/hostedClusters/`, and
the real repo's `main` is what Argo CD renders, so mock cluster files must never
land there. `lab.sh seed-day1` turns this folder into a local bare repo served
to the limb over the `insteadOf` trick (`DAY1_REPO_URL=https://git.test/day1-lab.git`).

What the files exercise (site `lab`, from MCE `ocp4-lab-mce-lab-a`):

| path | what create-vm sees |
|---|---|
| `ocp4-seed-lab-a/hostedClusters/ocp4-kubevirt-lab-a-{1..8}.yaml` | zone `a`'s eight KubeVirt clusters — each simulated by one pair of agent releases on cluster-64spr (see `../lab.sh`); `-4` carries its own `clusterName`, the rest derive it from the file name |
| `ocp4-seed-lab-a/hostedClusters/ocp4-kubevirt-lab-a-3.yaml` | a duplicate key: a `problems` line in the limb's log, never a failure |
| `ocp4-seed-lab-a/hostedClusters/ocp4-prep-other-lab-a.yaml` | a file in the seed folder NOT named `ocp4-kubevirt-lab-a…`: listed by the limb, filtered by the workflow |
| `ocp4-seed-lab-a/hostedClusters/README.md` | not a cluster file: ignored by the limb |
| `ocp4-seed-lab-b/hostedClusters/ocp4-kubevirt-lab-b-1.yaml` | zone `b`: ONE cluster whose kv-vm-api uses a WaitForFirstConsumer storage class — the rollback scenario |
| (no `ocp4-seed-lab-c/`) | zone `c`: `NoKvClusterError` |
| `ocp4-lab-mce-lab-a/hostedClusters/ocp4-lab-tenant-lab-a.yaml` | an MCE's own hosted cluster: never a candidate |
