# upi-provisioning-worker

The upi-lifecycle domain's HUB limb: ONE release, on the hub, in the workflows
namespace. It serves `create-upi-cluster` (docs/site/upi-lifecycle/create-upi-cluster.html).
Vendor it into redbull-platform as `gitops/charts/upi-provisioning-worker/` with its
`gitops/services/upi-provisioning-worker/app.yaml` to deploy it (CLAUDE.md §1).

Before it: the Secret `installer.secretName` (`pull-secret.json`,
`additional-trust-bundle.pem`, `ssh.pub`, `vsphere-password`), and
`installer.sites` filled with bootstrap-installer's site profiles.

## What it creates, per cluster (in its namespace)

| object | name | lifetime |
|---|---|---|
| PersistentVolumeClaim | `upi-<cluster>-work` | **kept for ever** — terraform's state is on it. Delete it only after `bootstrap.py --destroy`. |
| ConfigMap | `upi-<cluster>-config` | the cluster YAML bootstrap-installer reads |
| Job | `upi-<cluster>-prepare-<token>`, `upi-<cluster>-install-<token>` | 7 days after it ends |
| Secret | `upi-<cluster>-kubeconfig` | the admin kubeconfig and the node IPs (written by the prepare Job) |
| Secret (Argo CD's namespace) | `cluster-<cluster>` | the Argo CD cluster registration |

## RBAC

| who | verbs | resource | used by |
|---|---|---|---|
| worker | get, create | jobs | run_installer_step |
| worker | get, create | persistentvolumeclaims | run_installer_step |
| worker | get, create, update | configmaps | run_installer_step (sites, the cluster YAML) |
| worker | list; get | pods; pods/log | run_installer_step (a failed Job's log tail) |
| worker | get | secrets | await_upi_cluster_ready, register_upi_cluster_in_argocd (the kubeconfig) |
| worker | get, create, update | secrets in `argocd.namespace` | register_upi_cluster_in_argocd |
| installer Jobs | get, create, patch, update | secrets | the prepare Job's kubeconfig Secret |
