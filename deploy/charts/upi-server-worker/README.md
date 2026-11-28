# upi-server-worker

The upi-lifecycle domain's per-cluster limb: ONE release per UPI cluster, installed
INSIDE it (through that cluster's day2 `in-cluster` / `upi` folder, or by hand). It
serves `add-server-to-upi` (docs/site/upi-lifecycle/add-server-to-upi.html).

```bash
helm upgrade --install upi-server-worker deploy/charts/upi-server-worker -n redbull-workflows \
  --set upiCluster=ocp4-prod-mce-tlv-a \
  --set temporal.host=temporal-frontend.apps.hub.example.net:443 --set temporal.tls.enabled=true \
  --set-file temporal.tls.caCert=hub-ca.crt \
  --set serverScan.url=https://server-scan.apps.hub.example.net \
  --set iso.route.host=upi-isos.apps.ocp4-prod-mce-tlv-a.example.net
```

Before it: the Secret `credentials.existingSecret` (`SERVER_SCAN_API_TOKEN` with the
ADMIN role, and `<VENDOR>_BMC_USERNAME` / `_PASSWORD` for DELL, HP, INTERSIGHT).

## What it runs

- the worker (`activities.upi_server.worker_init`), polling `upi-server-activity-<upiCluster>`;
- an nginx sidecar serving the ISO volume on 8080, behind the Service and a plain-HTTP
  Route — the URL the BMCs mount (`ISO_BASE_URL`). ONE replica (`Recreate`): the ISO is
  on the volume of the pod that built it.

## RBAC

| verb | resource | used by |
|---|---|---|
| cluster-admin (`rbac.clusterAdmin: true`, the default) | — | `oc adm node-image create` (node_image.py): a namespace and a pod to build the ISO, the release, the pull secret |
| get, patch | nodes | inspect_upi_node, await_node_joined, finish_upi_node (labels) |
| get, list | certificatesigningrequests | await_node_joined |
| update | certificatesigningrequests/approval | await_node_joined (only this host's CSRs) |
| approve | signers kube-apiserver-client-kubelet, kubelet-serving | await_node_joined |

With `rbac.clusterAdmin: false` only the rows below the first are granted.
