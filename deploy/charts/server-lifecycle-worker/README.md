# server-lifecycle-worker

The install-server activity worker. **One release per MCE cluster, installed
inside that MCE.** It dials out to the hub's Temporal, polls
`server-lifecycle-activity-<mceCluster>`, and creates BareMetalHosts, Secrets
and NMStateConfigs on its own cluster as its own ServiceAccount — no kubeconfig
of any cluster is stored anywhere.

This README covers deploying the worker on remote MCEs that reach the hub's
Temporal **over TLS through an OpenShift Route with HTTP/2 enabled**.

---

## Architecture

```
MCE worker ──TLS + HTTP/2 :443──► hub router (reencrypt, HTTP/2 on) ──TLS + HTTP/2──► temporal-frontend:7233
hub clients ─────────────────────────── TLS ─────────────────────────────────────────► temporal-frontend:7233
```

- Workers only make **outbound** connections to the hub. Nothing on an MCE is
  exposed, and the hub never connects to an MCE.
- Temporal decides which worker gets a task by **task queue name**: a run for
  MCE `X` sends its cluster work to `server-lifecycle-activity-X`, which only
  the worker with `mceCluster: X` polls.
- **One certificate** (signed by a static CA) is used both by Temporal and by
  the Route. Workers and hub clients trust the **CA**, so the certificate
  renewing never requires a change on any client.
- Once Temporal has TLS, port 7233 accepts TLS only, so the clients already on
  the hub switch to TLS in the same window (step 3).

| Component | Changes |
|---|---|
| Hub router (default IngressController) | HTTP/2 enabled (cluster-wide) |
| Temporal | TLS on the frontend |
| Hub clients (orchestrator, API, hub workers, Temporal UI) | CA + two env vars |
| MCEs | This chart, with TLS values |

**Placeholders** used below: `<hub-domain>` (the hub's cluster domain, so the
Route host is `temporal-grpc.apps.<hub-domain>`), `<mce-egress-cidrs>` (the
MCEs' egress IPs, space-separated), `<tag>` (an image tag that includes TLS
support).

---

## Prerequisites

- OpenShift 4.19+ on the hub (for `spec.tls.externalCertificate` on Routes).
- cert-manager on the hub.
- Images built from a commit that includes `shared/temporal_client.py`
  (TLS support). Older images ignore the `TEMPORAL_TLS_*` settings.
- DNS on the MCEs resolves `*.apps.<hub-domain>`.
- Network: MCE nodes (or their EgressIPs) → hub ingress VIP, **TCP 443**,
  return traffic allowed, idle timeout above 30 s (the SDK pings every 30 s).

---

## Step 0 — Deploy images with TLS support (no behaviour change)

Roll the new images out to the hub first. With no `TEMPORAL_TLS_*` values set
they connect exactly as before, so this step is safe on its own.

## Step 1 — CA and certificate (hub, namespace `temporal`)

Create the CA **once**, on a workstation. It lasts 20 years so the 10-year
certificate it signs fits inside it (a certificate can never outlive its CA).

```bash
openssl req -x509 -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout ca.key -out ca.crt -days 7300 \
  -subj "/CN=redbull-temporal-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
```

Keep `ca.key` offline and backed up: anyone holding it can mint certificates
the workers will trust. `ca.crt` is public.

```yaml
# The CA. Holds a PRIVATE key → deliver as a SealedSecret / ExternalSecret,
# never as plain YAML in git.
apiVersion: v1
kind: Secret
metadata:
  name: temporal-ca
  namespace: temporal
type: kubernetes.io/tls
data:
  tls.crt: <base64 of ca.crt>
  tls.key: <base64 of ca.key>
---
# cert-manager SIGNS with this CA and never renews it, so the CA the clients
# hold never changes.
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: temporal-ca
  namespace: temporal
spec:
  ca:
    secretName: temporal-ca
---
# ONE certificate for Temporal and the Route: every name a client dials.
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: temporal-frontend-tls
  namespace: temporal
spec:
  secretName: temporal-frontend-tls          # tls.crt, tls.key, ca.crt
  dnsNames:
    - temporal-grpc.apps.<hub-domain>        # MCE workers, through the Route
    - temporal-frontend.temporal.svc         # hub clients
    - temporal-frontend.temporal.svc.cluster.local
  duration: 87600h                           # 10 years
  renewBefore: 720h                          # renewed 30 days before expiry
  privateKey:
    algorithm: ECDSA
    size: 256
    rotationPolicy: Always                   # fine: clients trust the CA, not this key
  issuerRef:
    name: temporal-ca
    kind: Issuer
```

Check:

```bash
oc wait certificate/temporal-frontend-tls -n temporal --for=condition=Ready --timeout=120s
```

## Step 2 — TLS on Temporal (Temporal release values)

> **Steps 2 and 3 are one maintenance window.** From the moment Temporal's
> frontend switches to TLS until the hub clients restart with the CA, they
> cannot reach Temporal. Running workflows are safe — their state is in
> Temporal's database — and continue once workers reconnect.

Shape for the official `temporalio/temporal` chart. Other charts use different
keys for the same two things: mount the Secret into **every** Temporal pod, and
put the `tls` block into Temporal's server config under `global`.

```yaml
server:
  additionalVolumes:
    - name: temporal-tls
      secret:
        secretName: temporal-frontend-tls
  additionalVolumeMounts:
    - name: temporal-tls
      mountPath: /certs
      readOnly: true
  config:
    tls:                                 # rendered as global.tls in Temporal's config
      refreshInterval: 1h                # picks up renewals without a restart
      frontend:
        server:
          certFile: /certs/tls.crt
          keyFile: /certs/tls.key
          requireClientAuth: false
        client:                          # Temporal's own services calling the frontend
          serverName: temporal-frontend.temporal.svc
          rootCaFiles:
            - /certs/ca.crt
```

If Temporal runs the `auto-setup` image, check its startup logs after the
switch: it registers the `default` namespace against the frontend and needs
the same CA if that step now fails.

## Step 3 — Hub clients switch to TLS (same window as step 2)

### 3a. The CA in the workflow namespace (public → ConfigMap)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: temporal-ca
  namespace: redbull-workflows
data:
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    <contents of ca.crt>
    -----END CERTIFICATE-----
```

### 3b. Two keys in the global config every hub client reads

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: workflows-orchestrator-config
  namespace: redbull-workflows
data:
  TEMPORAL_HOST: temporal-frontend.temporal.svc.cluster.local:7233
  TEMPORAL_NAMESPACE: default
  SEGMENTS_MANAGER_URL: http://segments-manager.segments-manager.svc.cluster.local:8000
  TEMPORAL_TLS_CA: /etc/temporal-tls/ca.crt                  # new
  TEMPORAL_TLS_SERVER_NAME: temporal-frontend.temporal.svc   # new
```

### 3c. The CA mounted in each hub Deployment

`workflows-orchestrator`, `workflows-orchestrator-api`,
`segment-lifecycle-worker`, `server-lifecycle-worker` (the hub's own) and
`server-provisioning-worker` — the same patch in each chart:

```yaml
spec:
  template:
    spec:
      volumes:
        - name: temporal-ca
          configMap:
            name: temporal-ca
      containers:
        - name: <container name>
          volumeMounts:
            - name: temporal-ca
              mountPath: /etc/temporal-tls
              readOnly: true
```

### 3d. Temporal UI (namespace `temporal`)

```yaml
spec:
  template:
    spec:
      volumes:
        - name: temporal-tls
          secret:
            secretName: temporal-frontend-tls
            items:
              - {key: ca.crt, path: ca.crt}
      containers:
        - name: ui
          env:
            - {name: TEMPORAL_TLS_CA, value: /certs/ca.crt}
            - {name: TEMPORAL_TLS_SERVER_NAME, value: temporal-frontend.temporal.svc}
            - {name: TEMPORAL_TLS_ENABLE_HOST_VERIFICATION, value: "true"}
          volumeMounts:
            - name: temporal-tls
              mountPath: /certs
              readOnly: true
```

### 3e. Restart and check

```bash
oc rollout restart deploy -n redbull-workflows
oc logs -n redbull-workflows deploy/workflows-orchestrator --tail=20     # no connection errors
oc logs -n temporal deploy/temporal-frontend --tail=50 | grep -i -E 'tls|error'
```

## Step 4 — HTTP/2 on the hub router

Only on the **hub**, and only on the IngressController that serves the
Temporal Route (normally `default`). Nothing is needed on the MCEs.

```yaml
apiVersion: operator.openshift.io/v1
kind: IngressController
metadata:
  name: default
  namespace: openshift-ingress-operator
  annotations:
    ingress.operator.openshift.io/default-enable-http2: "true"
```

Annotate the existing object rather than applying a whole IngressController:

```bash
oc annotate ingresscontroller default -n openshift-ingress-operator \
  ingress.operator.openshift.io/default-enable-http2=true
oc rollout status deploy/router-default -n openshift-ingress
```

The router pods restart once; long-lived connections through the router are
reset at that moment. HTTP/2 is used only for routes with their own
certificate — routes on the default `*.apps` certificate keep HTTP/1.1.

## Step 5 — Reencrypt Route to Temporal (hub, namespace `temporal`)

```yaml
# Lets the router read the certificate Secret (externalCertificate).
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: router-read-temporal-frontend-tls
  namespace: temporal
rules:
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [temporal-frontend-tls]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: router-read-temporal-frontend-tls
  namespace: temporal
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: router-read-temporal-frontend-tls
subjects:
  - kind: ServiceAccount
    name: router
    namespace: openshift-ingress
---
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: temporal-frontend-grpc
  namespace: temporal
  annotations:
    # The router parses HTTP in reencrypt mode; its default 30 s server
    # timeout is shorter than a worker's ~70 s long poll.
    haproxy.router.openshift.io/timeout: 5m
    # Temporal has no authentication of its own: admit only the MCEs.
    haproxy.router.openshift.io/ip_whitelist: "<mce-egress-cidrs>"
spec:
  host: temporal-grpc.apps.<hub-domain>
  to:
    kind: Service
    name: temporal-frontend
  port:
    targetPort: 7233              # or the Service port's name, e.g. grpc-rpc
  tls:
    termination: reencrypt
    externalCertificate:
      name: temporal-frontend-tls   # the certificate workers see; renewals picked up automatically
    destinationCACertificate: |     # the router verifies Temporal's certificate with your CA
      -----BEGIN CERTIFICATE-----
      <contents of ca.crt>
      -----END CERTIFICATE-----
    insecureEdgeTerminationPolicy: None
```

## Step 6 — Install this chart on each MCE

The Temporal and TLS values are the **same for every MCE** — put them once in
the chart's `values.yaml`:

```yaml
image:
  tag: <tag>
temporal:
  host: temporal-grpc.apps.<hub-domain>:443
  namespace: default              # the TEMPORAL namespace, must match the hub
  tls:
    enabled: true
    serverName: temporal-grpc.apps.<hub-domain>
    caCert: |
      -----BEGIN CERTIFICATE-----
      <contents of ca.crt>
      -----END CERTIFICATE-----
serverScan:
  url: https://<server-scan-route>/api/v1
```

Per MCE, only what differs (`values-<mce>.yaml`):

```yaml
mceCluster: ocp4-mce-site1-a      # unique per MCE; = the Segments Manager's cluster_name
credentials:
  existingSecret: server-lifecycle-credentials   # from SealedSecret / ExternalSecret
rbac:
  targetNamespaces:
    - <infraenv-namespace>
pxeMap:
  urls:
    site1: http://<pxe-vm-site1>:8080
```

Install with Argo CD (example ApplicationSet in
`deploy/argocd/server-lifecycle-worker-applicationset.yaml`) or by hand:

```bash
helm upgrade --install server-lifecycle-worker deploy/charts/server-lifecycle-worker \
  -n redbull-workflows --create-namespace \
  -f deploy/charts/server-lifecycle-worker/values.yaml \
  -f deploy/charts/server-lifecycle-worker/values-<mce>.yaml
```

The credentials Secret must exist in that namespace first. Its keys:
`SERVER_SCAN_API_TOKEN` (server-scan ADMIN role), `<VENDOR>_BMC_USERNAME` /
`<VENDOR>_BMC_PASSWORD` for HP, DELL, CISCO, INTERSIGHT, and `PXE_MAP_TOKEN`.

---

## Verify

From an MCE (a node or a pod):

```bash
# The router offers HTTP/2 and presents a certificate your CA signed
openssl s_client -connect temporal-grpc.apps.<hub-domain>:443 \
  -servername temporal-grpc.apps.<hub-domain> -CAfile ca.crt -alpn h2 </dev/null \
  | grep -E 'ALPN protocol|Verify return code'
#   expect: "ALPN protocol: h2" and "Verify return code: 0 (ok)"

# A Temporal call end to end
temporal operator cluster health --address temporal-grpc.apps.<hub-domain>:443 \
  --tls --tls-ca-path ca.crt --tls-server-name temporal-grpc.apps.<hub-domain>
#   expect: SERVING
```

On the hub, each MCE worker polls its own queue:

```bash
temporal task-queue describe \
  --task-queue server-lifecycle-activity-<mceCluster> --task-queue-type activity
```

| Symptom | Likely cause |
|---|---|
| `No ALPN negotiated` / `http/1.1` | HTTP/2 not enabled (step 4), or the Route has no own certificate (step 5) |
| `Verify return code` not 0 | Wrong CA on the client, or the certificate lacks the Route host (step 1) |
| Worker logs TLS / handshake errors | Image without TLS support, or `tls.caCert` missing / wrong |
| Router returns 503 | Router cannot verify Temporal (`destinationCACertificate`), or Temporal TLS not on |
| Workers reconnect every ~30 s | Raise `haproxy.router.openshift.io/timeout`; if it persists, the IngressController `tuningOptions.clientTimeout` |
| A run sits in one phase, `activity_queue` named | No worker polls that MCE's queue — check `mceCluster` and the worker's logs |

---

## Certificate lifetime

| Item | Lifetime | When it changes | Client change needed |
|---|---|---|---|
| CA (`ca.crt` / `ca.key`) | 20 years | Only when you replace it | Yes, once: put old + new CA side by side in `caCert` / the ConfigMap, then switch the Issuer |
| Temporal / Route certificate | 10 years, renewed 30 days before expiry | Automatically, by cert-manager | **None** — Temporal reloads it (`refreshInterval`), the router reads it (`externalCertificate`) |

## Rollback

1. Remove the TLS block from Temporal (step 2) and the two `TEMPORAL_TLS_*`
   keys (step 3b); restart Temporal and `oc rollout restart deploy -n redbull-workflows`.
2. Delete the Route and RBAC (step 5).
3. Optionally remove HTTP/2:
   `oc annotate ingresscontroller default -n openshift-ingress-operator ingress.operator.openshift.io/default-enable-http2-`

## Chart values

| Value | Required | Meaning |
|---|---|---|
| `mceCluster` | yes | The MCE this worker serves; names its queue. Unique per MCE |
| `temporal.host` | yes | `host:port` of Temporal as reachable from this MCE |
| `temporal.namespace` | | Temporal namespace (default `default`) |
| `temporal.tls.enabled` | | TLS to Temporal (required through a Route) |
| `temporal.tls.caCert` | with TLS | PEM of the CA that signed Temporal's / the Route's certificate |
| `temporal.tls.serverName` | | Name verified and sent as SNI; defaults to the host part of `temporal.host` |
| `serverScan.url` | yes | server-scan base URL including `/api/v1` |
| `pxeMap.urls` | | Site → PXE VM base URL, for IPMI (UCS) servers |
| `credentials.existingSecret` | | Secret with server-scan token, BMC credentials, PXE token |
| `rbac.targetNamespaces` | | InfraEnv namespaces the worker may write to |
| `image.repository` / `image.tag` | | Mirror path and tag in a disconnected environment |
| `replicaCount` | | Pods polling the same queue (load sharing + failover) |
