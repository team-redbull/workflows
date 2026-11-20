#!/usr/bin/env bash
# The create-vm lab's BASE services on a disposable OpenShift cluster, through Argo CD:
# an in-cluster git server (Gitea), Temporal, OpenShift Virtualization, the `inventory`
# namespace. README.md next to this file says what each piece is and why.
#
#   ./lab.sh up        deploy or converge everything (safe to re-run)
#   ./lab.sh status    what is running, every URL, where the credentials live
#   ./lab.sh verify    Temporal's default namespace + one Halted VM with a bound root disk
#                      + the duplicate-MAC answer; leaves no VM behind
#   LAB_CONFIRM=down ./lab.sh down     remove everything `up` created
#
# It talks to the cluster `oc` is logged in to and refuses any other than LAB_API_SERVER.
# It never writes a credential to disk: they are generated on first `up`, live in cluster
# Secrets (README), and are read back from there.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LAB_API_SERVER="${LAB_API_SERVER:-https://api.cluster-qcl8n.dyn.redhatworkshops.io:6443}"
# The owner's platform repo, READ ONLY: `git archive` of one commit, nothing else.
PLATFORM_REPO="${PLATFORM_REPO:-$(cd "$HERE/../../../.." && pwd)/redbull-platform}"
PLATFORM_SHA="${PLATFORM_SHA:-203bbf35111cb3a611d8cbba87cdf52af52d478d}"   # origin/main, 2026-11-20
PLATFORM_PATHS=(gitops/charts/temporal gitops/charts/openshift-virtualization)
# Where the lab repo is assembled before it is pushed. Rebuilt on every `up`.
LAB_WORKDIR="${LAB_WORKDIR:-${TMPDIR:-/tmp}/redbull-lab-create-vm}"

ARGO_NS=openshift-gitops
GIT_NS=lab-git
LABEL_KEY=redbull.io/lab
LABEL="$LABEL_KEY=create-vm"
GIT_SVC_URL=http://gitea-http.lab-git.svc.cluster.local:3000
GIT_ORG=redbull
GIT_REPO=platform
APPS=(lab-create-vm-git lab-create-vm-extras lab-create-vm-temporal lab-create-vm-openshift-virtualization)
VM_NS=inventory
VM_SC=ocs-external-storagecluster-cephfs
VM_MAC=0e:00:63:00:00:01

log() { printf '\n== %s\n' "$*" >&2; }
die() { printf 'lab.sh: %s\n' "$*" >&2; exit 1; }

guard() {
  local server
  server="$(oc whoami --show-server 2>/dev/null)" || die "oc is not logged in"
  [ "$server" = "$LAB_API_SERVER" ] ||
    die "oc points at $server, the lab is $LAB_API_SERVER (set LAB_API_SERVER to use another cluster)"
}

secret_value() { # namespace name key
  oc -n "$1" get secret "$2" -o "jsonpath={.data.$3}" | base64 -d
}

git_host() { oc -n "$GIT_NS" get route gitea -o 'jsonpath={.spec.host}'; }

# Gitea's API as the admin, over the Route. The password goes to curl on stdin, never argv.
gitea_api() { # method path [json body]
  local method="$1" path="$2" body="${3:-}" user pass
  user="$(secret_value "$GIT_NS" gitea-admin username)"
  pass="$(secret_value "$GIT_NS" gitea-admin password)"
  printf 'user = "%s:%s"\n' "$user" "$pass" |
    curl -sS -K - -X "$method" -H 'Content-Type: application/json' \
      ${body:+-d "$body"} "https://$(git_host)/api/v1$path"
}

app_state() {
  oc -n "$ARGO_NS" get application "$1" \
    -o 'jsonpath={.status.sync.status}/{.status.health.status}' 2>/dev/null || true
}

wait_app() { # name [seconds]
  local name="$1" deadline=$(( $(date +%s) + ${2:-900} )) state=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(app_state "$name")"
    [ "$state" = "Synced/Healthy" ] && { echo "$name: $state" >&2; return 0; }
    sleep 10
  done
  oc -n "$ARGO_NS" get application "$name" \
    -o 'jsonpath={.status.operationState.message}{"\n"}{.status.conditions}{"\n"}' >&2 || true
  die "$name is $state, not Synced/Healthy"
}

# ---------------------------------------------------------------------------------- up

up_git() {
  log "git server (Gitea) in $GIT_NS"
  # The namespace and the admin's generated password come first: the chart reads the
  # Secret, and a generated credential cannot come from git.
  oc get namespace "$GIT_NS" >/dev/null 2>&1 || oc create namespace "$GIT_NS"
  oc label namespace "$GIT_NS" "$LABEL" --overwrite >/dev/null
  if ! oc -n "$GIT_NS" get secret gitea-admin >/dev/null 2>&1; then
    oc -n "$GIT_NS" create secret generic gitea-admin \
      --from-literal=username=lab-admin --from-literal=password="$(openssl rand -hex 20)"
  fi
  oc -n "$GIT_NS" label secret gitea-admin "$LABEL" --overwrite >/dev/null

  oc apply -f "$HERE/argocd/project.yaml" -f "$HERE/argocd/app-git.yaml"
  wait_app lab-create-vm-git 600

  local host i
  host="$(git_host)"
  for i in $(seq 1 30); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "https://$host/api/healthz")" = 200 ] && break
    [ "$i" = 30 ] && die "https://$host/api/healthz does not answer 200"
    sleep 5
  done

  # The organisation every lab repo lives under (a push to redbull/<new>.git creates it).
  if [ "$(gitea_api GET "/orgs/$GIT_ORG" | jq -r '.username // empty')" != "$GIT_ORG" ]; then
    gitea_api POST /orgs "{\"username\":\"$GIT_ORG\",\"visibility\":\"private\"}" >/dev/null
  fi

  # Two access tokens of the admin: one that pushes, one that only reads (Argo CD's).
  # Gitea shows a token once, so they are kept in a Secret; lost Secret = new tokens.
  if ! oc -n "$GIT_NS" get secret lab-git-tokens >/dev/null 2>&1; then
    local user rw ro
    user="$(secret_value "$GIT_NS" gitea-admin username)"
    gitea_api DELETE "/users/$user/tokens/lab-rw" >/dev/null || true
    gitea_api DELETE "/users/$user/tokens/lab-ro" >/dev/null || true
    rw="$(gitea_api POST "/users/$user/tokens" \
      '{"name":"lab-rw","scopes":["write:repository","read:organization","read:user"]}' | jq -r .sha1)"
    ro="$(gitea_api POST "/users/$user/tokens" \
      '{"name":"lab-ro","scopes":["read:repository"]}' | jq -r .sha1)"
    [ "${#rw}" = 40 ] && [ "${#ro}" = 40 ] || die "Gitea did not return the two tokens"
    oc -n "$GIT_NS" create secret generic lab-git-tokens \
      --from-literal=username="$user" --from-literal=rw-token="$rw" --from-literal=ro-token="$ro"
  fi
  oc -n "$GIT_NS" label secret lab-git-tokens "$LABEL" --overwrite >/dev/null

  # Argo CD reads every repo of this Gitea with the read-only token (a credential
  # TEMPLATE: it matches by URL prefix, so a later repo needs nothing more).
  oc -n "$ARGO_NS" create secret generic lab-create-vm-git-creds \
    --from-literal=type=git --from-literal=url="$GIT_SVC_URL/" \
    --from-literal=username=x-access-token \
    --from-literal=password="$(secret_value "$GIT_NS" lab-git-tokens ro-token)" \
    --dry-run=client -o yaml | oc apply -f - >/dev/null
  oc -n "$ARGO_NS" label secret lab-create-vm-git-creds \
    argocd.argoproj.io/secret-type=repo-creds "$LABEL" --overwrite >/dev/null
}

# The lab repo = the platform's two charts as committed at PLATFORM_SHA + platform-extra/
# of this folder + ONE patch. Only the paths this lab owns are replaced; anything else
# already in the repo (charts pushed later for the workers) is left alone.
push_platform() {
  log "lab repo $GIT_ORG/$GIT_REPO"
  git -C "$PLATFORM_REPO" cat-file -e "$PLATFORM_SHA^{commit}" 2>/dev/null ||
    die "$PLATFORM_REPO has no commit $PLATFORM_SHA (set PLATFORM_REPO / PLATFORM_SHA)"
  [ -n "$LAB_WORKDIR" ] || die "LAB_WORKDIR is empty"
  local work="$LAB_WORKDIR/platform" token url path
  token="$(secret_value "$GIT_NS" lab-git-tokens rw-token)"
  # In memory only: never a remote of the work tree, never the laptop's credential helper.
  url="https://x-access-token:${token}@$(git_host)/$GIT_ORG/$GIT_REPO.git"
  scrub() { sed "s/${token}/***/g"; }
  git_lab() { git -C "$work" -c credential.helper= -c user.name=lab -c user.email=lab@redbull.invalid "$@"; }

  rm -rf "$work"
  mkdir -p "$work"
  git -C "$work" init -q -b main
  if git_lab ls-remote --exit-code "$url" refs/heads/main >/dev/null 2>&1; then
    git_lab fetch -q "$url" main 2>&1 | scrub >&2
    git_lab reset -q --hard FETCH_HEAD
  fi

  for path in "${PLATFORM_PATHS[@]}" lab/extras; do rm -rf "${work:?}/$path"; done
  git -C "$PLATFORM_REPO" archive "$PLATFORM_SHA" "${PLATFORM_PATHS[@]}" | tar -x -C "$work"
  cp -R "$HERE/platform-extra/." "$work/"

  # THE ONE PATCH to the platform's charts. Its HyperConverged asks for
  # `uninstallStrategy: BlockUninstallIfWorkloadExist`; HCO 4.22.16 accepts only
  # `BlockUninstallIfWorkloadsExist` (or RemoveWorkloads) and refuses the object, so the
  # Application never gets past wave 10. The template hard-codes it: no value to override.
  local hco="$work/gitops/charts/openshift-virtualization/templates/hyperconverged.yaml"
  if grep -q 'uninstallStrategy: BlockUninstallIfWorkloadExist$' "$hco"; then
    perl -pi -e 's/uninstallStrategy: BlockUninstallIfWorkloadExist$/uninstallStrategy: BlockUninstallIfWorkloadsExist/' "$hco"
  fi
  grep -q 'uninstallStrategy: BlockUninstallIfWorkloadsExist$' "$hco" ||
    die "the uninstallStrategy line of $hco is not what the patch expects"

  git_lab add -A
  if git_lab diff --cached --quiet; then
    echo "lab repo already holds this content ($(git_lab rev-parse --short HEAD))" >&2
  else
    git_lab commit -q -m "lab: charts from redbull-platform ${PLATFORM_SHA:0:7} + lab extras + uninstallStrategy patch"
    git_lab push -q "$url" HEAD:refs/heads/main 2>&1 | scrub >&2
    echo "pushed $(git_lab rev-parse --short HEAD)" >&2
  fi
}

up_apps() {
  log "Argo CD Applications"
  oc apply -f "$HERE/argocd/app-extras.yaml" -f "$HERE/argocd/app-temporal.yaml" \
    -f "$HERE/argocd/app-openshift-virtualization.yaml"
  # The temporal namespace is created by its own Application; the relay in extras
  # lands on a retry once it exists.
  wait_app lab-create-vm-temporal 900
  wait_app lab-create-vm-extras 600
  wait_app lab-create-vm-openshift-virtualization 1800
  log "HyperConverged"
  oc -n openshift-cnv wait hyperconverged kubevirt-hyperconverged \
    --for=condition=Available --timeout=30m
}

up() {
  guard
  up_git
  push_platform
  up_apps
  status
}

# ------------------------------------------------------------------------------ status

status() {
  guard
  log "Argo CD Applications ($LABEL)"
  oc -n "$ARGO_NS" get applications.argoproj.io -l "$LABEL" 2>&1 || true
  log "namespaces ($LABEL)"
  oc get namespaces -l "$LABEL" 2>&1 || true
  log "pods"
  local ns
  for ns in "$GIT_NS" temporal openshift-cnv; do
    printf '%-14s %s\n' "$ns" "$(oc -n "$ns" get pods --no-headers 2>/dev/null |
      awk '{n[$3]++} END {for (s in n) printf "%s=%d ", s, n[s]}')"
  done
  log "HyperConverged"
  oc -n openshift-cnv get hyperconverged kubevirt-hyperconverged \
    -o 'jsonpath={range .status.conditions[*]}{.type}={.status} {end}{"\n"}' 2>&1 || true
  oc -n openshift-cnv get csv -l operators.coreos.com/kubevirt-hyperconverged.openshift-cnv \
    -o 'jsonpath={range .items[*]}{.metadata.name} {.status.phase}{"\n"}{end}' 2>&1 || true
  log "addresses"
  local host
  host="$(git_host 2>/dev/null || true)"
  cat <<EOF
git, in-cluster http   $GIT_SVC_URL/$GIT_ORG/<repo>.git
git, https (Route)     https://$host/$GIT_ORG/<repo>.git
git web UI             https://$host/   (user/password: Secret $GIT_NS/gitea-admin)
git tokens             Secret $GIT_NS/lab-git-tokens  keys rw-token, ro-token
Temporal, in-cluster   temporal-frontend.temporal.svc.cluster.local:7233  (namespace default)
Temporal from laptop   oc -n temporal port-forward deploy/temporal-grpc-relay 7233:7233
Temporal UI            oc -n temporal port-forward svc/temporal-ui 8080:8080
VM namespace           $VM_NS
EOF
}

# ------------------------------------------------------------------------------ verify

vm_manifest() { # name
  sed "s/__NAME__/$1/g" "$HERE/test/vm-halted.yaml"
}

verify() {
  guard
  log "Temporal: namespace default, asked from inside the cluster"
  oc -n temporal exec deploy/temporal-frontend -c temporal -- \
    temporal operator namespace describe -n default \
    --address temporal-frontend.temporal.svc.cluster.local:7233 | sed -n '1,8p'

  log "git: the workers' clone shape, from a pod (https + x-access-token, no TLS verify)"
  local token host
  token="$(secret_value "$GIT_NS" lab-git-tokens ro-token)"
  host="$(git_host)"
  oc -n "$GIT_NS" delete pod lab-git-clone-check --ignore-not-found >/dev/null
  oc -n "$GIT_NS" run lab-git-clone-check --restart=Never --rm -i --quiet \
    --image=image-registry.openshift-image-registry.svc:5000/openshift/tools:latest \
    --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"lab-git-clone-check","image":"image-registry.openshift-image-registry.svc:5000/openshift/tools:latest","stdin":true,"stdinOnce":true,"command":["sh","-c","read -r url; cd /tmp && HOME=/tmp git -c http.sslVerify=false clone -q \"$url\" c && git -C c log --oneline -1"],"securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}}]}}' \
    <<<"https://x-access-token:${token}@${host}/$GIT_ORG/$GIT_REPO.git" 2>&1 | sed "s/${token}/***/g"

  log "a Halted VM with a 2Gi RWX Filesystem root disk on $VM_SC, MAC $VM_MAC"
  oc -n "$VM_NS" delete vm lab-verify-vm lab-verify-vm-dup --ignore-not-found >/dev/null
  vm_manifest lab-verify-vm | oc -n "$VM_NS" create -f -
  local i
  for i in $(seq 1 30); do
    oc -n "$VM_NS" get dv lab-verify-vm-root >/dev/null 2>&1 && break
    sleep 2
  done
  oc -n "$VM_NS" wait dv lab-verify-vm-root --for=jsonpath='{.status.phase}'=Succeeded --timeout=10m
  oc -n "$VM_NS" get vm lab-verify-vm -o json | jq -r '
    "VM created   \(.metadata.creationTimestamp)  printableStatus=\(.status.printableStatus)  runStrategy=\(.spec.runStrategy)"'
  oc -n "$VM_NS" get dv lab-verify-vm-root -o json | jq -r '
    "DV Succeeded \(.status.conditions[] | select(.type=="Ready") | .lastTransitionTime)  phase=\(.status.phase)"'
  oc -n "$VM_NS" get pvc lab-verify-vm-root -o json | jq -r '
    "PVC          \(.status.phase) \(.spec.accessModes|join(",")) \(.spec.volumeMode) \(.status.capacity.storage) \(.spec.storageClassName)"'
  oc -n "$VM_NS" get vmi lab-verify-vm >/dev/null 2>&1 && die "the Halted VM has a VMI"

  # KubeMacPool (CNV 4.22.16) does NOT refuse it: 201 Created. Its collision metric
  # (kmp_mac_collisions) stayed empty for two Halted VMs, so nothing flags it either.
  log "a second VM with the same MAC: the API's own status line"
  vm_manifest lab-verify-vm-dup | oc -n "$VM_NS" create -v=6 -f - 2>&1 |
    grep -E '"Response" verb="POST"|created|Error from server' | sed 's/^.*"Response" //' || true
  oc -n "$VM_NS" get vm -o json | jq -r '.items[] |
    "\(.metadata.name)  mac=\(.spec.template.spec.domain.devices.interfaces[0].macAddress)  \(.status.printableStatus)"'

  log "cleanup"
  oc -n "$VM_NS" delete vm lab-verify-vm lab-verify-vm-dup --ignore-not-found --wait
  for i in $(seq 1 30); do
    [ -z "$(oc -n "$VM_NS" get dv,pvc -o name 2>/dev/null | grep lab-verify-vm || true)" ] && break
    sleep 2
  done
  oc -n "$VM_NS" get vm,dv,pvc 2>&1
}

# -------------------------------------------------------------------------------- down

down() {
  guard
  [ "${LAB_CONFIRM:-}" = down ] || die "this deletes the whole lab; run: LAB_CONFIRM=down $0 down"

  # 1. CNV refuses to uninstall while a VM exists (BlockUninstallIfWorkloadsExist).
  log "VMs"
  if oc get crd virtualmachines.kubevirt.io >/dev/null 2>&1; then
    oc -n "$VM_NS" delete vm --all --wait --ignore-not-found || true
    [ -z "$(oc get vm -A -o name 2>/dev/null)" ] ||
      die "VirtualMachines exist outside $VM_NS; delete them first: oc get vm -A"
  fi

  # 2. The HyperConverged goes while its operator still runs: stop Argo CD healing it
  #    back, delete it, and wait for the operator to finish its own teardown.
  log "HyperConverged"
  if oc -n "$ARGO_NS" get application lab-create-vm-openshift-virtualization >/dev/null 2>&1; then
    oc -n "$ARGO_NS" patch application lab-create-vm-openshift-virtualization --type merge \
      -p '{"spec":{"syncPolicy":{"automated":null}}}'
  fi
  #    (That teardown is also what removes the KubeVirt and CDI CRDs: only 8 of the 41
  #    carry OLM's label, the rest belong to the component operators.)
  if oc get crd hyperconvergeds.hco.kubevirt.io >/dev/null 2>&1; then
    oc -n openshift-cnv delete hyperconverged kubevirt-hyperconverged \
      --ignore-not-found --wait --timeout=20m || true
    [ -z "$(oc -n openshift-cnv get hyperconverged -o name 2>/dev/null)" ] ||
      die "the HyperConverged is still there; nothing else was removed. See its conditions, then re-run"
  fi

  # 3. The Applications that render from the lab repo go BEFORE the git server: a
  #    finalizer's cascade needs a render, and a render needs the repo (the platform
  #    repo's CLAUDE.md has the incident). extras has no finalizer: it only disappears.
  log "Applications rendered from the lab repo"
  oc -n "$ARGO_NS" delete application lab-create-vm-extras --ignore-not-found --wait
  oc -n "$ARGO_NS" delete application lab-create-vm-temporal \
    lab-create-vm-openshift-virtualization --ignore-not-found --wait --timeout=15m

  # 4. What OLM leaves behind once the Subscription is gone (Red Hat's uninstall steps).
  log "CNV operator, namespace and CRDs"
  oc -n openshift-cnv delete subscription.operators.coreos.com kubevirt-hyperconverged --ignore-not-found
  oc -n openshift-cnv delete csv -l operators.coreos.com/kubevirt-hyperconverged.openshift-cnv --ignore-not-found
  oc delete namespace openshift-cnv --ignore-not-found --wait --timeout=15m
  oc delete crd -l operators.coreos.com/kubevirt-hyperconverged.openshift-cnv --ignore-not-found
  oc delete operator.operators.coreos.com kubevirt-hyperconverged.openshift-cnv --ignore-not-found 2>/dev/null || true

  log "temporal, inventory"
  oc delete namespace temporal "$VM_NS" --ignore-not-found --wait --timeout=10m

  # 5. The git server last, then what points at it.
  log "git server"
  oc -n "$ARGO_NS" delete application lab-create-vm-git --ignore-not-found --wait --timeout=10m
  oc -n "$ARGO_NS" delete secret lab-create-vm-git-creds --ignore-not-found
  oc delete namespace "$GIT_NS" --ignore-not-found --wait --timeout=10m
  oc -n "$ARGO_NS" delete appproject lab-create-vm --ignore-not-found

  log "left with the label $LABEL (expect nothing)"
  oc get namespaces -l "$LABEL" 2>&1 || true
  oc -n "$ARGO_NS" get applications.argoproj.io,appprojects.argoproj.io,secrets -l "$LABEL" 2>&1 || true
}

case "${1:-}" in
  up | status | verify | down) "$1" ;;
  *) die "usage: $0 up | status | verify | down" ;;
esac
