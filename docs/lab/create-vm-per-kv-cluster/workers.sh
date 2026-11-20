#!/usr/bin/env bash
# The create-vm lab's OWN services, on top of lab.sh's base (git, Temporal, OpenShift
# Virtualization): the brain, the cluster-lifecycle worker (it reads day1) and a
# vm-lifecycle worker INSIDE the lab's one real KV cluster — this cluster — all from the
# branch you are on, all deployed by Argo CD from the in-cluster git.
#
#   ./workers.sh up        images + charts + day1 + secrets + Applications (safe to re-run)
#   ./workers.sh images    rebuild the three images on the cluster and roll the pods
#   ./workers.sh charts    push the three charts into the lab repo (Argo CD then syncs)
#   ./workers.sh day1      push ./day1 as the lab's day1 repo
#   ./workers.sh status    pods, queues, the API's address
#   ./workers.sh api       port-forward the trigger API to localhost:8088 (foreground)
#   ./workers.sh fleet     how to attach the fake KV clusters (tests/kv_fleet.py)
#   LAB_CONFIRM=down ./workers.sh down     remove what `up` created (lab.sh's base stays)
#
# It never writes a credential to disk and never pushes anywhere but the lab's own git.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"

LAB_API_SERVER="${LAB_API_SERVER:-https://api.cluster-qcl8n.dyn.redhatworkshops.io:6443}"
# redbull-platform with this change's charts: the worktree of its feature branch.
PLATFORM_REPO="${PLATFORM_REPO:-$(cd "$REPO/.." && pwd)/redbull-platform-vm-lifecycle}"
PLATFORM_REF="${PLATFORM_REF:-HEAD}"
LAB_WORKDIR="${LAB_WORKDIR:-${TMPDIR:-/tmp}/redbull-lab-create-vm}"

ARGO_NS=openshift-gitops
GIT_NS=lab-git
NS=redbull-workflows
GIT_ORG=redbull
LABEL_KEY=redbull.io/lab
LABEL="$LABEL_KEY=create-vm"
# name:the folder of its Dockerfile. kv-fleet is the lab's own: the fake KV clusters, as a pod.
IMAGES=(workflows-orchestrator:workflow_domains cluster-lifecycle-worker:activities/cluster_lifecycle vm-lifecycle-worker:activities/vm_lifecycle kv-fleet:docs/lab/create-vm-per-kv-cluster/kv-fleet)
APPS=(workflows-orchestrator cluster-lifecycle-worker vm-lifecycle-worker-a vm-lifecycle-worker-i kv-fleet)

log() { printf '\n== %s\n' "$*" >&2; }
die() { printf 'workers.sh: %s\n' "$*" >&2; exit 1; }

guard() {
  local server
  server="$(oc whoami --show-server 2>/dev/null)" || die "oc is not logged in"
  [ "$server" = "$LAB_API_SERVER" ] ||
    die "oc points at $server, the lab is $LAB_API_SERVER (set LAB_API_SERVER to use another cluster)"
}

secret_value() { oc -n "$1" get secret "$2" -o "jsonpath={.data.$3}" | base64 -d; }
git_host() { oc -n "$GIT_NS" get route gitea -o 'jsonpath={.spec.host}'; }

namespace() {
  oc get namespace "$NS" >/dev/null 2>&1 || oc create namespace "$NS"
  oc label namespace "$NS" "$LABEL" --overwrite >/dev/null
}

# ------------------------------------------------------------------------------ images

# Built ON the cluster (a binary Docker build per image, into its internal registry), so
# no registry outside the lab is involved. The context is the repo as .dockerignore
# leaves it, minus what no image copies.
images() {
  guard
  namespace
  local context="$LAB_WORKDIR/context" spec name dir
  log "build context"
  rm -rf "$context"
  mkdir -p "$context"
  rsync -a --delete --exclude .git --exclude .venv --exclude .claude --exclude .agents \
    --exclude '__pycache__' --exclude tests --exclude docs --exclude deploy --exclude '*.md' \
    "$REPO/" "$context/"
  # What only the lab's kv-fleet image copies: the simulator, and its Dockerfile.
  mkdir -p "$context/tests" "$context/docs/lab/create-vm-per-kv-cluster/kv-fleet"
  cp "$REPO/tests/kv_fleet.py" "$context/tests/"
  cp "$HERE/kv-fleet/Dockerfile" "$context/docs/lab/create-vm-per-kv-cluster/kv-fleet/"
  for spec in "${IMAGES[@]}"; do
    name="${spec%%:*}"
    dir="${spec#*:}"
    log "image $name"
    if ! oc -n "$NS" get buildconfig "$name" >/dev/null 2>&1; then
      oc -n "$NS" new-build --name "$name" --binary --strategy docker --to "$name:lab" >/dev/null
      oc -n "$NS" label buildconfig "$name" "$LABEL" --overwrite >/dev/null
    fi
    oc -n "$NS" patch buildconfig "$name" --type merge \
      -p "{\"spec\":{\"strategy\":{\"dockerStrategy\":{\"dockerfilePath\":\"$dir/Dockerfile\"}}}}" >/dev/null
    oc -n "$NS" start-build "$name" --from-dir "$context" --follow --wait 2>&1 | tail -4
  done
  # Pods already running pull the new image only when they are replaced.
  if oc -n "$NS" get deployment -o name 2>/dev/null | grep -q .; then
    log "rolling the pods onto the new images"
    oc -n "$NS" rollout restart deployment >/dev/null
  fi
}

# ------------------------------------------------------------------------------ git

# Replace `paths` of lab repo `repo` with what `fill <work dir>` writes, and push.
push_repo() { # repo message fill-function path...
  local repo="$1" message="$2" fill="$3"
  shift 3
  [ -n "$LAB_WORKDIR" ] || die "LAB_WORKDIR is empty"
  local work="$LAB_WORKDIR/$repo" token url path
  token="$(secret_value "$GIT_NS" lab-git-tokens rw-token)"
  # In memory only: never a remote of the work tree, never the laptop's credential helper.
  url="https://x-access-token:${token}@$(git_host)/$GIT_ORG/$repo.git"
  scrub() { sed "s/${token}/***/g"; }
  git_lab() { git -C "$work" -c credential.helper= -c user.name=lab -c user.email=lab@redbull.invalid "$@"; }
  rm -rf "$work"
  mkdir -p "$work"
  git -C "$work" init -q -b main
  if git_lab ls-remote --exit-code "$url" refs/heads/main >/dev/null 2>&1; then
    git_lab fetch -q "$url" main 2>&1 | scrub >&2
    git_lab reset -q --hard FETCH_HEAD
  fi
  for path in "$@"; do rm -rf "${work:?}/$path"; done
  "$fill" "$work"
  git_lab add -A
  if git_lab diff --cached --quiet; then
    echo "$repo already holds this content ($(git_lab rev-parse --short HEAD 2>/dev/null || echo empty))" >&2
  else
    git_lab commit -q -m "$message"
    git_lab push -q "$url" HEAD:refs/heads/main 2>&1 | scrub >&2
    echo "$repo: pushed $(git_lab rev-parse --short HEAD)" >&2
  fi
}

fill_charts() { # work dir
  # The brain's chart and the cluster-lifecycle worker's from redbull-platform's feature
  # branch (it adds the placement buffer); the vm-lifecycle worker's from THIS repo, where
  # it is edited (CI copies it into redbull-platform on main).
  git -C "$PLATFORM_REPO" archive "$PLATFORM_REF" gitops/charts/workflows-orchestrator \
    gitops/charts/cluster-lifecycle-worker | tar -x -C "$1"
  mkdir -p "$1/gitops/charts" "$1/lab"
  cp -R "$REPO/deploy/charts/vm-lifecycle-worker" "$1/gitops/charts/vm-lifecycle-worker"
  # …and the lab's fake KV clusters, plain manifests.
  cp -R "$HERE/platform-extra/lab/kv-fleet" "$1/lab/kv-fleet"
}

charts() {
  guard
  log "charts -> lab repo $GIT_ORG/platform"
  git -C "$PLATFORM_REPO" rev-parse --verify -q "$PLATFORM_REF^{commit}" >/dev/null ||
    die "$PLATFORM_REPO has no $PLATFORM_REF (set PLATFORM_REPO / PLATFORM_REF)"
  push_repo platform "lab: the workers' charts (workflows $(git -C "$REPO" rev-parse --short HEAD), platform $(git -C "$PLATFORM_REPO" rev-parse --short "$PLATFORM_REF"))" \
    fill_charts gitops/charts/workflows-orchestrator gitops/charts/cluster-lifecycle-worker gitops/charts/vm-lifecycle-worker lab/kv-fleet
}

fill_day1() { cp -R "$HERE/day1/." "$1/"; }

day1() {
  guard
  log "day1 -> lab repo $GIT_ORG/day1"
  push_repo day1 "lab: the mock day1 (seed folders per zone, KV cluster files)" fill_day1 sites
}

# ------------------------------------------------------------------------------ secrets

# Out of band, as in production: the charts only NAME these Secrets.
secrets() {
  guard
  namespace
  log "secrets in $NS"
  # The day1 token the cluster-lifecycle worker reads with: the lab git's READ token.
  oc -n "$NS" create secret generic redbull-git-token \
    --from-literal=REDBULL_GIT_TOKEN="$(secret_value "$GIT_NS" lab-git-tokens ro-token)" \
    --dry-run=client -o yaml | oc apply -f - >/dev/null
  # provision-cluster's shortcuts token: required by the worker's settings, used by nothing here.
  oc -n "$NS" create secret generic shortcuts-api-token \
    --from-literal=SHORTCUTS_API_TOKEN=unused-in-the-lab --dry-run=client -o yaml | oc apply -f - >/dev/null
  oc -n "$NS" label secret redbull-git-token shortcuts-api-token "$LABEL" --overwrite >/dev/null
  # The images are in this namespace's own image streams; the pods' ServiceAccounts pull them.
  oc -n "$NS" policy add-role-to-group system:image-puller "system:serviceaccounts:$NS" >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------------------ apps

wait_app() { # name [seconds]
  local name="$1" deadline=$((SECONDS + ${2:-600})) state=""
  while [ $SECONDS -lt $deadline ]; do
    state="$(oc -n "$ARGO_NS" get application "$name" \
      -o 'jsonpath={.status.sync.status}/{.status.health.status}' 2>/dev/null || true)"
    [ "$state" = "Synced/Healthy" ] && { echo "$name: $state" >&2; return 0; }
    sleep 5
  done
  die "$name is $state after ${2:-600}s: oc -n $ARGO_NS describe application $name"
}

apps() {
  guard
  log "Argo CD Applications"
  local name
  for name in "${APPS[@]}"; do oc apply -f "$HERE/argocd/app-$name.yaml"; done
  for name in "${APPS[@]}"; do
    oc -n "$ARGO_NS" annotate application "lab-create-vm-$name" argocd.argoproj.io/refresh=hard --overwrite >/dev/null
    wait_app "lab-create-vm-$name" 600
  done
  oc -n "$NS" rollout status deployment --timeout=300s
}

up() {
  guard
  images
  charts
  day1
  secrets
  apps
  status
}

# ------------------------------------------------------------------------------ status

status() {
  guard
  log "Argo CD Applications"
  oc -n "$ARGO_NS" get applications.argoproj.io -l "$LABEL" 2>&1 || true
  log "pods in $NS"
  oc -n "$NS" get pods -o wide 2>&1 || true
  log "what each worker polls"
  oc -n "$NS" logs deployment/cluster-lifecycle-worker --tail=200 2>/dev/null | grep -m1 'polling queue' || true
  local release
  for release in vm-lifecycle-worker-a vm-lifecycle-worker-i; do
    oc -n "$NS" logs "deployment/$release" --tail=200 2>/dev/null | grep -m1 'polling queue' || true
  done
  cat <<EOF

trigger API          ./workers.sh api    then http://localhost:8088/docs
  one VM on the real cluster:
    curl -s -X POST localhost:8088/workflows/vm-lifecycle/create-vm -H 'content-type: application/json' \\
      -d '{"mce_cluster": "ocp4-prod-mce-five-a", "infra_env": "c200-kv2-five-az1"}'
  its progress and result:
    curl -s localhost:8088/workflows/runs/<workflow_id> | jq
Temporal UI          oc -n temporal port-forward svc/temporal-ui 8080:8080
the fake KV clusters ./workers.sh fleet
EOF
}

api() {
  guard
  exec oc -n "$NS" port-forward svc/workflows-orchestrator-api 8088:8080
}

fleet() {
  guard
  cat <<EOF
The lab has ONE real KV cluster. The rest of its fleet is tests/kv_fleet.py, running as
the pod kv-fleet in $NS (Application lab-create-vm-kv-fleet): fake KV clusters, each
polling its own queue on the lab's Temporal and running the REAL worker code over an
in-memory cluster. day1 (./day1) lists them: e-h under zone az2 of site five, the
bat-yam three under zone az1 of site bat-yam. So

  {"mce_cluster": "ocp4-prod-mce-five-a", "infra_env": "c200-kv32-five-az2", "count": 10}

fills e, then f, g, h, two VMs each (100 cores, the 15% buffer), then finds no capacity.
(From a laptop the simulator cannot long-poll Temporal through oc port-forward: its
clusters then show as no-worker. That is why it runs in the cluster.)

what the fake clusters hold now:
EOF
  oc -n "$NS" logs deployment/kv-fleet --tail=40 2>/dev/null | grep 'VMs, requested' | tail -7 || true
  echo "for an empty fleet again:  oc -n $NS rollout restart deployment/kv-fleet"
}

# -------------------------------------------------------------------------------- down

down() {
  guard
  [ "${LAB_CONFIRM:-}" = down ] || die "this removes the lab's workers and VMs; run: LAB_CONFIRM=down $0 down"
  log "VMs the lab's runs created"
  oc get vm -A -l redbull.io/created-by --no-headers 2>/dev/null || true
  oc delete vm -A -l redbull.io/created-by --wait --ignore-not-found 2>/dev/null || true
  log "Applications"
  local name
  for name in "${APPS[@]}"; do
    oc -n "$ARGO_NS" delete application "lab-create-vm-$name" --ignore-not-found --wait --timeout=10m
  done
  log "namespace $NS (builds, image streams, secrets)"
  oc delete namespace "$NS" --ignore-not-found --wait --timeout=10m
}

case "${1:-}" in
  up | images | charts | day1 | secrets | apps | status | api | fleet | down) "$1" ;;
  *) die "usage: $0 up | images | charts | day1 | secrets | apps | status | api | fleet | down" ;;
esac
