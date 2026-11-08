#!/usr/bin/env bash
# The create-vm lab on ONE OpenShift Virtualization cluster: several KubeVirt "clusters"
# simulated by agent releases pinned to nodes, a mock day1 served locally, the brain and the
# vm-lifecycle limb run from source. Built and verified 2026-11-05 on cluster-64spr (README.md).
#
#   ./lab.sh images       build kv-capacity / kv-vm-api on the cluster (BuildConfigs in ns kv-agents)
#   ./lab.sh up           Secrets + one release pair per simulated cluster (needs KV_CAPACITY_TOKEN, KV_VM_API_TOKEN)
#   ./lab.sh status       routes, pods, our VMs
#   ./lab.sh seed-day1    the mock day1 tree → a local bare repo; prints the limb's DAY1_* env
#   ./lab.sh env          the limb's env block (templates, tokens, policy)
#   ./lab.sh down         delete ONLY our VMs (ocp-x201-*) and the releases, then ns kv-agents
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
PLATFORM="${PLATFORM:-$(dirname "$REPO")/redbull-platform}"
AGENTS="${AGENTS:-$(dirname "$REPO")/kv-agents}"
NS="${NS:-kv-agents}"
CHASSIS_NS="${CHASSIS_NS:-redfish-vms}"
APPS="${APPS:-$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}')}"
REGISTRY="image-registry.openshift-image-registry.svc:5000/$NS"
STORAGE_OK="${STORAGE_OK:-ocs-external-storagecluster-cephfs}"          # Immediate, RWX Filesystem binds
STORAGE_WFFC="${STORAGE_WFFC:-ocs-external-storagecluster-ceph-rbd}"     # WaitForFirstConsumer: never binds Halted
CP="${CP:-control-plane-cluster-64spr-1}"
W1="${W1:-worker-cluster-64spr-1}"
W2="${W2:-worker-cluster-64spr-2}"
SCRATCH="${SCRATCH:-${TMPDIR:-/tmp}/create-vm-lab}"

# cluster | kv-capacity node selector ("" = every node) | vm nodeSelector JSON | storage class | capacity secret | mode
# `-6` is deployed then scaled to 0 (no-agent: the router's 503); `-7` is in day1 only (no-agent too);
# `-5` selects no node (no-nodes); `-8` holds a token the limb does not (refused-token); `b-1` never binds.
MATRIX=$(cat <<TABLE
kubevirt-lab-a-1|kubernetes.io/hostname=$CP|{"kubernetes.io/hostname":"$CP"}|$STORAGE_OK|kv-capacity-token|run
kubevirt-lab-a-2|kubernetes.io/hostname=$W1|{"kubernetes.io/hostname":"$W1"}|$STORAGE_OK|kv-capacity-token|run
kubevirt-lab-a-3|kubernetes.io/hostname=$W2|{"kubernetes.io/hostname":"$W2"}|$STORAGE_OK|kv-capacity-token|run
kubevirt-lab-a-4||{}|$STORAGE_OK|kv-capacity-token|run
kubevirt-lab-a-5|kubernetes.io/hostname=nowhere|{"kubernetes.io/hostname":"nowhere"}|$STORAGE_OK|kv-capacity-token|run
kubevirt-lab-a-6|kubernetes.io/hostname=$CP|{"kubernetes.io/hostname":"$CP"}|$STORAGE_OK|kv-capacity-token|scaled-to-zero
kubevirt-lab-a-8|kubernetes.io/hostname=$CP|{"kubernetes.io/hostname":"$CP"}|$STORAGE_OK|kv-capacity-token-wrong|run
kubevirt-lab-b-1|kubernetes.io/hostname=$CP|{"kubernetes.io/hostname":"$CP"}|$STORAGE_WFFC|kv-capacity-token|run
TABLE
)

images() {
  oc get ns "$NS" >/dev/null 2>&1 || oc create ns "$NS"
  local src="$SCRATCH/kv-agents-src"
  rm -rf "$src" && mkdir -p "$src"
  (cd "$AGENTS" && COPYFILE_DISABLE=1 tar --exclude=./.venv --exclude='*/__pycache__' --exclude=./.pytest_cache -cf - .) | (cd "$src" && tar -xf -)
  find "$src" -name '._*' -delete
  for agent in kv-capacity kv-vm-api; do
    oc -n "$NS" get bc "$agent" >/dev/null 2>&1 || {
      oc -n "$NS" new-build --binary --strategy docker --name "$agent" >/dev/null
      oc -n "$NS" patch bc "$agent" --type merge -p "{\"spec\":{\"strategy\":{\"dockerStrategy\":{\"dockerfilePath\":\"Dockerfile.$agent\"}}}}" >/dev/null
    }
    oc -n "$NS" start-build "$agent" --from-dir="$src" --follow
  done
}

up() {
  : "${KV_CAPACITY_TOKEN:?export KV_CAPACITY_TOKEN (the fleet token the limb sends)}"
  : "${KV_VM_API_TOKEN:?export KV_VM_API_TOKEN}"
  oc get ns "$NS" >/dev/null 2>&1 || oc create ns "$NS"
  oc -n "$NS" create secret generic kv-capacity-token --from-literal=API_TOKEN="$KV_CAPACITY_TOKEN" --dry-run=client -o yaml | oc apply -f -
  oc -n "$NS" create secret generic kv-vm-api-token --from-literal=API_TOKEN="$KV_VM_API_TOKEN" --dry-run=client -o yaml | oc apply -f -
  oc -n "$NS" create secret generic kv-capacity-token-wrong --from-literal=API_TOKEN="not-the-fleet-token" --dry-run=client -o yaml | oc apply -f -
  while IFS='|' read -r cluster selector vmsel storage capsecret mode; do
    [ -n "$cluster" ] || continue
    local suffix="${cluster#kubevirt-}"
    helm upgrade --install "kv-capacity-$suffix" "$PLATFORM/gitops/charts/kv-capacity" -n "$NS" \
      --set image.repository="$REGISTRY/kv-capacity" --set image.tag=latest --set image.pullPolicy=Always \
      --set config.clusterName="$cluster" --set-string config.nodeSelector="$selector" \
      --set-json "config.chassisNamespaces=[\"$CHASSIS_NS\"]" --set config.existingSecret="$capsecret" \
      --set route.host="kv-capacity-$cluster.$APPS" >/dev/null
    helm upgrade --install "kv-vm-api-$suffix" "$PLATFORM/gitops/charts/kv-vm-api" -n "$NS" \
      --set image.repository="$REGISTRY/kv-vm-api" --set image.tag=latest --set image.pullPolicy=Always \
      --set vm.storageClassName="$storage" --set-json "vm.nodeSelector=$vmsel" \
      --set route.host="kv-vm-api-$cluster.$APPS" >/dev/null
    if [ "$mode" = scaled-to-zero ]; then
      oc -n "$NS" scale deploy "kv-capacity-$suffix" "kv-vm-api-$suffix" --replicas=0 >/dev/null
    fi
    echo "release pair $suffix: $mode"
  done <<<"$MATRIX"
  oc -n "$NS" rollout status deploy --timeout=180s 2>/dev/null | grep -v "successfully" || true
  status
}

status() {
  oc -n "$NS" get pods -o wide 2>/dev/null | sed 's/  */ /g'
  oc -n "$NS" get routes -o custom-columns=NAME:.metadata.name,HOST:.spec.host 2>/dev/null
  oc -n "$CHASSIS_NS" get vm -o custom-columns=NAME:.metadata.name,STATUS:.status.printableStatus,RUN:.spec.runStrategy,NODE:.spec.template.spec.nodeSelector 2>/dev/null | grep -E "NAME|ocp-x201" || true
}

seed_day1() {
  : "${DAY1_GIT_TOKEN:=lab-day1-token}"
  mkdir -p "$SCRATCH"
  local bare="$SCRATCH/day1-lab.git" work="$SCRATCH/day1-lab-work"
  rm -rf "$bare" "$work"
  git init -q --bare --initial-branch=main "$bare"
  git clone -q "$bare" "$work"
  cp -R "$HERE/day1/." "$work/"
  git -C "$work" add -A
  git -C "$work" -c user.name=lab -c user.email=lab@test commit -q -m "mock day1 for the create-vm lab"
  git -C "$work" push -q origin HEAD:main
  # The limb injects the token into https:// URLs only: map that URL onto the bare repo.
  cat >"$SCRATCH/gitconfig" <<CONF
[url "$SCRATCH/"]
	insteadOf = https://x-access-token:$DAY1_GIT_TOKEN@git.test/
	insteadOf = https://git.test/
CONF
  echo "export DAY1_REPO_URL=https://git.test/day1-lab.git"
  echo "export DAY1_GIT_TOKEN=$DAY1_GIT_TOKEN"
  echo "export GIT_CONFIG_GLOBAL=$SCRATCH/gitconfig GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0"
  echo "# day1 main: $(git -C "$bare" rev-parse main)"
}

env_block() {
  cat <<ENV
export TEMPORAL_HOST=\${TEMPORAL_HOST:-localhost:7234} TEMPORAL_NAMESPACE=default
export KV_CAPACITY_URL_TEMPLATE=https://kv-capacity-{cluster}.$APPS
export KV_VM_API_URL_TEMPLATE=https://kv-vm-api-{cluster}.$APPS
export KV_CAPACITY_TOKEN=\${KV_CAPACITY_TOKEN:?} KV_VM_API_TOKEN=\${KV_VM_API_TOKEN:?}
export HEADROOM_BUFFER_PERCENT=\${HEADROOM_BUFFER_PERCENT:-30} VM_MAC_PREFIX=02:a1:b2 KV_AGENT_CA=
ENV
}

down() {
  for vm in $(oc -n "$CHASSIS_NS" get vm -o name 2>/dev/null | grep -E '/ocp-x201-' || true); do
    oc -n "$CHASSIS_NS" delete "$vm" --wait=false
  done
  for release in $(helm -n "$NS" list -q 2>/dev/null); do helm -n "$NS" uninstall "$release" >/dev/null && echo "uninstalled $release"; done
  oc delete ns "$NS" --wait=false 2>/dev/null || true
}

case "${1:-}" in
  images) images ;;
  up) up ;;
  status) status ;;
  seed-day1) seed_day1 ;;
  env) env_block ;;
  down) down ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
