"""bootstrap-installer as Kubernetes Jobs — the names, the cluster YAML and the manifests. Pure.

Plain parameters, no settings, no Temporal, no I/O (CLAUDE.md §1): what hub_api.py
applies and what the tests pin.

THE INSTALLER IS team-redbull/bootstrap-installer (`main`: disconnected vSphere,
`platform: none`, no VIPs — DNS round-robin), run as it is. Decided with the
operator 2026-11-27: the workflow drives it, it stays the ONE place the install
logic lives. Two invocations, one Job each (shared/models/upi_lifecycle.InstallerStep):

  prepare  `bootstrap.py --skip-terraform --skip-argocd`: IPs from the segment,
           install-config / tfvars / v4InternalSubnet, ignition, the *.apps DNS
           record. Then the script stores the admin kubeconfig and the node IP
           lists (of cluster-context.yaml, which also holds the vCenter password,
           so never the file itself) in the Secret `upi-<cluster>-kubeconfig`.
  install  `bootstrap.py --skip-ignition --skip-dns --skip-argocd`: terraform
           apply (it asks "yes" on stdin, so the script answers it) and its CSR
           loop. A loop that times out exits 0 — the workflow's own readiness
           check (await_upi_cluster_ready) is what decides.

What the Job gets:
  /work            the cluster's work volume (PVC, never deleted: terraform state).
                   bootstrap.py `--work-dir /work` keeps everything in /work/<cluster>.
  /cluster         the run's cluster YAML (ConfigMap `upi-<cluster>-config`).
  /app/config/sites  the site profiles (the limb's sites ConfigMap), over the image's.
  /secrets/install pull secret, trust bundle, SSH key (INSTALLER_SECRET_NAME);
                   VSPHERE_PASSWORD from its `vsphere-password` key — the site
                   profile names it with `vcenter_password_env: VSPHERE_PASSWORD`.
The image's /app is read-only under OpenShift's restricted SCC, and
bootstrap-installer writes into its terraform folder (tfvars, .providers.tfrc,
.terraform), so the script copies /app/vsphere into the work volume first and the
cluster YAML points `terraform_dir` there.

A `prepare` refuses a work volume whose terraform state still lists resources: a
cluster terraform made must be destroyed (`bootstrap.py --destroy`) before it is
prepared again, or new ignition would meet old VMs.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import yaml

from shared.exceptions import InstallerConfigRejectedError
from shared.models.upi_lifecycle import InstallerStep, InstallerStepRequest, run_owned_install_keys

WORK_MOUNT = "/work"
CLUSTER_MOUNT = "/cluster"
SITES_MOUNT = "/app/config/sites"
SECRETS_MOUNT = "/secrets/install"
CLUSTER_FILE = "cluster.yaml"

PULL_SECRET_KEY = "pull-secret.json"
TRUST_BUNDLE_KEY = "additional-trust-bundle.pem"
SSH_KEY_KEY = "ssh.pub"
VSPHERE_PASSWORD_KEY = "vsphere-password"

KUBECONFIG_KEY = "kubeconfig"
NODES_KEY = "nodes.yaml"

APP_LABEL = "app.kubernetes.io/managed-by"
APP_VALUE = "upi-provisioning-worker"
CLUSTER_LABEL = "redbull.io/upi-cluster"
STEP_LABEL = "redbull.io/installer-step"

# The log lines a failure carries (the message is recorded in history verbatim).
LOG_TAIL_LINES = 40
LOG_TAIL_CHARS = 3000


def pvc_name(cluster: str) -> str:
    return f"upi-{cluster}-work"


def config_map_name(cluster: str) -> str:
    return f"upi-{cluster}-config"


def kubeconfig_secret_name(cluster: str) -> str:
    return f"upi-{cluster}-kubeconfig"


def job_name(cluster: str, step: InstallerStep, run_token: str) -> str:
    """`upi-<cluster>-<step>-<token>`: the same for every attempt of one run, new for a new run.

    At most 4 + 27 + 1 + 7 + 1 + 8 = 48 characters, under the 63 a Job's pods allow.
    """
    return f"upi-{cluster}-{step.value}-{run_token}"


def rejected_keys(install_config: dict[str, Any]) -> list[str]:
    """The keys of `install_config` the run owns (sorted) — the edge and the limb both refuse them."""
    return run_owned_install_keys(install_config)


def vm_network_for(template: str, *, request: InstallerStepRequest) -> str:
    """The vSphere port group: the request's own, else VM_NETWORK_TEMPLATE filled in."""
    if request.vm_network:
        return request.vm_network
    try:
        return template.format(
            epg_name=request.epg_name, vlan_id=request.vlan_id, site=request.site, cluster=request.cluster
        )
    except (KeyError, IndexError, ValueError) as err:
        raise InstallerConfigRejectedError(
            f"VM_NETWORK_TEMPLATE {template!r} names a field it cannot have ({err}); "
            "it may use {epg_name}, {vlan_id}, {site}, {cluster}"
        ) from None


def cluster_yaml(request: InstallerStepRequest, *, vm_network: str) -> str:
    """bootstrap-installer's cluster YAML (the top layer): `install_config` + the run's keys."""
    refused = rejected_keys(request.install_config)
    if refused:
        raise InstallerConfigRejectedError(
            f"install_config sets keys the run owns: {', '.join(refused)} — the segment, the "
            "names, the secret paths and Argo CD are the workflow's"
        )
    document: dict[str, Any] = dict(request.install_config)
    document.update({
        "cluster_name": request.cluster,
        "site": request.site,
        "segment": request.segment,
        "vlan_id": request.vlan_id,
        "vm_network": vm_network,
        "pull_secret_path": f"{SECRETS_MOUNT}/{PULL_SECRET_KEY}",
        "additional_trust_bundle_path": f"{SECRETS_MOUNT}/{TRUST_BUNDLE_KEY}",
        "ssh_public_key_path": f"{SECRETS_MOUNT}/{SSH_KEY_KEY}",
        "terraform_dir": f"{WORK_MOUNT}/{request.cluster}/vsphere",
    })
    header = (
        f"# Written by the upi-provisioning worker for create-upi-cluster ({request.cluster}).\n"
        "# Layers: config/defaults.yaml -> config/sites/<site>.yaml -> this file.\n"
    )
    return header + yaml.safe_dump(document, sort_keys=True, default_flow_style=False)


def config_digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def job_script(cluster: str, step: InstallerStep, *, csr_timeout_minutes: int) -> str:
    """The Job's shell: one bootstrap.py invocation, with what it needs around it."""
    work = f"{WORK_MOUNT}/{cluster}"
    q = _shell_quote
    lines = [
        "set -eu",
        "export HOME=/tmp",
        f"W={q(work)}",
        'mkdir -p "$W"',
        # A writable copy of the terraform code (and its provider mirror), fresh each step.
        'rm -rf "$W/vsphere" && cp -r /app/vsphere "$W/vsphere"',
    ]
    args = f"--config {q(CLUSTER_MOUNT + '/' + CLUSTER_FILE)} --work-dir {q(WORK_MOUNT)}"
    if step == InstallerStep.PREPARE:
        lines += [
            # Refuse a volume whose terraform state still lists resources (see the module doc).
            'if [ -s "$W/terraform.tfstate" ] && python3 -c '
            "'import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get(\"resources\") else 1)' "
            '"$W/terraform.tfstate"; then',
            '  echo "REFUSED: $W/terraform.tfstate still lists resources - destroy the cluster '
            '(bootstrap.py --destroy) before preparing it again" >&2',
            "  exit 3",
            "fi",
            f"python3 bootstrap.py {args} --skip-terraform --skip-argocd",
            # The admin kubeconfig and the IPs the cluster must have, for the limb.
            'KC=""',
            'for f in "$W/auth/kubeconfig" "$W/install/auth/kubeconfig" "$W/ignition/auth/kubeconfig"; do',
            '  if [ -s "$f" ]; then KC="$f"; break; fi',
            "done",
            '[ -n "$KC" ] || { echo "no auth/kubeconfig under $W after prepare" >&2; exit 4; }',
            # ONLY the node IP lists of cluster-context.yaml: the file also holds the
            # vCenter password (bootstrap-installer dumps its whole template context).
            "python3 -c 'import sys, yaml; c = yaml.safe_load(open(sys.argv[1])) or {}; "
            'yaml.safe_dump({k: c.get(k) or [] for k in ("control_plane_ips", "infra_ips", "compute_ips")}, '
            "sys.stdout)' \"$W/cluster-context.yaml\" > /tmp/nodes.yaml",
            f"oc create secret generic {q(kubeconfig_secret_name(cluster))}"
            f' --from-file={KUBECONFIG_KEY}="$KC"'
            f" --from-file={NODES_KEY}=/tmp/nodes.yaml"
            " --dry-run=client -o yaml"
            f" | oc label --local -f - {CLUSTER_LABEL}={q(cluster)} -o yaml"
            " | oc apply -f -",
        ]
    else:
        lines += [
            # terraform's plan asks for "yes" on stdin before it applies.
            f"printf 'yes\\n' | python3 bootstrap.py {args} --skip-ignition --skip-dns --skip-argocd"
            f" --csr-timeout {int(csr_timeout_minutes)}",
        ]
    return "\n".join(lines) + "\n"


def _labels(cluster: str, step: InstallerStep | None = None) -> dict[str, str]:
    labels = {APP_LABEL: APP_VALUE, CLUSTER_LABEL: cluster}
    if step is not None:
        labels[STEP_LABEL] = step.value
    return labels


def pvc_manifest(cluster: str, *, size: str, storage_class: str) -> dict[str, Any]:
    spec: dict[str, Any] = {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": size}}}
    if storage_class:
        spec["storageClassName"] = storage_class
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name(cluster), "labels": _labels(cluster)},
        "spec": spec,
    }


def config_map_manifest(cluster: str, text: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": config_map_name(cluster),
            "labels": _labels(cluster),
            "annotations": {"redbull.io/config-digest": config_digest(text)},
        },
        "data": {CLUSTER_FILE: text},
    }


def job_manifest(
    cluster: str,
    step: InstallerStep,
    run_token: str,
    *,
    image: str,
    service_account: str,
    secret_name: str,
    sites_config_map: str,
    active_deadline_seconds: int,
    csr_timeout_minutes: int,
) -> dict[str, Any]:
    """The Job: one attempt (backoffLimit 0 — a retried terraform is the person's call), kept 7 days."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name(cluster, step, run_token), "labels": _labels(cluster, step)},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": int(active_deadline_seconds),
            "ttlSecondsAfterFinished": 7 * 24 * 3600,
            "template": {
                "metadata": {"labels": _labels(cluster, step)},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": service_account,
                    "containers": [{
                        "name": "bootstrap-installer",
                        "image": image,
                        "workingDir": "/app",
                        "command": ["/bin/sh", "-c"],
                        "args": [job_script(cluster, step, csr_timeout_minutes=csr_timeout_minutes)],
                        "env": [{
                            "name": "VSPHERE_PASSWORD",
                            "valueFrom": {"secretKeyRef": {"name": secret_name, "key": VSPHERE_PASSWORD_KEY}},
                        }],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "1Gi"},
                            "limits": {"memory": "4Gi"},
                        },
                        "volumeMounts": [
                            {"name": "work", "mountPath": WORK_MOUNT},
                            {"name": "cluster", "mountPath": CLUSTER_MOUNT, "readOnly": True},
                            {"name": "sites", "mountPath": SITES_MOUNT, "readOnly": True},
                            {"name": "install", "mountPath": SECRETS_MOUNT, "readOnly": True},
                        ],
                    }],
                    "volumes": [
                        {"name": "work", "persistentVolumeClaim": {"claimName": pvc_name(cluster)}},
                        {"name": "cluster", "configMap": {"name": config_map_name(cluster)}},
                        {"name": "sites", "configMap": {"name": sites_config_map}},
                        {"name": "install", "secret": {"secretName": secret_name}},
                    ],
                },
            },
        },
    }


def bounded_log_tail(text: str) -> str:
    """The last LOG_TAIL_LINES lines, at most LOG_TAIL_CHARS characters, secrets masked."""
    lines = text.splitlines()[-LOG_TAIL_LINES:]
    tail = "\n".join(lines)[-LOG_TAIL_CHARS:]
    return _mask(tail)


_SECRETISH = re.compile(r"(?i)(password|token|secret|pullSecret)(\s*[:=]\s*)(\S+)")


def _mask(text: str) -> str:
    return _SECRETISH.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)


def site_profile_present(sites: dict[str, str], site: str) -> bool:
    """Whether the sites ConfigMap holds `<site>.yaml` (bootstrap-installer's site profile)."""
    return f"{site}.yaml" in sites


def expected_ips(context_text: str | None) -> list[str]:
    """The node IPs the prepare Job stored (control plane, infra, compute), from cluster-context.yaml."""
    if not context_text:
        return []
    try:
        context = yaml.safe_load(context_text) or {}
    except yaml.YAMLError:
        return []
    if not isinstance(context, dict):
        return []
    ips: list[str] = []
    for key in ("control_plane_ips", "infra_ips", "compute_ips"):
        value = context.get(key) or []
        if isinstance(value, list):
            ips += [str(ip) for ip in value if ip]
    return ips
