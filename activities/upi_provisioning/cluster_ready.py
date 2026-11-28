"""When an installed UPI cluster serves, and which of its CSRs to approve — pure rules.

No I/O, no settings, no Temporal: what await_upi_cluster_ready applies to what it
reads, and what the tests pin.

CSRs: a brand-new cluster's nodes, exactly as bootstrap-installer's own loop
approves them — but only the two shapes a node makes, never any pending CSR: a
client CSR from the machine-config operator's node-bootstrapper, and a serving CSR
from a `system:node:<name>` user.

READY: every IP the prepare Job recorded (control plane, infra, compute) is the
InternalIP of a Ready Node, and ClusterVersion reports Available=True. With no IPs
recorded (a Secret from an older Job), at least one Node and all Nodes Ready.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from shared.models.upi_lifecycle import UpiNode

NODE_BOOTSTRAPPER = "system:serviceaccount:openshift-machine-config-operator:node-bootstrapper"
NODE_USER_PREFIX = "system:node:"


def is_pending(csr: dict[str, Any]) -> bool:
    return not (csr.get("status") or {}).get("conditions")


def is_node_csr(csr: dict[str, Any]) -> bool:
    username = str((csr.get("spec") or {}).get("username") or "")
    return username == NODE_BOOTSTRAPPER or username.startswith(NODE_USER_PREFIX)


def to_approve(csrs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [c for c in csrs if is_pending(c) and is_node_csr(c)]


def node_view(node: dict[str, Any]) -> UpiNode:
    metadata = node.get("metadata") or {}
    status = node.get("status") or {}
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions") or [])
    ip = next((a.get("address") for a in status.get("addresses") or [] if a.get("type") == "InternalIP"), None)
    roles = sorted(
        key.split("/", 1)[1]
        for key in (metadata.get("labels") or {})
        if key.startswith("node-role.kubernetes.io/") and "/" in key
    )
    return UpiNode(name=str(metadata.get("name") or ""), internal_ip=ip, ready=ready, roles=roles)


def cluster_version(cv: dict[str, Any] | None) -> tuple[bool, str | None]:
    """(Available, the version it reports) of a ClusterVersion."""
    if not cv:
        return False, None
    status = cv.get("status") or {}
    available = any(
        c.get("type") == "Available" and c.get("status") == "True" for c in status.get("conditions") or []
    )
    version = (status.get("desired") or {}).get("version")
    history = status.get("history") or []
    if history and history[0].get("state") == "Completed":
        version = history[0].get("version") or version
    return available, version


def missing(nodes: list[UpiNode], expected_ips: list[str], available: bool, version: str | None = "") -> list[str]:
    """What still keeps the cluster from serving — empty when it serves.

    A served cluster also reports its version: an MCE's day1 `version.yaml` is
    written from it."""
    problems = []
    ready_ips = {n.internal_ip for n in nodes if n.ready}
    if expected_ips:
        absent = [ip for ip in expected_ips if ip not in ready_ips]
        if absent:
            problems.append(f"no Ready node at {', '.join(absent)}")
    elif not nodes or not all(n.ready for n in nodes):
        problems.append(f"{sum(1 for n in nodes if n.ready)}/{len(nodes)} nodes Ready")
    if not available:
        problems.append("ClusterVersion not Available")
    elif not version:
        problems.append("ClusterVersion reports no version")
    return problems


def argocd_cluster_secret(
    *, cluster: str, kind: str, server: str, token: str, ca_b64: str
) -> dict[str, Any]:
    """Argo CD's declarative cluster Secret, as bootstrap-installer's argocd.py writes it."""
    config = {"bearerToken": token, "tlsClientConfig": {"insecure": False, "caData": ca_b64}}
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": argocd_secret_name(cluster),
            "labels": {
                "argocd.argoproj.io/secret-type": "cluster",
                "app.kubernetes.io/managed-by": "upi-provisioning-worker",
                "redbull.io/upi-cluster": cluster,
                "redbull.io/upi-kind": kind,
            },
        },
        "stringData": {"name": cluster, "server": server, "config": json.dumps(config, sort_keys=True)},
    }


def argocd_secret_name(cluster: str) -> str:
    return f"cluster-{cluster}"


def secret_string(secret: dict[str, Any], key: str) -> str | None:
    """A key of a Secret as read back (base64 `data`), decoded."""
    value = (secret.get("data") or {}).get(key)
    return base64.b64decode(value).decode("utf-8", "replace") if value else None


def same_registration(secret: dict[str, Any], wanted: dict[str, Any]) -> bool:
    """Whether the Secret read back already says what `wanted` would write."""
    want = wanted["stringData"]
    if secret_string(secret, "name") != want["name"] or secret_string(secret, "server") != want["server"]:
        return False
    try:
        return json.loads(secret_string(secret, "config") or "{}") == json.loads(want["config"])
    except ValueError:
        return False
