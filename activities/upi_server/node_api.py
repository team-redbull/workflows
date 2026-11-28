"""The UPI cluster's own API, as the upi-server worker's ServiceAccount — Nodes and CSRs.

In-cluster config (a kubeconfig for a developer). Synchronous SDK, every public
function async through `asyncio.to_thread`. Plain parameters (CLAUDE.md §1).

WHICH CSRs ARE THIS HOST'S (`csr_is_for_host`, pure): the kubelet's two requests,
and nothing else —
  * the client CSR: requested by the machine-config operator's node-bootstrapper,
    its subject CN `system:node:<hostname>` (read from the PEM request itself);
  * the serving CSR: requested BY `system:node:<hostname>`.
A pending CSR of any other host or user is never approved here.

CLASSIFICATION: 403 -> UpiNodeAccessError (permanent: the chart's role); anything
else — 5xx, 401, a timeout — UpiNodeApiError (retried). 404 on a read is None.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from typing import Any, TypeVar

import urllib3
from cryptography import x509
from cryptography.x509.oid import NameOID
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from shared.exceptions import UpiNodeAccessError, UpiNodeApiError

T = TypeVar("T")
REQUEST_TIMEOUT = (5, 20)
NODE_BOOTSTRAPPER = "system:serviceaccount:openshift-machine-config-operator:node-bootstrapper"
_api_client: k8s_client.ApiClient | None = None


def _client() -> k8s_client.ApiClient:
    global _api_client
    if _api_client is None:
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        _api_client = k8s_client.ApiClient()
    return _api_client


def _detail(exc: k8s_client.ApiException) -> str:
    try:
        body = json.loads(exc.body or "")
        return str((body or {}).get("message") or exc.reason)[:300]
    except (TypeError, ValueError, AttributeError):
        return str(exc.reason)[:300]


async def _call(what: str, fn: Callable[[], T], *, absent: bool = False) -> T | None:
    def run() -> T | None:
        try:
            return fn()
        except k8s_client.ApiException as exc:
            if exc.status == 404 and absent:
                return None
            if exc.status == 403:
                raise UpiNodeAccessError(f"the cluster refused the worker {what} (403): {_detail(exc)}") from None
            raise UpiNodeApiError(f"the cluster answered {exc.status} to {what}: {_detail(exc)}") from None
        except urllib3.exceptions.HTTPError as exc:
            raise UpiNodeApiError(f"the cluster's API could not be reached for {what}: {type(exc).__name__}") from None

    return await asyncio.to_thread(run)


def _plain(obj: Any) -> dict[str, Any]:
    plain = _client().sanitize_for_serialization(obj)
    return plain if isinstance(plain, dict) else {}


async def get_node(name: str) -> dict[str, Any] | None:
    core = k8s_client.CoreV1Api(_client())
    node = await _call(f"a read of Node {name}", lambda: core.read_node(name, _request_timeout=REQUEST_TIMEOUT), absent=True)
    return None if node is None else _plain(node)


async def list_csrs() -> list[dict[str, Any]]:
    certs = k8s_client.CertificatesV1Api(_client())
    found = await _call("a list of CSRs", lambda: certs.list_certificate_signing_request(_request_timeout=REQUEST_TIMEOUT))
    return [_plain(c) for c in (found.items or [])] if found else []


async def approve_csr(csr: dict[str, Any], hostname: str) -> None:
    certs = k8s_client.CertificatesV1Api(_client())
    body = dict(csr)
    status = dict(body.get("status") or {})
    status["conditions"] = [*(status.get("conditions") or []), {
        "type": "Approved", "status": "True", "reason": "AddServerToUpi",
        "message": f"approved by add-server-to-upi for {hostname}",
    }]
    body["status"] = status
    name = csr["metadata"]["name"]
    await _call(f"the approval of CSR {name}", lambda: certs.replace_certificate_signing_request_approval(
        name, body, _request_timeout=REQUEST_TIMEOUT))


async def label_node(name: str, labels: dict[str, str]) -> dict[str, str]:
    """Merge-patch the Node's labels; returns its labels read back."""
    core = k8s_client.CoreV1Api(_client())
    await _call(f"the labelling of Node {name}", lambda: core.patch_node(
        name, {"metadata": {"labels": labels}}, _request_timeout=REQUEST_TIMEOUT))
    node = await get_node(name)
    return dict(((node or {}).get("metadata") or {}).get("labels") or {})


# --- pure ---------------------------------------------------------------------------------


def is_pending(csr: dict[str, Any]) -> bool:
    return not (csr.get("status") or {}).get("conditions")


def subject_cn(csr: dict[str, Any]) -> str | None:
    """The CN of the CSR's PEM request, or None when it does not parse."""
    request = (csr.get("spec") or {}).get("request")
    if not request:
        return None
    try:
        parsed = x509.load_pem_x509_csr(base64.b64decode(request))
        names = parsed.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    except (ValueError, TypeError):
        return None
    return str(names[0].value) if names else None


def csr_is_for_host(csr: dict[str, Any], hostname: str) -> bool:
    """Whether a CSR is this host's kubelet client or serving request (see the module doc)."""
    username = str((csr.get("spec") or {}).get("username") or "")
    node_user = f"system:node:{hostname}"
    if username == node_user:
        return True
    return username == NODE_BOOTSTRAPPER and subject_cn(csr) == node_user


def node_ready(node: dict[str, Any] | None) -> bool:
    conditions = ((node or {}).get("status") or {}).get("conditions") or []
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)


def node_ip(node: dict[str, Any] | None) -> str | None:
    addresses = ((node or {}).get("status") or {}).get("addresses") or []
    return next((a.get("address") for a in addresses if a.get("type") == "InternalIP"), None)


def node_roles(node: dict[str, Any] | None) -> list[str]:
    labels = ((node or {}).get("metadata") or {}).get("labels") or {}
    return sorted(k.split("/", 1)[1] for k in labels if k.startswith("node-role.kubernetes.io/"))
