"""An installed UPI cluster's API, as its ADMIN — through the kubeconfig its prepare Job stored.

The kubeconfig is the installer's own (`system:admin`, client certificate) and is
read from the hub Secret `upi-<cluster>-kubeconfig` on every call: nothing of it
enters history, a log or an error. Parsed BY THIS MODULE, credentials written to a
per-call temporary directory deleted when the call ends — the hosted-cluster
pattern (activities/server_lifecycle/hosted_cluster_api.py): the SDK's own loader
keeps certificate files for the life of the process. TLS is verified against the
kubeconfig's CA, the server is the kubeconfig's own (`https://api.<cluster>…:6443`).

CLASSIFICATION: a missing or unusable kubeconfig, a 403 and a TLS verification
failure -> UpiClusterAccessError (permanent); anything else — a 5xx, a 401 (the
API not serving the certificate yet), a refused connection while the cluster comes
up, a timeout — UpiClusterApiError (retried; inside a wait, just polled again).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import pathlib
import tempfile
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

import urllib3
import yaml
from kubernetes import client as k8s_client

from shared.exceptions import UpiClusterAccessError, UpiClusterApiError

T = TypeVar("T")
REQUEST_TIMEOUT = (5, 20)
CONFIG_GROUP, CONFIG_VERSION = "config.openshift.io", "v1"


class Credentials:
    """The current context's server, CA, client certificate/key or token — never logged."""

    def __init__(self, kubeconfig: str, cluster: str) -> None:
        try:
            config = yaml.safe_load(kubeconfig)
        except yaml.YAMLError:
            raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} does not parse as YAML") from None
        if not isinstance(config, dict):
            raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} is not a kubeconfig")
        context = self._named(config.get("contexts"), config.get("current-context"), "context", cluster)
        cluster_entry = self._named(config.get("clusters"), context.get("cluster"), "cluster", cluster)
        user_entry = self._named(config.get("users"), context.get("user"), "user", cluster)
        self.server = str(cluster_entry.get("server") or "")
        self.ca_b64 = str(cluster_entry.get("certificate-authority-data") or "")
        self.ca = self._decoded(self.ca_b64, "CA", cluster)
        self.cert = self._decoded(user_entry.get("client-certificate-data"), "client certificate", cluster)
        self.key = self._decoded(user_entry.get("client-key-data"), "client key", cluster)
        self.token = user_entry.get("token") or None
        if not self.server or self.ca is None:
            raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} names no server or no CA")
        if not (self.cert and self.key) and not self.token:
            raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} carries no client certificate or token")

    @staticmethod
    def _named(entries: Any, name: Any, kind: str, cluster: str) -> dict[str, Any]:
        entries = [e for e in entries or [] if isinstance(e, dict)]
        for entry in entries:
            if name and entry.get("name") == name:
                return entry.get(kind) or {}
        if len(entries) == 1:
            return entries[0].get(kind) or {}
        raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} has no usable {kind} entry")

    @staticmethod
    def _decoded(value: Any, field: str, cluster: str) -> bytes | None:
        if not value:
            return None
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, TypeError, ValueError):
            raise UpiClusterAccessError(f"the admin kubeconfig of {cluster} carries a {field} that is not base64") from None


@contextlib.contextmanager
def _api(creds: Credentials) -> Iterator[k8s_client.ApiClient]:
    with tempfile.TemporaryDirectory(prefix="upi-api-") as tmp:
        directory = pathlib.Path(tmp)

        def file(name: str, content: bytes) -> str:
            path = directory / name
            path.write_bytes(content)
            path.chmod(0o600)
            return str(path)

        configuration = k8s_client.Configuration()
        configuration.host = creds.server
        configuration.verify_ssl = True
        configuration.ssl_ca_cert = file("ca.crt", creds.ca or b"")
        if creds.cert and creds.key:
            configuration.cert_file = file("tls.crt", creds.cert)
            configuration.key_file = file("tls.key", creds.key)
        if creds.token:
            configuration.api_key = {"authorization": f"Bearer {creds.token}"}
        configuration.retries = False
        with k8s_client.ApiClient(configuration) as api_client:
            yield api_client


def _classify(exc: Exception, cluster: str, what: str) -> Exception:
    if isinstance(exc, k8s_client.ApiException):
        if exc.status == 403:
            return UpiClusterAccessError(f"{cluster}'s API refused its admin {what} (403)")
        return UpiClusterApiError(f"{cluster}'s API answered {exc.status} to {what}")
    reason = getattr(exc, "reason", None)
    if isinstance(exc, urllib3.exceptions.SSLError) or isinstance(reason, urllib3.exceptions.SSLError):
        return UpiClusterAccessError(
            f"TLS to {cluster}'s API failed verification against its admin kubeconfig's CA"
        )
    return UpiClusterApiError(f"{cluster}'s API could not be reached for {what}: {type(reason or exc).__name__}")


async def call(creds: Credentials, cluster: str, what: str, fn: Callable[[k8s_client.ApiClient], T],
               *, absent: bool = False) -> T | None:
    """One call (or a few, inside `fn`) as the cluster's admin, off the event loop."""

    def run() -> T | None:
        with _api(creds) as api_client:
            try:
                return fn(api_client)
            except k8s_client.ApiException as exc:
                if exc.status == 404 and absent:
                    return None
                raise _classify(exc, cluster, what) from None
            except urllib3.exceptions.HTTPError as exc:
                raise _classify(exc, cluster, what) from None

    return await asyncio.to_thread(run)


def plain(api_client: k8s_client.ApiClient, obj: Any) -> dict[str, Any]:
    result = api_client.sanitize_for_serialization(obj)
    return result if isinstance(result, dict) else {}


# --- the reads and writes the limb makes -------------------------------------------------


def list_nodes(api_client: k8s_client.ApiClient) -> list[dict[str, Any]]:
    nodes = k8s_client.CoreV1Api(api_client).list_node(_request_timeout=REQUEST_TIMEOUT)
    return [plain(api_client, n) for n in nodes.items or []]


def list_csrs(api_client: k8s_client.ApiClient) -> list[dict[str, Any]]:
    csrs = k8s_client.CertificatesV1Api(api_client).list_certificate_signing_request(_request_timeout=REQUEST_TIMEOUT)
    return [plain(api_client, c) for c in csrs.items or []]


def approve_csr(api_client: k8s_client.ApiClient, csr: dict[str, Any], reason: str, message: str) -> None:
    """Add an Approved condition, as `oc adm certificate approve` does."""
    body = dict(csr)
    status = dict(body.get("status") or {})
    conditions = list(status.get("conditions") or [])
    conditions.append({"type": "Approved", "status": "True", "reason": reason, "message": message})
    status["conditions"] = conditions
    body["status"] = status
    k8s_client.CertificatesV1Api(api_client).replace_certificate_signing_request_approval(
        csr["metadata"]["name"], body, _request_timeout=REQUEST_TIMEOUT
    )


def read_cluster_version(api_client: k8s_client.ApiClient) -> dict[str, Any] | None:
    try:
        found = k8s_client.CustomObjectsApi(api_client).get_cluster_custom_object(
            CONFIG_GROUP, CONFIG_VERSION, "clusterversions", "version", _request_timeout=REQUEST_TIMEOUT
        )
    except k8s_client.ApiException as exc:
        if exc.status == 404:
            return None
        raise
    return found if isinstance(found, dict) else None


ARGOCD_SA = "argocd-manager"
ARGOCD_SA_NAMESPACE = "kube-system"
ARGOCD_TOKEN_SECRET = "argocd-manager-token"
ARGOCD_BINDING = "argocd-manager-cluster-admin"


def ensure_argocd_manager(api_client: k8s_client.ApiClient) -> str | None:
    """bootstrap-installer argocd.py's three objects, found before created; the token once populated."""
    core = k8s_client.CoreV1Api(api_client)
    rbac = k8s_client.RbacAuthorizationV1Api(api_client)

    def absent(read: Callable[[], Any]) -> bool:
        try:
            read()
            return False
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                return True
            raise

    if absent(lambda: core.read_namespaced_service_account(ARGOCD_SA, ARGOCD_SA_NAMESPACE, _request_timeout=REQUEST_TIMEOUT)):
        core.create_namespaced_service_account(
            ARGOCD_SA_NAMESPACE, {"metadata": {"name": ARGOCD_SA}}, _request_timeout=REQUEST_TIMEOUT)
    if absent(lambda: core.read_namespaced_secret(ARGOCD_TOKEN_SECRET, ARGOCD_SA_NAMESPACE, _request_timeout=REQUEST_TIMEOUT)):
        core.create_namespaced_secret(ARGOCD_SA_NAMESPACE, {
            "metadata": {"name": ARGOCD_TOKEN_SECRET,
                         "annotations": {"kubernetes.io/service-account.name": ARGOCD_SA}},
            "type": "kubernetes.io/service-account-token",
        }, _request_timeout=REQUEST_TIMEOUT)
    if absent(lambda: rbac.read_cluster_role_binding(ARGOCD_BINDING, _request_timeout=REQUEST_TIMEOUT)):
        rbac.create_cluster_role_binding({
            "metadata": {"name": ARGOCD_BINDING},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
            "subjects": [{"kind": "ServiceAccount", "name": ARGOCD_SA, "namespace": ARGOCD_SA_NAMESPACE}],
        }, _request_timeout=REQUEST_TIMEOUT)
    secret = core.read_namespaced_secret(ARGOCD_TOKEN_SECRET, ARGOCD_SA_NAMESPACE, _request_timeout=REQUEST_TIMEOUT)
    token = (secret.data or {}).get("token")
    return base64.b64decode(token).decode() if token else None
