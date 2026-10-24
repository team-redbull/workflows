"""A hosted cluster's own API server — the one read uninstall-server makes of it.

Split out like cluster_api.py: one technology, plain parameters, no settings and
no Temporal. cluster_api.py talks to the MCE this worker runs in, as the pod's
ServiceAccount; this module talks to a HOSTED cluster whose control plane runs
on that MCE, as the hosted cluster's admin — so it is the only place an admin
kubeconfig is ever handled, and it handles one per call:

  * PARSED HERE, NOT BY THE KUBERNETES CLIENT'S LOADER. That loader writes
    inline certificates and keys to temporary files it caches for the life of
    the PROCESS. Here the CA, certificate and key go into a directory that is
    deleted the moment the call returns.
  * THE SERVER IS REPLACED by the control plane's in-cluster Service,
    `https://kube-apiserver.<hcp-namespace>.svc:6443`. The kubeconfig names
    the external endpoint, which the pod network may not reach; the Service
    is always reachable from the MCE. TLS is still verified, hostname
    included, against the kubeconfig's own CA: HyperShift's API server
    serves a second, "private" certificate by SNI (`namedCertificates`,
    `pki.ReconcileKASServerPrivateCertSecret`) whose names are exactly
    `kube-apiserver`, `kube-apiserver.<ns>.svc` and `….svc.cluster.local`.
    An admin kubeconfig carrying only a custom external CA would fail that
    check — HostedClusterAccessError, which says TLS verification failed.
  * NOTHING FROM THE KUBECONFIG ENTERS AN ERROR. Activity errors are recorded
    verbatim in Temporal history; messages here name only the hosted cluster
    and what was wrong, never a value read from the file.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import pathlib
import tempfile
from typing import Any

import urllib3
import yaml
from kubernetes import client as k8s_client

from shared.exceptions import HostedClusterAccessError, HostedClusterUnreachableError

_NOT_FOUND = 404
_UNAUTHORIZED = 401
_FORBIDDEN = 403

# (connect, read) seconds — well under the activity's 90 s start_to_close.
_REQUEST_TIMEOUT = (10, 30)


def kas_service_url(hcp_namespace: str) -> str:
    """The hosted control plane's API server, by its in-cluster Service on the MCE."""
    return f"https://kube-apiserver.{hcp_namespace}.svc:6443"


def _named(entries: Any, name: str | None, kind: str, cluster: str) -> dict[str, Any]:
    """The kubeconfig entry of this name, or the only one when no name is given."""
    entries = [e for e in entries or [] if isinstance(e, dict)]
    if name is not None:
        for entry in entries:
            if entry.get("name") == name:
                return entry.get(kind) or {}
    elif len(entries) == 1:
        return entries[0].get(kind) or {}
    raise HostedClusterAccessError(
        f"the admin kubeconfig of hosted cluster {cluster} has no usable {kind} entry"
    )


def _decoded(value: Any, field: str, cluster: str) -> bytes | None:
    if not value:
        return None
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, TypeError, ValueError):
        raise HostedClusterAccessError(
            f"the admin kubeconfig of hosted cluster {cluster} carries a {field} "
            "that is not base64"
        ) from None


def _credentials(kubeconfig: str, cluster: str) -> dict[str, Any]:
    """The CA, client certificate / key and token of the kubeconfig's current context."""
    try:
        config = yaml.safe_load(kubeconfig)
    except yaml.YAMLError:
        raise HostedClusterAccessError(
            f"the admin kubeconfig of hosted cluster {cluster} does not parse as YAML"
        ) from None
    if not isinstance(config, dict):
        raise HostedClusterAccessError(
            f"the admin kubeconfig of hosted cluster {cluster} is not a kubeconfig"
        )
    context: dict[str, Any] = {}
    current = config.get("current-context")
    if current or len(config.get("contexts") or []) == 1:
        context = _named(config.get("contexts"), current or None, "context", cluster)
    cluster_entry = _named(config.get("clusters"), context.get("cluster"), "cluster", cluster)
    user_entry = _named(config.get("users"), context.get("user"), "user", cluster)

    ca = _decoded(cluster_entry.get("certificate-authority-data"), "CA", cluster)
    if ca is None:
        raise HostedClusterAccessError(
            f"the admin kubeconfig of hosted cluster {cluster} carries no "
            "certificate-authority-data, so its API server cannot be verified"
        )
    cert = _decoded(user_entry.get("client-certificate-data"), "client certificate", cluster)
    key = _decoded(user_entry.get("client-key-data"), "client key", cluster)
    token = user_entry.get("token") or None
    if not (cert and key) and not token:
        raise HostedClusterAccessError(
            f"the admin kubeconfig of hosted cluster {cluster} carries neither a "
            "client certificate and key nor a token"
        )
    return {"ca": ca, "cert": cert, "key": key, "token": token}


def _classify(exc: Exception, cluster: str, node_name: str) -> Exception:
    """One failure of the hosted API call as a classified domain error."""
    if isinstance(exc, k8s_client.ApiException):
        if exc.status == _FORBIDDEN:
            return HostedClusterAccessError(
                f"the API server of hosted cluster {cluster} refused the admin "
                f"kubeconfig a read of Node {node_name} (403)"
            )
        if exc.status == _UNAUTHORIZED:
            return HostedClusterUnreachableError(
                f"the API server of hosted cluster {cluster} rejected the admin "
                "credential (401); it is re-read on the next attempt"
            )
        return HostedClusterUnreachableError(
            f"the API server of hosted cluster {cluster} answered {exc.status} "
            f"reading Node {node_name}"
        )
    reason = getattr(exc, "reason", None)
    if isinstance(exc, urllib3.exceptions.SSLError) or isinstance(
        reason, urllib3.exceptions.SSLError
    ):
        # The same CA and the same certificate on every attempt: not transient.
        return HostedClusterAccessError(
            f"TLS to the API server of hosted cluster {cluster} failed verification "
            f"against its admin kubeconfig's CA ({type(reason or exc).__name__})"
        )
    return HostedClusterUnreachableError(
        f"the API server of hosted cluster {cluster} could not be reached: "
        f"{type(reason or exc).__name__}"
    )


def _node_exists(kubeconfig: str, server: str, node_name: str, cluster: str) -> bool:
    credentials = _credentials(kubeconfig, cluster)
    with tempfile.TemporaryDirectory(prefix="hosted-kas-") as tmp:
        directory = pathlib.Path(tmp)

        def _file(name: str, content: bytes) -> str:
            path = directory / name
            path.write_bytes(content)
            path.chmod(0o600)
            return str(path)

        configuration = k8s_client.Configuration()
        configuration.host = server
        configuration.verify_ssl = True
        configuration.ssl_ca_cert = _file("ca.crt", credentials["ca"])
        if credentials["cert"] and credentials["key"]:
            configuration.cert_file = _file("tls.crt", credentials["cert"])
            configuration.key_file = _file("tls.key", credentials["key"])
        if credentials["token"]:
            configuration.api_key = {"authorization": f"Bearer {credentials['token']}"}
        # No retries inside the call: the activity's own retry policy owns that.
        configuration.retries = False

        try:
            with k8s_client.ApiClient(configuration) as api_client:
                k8s_client.CoreV1Api(api_client).read_node(
                    name=node_name, _request_timeout=_REQUEST_TIMEOUT
                )
        except k8s_client.ApiException as exc:
            if exc.status == _NOT_FOUND:
                return False
            raise _classify(exc, cluster, node_name) from None
        except urllib3.exceptions.HTTPError as exc:
            raise _classify(exc, cluster, node_name) from None
    return True


async def node_exists(kubeconfig: str, server: str, node_name: str, cluster: str) -> bool:
    """Whether the hosted cluster has a Node of this name.

    `kubeconfig` is the Secret's text, `server` the URL to use instead of the
    one inside it, `cluster` only names the hosted cluster in errors. Blocking
    SDK, so it runs in a thread, like every call in cluster_api.py.
    """
    return await asyncio.to_thread(_node_exists, kubeconfig, server, node_name, cluster)
