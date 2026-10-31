"""A hosted cluster's own API server — what uninstall-server reads and writes there.

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

The WRITES — a cordon, Pod evictions, a Node delete — are only ever for a Node
HyperShift lost track of (no Machine behind it), the one case where CAPI will
not drain and delete it itself. Which Node is the server's and which Pods a
drain moves are decided in hosted_resources.py; this module only carries them
out.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import pathlib
import tempfile
from typing import Any

import urllib3
import yaml
from kubernetes import client as k8s_client

from activities.server_lifecycle import hosted_resources
from shared.exceptions import HostedClusterAccessError, HostedClusterUnreachableError

_NOT_FOUND = 404
_UNAUTHORIZED = 401
_FORBIDDEN = 403
_TOO_MANY_REQUESTS = 429

# `_call(..., absent=...)` without `absent`: a 404 is an error like any other.
_RAISE = object()

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


def _classify(exc: Exception, cluster: str, what: str) -> Exception:
    """One failure of a hosted API call as a classified domain error.

    `what` names the call (`a read of Node node-1`), never anything from the
    kubeconfig.
    """
    if isinstance(exc, k8s_client.ApiException):
        if exc.status == _FORBIDDEN:
            return HostedClusterAccessError(
                f"the API server of hosted cluster {cluster} refused the admin "
                f"kubeconfig {what} (403)"
            )
        if exc.status == _UNAUTHORIZED:
            return HostedClusterUnreachableError(
                f"the API server of hosted cluster {cluster} rejected the admin "
                "credential (401); it is re-read on the next attempt"
            )
        return HostedClusterUnreachableError(
            f"the API server of hosted cluster {cluster} answered {exc.status} to {what}"
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


def _status_message(exc: k8s_client.ApiException) -> str:
    """The API server's own sentence (`Cannot evict pod as it would violate…`)."""
    try:
        body = json.loads(exc.body or "")
    except (TypeError, ValueError):
        return str(exc.reason)
    return str(body.get("message") or exc.reason) if isinstance(body, dict) else str(exc.reason)


@contextlib.contextmanager
def _hosted_api(kubeconfig: str, server: str, cluster: str):
    """(ApiClient, CoreV1Api) as the hosted cluster's admin, credentials in a temp dir.

    The directory — CA, certificate, key — is deleted the moment the block ends.
    """
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

        with k8s_client.ApiClient(configuration) as api_client:
            yield api_client, k8s_client.CoreV1Api(api_client)


def _call(fn, cluster: str, what: str, *, absent: Any = _RAISE):
    """One hosted API call; a 404 is `absent` when given, every other failure classified."""
    try:
        return fn()
    except k8s_client.ApiException as exc:
        if exc.status == _NOT_FOUND and absent is not _RAISE:
            return absent
        raise _classify(exc, cluster, what) from None
    except urllib3.exceptions.HTTPError as exc:
        raise _classify(exc, cluster, what) from None


def _plain(api_client, obj: Any) -> dict[str, Any]:
    """An SDK model as the API's own JSON shape (camelCase), so hosted_resources reads dicts."""
    plain = api_client.sanitize_for_serialization(obj)
    return plain if isinstance(plain, dict) else {}


def _qualified(obj: dict[str, Any]) -> str:
    metadata = obj.get("metadata") or {}
    return f"{metadata.get('namespace')}/{metadata.get('name')}"


def _node_exists(kubeconfig: str, server: str, node_name: str, cluster: str) -> bool:
    with _hosted_api(kubeconfig, server, cluster) as (_api_client, core):
        node = _call(
            lambda: core.read_node(name=node_name, _request_timeout=_REQUEST_TIMEOUT),
            cluster,
            f"a read of Node {node_name}",
            absent=None,
        )
    return node is not None


async def node_exists(kubeconfig: str, server: str, node_name: str, cluster: str) -> bool:
    """Whether the hosted cluster has a Node of this name.

    `kubeconfig` is the Secret's text, `server` the URL to use instead of the
    one inside it, `cluster` only names the hosted cluster in errors. Blocking
    SDK, so it runs in a thread, like every call in cluster_api.py.
    """
    return await asyncio.to_thread(_node_exists, kubeconfig, server, node_name, cluster)


def _list_nodes(kubeconfig: str, server: str, cluster: str) -> list[dict[str, Any]]:
    with _hosted_api(kubeconfig, server, cluster) as (api_client, core):
        nodes = _call(
            lambda: core.list_node(_request_timeout=_REQUEST_TIMEOUT), cluster, "a list of Nodes"
        )
        return list(_plain(api_client, nodes).get("items") or [])


async def list_nodes(kubeconfig: str, server: str, cluster: str) -> list[dict[str, Any]]:
    """Every Node of the hosted cluster, as plain dicts."""
    return await asyncio.to_thread(_list_nodes, kubeconfig, server, cluster)


def _read_owned_node(core, api_client, node_name: str, provider_id: str | None, cluster: str):
    """(node, matches): None when absent; matches=False when its providerID is not the one matched."""
    node = _call(
        lambda: core.read_node(name=node_name, _request_timeout=_REQUEST_TIMEOUT),
        cluster,
        f"a read of Node {node_name}",
        absent=None,
    )
    if node is None:
        return None, True
    plain = _plain(api_client, node)
    # Still exactly the Node that was matched: same providerID, or still none.
    return plain, hosted_resources.node_provider_id(plain) == provider_id


def _drain_node(
    kubeconfig: str, server: str, node_name: str, provider_id: str | None, cluster: str
) -> dict[str, Any]:
    with _hosted_api(kubeconfig, server, cluster) as (api_client, core):
        node, matches = _read_owned_node(core, api_client, node_name, provider_id, cluster)
        if node is None:
            return {"found": False}
        if not matches:
            return {"found": True, "matches": False}
        cordoned = False
        if not (node.get("spec") or {}).get("unschedulable"):
            _call(
                lambda: core.patch_node(
                    name=node_name, body={"spec": {"unschedulable": True}},
                    _request_timeout=_REQUEST_TIMEOUT,
                ),
                cluster,
                f"a cordon of Node {node_name}",
            )
            cordoned = True
        pods = _call(
            lambda: core.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}", _request_timeout=_REQUEST_TIMEOUT
            ),
            cluster,
            f"a list of the Pods on Node {node_name}",
        )
        to_move = [
            pod for pod in _plain(api_client, pods).get("items") or []
            if hosted_resources.pod_to_evict(pod)
        ]
        evicted: list[str] = []
        blocked: list[str] = []
        for pod in to_move:
            metadata = pod.get("metadata") or {}
            if metadata.get("deletionTimestamp"):
                continue  # already going: wait for it, do not evict it twice
            name, namespace = metadata.get("name"), metadata.get("namespace")
            eviction = k8s_client.V1Eviction(
                metadata=k8s_client.V1ObjectMeta(name=name, namespace=namespace)
            )
            try:
                core.create_namespaced_pod_eviction(
                    name=name, namespace=namespace, body=eviction, _request_timeout=_REQUEST_TIMEOUT
                )
            except k8s_client.ApiException as exc:
                if exc.status == _NOT_FOUND:
                    continue
                # 429: a PodDisruptionBudget refuses it now; 5xx: e.g. a Pod
                # under two budgets. Both are reported and retried next pass,
                # inside the workflow's drain deadline, not by an activity
                # retry that would hide them.
                if exc.status == _TOO_MANY_REQUESTS or exc.status >= 500:
                    blocked.append(f"{namespace}/{name}: {_status_message(exc)}")
                    continue
                raise _classify(exc, cluster, f"an eviction of Pod {namespace}/{name}") from None
            except urllib3.exceptions.HTTPError as exc:
                raise _classify(exc, cluster, f"an eviction of Pod {namespace}/{name}") from None
            evicted.append(f"{namespace}/{name}")
        return {
            "found": True,
            "cordoned": cordoned,
            "evicted": evicted,
            "remaining": sorted(_qualified(pod) for pod in to_move),
            "blocked": blocked,
        }


async def drain_node(
    kubeconfig: str, server: str, node_name: str, provider_id: str | None, cluster: str
) -> dict[str, Any]:
    """One drain pass over a Node: cordon it, evict every Pod a drain moves.

    Returns {found, matches, cordoned, evicted, remaining, blocked}: `remaining`
    lists the Pods to move that were still on the Node when the pass began —
    the drain is done when a pass finds none. Nothing is done to a Node whose
    providerID is no longer the one it was matched with (`matches` False).
    """
    return await asyncio.to_thread(_drain_node, kubeconfig, server, node_name, provider_id, cluster)


def _delete_node(
    kubeconfig: str, server: str, node_name: str, provider_id: str | None, cluster: str
) -> dict[str, Any]:
    with _hosted_api(kubeconfig, server, cluster) as (api_client, core):
        node, matches = _read_owned_node(core, api_client, node_name, provider_id, cluster)
        if node is None:
            return {"found": False}
        if not matches:
            return {"found": True, "matches": False}
        if hosted_resources.node_ready(node):
            # A kubelet still runs as it: deleting would only see it registered again.
            return {"found": True, "ready": True}
        deleted = _call(
            lambda: core.delete_node(name=node_name, _request_timeout=_REQUEST_TIMEOUT),
            cluster,
            f"a delete of Node {node_name}",
            absent=False,
        )
        return {"found": deleted is not False, "deleted": deleted is not False}


async def delete_node(
    kubeconfig: str, server: str, node_name: str, provider_id: str | None, cluster: str
) -> dict[str, Any]:
    """Delete a Node nothing runs as any more. Returns {found, matches, ready, deleted}.

    Refuses (deleted False) a Node still Ready — a running kubelet would
    register it again at once — or one whose providerID is no longer the one it
    was matched with (`matches` False).
    """
    return await asyncio.to_thread(_delete_node, kubeconfig, server, node_name, provider_id, cluster)


# --- shared with the cluster-upgrade limb ---------------------------------------
# The cluster-upgrade limb reads other objects in the same hosted clusters, the
# same way (activities/cluster_upgrade/hosted_upgrade_api.py). Admin kubeconfigs
# are handled in exactly one place, this module, so it reuses these rather than
# keeping a second copy of the parsing, the temp directory and the error rules.
open_hosted_api = _hosted_api
call_hosted = _call
as_plain = _plain
HOSTED_REQUEST_TIMEOUT = _REQUEST_TIMEOUT
