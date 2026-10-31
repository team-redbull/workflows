"""What the cluster-upgrade limb reads and writes inside a hosted cluster, as its admin.

One technology, plain parameters, no settings and no Temporal (CLAUDE.md §1),
like activities/server_lifecycle/hosted_cluster_api.py, whose admin-kubeconfig
handling it reuses: credentials in a per-call temp directory, the control
plane's in-cluster Service instead of the external endpoint, TLS verified, and
nothing from the kubeconfig in any error.

It only fetches and writes. What the objects mean is upgrade_state.py's.

The one WRITE is an admin ack (DESIGN §6.2a, D16): one key of
openshift-config/admin-acks, only for a gate openshift-config-managed/admin-gates
holds, read back before returning.
"""

from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client

from activities.server_lifecycle.hosted_cluster_api import (
    HOSTED_REQUEST_TIMEOUT as _REQUEST_TIMEOUT,
    as_plain,
    call_hosted,
    open_hosted_api,
)
from shared.exceptions import AdminGateNotFoundError, HostedClusterUnreachableError

# Where the CVO keeps the gates (from the release payload) and the acks
# (create-only: the CVO makes it once and never overwrites it, seen live).
ADMIN_GATES = ("openshift-config-managed", "admin-gates")
ADMIN_ACKS = ("openshift-config", "admin-acks")


def _custom(api_client) -> k8s_client.CustomObjectsApi:
    return k8s_client.CustomObjectsApi(api_client)


def _cluster_object(api_client, group: str, version: str, plural: str, name: str, cluster: str):
    """A cluster-scoped object, or None when absent."""
    return call_hosted(
        lambda: _custom(api_client).get_cluster_custom_object(
            group=group, version=version, plural=plural, name=name, _request_timeout=_REQUEST_TIMEOUT
        ),
        cluster,
        f"a read of {plural}/{name}",
        absent=None,
    )


def _config_map(api_client, core, namespace: str, name: str, cluster: str) -> dict[str, Any] | None:
    found = call_hosted(
        lambda: core.read_namespaced_config_map(name=name, namespace=namespace, _request_timeout=_REQUEST_TIMEOUT),
        cluster,
        f"a read of ConfigMap {namespace}/{name}",
        absent=None,
    )
    return None if found is None else as_plain(api_client, found)


def _read_upgrade_objects(kubeconfig: str, server: str, cluster: str, apis: list[str]) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        cluster_version = _cluster_object(
            api_client, "config.openshift.io", "v1", "clusterversions", "version", cluster
        )
        operators = call_hosted(
            lambda: _custom(api_client).list_cluster_custom_object(
                group="config.openshift.io", version="v1", plural="clusteroperators",
                _request_timeout=_REQUEST_TIMEOUT,
            ),
            cluster,
            "a list of ClusterOperators",
        )
        return {
            "cluster_version": cluster_version or {},
            "cluster_operators": list((operators or {}).get("items") or []),
            "admin_gates": _config_map(api_client, core, *ADMIN_GATES, cluster),
            "admin_acks": _config_map(api_client, core, *ADMIN_ACKS, cluster),
            "api_request_counts": {
                api: _cluster_object(api_client, "apiserver.openshift.io", "v1", "apirequestcounts", api, cluster)
                for api in apis
            },
        }


async def read_upgrade_objects(kubeconfig: str, server: str, cluster: str, apis: list[str]) -> dict[str, Any]:
    """The hosted ClusterVersion, ClusterOperators, admin-gates, admin-acks and the APIRequestCount of each api.

    Absent objects are None (an API nobody called has no APIRequestCount).
    Blocking SDK, so it runs in a thread.
    """
    return await asyncio.to_thread(_read_upgrade_objects, kubeconfig, server, cluster, apis)


def _items(api_client, response) -> list[dict[str, Any]]:
    plain = response if isinstance(response, dict) else as_plain(api_client, response)
    return list((plain or {}).get("items") or [])


def _read_health_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        custom = _custom(api_client)
        policy = k8s_client.PolicyV1Api(api_client)
        admission = k8s_client.AdmissionregistrationV1Api(api_client)
        certificates = k8s_client.CertificatesV1Api(api_client)
        t = _REQUEST_TIMEOUT

        def listing(fn, what):
            return _items(api_client, call_hosted(fn, cluster, what))

        pdbs = listing(lambda: policy.list_pod_disruption_budget_for_all_namespaces(_request_timeout=t),
                       "a list of PodDisruptionBudgets")
        webhooks = (
            listing(lambda: admission.list_validating_webhook_configuration(_request_timeout=t),
                    "a list of ValidatingWebhookConfigurations")
            + listing(lambda: admission.list_mutating_webhook_configuration(_request_timeout=t),
                      "a list of MutatingWebhookConfigurations")
        )
        # Pods only where a customer PDB is at 0, Endpoints only where a
        # fail-closed webhook's Service lives: a big cluster's every Pod is
        # not needed to answer either question.
        pdb_namespaces = sorted({
            (pdb.get("metadata") or {}).get("namespace")
            for pdb in pdbs if (pdb.get("status") or {}).get("disruptionsAllowed", 1) == 0
        } - {None})
        service_namespaces = sorted({
            ((w.get("clientConfig") or {}).get("service") or {}).get("namespace")
            for c in webhooks for w in c.get("webhooks") or []
        } - {None})
        return {
            "nodes": listing(lambda: core.list_node(_request_timeout=t), "a list of Nodes"),
            "csrs": listing(lambda: certificates.list_certificate_signing_request(_request_timeout=t),
                            "a list of CertificateSigningRequests"),
            "pdbs": pdbs,
            "pods": [
                pod for namespace in pdb_namespaces
                for pod in listing(lambda: core.list_namespaced_pod(namespace, _request_timeout=t),
                                   f"a list of Pods in {namespace}")
            ],
            "webhooks": [as_plain(api_client, c) if not isinstance(c, dict) else c for c in webhooks],
            "endpoints": [
                e for namespace in service_namespaces
                for e in listing(lambda: core.list_namespaced_endpoints(namespace, _request_timeout=t),
                                 f"a list of Endpoints in {namespace}")
            ],
            "image_digest_mirror_sets": listing(
                lambda: custom.list_cluster_custom_object(
                    group="config.openshift.io", version="v1", plural="imagedigestmirrorsets", _request_timeout=t,
                ),
                "a list of ImageDigestMirrorSets",
            ),
        }


async def read_health_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    """Nodes, CSRs, PDBs (and the Pods they could cover), webhooks (and their Endpoints), IDMS."""
    return await asyncio.to_thread(_read_health_objects, kubeconfig, server, cluster)


def _read_nodepool_objects(kubeconfig: str, server: str, cluster: str, nodepool: str, label: str) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        policy = k8s_client.PolicyV1Api(api_client)
        t = _REQUEST_TIMEOUT
        nodes = _items(api_client, call_hosted(
            lambda: core.list_node(label_selector=f"{label}={nodepool}", _request_timeout=t),
            cluster, f"a list of the Nodes of NodePool {nodepool}",
        ))
        # Pods and PDBs only around cordoned Nodes: a drain is what can block.
        pods = [
            pod
            for node in nodes if (node.get("spec") or {}).get("unschedulable")
            for pod in _items(api_client, call_hosted(
                lambda node=node: core.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node['metadata']['name']}", _request_timeout=t),
                cluster, f"a list of the Pods on Node {node['metadata']['name']}",
            ))
        ]
        pdbs = [
            pdb
            for namespace in sorted({(p.get("metadata") or {}).get("namespace") for p in pods} - {None})
            for pdb in _items(api_client, call_hosted(
                lambda namespace=namespace: policy.list_namespaced_pod_disruption_budget(namespace, _request_timeout=t),
                cluster, f"a list of the PodDisruptionBudgets in {namespace}",
            ))
        ]
        return {"nodes": nodes, "pods": pods, "pdbs": pdbs}


async def read_nodepool_objects(kubeconfig: str, server: str, cluster: str, nodepool: str, label: str) -> dict[str, Any]:
    """A NodePool's Nodes (by HyperShift's label), and the Pods and PDBs around those cordoned."""
    return await asyncio.to_thread(_read_nodepool_objects, kubeconfig, server, cluster, nodepool, label)


# The data plane a control-plane step depends on (AGENT.md #15): its operators
# run on worker nodes.
DATA_PLANE_NAMESPACES = ("openshift-ingress", "openshift-image-registry", "openshift-console")


def _read_evidence_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        t = _REQUEST_TIMEOUT
        return {
            "data_plane_pods": [
                pod for namespace in DATA_PLANE_NAMESPACES
                for pod in _items(api_client, call_hosted(
                    lambda namespace=namespace: core.list_namespaced_pod(namespace, _request_timeout=t),
                    cluster, f"a list of Pods in {namespace}",
                ))
            ],
            "warning_events": _items(api_client, call_hosted(
                lambda: core.list_event_for_all_namespaces(field_selector="type=Warning", limit=200, _request_timeout=t),
                cluster, "a list of Warning events",
            )),
        }


async def read_evidence_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    """The data plane's Pods and recent Warning events, for the agent's evidence. Never a Secret."""
    return await asyncio.to_thread(_read_evidence_objects, kubeconfig, server, cluster)


def _give_admin_ack(kubeconfig: str, server: str, cluster: str, gate: str) -> bool:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        gates = _config_map(api_client, core, *ADMIN_GATES, cluster) or {}
        if gate not in (gates.get("data") or {}):
            raise AdminGateNotFoundError(
                f"hosted cluster {cluster} holds no admin gate {gate} in "
                f"{'/'.join(ADMIN_GATES)}; nothing was written"
            )
        acks = _config_map(api_client, core, *ADMIN_ACKS, cluster)
        if acks is not None and (acks.get("data") or {}).get(gate) == "true":
            return False
        namespace, name = ADMIN_ACKS
        if acks is None:
            # The release creates it; recreate it only as it would be, empty but for this key.
            call_hosted(
                lambda: core.create_namespaced_config_map(
                    namespace=namespace,
                    body=k8s_client.V1ConfigMap(
                        metadata=k8s_client.V1ObjectMeta(name=name, namespace=namespace), data={gate: "true"}
                    ),
                    _request_timeout=_REQUEST_TIMEOUT,
                ),
                cluster,
                f"a create of ConfigMap {namespace}/{name}",
            )
        else:
            call_hosted(
                lambda: core.patch_namespaced_config_map(
                    name=name, namespace=namespace, body={"data": {gate: "true"}},
                    _request_timeout=_REQUEST_TIMEOUT,
                ),
                cluster,
                f"an ack of {gate} in ConfigMap {namespace}/{name}",
            )
        written = _config_map(api_client, core, namespace, name, cluster) or {}
        if (written.get("data") or {}).get(gate) != "true":
            # Retryable: the next attempt reads and writes again.
            raise HostedClusterUnreachableError(
                f"hosted cluster {cluster}: the ack of {gate} did not read back as \"true\""
            )
        return True


async def give_admin_ack(kubeconfig: str, server: str, cluster: str, gate: str) -> bool:
    """Ack one admin gate. True when this call wrote it, False when it was already "true".

    Raises AdminGateNotFoundError (permanent) for a gate admin-gates does not hold.
    """
    return await asyncio.to_thread(_give_admin_ack, kubeconfig, server, cluster, gate)
