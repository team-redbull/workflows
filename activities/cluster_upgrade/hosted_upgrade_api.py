"""What the cluster-upgrade limb reads and writes inside a hosted cluster, as its admin.

One technology, plain parameters, no settings and no Temporal (CLAUDE.md §1),
like activities/server_lifecycle/hosted_cluster_api.py, whose admin-kubeconfig
handling it reuses: credentials in a per-call temp directory, the control
plane's in-cluster Service instead of the external endpoint, TLS verified, and
nothing from the kubeconfig in any error.

It only fetches and writes. What the objects mean is upgrade_state.py's — also
where it decides what to fetch next (which namespaces' Pods, whether an ack is
already there), as hosted_cluster_api.py asks hosted_resources.py. Each read
opens ONE session and reads only what its caller uses.

Its WRITES are an admin ack (DESIGN §6.2a, D16) — one key of
openshift-config/admin-acks, only for a gate openshift-config-managed/admin-gates
holds, read back before returning — and the tier-2 actions a person approved
(DESIGN §8.3), which action_catalog.py checked first: a pod delete, a Job from
a CronJob, and the delete of a Job that has failed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from kubernetes import client as k8s_client

from activities.cluster_upgrade import upgrade_state
from activities.server_lifecycle.hosted_cluster_api import (
    HOSTED_REQUEST_TIMEOUT as _REQUEST_TIMEOUT,
    as_plain,
    call_hosted,
    open_hosted_api,
)
from shared.exceptions import AdminAckNotAppliedError, AdminGateNotFoundError

# Where the CVO keeps the gates (from the release payload) and the acks
# (create-only: the CVO makes it once and never overwrites it, seen live).
ADMIN_GATES = ("openshift-config-managed", "admin-gates")
ADMIN_ACKS = ("openshift-config", "admin-acks")

# Warning events are listed by key, not by time: every page is followed, up to
# _EVENT_CAP events, so warning_events picks the newest from all of them rather
# than from whichever 200 sort first. A cluster with more keeps the first
# _EVENT_CAP by key.
_EVENT_PAGE = 200
_EVENT_CAP = 2000


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


def _plain(api_client, response) -> dict[str, Any]:
    return (response if isinstance(response, dict) else as_plain(api_client, response)) or {}


def _items(api_client, response) -> list[dict[str, Any]]:
    return list(_plain(api_client, response).get("items") or [])


def _cluster_version(api_client, cluster: str) -> dict[str, Any]:
    return _cluster_object(api_client, "config.openshift.io", "v1", "clusterversions", "version", cluster) or {}


def _cluster_operators(api_client, cluster: str) -> list[dict[str, Any]]:
    return _items(api_client, call_hosted(
        lambda: _custom(api_client).list_cluster_custom_object(
            group="config.openshift.io", version="v1", plural="clusteroperators", _request_timeout=_REQUEST_TIMEOUT,
        ),
        cluster,
        "a list of ClusterOperators",
    ))


def _all_pages(api_client, page: Callable[[str | None], Any], cluster: str, what: str, cap: int) -> list[dict[str, Any]]:
    """Every item of a paged list, following `metadata.continue`, up to `cap` items."""
    items: list[dict[str, Any]] = []
    token: str | None = None
    while len(items) < cap:
        plain = _plain(api_client, call_hosted(lambda token=token: page(token), cluster, what))
        items.extend(plain.get("items") or [])
        token = (plain.get("metadata") or {}).get("continue") or None
        if token is None:
            break
    return items[:cap]


def _read_cluster_operators(kubeconfig: str, server: str, cluster: str) -> list[dict[str, Any]]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, _core):
        return _cluster_operators(api_client, cluster)


async def read_cluster_operators(kubeconfig: str, server: str, cluster: str) -> list[dict[str, Any]]:
    """The hosted ClusterOperators, and nothing else: what a control-plane watch polls."""
    return await asyncio.to_thread(_read_cluster_operators, kubeconfig, server, cluster)


def _read_admin_ack_objects(kubeconfig: str, server: str, cluster: str, apis: list[str]) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        return {
            "cluster_version": _cluster_version(api_client, cluster),
            "admin_gates": _config_map(api_client, core, *ADMIN_GATES, cluster),
            "admin_acks": _config_map(api_client, core, *ADMIN_ACKS, cluster),
            "api_request_counts": {
                api: _cluster_object(api_client, "apiserver.openshift.io", "v1", "apirequestcounts", api, cluster)
                for api in apis
            },
        }


async def read_admin_ack_objects(kubeconfig: str, server: str, cluster: str, apis: list[str]) -> dict[str, Any]:
    """The hosted ClusterVersion, admin-gates, admin-acks and the APIRequestCount of each api.

    Absent objects are None (an API nobody called has no APIRequestCount).
    Blocking SDK, so it runs in a thread.
    """
    return await asyncio.to_thread(_read_admin_ack_objects, kubeconfig, server, cluster, apis)


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
        # Pods only where a customer's PDB is at 0 (blocking_pdbs judges no
        # other), Endpoints only where a webhook's Service lives: a big
        # cluster's every Pod is not needed to answer either question.
        return {
            "cluster_version": _cluster_version(api_client, cluster),
            "cluster_operators": _cluster_operators(api_client, cluster),
            "nodes": listing(lambda: core.list_node(_request_timeout=t), "a list of Nodes"),
            "csrs": listing(lambda: certificates.list_certificate_signing_request(_request_timeout=t),
                            "a list of CertificateSigningRequests"),
            "pdbs": pdbs,
            "pods": [
                pod for namespace in upgrade_state.blocking_pdb_namespaces(pdbs)
                for pod in listing(lambda namespace=namespace: core.list_namespaced_pod(namespace, _request_timeout=t),
                                   f"a list of Pods in {namespace}")
            ],
            "webhooks": webhooks,
            "endpoints": [
                e for namespace in upgrade_state.webhook_service_namespaces(webhooks)
                for e in listing(lambda namespace=namespace: core.list_namespaced_endpoints(namespace, _request_timeout=t),
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
    """In one session: ClusterVersion, ClusterOperators, Nodes, CSRs, PDBs (and the Pods they
    could cover), webhooks (and their Endpoints), IDMS."""
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


def _read_gate_objects(kubeconfig: str, server: str, cluster: str, nodepool: str, label: str, workload: bool) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        t = _REQUEST_TIMEOUT
        nodes = _items(api_client, call_hosted(
            lambda: core.list_node(label_selector=f"{label}={nodepool}", _request_timeout=t),
            cluster, f"a list of the Nodes of NodePool {nodepool}",
        ))
        if not workload:
            return {"nodes": nodes}
        policy, apps = k8s_client.PolicyV1Api(api_client), k8s_client.AppsV1Api(api_client)
        pods = [
            pod
            for node in nodes
            for pod in _items(api_client, call_hosted(
                lambda node=node: core.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node['metadata']['name']}", _request_timeout=t),
                cluster, f"a list of the Pods on Node {node['metadata']['name']}",
            ))
        ] + _items(api_client, call_hosted(
            lambda: core.list_pod_for_all_namespaces(field_selector="spec.nodeName=", _request_timeout=t),
            cluster, "a list of the Pods not scheduled yet",
        ))
        return {
            "nodes": nodes,
            "pods": pods,
            "pdbs": _items(api_client, call_hosted(
                lambda: policy.list_pod_disruption_budget_for_all_namespaces(_request_timeout=t),
                cluster, "a list of PodDisruptionBudgets",
            )),
            "statefulsets": _items(api_client, call_hosted(
                lambda: apps.list_stateful_set_for_all_namespaces(_request_timeout=t),
                cluster, "a list of StatefulSets",
            )),
        }


async def read_gate_objects(
    kubeconfig: str, server: str, cluster: str, nodepool: str, label: str, workload: bool
) -> dict[str, Any]:
    """A NodePool's Nodes; with `workload`, also the Pods on them or unscheduled, every PDB and StatefulSet."""
    return await asyncio.to_thread(_read_gate_objects, kubeconfig, server, cluster, nodepool, label, workload)


# The data plane a control-plane step depends on (AGENT.md #15): its operators
# run on worker nodes.
DATA_PLANE_NAMESPACES = ("openshift-ingress", "openshift-image-registry", "openshift-console")


def _read_evidence_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        t = _REQUEST_TIMEOUT
        return {
            "cluster_operators": _cluster_operators(api_client, cluster),
            "data_plane_pods": [
                pod for namespace in DATA_PLANE_NAMESPACES
                for pod in _items(api_client, call_hosted(
                    lambda namespace=namespace: core.list_namespaced_pod(namespace, _request_timeout=t),
                    cluster, f"a list of Pods in {namespace}",
                ))
            ],
            "warning_events": _all_pages(
                api_client,
                lambda token: core.list_event_for_all_namespaces(
                    field_selector="type=Warning", limit=_EVENT_PAGE, _continue=token, _request_timeout=t,
                ),
                cluster, "a list of Warning events", _EVENT_CAP,
            ),
        }


async def read_evidence_objects(kubeconfig: str, server: str, cluster: str) -> dict[str, Any]:
    """In one session: the ClusterOperators, the data plane's Pods and the Warning events
    (every page, up to _EVENT_CAP), for the agent's evidence. Never a Secret."""
    return await asyncio.to_thread(_read_evidence_objects, kubeconfig, server, cluster)


def _give_admin_ack(kubeconfig: str, server: str, cluster: str, gate: str) -> bool:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, core):
        if not upgrade_state.gate_held(_config_map(api_client, core, *ADMIN_GATES, cluster), gate):
            raise AdminGateNotFoundError(
                f"hosted cluster {cluster} holds no admin gate {gate} in "
                f"{'/'.join(ADMIN_GATES)}; nothing was written"
            )
        acks = _config_map(api_client, core, *ADMIN_ACKS, cluster)
        if upgrade_state.acked(acks, gate):
            return False
        namespace, name = ADMIN_ACKS
        patch = upgrade_state.ack_patch(gate)
        if acks is None:
            # The release creates it; recreate it only as it would be, empty but for this key.
            call_hosted(
                lambda: core.create_namespaced_config_map(
                    namespace=namespace,
                    body=k8s_client.V1ConfigMap(
                        metadata=k8s_client.V1ObjectMeta(name=name, namespace=namespace), data=patch["data"]
                    ),
                    _request_timeout=_REQUEST_TIMEOUT,
                ),
                cluster,
                f"a create of ConfigMap {namespace}/{name}",
            )
        else:
            call_hosted(
                lambda: core.patch_namespaced_config_map(
                    name=name, namespace=namespace, body=patch, _request_timeout=_REQUEST_TIMEOUT,
                ),
                cluster,
                f"an ack of {gate} in ConfigMap {namespace}/{name}",
            )
        if not upgrade_state.acked(_config_map(api_client, core, namespace, name, cluster), gate):
            # Retryable: the next attempt reads and writes again.
            raise AdminAckNotAppliedError(
                f"hosted cluster {cluster}: the ack of {gate} in ConfigMap {namespace}/{name} "
                "did not read back as \"true\""
            )
        return True


async def give_admin_ack(kubeconfig: str, server: str, cluster: str, gate: str) -> bool:
    """Ack one admin gate. True when this call wrote it, False when it was already "true".

    Raises AdminGateNotFoundError (permanent) for a gate admin-gates does not
    hold, AdminAckNotAppliedError (retryable) for an ack that did not read back.
    """
    return await asyncio.to_thread(_give_admin_ack, kubeconfig, server, cluster, gate)


# --- tier-2 actions a person approved (DESIGN §8.3) -------------------------------

_CONFLICT = 409


def _delete_pod(kubeconfig: str, server: str, cluster: str, namespace: str, name: str, uid: str | None) -> str:
    with open_hosted_api(kubeconfig, server, cluster) as (_api_client, core):
        body = k8s_client.V1DeleteOptions(preconditions=k8s_client.V1Preconditions(uid=uid)) if uid else None

        def _delete() -> str:
            try:
                core.delete_namespaced_pod(name=name, namespace=namespace, body=body, _request_timeout=_REQUEST_TIMEOUT)
            except k8s_client.ApiException as exc:
                if exc.status == _CONFLICT and uid:
                    return "replaced"  # the UID precondition failed: a new pod holds the name
                raise
            return "deleted"

        return call_hosted(_delete, cluster, f"a delete of Pod {namespace}/{name}", absent="gone")


async def delete_pod(kubeconfig: str, server: str, cluster: str, namespace: str, name: str, uid: str | None) -> str:
    """Delete one Pod: "deleted", "gone" (absent already) or "replaced" (its UID is not `uid`)."""
    return await asyncio.to_thread(_delete_pod, kubeconfig, server, cluster, namespace, name, uid)


def _job_from_cronjob(cronjob: dict[str, Any], job_name: str, annotations: dict[str, str]) -> dict[str, Any]:
    """The Job `oc create job --from=cronjob/<name>` makes — the jobTemplate, owned by the CronJob —
    carrying `annotations` too: which request made it."""
    metadata = cronjob.get("metadata") or {}
    template = (cronjob.get("spec") or {}).get("jobTemplate") or {}
    template_meta = template.get("metadata") or {}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": metadata.get("namespace"),
            "labels": dict(template_meta.get("labels") or {}),
            "annotations": {
                **(template_meta.get("annotations") or {}), "cronjob.kubernetes.io/instantiate": "manual",
                **annotations,
            },
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": metadata.get("name"),
                                 "uid": metadata.get("uid"), "controller": True}],
        },
        "spec": template.get("spec") or {},
    }


def _rerun_cronjob(
    kubeconfig: str, server: str, cluster: str, namespace: str, name: str, job_name: str, annotations: dict[str, str]
) -> str:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, _core):
        batch = k8s_client.BatchV1Api(api_client)
        cronjob = call_hosted(
            lambda: batch.read_namespaced_cron_job(name=name, namespace=namespace, _request_timeout=_REQUEST_TIMEOUT),
            cluster, f"a read of CronJob {namespace}/{name}", absent=None,
        )
        if cronjob is None:
            return "missing"
        body = _job_from_cronjob(as_plain(api_client, cronjob), job_name, annotations)

        def _create() -> str:
            try:
                batch.create_namespaced_job(namespace=namespace, body=body, _request_timeout=_REQUEST_TIMEOUT)
            except k8s_client.ApiException as exc:
                if exc.status == _CONFLICT:
                    return "taken"
                raise
            return "created"

        if call_hosted(_create, cluster, f"a create of Job {namespace}/{job_name}") == "created":
            return "created"
        # The name is taken: this request's own Job (an earlier attempt made it)
        # only if it carries this request's annotations; anyone else's is left alone.
        existing = call_hosted(
            lambda: batch.read_namespaced_job(name=job_name, namespace=namespace, _request_timeout=_REQUEST_TIMEOUT),
            cluster, f"a read of Job {namespace}/{job_name}", absent=None,
        )
        if existing is None:
            return "vanished"
        return "exists" if upgrade_state.annotated_with(as_plain(api_client, existing), annotations) else "conflict"


async def rerun_cronjob(
    kubeconfig: str, server: str, cluster: str, namespace: str, name: str, job_name: str, annotations: dict[str, str]
) -> str:
    """A Job from the CronJob, named `job_name` and carrying `annotations`.

    "created"; "exists" — the name is taken by a Job carrying these annotations,
    this request's own from an earlier attempt; "conflict" — taken by any other
    Job, left alone and nothing created; "vanished" — taken, then gone when
    read back, nothing created by this attempt; "missing" — no CronJob.
    """
    return await asyncio.to_thread(_rerun_cronjob, kubeconfig, server, cluster, namespace, name, job_name, annotations)


def _delete_failed_job(kubeconfig: str, server: str, cluster: str, namespace: str, name: str, uid: str | None) -> str:
    with open_hosted_api(kubeconfig, server, cluster) as (api_client, _core):
        batch = k8s_client.BatchV1Api(api_client)
        job = call_hosted(
            lambda: batch.read_namespaced_job(name=name, namespace=namespace, _request_timeout=_REQUEST_TIMEOUT),
            cluster, f"a read of Job {namespace}/{name}", absent=None,
        )
        if job is None:
            return "gone"
        job = as_plain(api_client, job)
        current = (job.get("metadata") or {}).get("uid")
        if uid and current != uid:
            return "replaced"
        if not upgrade_state.job_failed(job):
            return "not-failed"
        # The UID read just now is the precondition, so a Job recreated under the
        # name in between is never the one deleted; Background takes its Pods too.
        body = k8s_client.V1DeleteOptions(
            propagation_policy="Background", preconditions=k8s_client.V1Preconditions(uid=current),
        )

        def _delete() -> str:
            try:
                batch.delete_namespaced_job(name=name, namespace=namespace, body=body, _request_timeout=_REQUEST_TIMEOUT)
            except k8s_client.ApiException as exc:
                if exc.status == _CONFLICT:
                    return "replaced"
                raise
            return "deleted"

        return call_hosted(_delete, cluster, f"a delete of Job {namespace}/{name}", absent="gone")


async def delete_failed_job(kubeconfig: str, server: str, cluster: str, namespace: str, name: str, uid: str | None) -> str:
    """Delete one Job only if it has failed: "deleted", "gone", "replaced" (its UID is not `uid`) or "not-failed"."""
    return await asyncio.to_thread(_delete_failed_job, kubeconfig, server, cluster, namespace, name, uid)
