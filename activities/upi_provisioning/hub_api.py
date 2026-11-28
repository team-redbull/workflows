"""The HUB's own Kubernetes API — every call the upi-provisioning worker makes to it.

The worker runs on the hub as its own ServiceAccount (in-cluster config; a
kubeconfig for a developer). Plain parameters, no settings, no Temporal (CLAUDE.md
§1). The SDK is synchronous: every public function is async and runs it in
`asyncio.to_thread`, so a slow call never stalls the other activities.

CLASSIFICATION: 403, 400, 422 -> HubAccessError (permanent: the chart's Role, or a
manifest the API refuses); anything else — 5xx, 401, 409 of a race, a timeout, a
dropped connection — HubApiError (retried). 404 is an answer (None), never an
error, where the caller asked "is it there".
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from typing import Any, TypeVar

import urllib3
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from shared.exceptions import HubAccessError, HubApiError

T = TypeVar("T")
REQUEST_TIMEOUT = (5, 30)
MAX_DETAIL_CHARS = 400
_PERMANENT = (400, 403, 422)
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
        message = body.get("message") if isinstance(body, dict) else None
    except (TypeError, ValueError):
        message = None
    return str(message or exc.reason)[:MAX_DETAIL_CHARS]


async def _call(what: str, fn: Callable[[], T], *, absent: bool = False) -> T | None:
    def run() -> T | None:
        try:
            return fn()
        except k8s_client.ApiException as exc:
            if exc.status == 404 and absent:
                return None
            if exc.status in _PERMANENT:
                raise HubAccessError(f"the hub refused {what} ({exc.status}): {_detail(exc)}") from None
            raise HubApiError(f"the hub answered {exc.status} to {what}: {_detail(exc)}") from None
        except urllib3.exceptions.HTTPError as exc:
            raise HubApiError(f"the hub's API could not be reached for {what}: {type(exc).__name__}") from None

    return await asyncio.to_thread(run)


def _plain(obj: Any) -> dict[str, Any]:
    plain = _client().sanitize_for_serialization(obj)
    return plain if isinstance(plain, dict) else {}


async def ensure_pvc(namespace: str, manifest: dict[str, Any]) -> bool:
    """Create the claim unless one of that name exists (never changed, never deleted). True if created."""
    core = k8s_client.CoreV1Api(_client())
    name = manifest["metadata"]["name"]
    found = await _call(f"a read of PVC {name}", lambda: core.read_namespaced_persistent_volume_claim(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    if found is not None:
        return False
    await _call(f"the create of PVC {name}", lambda: core.create_namespaced_persistent_volume_claim(
        namespace, manifest, _request_timeout=REQUEST_TIMEOUT))
    return True


async def ensure_config_map(namespace: str, manifest: dict[str, Any]) -> bool:
    """Create the ConfigMap, or replace it when its data differs. True if written."""
    core = k8s_client.CoreV1Api(_client())
    name = manifest["metadata"]["name"]
    found = await _call(f"a read of ConfigMap {name}", lambda: core.read_namespaced_config_map(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    if found is None:
        await _call(f"the create of ConfigMap {name}", lambda: core.create_namespaced_config_map(
            namespace, manifest, _request_timeout=REQUEST_TIMEOUT))
        return True
    if (found.data or {}) == manifest["data"]:
        return False
    body = dict(manifest)
    body["metadata"] = {**manifest["metadata"], "resourceVersion": found.metadata.resource_version}
    await _call(f"the replace of ConfigMap {name}", lambda: core.replace_namespaced_config_map(
        name, namespace, body, _request_timeout=REQUEST_TIMEOUT))
    return True


async def read_config_map_data(namespace: str, name: str) -> dict[str, str] | None:
    core = k8s_client.CoreV1Api(_client())
    found = await _call(f"a read of ConfigMap {name}", lambda: core.read_namespaced_config_map(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    return None if found is None else dict(found.data or {})


async def get_job(namespace: str, name: str) -> dict[str, Any] | None:
    batch = k8s_client.BatchV1Api(_client())
    found = await _call(f"a read of Job {name}", lambda: batch.read_namespaced_job(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    return None if found is None else _plain(found)


async def create_job(namespace: str, manifest: dict[str, Any]) -> dict[str, Any]:
    """Create the Job; one that appeared meanwhile (a 409 of a parallel attempt) is read instead."""
    batch = k8s_client.BatchV1Api(_client())
    name = manifest["metadata"]["name"]

    def create() -> Any:
        try:
            return batch.create_namespaced_job(namespace, manifest, _request_timeout=REQUEST_TIMEOUT)
        except k8s_client.ApiException as exc:
            if exc.status == 409:
                return batch.read_namespaced_job(name, namespace, _request_timeout=REQUEST_TIMEOUT)
            raise

    return _plain(await _call(f"the create of Job {name}", create))


async def job_log_tail(namespace: str, job: str, lines: int) -> str:
    """The last `lines` lines of the Job's newest pod's log ('' when none can be read)."""
    core = k8s_client.CoreV1Api(_client())
    pods = await _call(f"a list of Job {job}'s pods", lambda: core.list_namespaced_pod(
        namespace, label_selector=f"job-name={job}", _request_timeout=REQUEST_TIMEOUT))
    items = sorted(pods.items or [], key=lambda p: p.metadata.creation_timestamp or 0)
    if not items:
        return ""
    name = items[-1].metadata.name
    try:
        log = await _call(f"the log of pod {name}", lambda: core.read_namespaced_pod_log(
            name, namespace, tail_lines=lines, _request_timeout=REQUEST_TIMEOUT), absent=True)
    except HubApiError:
        return ""
    return log or ""


async def read_secret(namespace: str, name: str) -> dict[str, str] | None:
    """The Secret's data, decoded; None when absent."""
    core = k8s_client.CoreV1Api(_client())
    found = await _call(f"a read of Secret {name}", lambda: core.read_namespaced_secret(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    if found is None:
        return None
    return {k: base64.b64decode(v).decode("utf-8", "replace") for k, v in (found.data or {}).items()}


async def read_secret_object(namespace: str, name: str) -> dict[str, Any] | None:
    core = k8s_client.CoreV1Api(_client())
    found = await _call(f"a read of Secret {name}", lambda: core.read_namespaced_secret(
        name, namespace, _request_timeout=REQUEST_TIMEOUT), absent=True)
    return None if found is None else _plain(found)


async def create_secret(namespace: str, manifest: dict[str, Any]) -> None:
    core = k8s_client.CoreV1Api(_client())
    name = manifest["metadata"]["name"]
    await _call(f"the create of Secret {name}", lambda: core.create_namespaced_secret(
        namespace, manifest, _request_timeout=REQUEST_TIMEOUT))


async def replace_secret(namespace: str, manifest: dict[str, Any], resource_version: str) -> None:
    core = k8s_client.CoreV1Api(_client())
    name = manifest["metadata"]["name"]
    body = dict(manifest)
    body["metadata"] = {**manifest["metadata"], "resourceVersion": resource_version}
    await _call(f"the replace of Secret {name}", lambda: core.replace_namespaced_secret(
        name, namespace, body, _request_timeout=REQUEST_TIMEOUT))
