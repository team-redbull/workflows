"""The target cluster's Kubernetes API — every call this limb makes to it.

Split out of activities.py for the reason values_repo.py is: it owns ONE
technology end to end, takes plain parameters, and holds no settings and no
Temporal — the @activity.defn wrappers in activities.py own both. So this module
is the only place that imports the kubernetes client, and the only place that has
to know it is SYNCHRONOUS: every public function here is async and runs the
blocking SDK in `asyncio.to_thread`, because calling it on the event loop
directly would stall every other activity on this worker's queue.

Two rules shape the rest of it.

IDEMPOTENCY. A create treats ALREADY EXISTS as success, which is what makes a
re-run converge instead of failing. It is not an upsert: the existing object is
left exactly as it is, so a BMC credential or a bond an operator corrected by
hand survives. `differences` is what keeps that from becoming a lie — reporting
success for an object that names a different BMC, boot MAC, InfraEnv or VLAN
would describe an installation that is not the one on the cluster.

CLASSIFICATION. Anything left as BmhResourceError retries every minute forever
with the run sitting RUNNING rather than FAILED, so every status a retry cannot
fix is named in `_classify` rather than defaulted.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from shared.exceptions import (
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    BmhResourceError,
)
from shared.models.server_lifecycle import CreatedResource

_NOT_FOUND = 404
_FORBIDDEN = 403
_ALREADY_EXISTS = 409
_INVALID = (400, 422)

# Which fields of an object already on the cluster disagree with the one we
# would have created. Pure: the builders in bmh_resources.py supply it.
Differences = Callable[[dict[str, Any], dict[str, Any]], list[str]]

_MAX_BODY_CHARS = 500


def _load_kube() -> None:
    """Load in-cluster config, falling back to a kubeconfig for local runs.

    Called at the start of every operation rather than once at import: the
    kubernetes client's global configuration is process-wide and idempotent to
    set, and doing it here means no operation can depend on another having run
    first.
    """
    try:
        k8s_config.load_incluster_config()
    except Exception:  # noqa: BLE001 — any failure here means "not in a pod"
        k8s_config.load_kube_config()


def _detail(exc: k8s_client.ApiException) -> str:
    """The API server's own message, not just the HTTP status phrase.

    `exc.reason` is "Unprocessable Entity" and says nothing. The sentence an
    operator acts on is in the Status body — a rejected resource name comes back
    as "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric
    characters...", which is the entire diagnosis. That was found the hard way:
    a live 422 on a Secret named after an uppercase vendor serial was unreadable
    from `reason` alone.

    Safe to put in an error message, and that matters because activity errors
    are recorded verbatim in Temporal history: a Status body describes the
    REQUEST's faults (message, reason, details), never the request body, so a
    rejected Secret cannot leak its credentials through here.
    """
    body = getattr(exc, "body", None)
    if not body:
        return str(exc.reason)
    try:
        status = json.loads(body)
    except (TypeError, ValueError):
        return str(body)[:_MAX_BODY_CHARS]
    if isinstance(status, dict) and status.get("message"):
        return str(status["message"])
    return str(exc.reason)


def _classify(
    exc: k8s_client.ApiException, verb: str, kind: str, name: str
) -> Exception:
    """One ApiException as a classified domain error.

    401 is deliberately NOT named permanent: a projected ServiceAccount token is
    rotated under the pod, so a rejected one is the transient case and retrying
    is the correct response. 403 is the opposite — RBAC is a deployment gap.
    """
    detail = _detail(exc)
    if exc.status == _NOT_FOUND:
        # Either the resource TYPE or the NAMESPACE is absent, and a namespaced
        # request cannot tell them apart. Both are deployment gaps.
        return BmhPrerequisiteMissingError(
            f"Cannot {verb} {kind} {name}: the target cluster has no such "
            f"resource type, or no namespace for it — Metal3 / the Assisted "
            f"Installer is not installed, or the InfraEnv namespace is missing "
            f"({detail})"
        )
    if exc.status == _FORBIDDEN:
        return BmhPrerequisiteMissingError(
            f"Cannot {verb} {kind} {name}: this worker's ServiceAccount is not "
            f"authorized for it ({detail}). RBAC is a deployment gap, not a "
            "transient failure"
        )
    if exc.status in _INVALID:
        return BmhRequestInvalidError(
            f"The API server rejected {kind} {name} as invalid "
            f"({exc.status}): {detail}. The body is identical on every attempt, "
            "so this cannot clear"
        )
    return BmhResourceError(
        f"Failed to {verb} {kind} {name}: {exc.status} {detail}"
    )


def _create_if_absent(
    create: Callable[[], None],
    kind: str,
    name: str,
    on_exists: Callable[[], list[str]] | None = None,
) -> CreatedResource:
    """Run one create, treating ALREADY EXISTS as success once it MATCHES.

    `on_exists` reads the existing object back and returns the fields that
    disagree with the one we would have created. `None` means an existing object
    is accepted unconditionally — the Secret's case on purpose: the only way to
    compare one is to read a credential back out of the cluster, and the design
    never rotates a credential an operator may have corrected, so there is
    nothing a comparison could act on.
    """
    _load_kube()
    try:
        create()
    except k8s_client.ApiException as exc:
        if exc.status != _ALREADY_EXISTS:
            raise _classify(exc, "create", kind, name) from exc
        if on_exists is None:
            return CreatedResource(kind=kind, name=name, changed=False)
        try:
            mismatches = on_exists()
        except k8s_client.ApiException as read_exc:
            # It answered 409 and then would not be read — most likely deleted
            # in between, or a read this ServiceAccount is not allowed.
            # Classified rather than left as a raw ApiException, which would
            # carry the client's whole response body into Temporal history and
            # retry forever as an unrecognised error type.
            raise _classify(read_exc, "read back", kind, name) from read_exc
        if mismatches:
            raise BmhConflictError(
                f"{kind} {name} already exists and does not match this request: "
                + "; ".join(mismatches)
                + ". Nothing was overwritten — reconcile the existing resource "
                "or install a different server"
            ) from exc
        return CreatedResource(kind=kind, name=name, changed=False)
    return CreatedResource(kind=kind, name=name, changed=True)


async def create_secret_if_absent(
    namespace: str, body: dict[str, Any]
) -> CreatedResource:
    """Create one Secret, accepting an existing one of the same name."""
    name = body["metadata"]["name"]

    def _create() -> None:
        k8s_client.CoreV1Api().create_namespaced_secret(namespace=namespace, body=body)

    return await asyncio.to_thread(_create_if_absent, _create, "Secret", name)


async def create_custom_object_if_absent(
    *,
    group: str,
    version: str,
    plural: str,
    kind: str,
    namespace: str,
    body: dict[str, Any],
    differences: Differences,
) -> CreatedResource:
    """Create one namespaced custom resource, converging on an existing match."""
    name = body["metadata"]["name"]

    def _create() -> None:
        k8s_client.CustomObjectsApi().create_namespaced_custom_object(
            group=group, version=version, namespace=namespace, plural=plural, body=body
        )

    def _on_exists() -> list[str]:
        existing = k8s_client.CustomObjectsApi().get_namespaced_custom_object(
            group=group, version=version, namespace=namespace, plural=plural, name=name
        )
        return differences(body, existing)

    return await asyncio.to_thread(
        _create_if_absent, _create, kind, name, _on_exists
    )


async def read_custom_object(
    *,
    group: str,
    version: str,
    plural: str,
    kind: str,
    namespace: str,
    name: str,
) -> dict[str, Any] | None:
    """One namespaced custom resource, or None when it does not exist.

    A 404 is NOT an error here: it is the normal answer both while Ironic has
    not picked a host up yet and when probing whether a candidate is already
    installed. Every other status is classified — notably 403, so a missing
    `get` verb in this worker's RBAC fails the run instead of being retried
    forever as a transient read failure.
    """

    def _read() -> dict[str, Any] | None:
        _load_kube()
        try:
            return k8s_client.CustomObjectsApi().get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
            )
        except k8s_client.ApiException as exc:
            if exc.status == _NOT_FOUND:
                return None
            raise _classify(exc, "read", kind, name) from exc

    return await asyncio.to_thread(_read)
