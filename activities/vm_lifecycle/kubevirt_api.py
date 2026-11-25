"""The KV cluster's own Kubernetes API — every call its vm-lifecycle worker makes to it.

The worker runs INSIDE the KV cluster and talks to its own API server as its
own ServiceAccount (in-cluster config; a kubeconfig for a developer). This is
the only module of the limb that imports the kubernetes client, and the only
one that has to know it is SYNCHRONOUS: every public method is async and runs
the blocking SDK in `asyncio.to_thread`, so one slow call never stalls the
other activities on this worker's queue (the pattern of
activities/server_lifecycle/cluster_api.py; the classification is this limb's own).

Plain parameters, no settings and no Temporal (CLAUDE.md §1). Objects travel
as the API server's own JSON (camelCase), read with `_preload_content=False`
so a ten-thousand-pod list is never turned into ten thousand SDK models; the
pod list is paged and each page reduced as it arrives.

CLASSIFICATION — everything a retry cannot fix is named, the rest is transient:
  * KubeMacPool's "failed to allocate requested mac address" -> MacRefused, at
    ANY status and matched FIRST (it arrives as an admission denial that would
    otherwise read as an invalid request). The worker requests no MAC since
    2026-11-25 — KubeMacPool assigns one — so this is KubeMacPool unable to
    give a VM one, which the caller reports as VmMacNotAssignedError.
  * 409 AlreadyExists on a create -> NameTaken: the caller draws another MAC.
  * 404 -> None / False / NotFound: each caller knows what an absent object means.
  * 403 -> KvClusterAccessError (permanent): the chart's ClusterRole lacks a verb.
  * 400, 422 -> VmRequestRejectedError (permanent): the same body is refused again.
  * everything else — 5xx, 401 (a rotated ServiceAccount token), 429, 410, a
    timeout, a dropped connection — KvClusterApiError (retried).
Every message is bounded: an activity error is recorded verbatim in history.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any, TypeVar

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from shared.exceptions import KvClusterAccessError, KvClusterApiError, VmRequestRejectedError

KUBEVIRT_GROUP, KUBEVIRT_VERSION = "kubevirt.io", "v1"
CDI_GROUP, CDI_VERSION = "cdi.kubevirt.io", "v1beta1"
VIRTUAL_MACHINES = "virtualmachines"
VIRTUAL_MACHINE_INSTANCES = "virtualmachineinstances"
DATA_VOLUMES = "datavolumes"

# (connect, read) seconds on EVERY call: the SDK's own default is no timeout
# at all. Read at 20 s, the bound the figures read promises the hub
# (docs/design/create-vm-placement.md §6.3.2).
REQUEST_TIMEOUT = (5, 20)
PAGE_SIZE = 500
UNFINISHED_PODS = "status.phase!=Succeeded,status.phase!=Failed"
MAX_DETAIL_CHARS = 400
_KUBEMACPOOL_REFUSAL = "failed to allocate requested mac address"

_NOT_FOUND = 404
_FORBIDDEN = 403
_CONFLICT = 409
_INVALID = (400, 422)

T = TypeVar("T")


class NameTaken(Exception):
    """A VirtualMachine of that name already exists in the namespace (409 AlreadyExists)."""


class MacRefused(Exception):
    """KubeMacPool refused the MAC: another VM of the cluster holds it."""


class NotFound(Exception):
    """The create answered 404: the namespace is missing, or the cluster does not serve VirtualMachines."""


def bounded(text: str) -> str:
    """At most MAX_DETAIL_CHARS of a dependency's own wording, saying how much was cut."""
    text = " ".join(str(text).split())
    if len(text) <= MAX_DETAIL_CHARS:
        return text
    return f"{text[:MAX_DETAIL_CHARS]}… ({len(text) - MAX_DETAIL_CHARS} more characters)"


def _status(exc: k8s_client.ApiException) -> dict:
    try:
        parsed = json.loads(exc.body) if exc.body else {}
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def detail(exc: BaseException) -> str:
    """The API server's own sentence (its Status message), not the bare HTTP phrase; bounded."""
    if isinstance(exc, k8s_client.ApiException):
        status = _status(exc)
        if status.get("message"):
            return bounded(str(status["message"]))
        if exc.body:
            return bounded(str(exc.body))
        return bounded(f"{exc.status} {exc.reason}")
    return bounded(f"{type(exc).__name__}: {exc}")


def is_mac_refusal(exc: BaseException) -> bool:
    if not isinstance(exc, k8s_client.ApiException):
        return False
    text = f"{exc.body or ''} {exc.reason or ''}".lower()
    return _KUBEMACPOOL_REFUSAL in text


def is_already_exists(exc: BaseException) -> bool:
    return (
        isinstance(exc, k8s_client.ApiException)
        and exc.status == _CONFLICT
        and (_status(exc).get("reason") == "AlreadyExists" or "already exists" in f"{exc.body or ''}".lower())
    )


def classify(exc: BaseException, doing: str) -> Exception:
    """One failure of a call as the error the workflow classifies (see the module docstring)."""
    said = detail(exc)
    if isinstance(exc, k8s_client.ApiException):
        if exc.status == _FORBIDDEN:
            return KvClusterAccessError(
                f"this cluster's API refused its vm-lifecycle worker's ServiceAccount while {doing} (403): {said}. "
                "The chart's ClusterRole is missing a verb"
            )
        if exc.status in _INVALID:
            return VmRequestRejectedError(f"this cluster's API refused the request while {doing} ({exc.status}): {said}")
        return KvClusterApiError(f"this cluster's API failed while {doing} ({exc.status}): {said}")
    return KvClusterApiError(f"this cluster's API could not be reached while {doing}: {said}")


def _load_kube() -> None:
    """In-cluster config, falling back to a kubeconfig for a developer's run."""
    try:
        k8s_config.load_incluster_config()
    except Exception:  # noqa: BLE001 — any failure here means "not in a pod"
        k8s_config.load_kube_config()


def _decode(response: Any) -> dict:
    """A `_preload_content=False` response, or an already-decoded object, as a dict."""
    if isinstance(response, dict):
        return response
    data = getattr(response, "data", response)
    parsed = json.loads(data)
    if not isinstance(parsed, dict):
        raise ValueError(f"the API answered a {type(parsed).__name__}, not an object")
    return parsed


class KubevirtApi:
    """The reads and writes the worker makes on its own cluster, each one bounded call.

    `core` and `custom` are the kubernetes client's CoreV1Api and
    CustomObjectsApi; left None they are built from the pod's own
    ServiceAccount on first use (the unit tests hand in fakes).
    """

    def __init__(self, core: Any = None, custom: Any = None) -> None:
        self._core = core
        self._custom = custom

    # --- plumbing ---------------------------------------------------------------------

    def _clients(self) -> tuple[Any, Any]:
        if self._core is None or self._custom is None:
            _load_kube()
            self._core = self._core or k8s_client.CoreV1Api()
            self._custom = self._custom or k8s_client.CustomObjectsApi()
        return self._core, self._custom

    async def _run(self, doing: str, call: Callable[[], T]) -> T:
        """`call` in a thread; anything it raises classified (never a bare SDK error)."""
        try:
            return await asyncio.to_thread(call)
        except (NameTaken, MacRefused, NotFound):
            raise
        except Exception as exc:  # noqa: BLE001 — classified, every kind of it
            raise classify(exc, doing) from None

    @staticmethod
    def _pages(fetch: Callable[[str | None], Any], keep: Callable[[dict], T | None] | None = None) -> list[Any]:
        """Every item of a paged list, each page reduced through `keep` as it arrives."""
        items: list[Any] = []
        token: str | None = None
        while True:
            page = _decode(fetch(token))
            for item in page.get("items") or []:
                if keep is None:
                    items.append(item)
                else:
                    kept = keep(item)
                    if kept is not None:
                        items.append(kept)
            token = (page.get("metadata") or {}).get("continue") or None
            if not token:
                return items

    # --- reads: the figures -------------------------------------------------------------

    async def list_nodes(self) -> list[dict]:
        core, _ = self._clients()
        return await self._run(
            "listing nodes",
            lambda: self._pages(
                lambda token: core.list_node(
                    limit=PAGE_SIZE, _continue=token, _preload_content=False, _request_timeout=REQUEST_TIMEOUT
                )
            ),
        )

    async def list_unfinished_pods(self, keep: Callable[[dict], T | None]) -> list[T]:
        """Every pod that is not Succeeded or Failed, cluster-wide, each reduced through `keep`."""
        core, _ = self._clients()
        return await self._run(
            "listing pods",
            lambda: self._pages(
                lambda token: core.list_pod_for_all_namespaces(
                    field_selector=UNFINISHED_PODS, limit=PAGE_SIZE, _continue=token,
                    _preload_content=False, _request_timeout=REQUEST_TIMEOUT,
                ),
                keep,
            ),
        )

    async def list_vms(self, label_selector: str | None = None) -> list[dict]:
        """VirtualMachines in every namespace, optionally only those matching a label selector."""
        return await self._list_custom(KUBEVIRT_GROUP, KUBEVIRT_VERSION, VIRTUAL_MACHINES, label_selector)

    async def list_vmis(self) -> list[dict]:
        return await self._list_custom(KUBEVIRT_GROUP, KUBEVIRT_VERSION, VIRTUAL_MACHINE_INSTANCES, None)

    async def _list_custom(self, group: str, version: str, plural: str, label_selector: str | None) -> list[dict]:
        _, custom = self._clients()
        kwargs: dict[str, Any] = {"limit": PAGE_SIZE, "_request_timeout": REQUEST_TIMEOUT}
        if label_selector:
            kwargs["label_selector"] = label_selector

        def fetch(token: str | None) -> Any:
            return custom.list_cluster_custom_object(
                group, version, plural, **kwargs, **({"_continue": token} if token else {})
            )

        return await self._run(f"listing {plural}", lambda: self._pages(fetch))

    # --- reads and writes: one VM -------------------------------------------------------

    async def namespace_exists(self, namespace: str) -> bool:
        core, _ = self._clients()

        def call() -> bool:
            try:
                core.read_namespace(namespace, _preload_content=False, _request_timeout=REQUEST_TIMEOUT)
            except k8s_client.ApiException as exc:
                if exc.status == _NOT_FOUND:
                    return False
                raise
            return True

        return await self._run(f"reading namespace {namespace}", call)

    async def get_vm(self, namespace: str, name: str) -> dict | None:
        return await self._get_custom(KUBEVIRT_GROUP, KUBEVIRT_VERSION, VIRTUAL_MACHINES, namespace, name)

    async def get_datavolume(self, namespace: str, name: str) -> dict | None:
        return await self._get_custom(CDI_GROUP, CDI_VERSION, DATA_VOLUMES, namespace, name)

    async def _get_custom(self, group: str, version: str, plural: str, namespace: str, name: str) -> dict | None:
        _, custom = self._clients()

        def call() -> dict | None:
            try:
                return _decode(
                    custom.get_namespaced_custom_object(
                        group, version, namespace, plural, name, _request_timeout=REQUEST_TIMEOUT
                    )
                )
            except k8s_client.ApiException as exc:
                if exc.status == _NOT_FOUND:
                    return None
                raise

        return await self._run(f"reading {plural} {namespace}/{name}", call)

    async def pvc_phase(self, namespace: str, name: str) -> str | None:
        """The PersistentVolumeClaim's phase (`Bound`, `Pending`); None when there is none."""
        core, _ = self._clients()

        def call() -> str | None:
            try:
                pvc = _decode(
                    core.read_namespaced_persistent_volume_claim(
                        name, namespace, _preload_content=False, _request_timeout=REQUEST_TIMEOUT
                    )
                )
            except k8s_client.ApiException as exc:
                if exc.status == _NOT_FOUND:
                    return None
                raise
            return (pvc.get("status") or {}).get("phase")

        return await self._run(f"reading PersistentVolumeClaim {namespace}/{name}", call)

    async def create_vm(self, namespace: str, body: dict) -> dict:
        """Create the VirtualMachine. NameTaken / MacRefused: draw again. NotFound: no such
        namespace, or the cluster serves no VirtualMachines."""
        _, custom = self._clients()
        name = (body.get("metadata") or {}).get("name", "")

        def call() -> dict:
            try:
                return _decode(
                    custom.create_namespaced_custom_object(
                        KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, VIRTUAL_MACHINES, body,
                        _request_timeout=REQUEST_TIMEOUT,
                    )
                )
            except k8s_client.ApiException as exc:
                if is_mac_refusal(exc):
                    raise MacRefused(detail(exc)) from None
                if is_already_exists(exc):
                    raise NameTaken(detail(exc)) from None
                if exc.status == _NOT_FOUND:
                    raise NotFound(detail(exc)) from None
                raise

        return await self._run(f"creating VirtualMachine {namespace}/{name}", call)

    async def delete_vm(self, namespace: str, name: str, uid: str | None = None) -> bool:
        """Delete the VirtualMachine, only while it is still the object `uid` names.

        False when it was already gone — or is no longer that object (a failed
        UID precondition answers 409): either way the VM that was meant is gone.
        """
        return await self._delete_custom(KUBEVIRT_GROUP, KUBEVIRT_VERSION, VIRTUAL_MACHINES, namespace, name, uid)

    async def delete_datavolume(self, namespace: str, name: str) -> bool:
        return await self._delete_custom(CDI_GROUP, CDI_VERSION, DATA_VOLUMES, namespace, name, None)

    async def _delete_custom(
        self, group: str, version: str, plural: str, namespace: str, name: str, uid: str | None
    ) -> bool:
        _, custom = self._clients()
        body = k8s_client.V1DeleteOptions(preconditions=k8s_client.V1Preconditions(uid=uid)) if uid else None

        def call() -> bool:
            try:
                custom.delete_namespaced_custom_object(
                    group, version, namespace, plural, name, body=body, _request_timeout=REQUEST_TIMEOUT
                )
            except k8s_client.ApiException as exc:
                if exc.status == _NOT_FOUND or (uid is not None and exc.status == _CONFLICT):
                    return False
                raise
            return True

        return await self._run(f"deleting {plural} {namespace}/{name}", call)
