"""The kv-vm-api agent's API: PUT/GET/DELETE <url>/v1/vms/{name}. Plain parameters.

One agent per KubeVirt cluster, behind a Route. It renders the platform's
vm.yaml template server-side and owns the VM's shape; this side sends the
identity (namespace, size, MAC, labels) and reads back the state. Every
failure is classified here (CLAUDE.md §5), so the workflow's retry policy can
stop on the deterministic ones:

  * transport, timeout, 5xx, a 2xx that is not the model expected —
    KvVmApiUnavailableError (retried; the PUT is idempotent by name + identity,
    the DELETE by name + MAC, so a retry after a lost answer is safe);
  * 401/403 — KvAgentAuthError (permanent);
  * 409 — VmIdentityConflictError (permanent): the name holds ANOTHER identity
    (on a PUT) or the VM does not carry the caller's MAC (on a DELETE);
  * 404 on a PUT — VmNamespaceNotFoundError (permanent); on a GET it is None
    and on a DELETE it is "already gone" (False) — both normal answers;
  * 422 — VmRequestRejectedError (permanent), the agent's wording carried.

The token travels in the Authorization header only, never in a URL or a
message. One httpx client per call.
"""

from __future__ import annotations

import ssl

import httpx
from pydantic import BaseModel, ValidationError

from activities.vm_lifecycle.kv_capacity_api import auth_headers
from shared.exceptions import (
    KvAgentAuthError,
    KvVmApiUnavailableError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
)
from shared.models.vm_lifecycle import KvApiError, KvVmConflict, KvVmCreateRequest, KvVmCreateResponse, KvVmState

VMS_PATH = "/v1/vms"


def _detail(response: httpx.Response) -> str:
    """The agent's own sentence for a person, when its body is a KvApiError; else the raw text."""
    try:
        error = KvApiError.model_validate_json(response.content)
    except ValidationError:
        return response.text[:300]
    return f"{error.error}: {error.detail}" if error.detail else error.error


def _conflict(response: httpx.Response, what: str) -> VmIdentityConflictError:
    try:
        conflict = KvVmConflict.model_validate_json(response.content)
    except ValidationError:
        return VmIdentityConflictError(f"{what}: kv-vm-api answered 409: {response.text[:300]}")
    existing = conflict.existing
    held = (
        f" (it holds namespace {existing.namespace}, mac {existing.mac}, {existing.cpu} vCPU, "
        f"{existing.memory_gib} GiB)" if existing is not None else ""
    )
    return VmIdentityConflictError(f"{what}: {conflict.reason}{held}")


async def _send(
    method: str, url: str, token: str, *, verify: ssl.SSLContext | bool, timeout: float, **kwargs
) -> httpx.Response:
    try:
        async with httpx.AsyncClient(timeout=timeout, verify=verify) as client:
            response = await client.request(method, url, headers=auth_headers(token), **kwargs)
    except httpx.TimeoutException as err:
        raise KvVmApiUnavailableError(f"kv-vm-api did not answer {method} in {timeout:g}s ({type(err).__name__})") from None
    except httpx.HTTPError as err:
        raise KvVmApiUnavailableError(f"kv-vm-api is unreachable ({type(err).__name__}: {err})") from None
    status = response.status_code
    if status in (401, 403):
        raise KvAgentAuthError(f"kv-vm-api refused the token on {method} (HTTP {status})")
    if status >= 500:
        raise KvVmApiUnavailableError(f"kv-vm-api answered HTTP {status} on {method}: {response.text[:300]}")
    return response


def _parse(model: type[BaseModel], response: httpx.Response, what: str):
    try:
        return model.model_validate_json(response.content)
    except ValidationError as err:
        errors = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'body'}: {e['msg']}" for e in err.errors()[:3])
        raise KvVmApiUnavailableError(f"{what}: kv-vm-api's reply is not a {model.__name__}: {errors}") from None


async def put_vm(
    url: str, token: str, *, name: str, request: KvVmCreateRequest, verify: ssl.SSLContext | bool, timeout: float
) -> KvVmCreateResponse:
    """PUT /v1/vms/{name}: 201 created, 200 the same identity already there (`created` False)."""
    what = f"VM {request.namespace}/{name}"
    response = await _send(
        "PUT", f"{url}{VMS_PATH}/{name}", token, verify=verify, timeout=timeout, json=request.model_dump(mode="json")
    )
    status = response.status_code
    if status == 409:
        raise _conflict(response, what)
    if status == 404:
        raise VmNamespaceNotFoundError(f"{what}: namespace {request.namespace} does not exist on the cluster: {_detail(response)}")
    if status == 422 or status == 400:
        raise VmRequestRejectedError(f"{what}: kv-vm-api refused the request: {_detail(response)}")
    if status >= 400:
        raise KvVmApiUnavailableError(f"{what}: kv-vm-api answered HTTP {status}: {response.text[:300]}")
    return _parse(KvVmCreateResponse, response, what)


async def get_vm(
    url: str, token: str, *, name: str, namespace: str, verify: ssl.SSLContext | bool, timeout: float
) -> KvVmState | None:
    """GET /v1/vms/{name}?namespace=: the VM, or None when the agent has no such VM (404)."""
    what = f"VM {namespace}/{name}"
    response = await _send(
        "GET", f"{url}{VMS_PATH}/{name}", token, verify=verify, timeout=timeout, params={"namespace": namespace}
    )
    if response.status_code == 404:
        return None
    if response.status_code >= 400:
        raise KvVmApiUnavailableError(f"{what}: kv-vm-api answered HTTP {response.status_code}: {response.text[:300]}")
    return _parse(KvVmState, response, what)


async def delete_vm(
    url: str, token: str, *, name: str, namespace: str, mac: str, verify: ssl.SSLContext | bool, timeout: float
) -> bool:
    """DELETE /v1/vms/{name}?namespace=&mac=: True deleted (204), False already gone (404).

    The MAC is the guard: the agent deletes only a VM carrying it and answers
    409 for any other, raised as VmIdentityConflictError — the caller's VM was
    replaced, and the replacement is not the caller's to delete.
    """
    what = f"VM {namespace}/{name}"
    response = await _send(
        "DELETE", f"{url}{VMS_PATH}/{name}", token, verify=verify, timeout=timeout,
        params={"namespace": namespace, "mac": mac},
    )
    status = response.status_code
    if status == 404:
        return False
    if status == 409:
        raise _conflict(response, f"{what} is not this run's (mac {mac})")
    if status >= 400:
        raise KvVmApiUnavailableError(f"{what}: kv-vm-api answered HTTP {status} on DELETE: {response.text[:300]}")
    return True
