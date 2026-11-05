"""The kv-capacity agent's API: GET <url>/v1/capacity -> KvCapacityReport. Plain parameters.

One agent per KubeVirt cluster, behind a Route, read-only (nodes, the pods'
requests, our Halted VMs' reservations, every VM's MAC). This client NEVER
raises per cluster: the rank fan-out needs every cluster's outcome side by
side, so a cluster that cannot be read comes back as a KvProbeFailure with
the reason the table shows (placement.SKIP_REASONS), and the activity
decides what the whole fan-out means (all unreachable: retried; all refused:
a wrong token, permanent).

  * a timeout, a connection error, a 5xx other than the router's 503 —
    `unreachable` (the cluster, its ingress or the agent is down: a retry
    may see it);
  * 404, or the OpenShift router's 503 for a Route with no endpoints —
    `no-agent` (not deployed there yet);
  * 401/403 — `refused-token`;
  * a 2xx that is not a KvCapacityReport, or a 4xx that is not one of the
    above — `malformed` (a contract mismatch: tests/test_kv_agents_contract.py).

The token travels in the Authorization header only, never in a URL or a
message. One httpx client per call.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import ValidationError

from shared.models.vm_lifecycle import KvCapacityReport

CAPACITY_PATH = "/v1/capacity"
ProbeReason = Literal["unreachable", "no-agent", "refused-token", "malformed"]


@dataclass(frozen=True)
class KvProbeFailure:
    """Why one cluster's report could not be read; `reason` is a placement.SKIP_REASONS key."""

    reason: ProbeReason
    detail: str


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


async def read_capacity(
    url: str, token: str, *, verify: ssl.SSLContext | bool, timeout: float
) -> KvCapacityReport | KvProbeFailure:
    """One GET; `timeout` is the per-cluster budget inside the activity's fan-out."""
    try:
        async with httpx.AsyncClient(timeout=timeout, verify=verify) as client:
            response = await client.get(f"{url}{CAPACITY_PATH}", headers=auth_headers(token))
    except httpx.TimeoutException as err:
        return KvProbeFailure("unreachable", f"kv-capacity did not answer in {timeout:g}s ({type(err).__name__})")
    except httpx.HTTPError as err:
        return KvProbeFailure("unreachable", f"kv-capacity is unreachable ({type(err).__name__}: {err})")
    status = response.status_code
    if status in (401, 403):
        return KvProbeFailure("refused-token", f"kv-capacity refused the token (HTTP {status})")
    if status == 404 or status == 503:
        return KvProbeFailure("no-agent", f"no kv-capacity behind the route (HTTP {status})")
    if status >= 500:
        return KvProbeFailure("unreachable", f"kv-capacity answered HTTP {status}")
    if status >= 400:
        return KvProbeFailure("malformed", f"kv-capacity refused the request: HTTP {status}: {response.text[:300]}")
    try:
        return KvCapacityReport.model_validate_json(response.content)
    except ValidationError as err:
        errors = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'body'}: {e['msg']}" for e in err.errors()[:3])
        return KvProbeFailure("malformed", f"kv-capacity's reply is not a KvCapacityReport: {errors}")
