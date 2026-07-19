"""Test-only mock of the external "next" connectivity (firewall) service.

This stands in for a black-box service owned by another team; the real
environment is air-gapped, so local runs and OpenShift e2e tests need a
stand-in (deployed via helm/mock-segment-connectivity/). The orchestrator treats the
three endpoints as opaque; this implementation only exists so those runs can
exercise the full submit -> poll -> complete cycle. In production you point
NEXT_URL at the real service and ignore this folder entirely.

Behavior:
  * POST /token-renewal-uri returns a static mock token.
  * POST /open-rules-uri requires an Authorization header (exercises the
    orchestrator's token wiring), validates the payload shape, stores the
    request in memory and returns {"id", "status": "pending"}.
  * GET /check-request-status/{id} reports "pending" until
    COMPLETION_DELAY_SECONDS have elapsed since submission (simulating the
    human approval), then "complete". Unknown ids -> 404.

State is in-memory only — restarting the mock forgets pending requests.
"""

from __future__ import annotations

import os
import random
import time
from typing import Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

# Simulated human-approval delay before a request turns "complete".
# 60s default: long enough to watch the pending request ids in the Segments
# Manager UI before they clear.
COMPLETION_DELAY_SECONDS = float(os.environ.get("COMPLETION_DELAY_SECONDS", "60"))

app = FastAPI(title="Mock next connectivity service (dev only)")

# request id -> submission time (monotonic)
_requests: dict[int, float] = {}


# --- payload models mirroring the next API contract (validation only) --------
class _Address(BaseModel):
    type: Literal["segment"]
    segment: str = Field(min_length=1)


class _Endpoint(BaseModel):
    system_name: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    addresses: list[_Address] = Field(min_length=1)


class _Port(BaseModel):
    type: Literal["port", "range"]
    port: int | None = None
    port_range_start: int | None = None
    port_range_end: int | None = None
    protocol: Literal["TCP", "UDP"]


class _Properties(BaseModel):
    source: _Endpoint
    destination: _Endpoint
    ports: list[_Port] = Field(min_length=1)


class OpenRulesPayload(BaseModel):
    ad_groups: list[str]
    comment: str = Field(min_length=1)
    properties: _Properties


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/token-renewal-uri")
async def renew_token() -> dict[str, str]:
    return {"access_token": "mock-next-token"}


@app.post("/open-rules-uri")
async def open_rules(
    payload: OpenRulesPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, int | str]:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    request_id = random.randint(100_000, 999_999)
    while request_id in _requests:  # ids must be unique
        request_id = random.randint(100_000, 999_999)
    _requests[request_id] = time.monotonic()

    return {"id": request_id, "status": "pending"}


@app.get("/check-request-status/{request_id}")
async def check_request_status(request_id: int) -> dict[str, str]:
    submitted_at = _requests.get(request_id)
    if submitted_at is None:
        raise HTTPException(status_code=404, detail=f"Unknown request id {request_id}")

    elapsed = time.monotonic() - submitted_at
    status = "complete" if elapsed >= COMPLETION_DELAY_SECONDS else "pending"
    return {"status": status}
