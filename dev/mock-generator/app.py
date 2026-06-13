"""LOCAL DEV ONLY — mock of the external team's IPAM / segment generator.

This stands in for a black-box service owned by another team. The orchestrator
treats `POST /generate` as opaque; this implementation only exists so local runs
return segments the Segments Manager will accept. In production you point
GENERATOR_URL at the real service and ignore this folder entirely.

Site-prefix knowledge lives ONLY here (never in the orchestrator): each site's
segment must start with its configured octet, so we read existing segments from
the Segments Manager and pick the next free VLAN + /24 within the site's prefix.
"""

from __future__ import annotations

import os

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SEGMENT_MANAGER_URL = os.environ.get("SEGMENT_MANAGER_URL")

# site -> first octet, e.g. "site1:192,site2:193,site3:194"
SITE_PREFIXES = {
    pair.split(":")[0]: int(pair.split(":")[1])
    for pair in os.environ.get(
        "SITE_PREFIXES", "site1:192,site2:193,site3:194"
    ).split(",")
    if pair
}

app = FastAPI(title="Mock Segment Generator (dev only)")


class GenerateRequest(BaseModel):
    site: str


class GeneratedSegment(BaseModel):
    site: str
    vlan_id: int
    segment: str
    epg_name: str
    dhcp: bool = False


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/generate", response_model=GeneratedSegment)
async def generate(req: GenerateRequest) -> GeneratedSegment:
    prefix = SITE_PREFIXES.get(req.site)
    if prefix is None:
        raise HTTPException(status_code=400, detail=f"Unknown site {req.site}")

    async with httpx.AsyncClient(base_url=SEGMENT_MANAGER_URL, timeout=10.0) as client:
        resp = await client.get("/api/segments")
        resp.raise_for_status()
        segments = resp.json()

    used_vlans = {s["vlan_id"] for s in segments if s.get("site") == req.site}
    # Track used third octets within this site's /24 space.
    used_octets = set()
    for s in segments:
        if s.get("site") != req.site:
            continue
        try:
            octets = s["segment"].split("/")[0].split(".")
            used_octets.add(int(octets[2]))
        except (KeyError, IndexError, ValueError):
            continue

    vlan_id = next(v for v in range(100, 4095) if v not in used_vlans)
    third = next(o for o in range(0, 256) if o not in used_octets)

    return GeneratedSegment(
        site=req.site,
        vlan_id=vlan_id,
        segment=f"{prefix}.168.{third}.0/24",
        epg_name=f"EPG_AUTO_{vlan_id}",
        dhcp=False,
    )
