"""A temporary page for cluster upgrades, until the ClickCluster Platform UI exists.

GET /ui/cluster-upgrade serves one self-contained HTML file (ui/index.html): no
build step and nothing from a CDN, because production is air-gapped. The page
calls the API it is served by, so it needs no CORS: `GET /workflows/runs` to
list upgrade-cluster runs, `GET /workflows/runs/{id}` for each one's progress,
and the decision routes of router.py for approve, continue and the rest.

Until the upgrade routes are mounted (DESIGN O18) there are no runs, so the page
also has a demo mode: demo-runs.json holds the `progress` and `result` the real
workflow returned in its test scenarios on the fake fleet
(`UPDATE_UI_DEMO=1 pytest tests/test_cluster_upgrade_ui.py` regenerates it).

The page has no auth of its own, like the rest of api.py.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/ui/cluster-upgrade", tags=["cluster-upgrade-ui"], include_in_schema=False)

UI_DIR = Path(__file__).parent / "ui"


@router.get("")
async def page() -> FileResponse:
    return FileResponse(UI_DIR / "index.html", media_type="text/html")


@router.get("/demo-runs.json")
async def demo_runs() -> FileResponse:
    return FileResponse(UI_DIR / "demo-runs.json", media_type="application/json")
