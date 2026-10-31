"""A temporary page for the server-lifecycle runs, until the ClickCluster Platform UI exists.

GET /ui/server-lifecycle serves one self-contained HTML file (ui/index.html),
the sibling of /ui/cluster-upgrade: no build step and nothing from a CDN,
because production is air-gapped. The page calls the API it is served by, so
it needs no CORS: `GET /workflows/runs?workflow_type=<class>` lists the runs of
install-server, remove-server, uninstall-server and publish-infraenv-boot,
`GET /workflows/runs/{id}` gives each one's progress, result or error, and the
router.py routes start an install-server or a remove-server run.

It shows what the workflows report and nothing more: a progress `phase` (plus
the server being tried, for install-server), the result, and the failure. An
install-server run names its candidates and why each was skipped only in the
failure it ends with when none could be installed; the page reads them out of
that message using `install_server._REJECTION_SUMMARY`, copied into the page
and kept equal to it by tests/test_server_lifecycle_ui.py.

Demo mode reads demo-runs.json: the status GET /workflows/runs/{id} returned
for runs driven by the workflows' own test harnesses, with demo names
(`UPDATE_UI_DEMO=1 pytest tests/test_server_lifecycle_ui.py` regenerates it).

The page has no auth of its own, like the rest of api.py.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/ui/server-lifecycle", tags=["server-lifecycle-ui"], include_in_schema=False)

UI_DIR = Path(__file__).parent / "ui"


@router.get("")
async def page() -> FileResponse:
    return FileResponse(UI_DIR / "index.html", media_type="text/html")


@router.get("/demo-runs.json")
async def demo_runs() -> FileResponse:
    return FileResponse(UI_DIR / "demo-runs.json", media_type="application/json")
