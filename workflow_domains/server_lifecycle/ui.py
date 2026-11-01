"""A temporary page for the server-lifecycle runs, until the ClickCluster Platform UI exists.

GET /ui/server-lifecycle serves one self-contained HTML file (ui/index.html),
the sibling of /ui/cluster-upgrade: no build step and nothing from a CDN,
because production is air-gapped. The page calls the API it is served by, so
it needs no CORS: `GET /workflows/runs?workflow_type=<class>` lists the runs of
add-server-to-mce, remove-server-from-mce, uninstall-server-from-hc and publish-infraenv-to-pxe-server,
`GET /workflows/runs/{id}` gives each one's progress, result or error, and the
router.py routes start an add-server-to-mce or a remove-server-from-mce run.

It shows what the workflows report and nothing more: a progress `phase` (plus
the server being tried, for add-server-to-mce), the result, and the failure. An
add-server-to-mce run names its candidates and why each was skipped only in the
failure it ends with when none could be installed; the page reads them out of
that message using `add_server_to_mce._REJECTION_SUMMARY`, copied into the page
and kept equal to it by tests/test_server_lifecycle_ui.py.

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
