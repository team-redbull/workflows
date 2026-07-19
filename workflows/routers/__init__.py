"""Per-domain APIRouters mounted by workflows/api.py.

One module per workflow domain (e.g. segment_connectivity), each exposing a
`router = APIRouter(prefix="/workflows/<domain>")`. Adding a workflow domain's
HTTP surface = add a module here + one include_router call in api.py.
"""
