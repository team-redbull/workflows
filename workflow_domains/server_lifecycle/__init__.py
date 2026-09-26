"""Server-lifecycle domain: its workflows and their HTTP surface.

Everything belonging to ONE domain lives together here — the workflow
definition (install_server.py), the pure selection policy it applies
(bond_selection.py) and the APIRouter that fronts it (router.py) — mirroring
activities/<domain>/ on the limb side. What stays outside, in
workflow_domains/routers/, is only what no single domain owns: the shared
dependencies/models and the domain-agnostic run-status route.
"""
