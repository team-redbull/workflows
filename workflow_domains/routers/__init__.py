"""API pieces that belong to NO single domain, mounted by workflow_domains/api.py.

A domain's own APIRouter lives with that domain's workflows
(workflow_domains/<domain>/router.py). What stays here is what every domain
shares — the Temporal client dependency (deps.py), the domain-agnostic response
models (models.py) — plus the run-status route (runs.py), which is deliberately
domain-agnostic because workflow ids are globally unique.
"""
