"""Segment-connectivity domain: its workflows and their HTTP surface.

Everything belonging to ONE domain lives together here — each workflow's
definition (open_segment_rules.py) plus the APIRouter that fronts them
(router.py) — mirroring activities/<domain>/ on the limb side. What stays
outside, in workflow_domains/routers/, is only what no single domain owns:
the shared dependencies/models and the domain-agnostic run-status route.
"""
