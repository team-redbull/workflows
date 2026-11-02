---
paths:
  - "workflow_domains/**/router.py"
  - "workflow_domains/**/ui.py"
  - "workflow_domains/routers/**"
  - "workflow_domains/api.py"
---
# The API: domain routers, temporary pages, `routers/`, `api.py`

Rationale: CLAUDE.md §2, §4, §6, §9. Checker ids in brackets.

- **Paths**: `APIRouter(prefix="/workflows/<domain-kebab>")`, one route per workflow
  under it; the bare prefix is never a route, and no `GET /{id}` lives under it. Status
  is `GET /workflows/runs/{workflow_id}`; lists are `GET /workflows/runs?workflow_type=`. [RT004, RT005, RT006]
- **A route that STARTS work answers 202** with the workflow id; the caller polls. A
  read-only QUESTION may be synchronous (`upgrade-targets`). Decision routes send an
  Update and map its answer (409 refused, 404 no such run, 504 no brain worker); they
  never read the run's state to second-guess its validator. [RT007]
- **Starting a run**: `client.start_workflow(<Class>.run, <Name>RunArgs(...),
  id=<name>_workflow_id(...), task_queue=<NAME>_WORKFLOW_QUEUE)`; a duplicate is a 409
  (an `already_running` item in a per-item report). [RT001, RT002, RT003, RT008]
- **Many items, many runs**: one run per item, a per-item report, never a batch workflow.
- **Edge strictness lives here**: a request model with `extra="forbid"`; required fields
  that are optional on the history-decoded model; a retired field answered with a 422
  that says what replaced it.
- **The external system validates**: its rejection is a FAILED run, not a 4xx here.
- **Renamed paths** answer 410 naming the new path and start nothing.
- **Temporary pages** are `ui.py` at `/ui/<domain-kebab>`: one self-contained HTML file,
  nothing from another host, only what the API reports, no demo mode, with a test. [RT009]
- **Mount** every domain router and page in `api.py`; no handlers there. [EP004]
- **No auth**: anyone reaching the Route can start a workflow; say so for a route that mutates.
- **Docs in the same change**: the workflow page's "how you start it and how you watch
  it" section, README's API table, and the `ui.py` page and its test when it offers the field.
