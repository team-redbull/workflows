---
paths:
  - "shared/**"
---
# The contract layer: `shared/`

The typed API between brain and limbs. Rationale: CLAUDE.md §1, §2, §5, §8. Checker ids in brackets.

- **Lightweight only**: `temporalio`, `pydantic`, `pydantic-settings`, stdlib. [SH001]
- **`models/<domain>.py`**: a Pydantic model for everything crossing a boundary. Never
  `extra="forbid"`: history and a lagging limb hold old payloads. A field added to a
  history-decoded model defaults to `None` and is required only on the request model;
  a field the limb returns is always emitted. [SH002]
- **`interfaces/<limb>.py`**: one module per LIMB; `@activity.defn async def`, a
  docstring (what, idempotency, classified errors), `...`. [IF001-IF004]
- **`consts.py`**: `<NAME>_WORKFLOW_QUEUE = "<name>-workflow"` per workflow;
  `<LIMB>_ACTIVITY_QUEUE = "<limb>-activity"` per hub limb or a
  `<limb>_activity_queue(mce)` builder per per-MCE limb (per KV cluster:
  `vm_lifecycle_activity_queue(kv_cluster)`); `LEGACY_*` queues only while old runs
  replay. [SH003]
- **`workflow_ids.py`**: one `<name>_workflow_id(...)` per scheme, carrying every
  dimension that scopes a run. Routers and workflows both import it. [SH004]
- **`exceptions.py`**: `OrchestratorError` subclasses whose docstring says
  deterministic, transient or `WORKFLOW-RAISED.` Named through the class everywhere. [SH005]
- **`settings/<limb>.py`**, one module per worker (+ `temporal.py`): `BaseSettings` with
  `extra="ignore"`, keys = ConfigMap/Secret keys lowercased, validators for structured
  knobs; never imported by workflow-side code. [SH006] The package's `__init__.py`
  imports NOTHING: a re-export loads every worker's settings into every image, and CI
  would rebuild them all for one worker's key. [SH007]
- **What a module here imports decides which images rebuild** (`scripts/image_changes.py`):
  keep per-limb things in per-limb modules; `exceptions.py` and `consts.py` are loaded
  by every image and stay single files on purpose (CLAUDE.md §8).
- **`temporal_client.py`** is the only place a client connects (`connect_temporal`). [EP005]
- **Docs in the same change**: a model or interface change updates the page of every
  workflow it touches (inputs, results, failures); a settings change updates
  `.env.example`, README's configuration table and the chart.
