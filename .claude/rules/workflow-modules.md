---
paths:
  - "workflow_domains/**/*.py"
---
# Workflow-side code: `workflow_domains/<domain>/*.py`

Workflow modules and the sandbox helpers beside them (policy modules, `dispatch.py`,
`gates.py`). Rationale: CLAUDE.md §2, §5, §6, §7. Checker ids in brackets; the
procedure for new or running workflows is the `redbull-workflows` skill.

- **One workflow per file**, class `<Name>Workflow`; its kebab name is reused for the
  queue `<name>-workflow`, the id builder `<name>_workflow_id`, the route, the models
  and the docs page. [WF011, SH003, SH004]
- **Sandbox imports**: every `shared.*` import inside
  `with workflow.unsafe.imports_passed_through():`. Never `shared.settings`,
  `shared.temporal_client`, `activities.*`, a router, `ui.py`, `api.py`, `httpx`,
  `kubernetes`, `os`, `time`, `random`, `uuid`. Sibling helpers in the domain are fine. [WF001, WF003]
- **`run()` takes ONE Pydantic argument** (`<Name>RunArgs`); `@workflow.init` takes the same. [WF009]
- **Every activity call** passes the stub from `shared/interfaces/<limb>.py` (directly
  or through a dispatch helper's parameter), `task_queue=` from `shared/consts.py`
  (constant or per-MCE builder), `start_to_close_timeout=`, `retry_policy=`. No
  `schedule_to_close_timeout`, except a best-effort step whose `ActivityError` the
  workflow catches. [WF005, WF006]
- **Retries are unbounded**: no `maximum_attempts` (the agent call is the one marked
  exception), a capped `maximum_interval`, permanent errors named through their CLASSES:
  `[e.__name__ for e in _PERMANENT_ACTIVITY_ERRORS]`. [WF007, WF008]
- **Failures from workflow code**: `ApplicationError(msg, type=<Class>.__name__)`, the
  class WORKFLOW-RAISED in `shared/exceptions.py`, never in `non_retryable_error_types`,
  never `non_retryable=True`; never a builtin or `shared.exceptions` class raised
  directly. Module-local sentinels caught in `run()` are fine. [WF010, WF012]
- **Time and randomness** via `workflow.sleep/now/uuid4/random`. A long MACHINE wait is
  one heartbeating activity; a HUMAN wait is `wait_condition` on a flag set by an Update
  handler, no timer. [WF004]
- **People decide through Updates with validators**, code stops a child through signals,
  and a stop flag is checked before every act (`_checkpoint()`), because cancellation
  does not stop work in flight (§5).
- **Progress**: `self._phase` and `@workflow.query def progress`. [WF014]
- **Logging**: `workflow.logger` only. [WF013]
- **Verify after mutate**: read back before recording; strict, absolute checks (§6, §7).
- **Runs in flight**: a command-changing edit is `workflow.patched(...)`-gated or the drain
  is stated; golden replays under `tests/histories/` must pass unchanged.
- **Register** the class in `main_worker_init.py::_WORKER_SPECS`. [EP002]
- **Docs in the same change**: the workflow's page under `docs/site/<domain>/` (phases,
  failures, inputs, re-runs), README, the domain's `ui.py` words. The hook names the pages. [DL003]
