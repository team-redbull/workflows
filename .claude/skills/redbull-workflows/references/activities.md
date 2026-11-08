# Add or change an activity

An activity is one external interaction (an HTTP call, a Kubernetes write, a git push,
a read-back) that Temporal may run MORE THAN ONCE. Everything below follows from that.

## Where the code goes

| where | what |
| --- | --- |
| `shared/interfaces/<limb>.py` | `@activity.defn async def verb_object(request: ReqModel) -> RespModel:` docstring, `...` |
| `activities/<limb>/activities.py` | the implementation, same signature, THIN: settings, `activity.logger`, one call into a technology module |
| `activities/<limb>/<technology>.py` | the logic: plain parameters in, typed values out, no settings and no Temporal, so it is tested with no worker (`cluster_api.py`, `values_repo.py`, `dhcp_api.py`) |
| `activities/<limb>/worker_init.py` | the name in the Worker's `activities=[...]` |

Workflows import the STUB and pass it to `workflow.execute_activity` on the limb's
queue. `tests/test_contract.py` compares stub and implementation hints;
`tests/test_worker_registration.py` checks every activity a workflow dispatches is
registered on the worker that polls that queue.

Arguments and returns are Pydantic models from the domain's `shared/models/<domain>.py`.
A field added to a returned model is always emitted, null included, so a workflow's
`"field" in model.model_fields_set` gate tells an old payload from a reported null.

## Idempotency: decide it before writing the call

Write the mechanism into the stub's docstring:

- **server-side idempotency key** (`allocate_segment`): a retry returns the same answer;
- **check-before-create / accept-existing** (`create_segment`; `cluster_api`'s create
  rule): an existing object with the same IDENTITY is success, a different one is a
  classified conflict; never compare state another actor may change;
- **re-read and compare the meaning** (`append_allocation_to_cluster_values`): never text
  equality, which turns every operator edit into a phantom conflict;
- **compare-and-set** where two runs may mutate one record (server-scan's install lock);
- **observe-only**: no side effect; "not there yet" is a normal typed answer
  (`found=False`), never an exception.

## Error classification: every failure is one of two kinds

Deterministic (non-retryable): bad credentials (401/403), a rejected request (400/422),
a record that must exist and does not (404), a conflict (409), a drained pool, a missing
branch. Transient (retried): everything else, including "unreachable is not missing".
Deterministic classes live in `shared/exceptions.py` with a docstring saying so; the
workflow names them through the classes (`[e.__name__ for e in _PERMANENT_ACTIVITY_ERRORS]`),
so a typo is an ImportError, not a silent retry. Carry the external system's own message
through: its wording is what the operator fixes. An unclassified permanent error
retries every minute forever with the run RUNNING.

## Long waits, HTTP, subprocesses, secrets

- A long machine wait is ONE heartbeating activity (`watch_control_plane`): it polls
  inside, heartbeats every poll (`heartbeat_timeout` set on the call), keeps its clocks in
  the heartbeat details so a retry resumes them, and returns `done | stalled | deadline`.
- `async with httpx.AsyncClient(base_url=..., timeout=_HTTP_TIMEOUT, verify=...)` created
  INSIDE the call; `_HTTP_TIMEOUT` below the workflow's `start_to_close_timeout`.
- Kubernetes: the admin kubeconfig decoded through the shared helper, per call.
- git and other binaries: `asyncio.create_subprocess_exec` with a per-command timeout, a
  fresh `TemporaryDirectory` per attempt, the token in memory only and SCRUBBED from
  every message before the exception is built (history records it verbatim);
  `activities/git_repo.py` holds the shared pieces, `activities/day1_repo.py` the day1 reader
  on top of it — code two limbs need lives there, in no domain, never in one limb for the
  other to import.
- `activity.logger.info` for each call's outcome, with the identifiers an operator searches for.

## Settings for a new endpoint, credential or knob

1. A field on the limb's `<Limb>ActivitySettings` (a service prefix for that service's
   own values only; structured values validated fail-fast).
2. `.env.example`, `tests/conftest.py`, `tests/test_settings.py` for a validator.
3. The chart renders it (ConfigMap, or a Secret behind an `existing*Secret` value) and
   ships BEFORE the image ([platform-chart.md](platform-chart.md)).

## RBAC

A per-MCE limb's ServiceAccount is its identity. A new Kubernetes verb or resource is a
row in the chart README's RBAC table and a rule in its templates, derived from the call.

## Tests and docs

Per [testing.md](testing.md): the technology module tested directly with plain
parameters; the activity through `ActivityEnvironment`; one test per classification
branch; one for the idempotent "already done" path. Then SKILL.md step 3: the page of
every workflow that calls the activity (its dependency and failures sections), the
chart README, CLAUDE.md §4 for a new dependency.
