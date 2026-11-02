# Tests: what to write and the harnesses to copy

`pytest.ini`: `pythonpath = .`, `asyncio_mode = auto`. `tests/conftest.py` disables
`.env` and sets every settings key; a new limb's keys go there first or its activities
module fails at import. Install with `.venv/bin/pip install -r requirements-dev.txt`
(every limb's requirements, so the contract test imports them all).

Run: `.venv/bin/python -m pytest -q` (about two minutes) and
`python3 scripts/check_conventions.py` (also run as `tests/test_conventions.py`).

## Workflow tests: `tests/test_<workflow>_workflow.py`

Copy the closest harness (`test_allocate_segment_workflow.py`; `cluster_upgrade_fakes.py`
for a fake fleet): `WorkflowEnvironment.start_time_skipping()`, a client with
`pydantic_data_converter`, one Worker per queue the workflow uses (its workflow queue
plus every limb queue, per-MCE queues included) with mock `@activity.defn` functions and
a call recorder, and `asyncio.wait_for(handle.result(), 60)` so a misclassified error
FAILS the test instead of hanging it. Pin:

1. the happy path and exactly what each activity received;
2. every permanent error: the run FAILS with that type after ONE attempt;
3. a transient error is retried and the run completes;
4. every workflow-raised `ApplicationError` by its type;
5. read-back gates and `model_fields_set` compatibility (key absent passes, null fails);
6. Updates: the validator refuses with a 409-shaped error when the run is not waiting;
7. waits: what the run does at its deadline; stops: nothing acts after a stop flag.

## Replay tests: `tests/test_<workflow>_replay.py`

Histories recorded from scenarios replay with `temporalio.worker.Replayer`, and the
GOLDEN histories under `tests/histories/<workflow>/` replay on the current code. Add a
golden once a workflow ships. A failing golden means `workflow.patched()`, never a
re-record (see modify-workflow.md).

## Activity and technology-module tests

The technology module directly with plain parameters (`respx` for HTTP, fake Kubernetes
clients, bare git repos in a `TemporaryDirectory`); the activity via
`temporalio.testing.ActivityEnvironment`; one test per classification branch; the
idempotent "already done" path; the token never in any raised message.

## API and UI tests

The router on a bare `FastAPI()` with `get_temporal_client` overridden by a fake client
recording `start_workflow` / update calls: 202 with the deterministic id and queue, 409
on a duplicate, 422 on a forbidden field, per-item reports, 410 on renamed paths, and
the decision routes' 409/404/504 mapping. A domain with `ui.py` has a test that the page
has words for every phase a run reports and offers only the request fields the API takes.

## Guards: never skip, xfail or loosen them

`test_contract.py` (stub vs implementation, every limb), `test_worker_registration.py`
(every dispatched activity is registered where it is sent), `test_error_classification.py`
(every non-retryable name is raisable; no workflow-raised type in the list),
`test_conventions.py` (the checker and its self-tests), the replay goldens. When one of
them is wrong, the rule changes: CLAUDE.md, the rules file and the checker together.
