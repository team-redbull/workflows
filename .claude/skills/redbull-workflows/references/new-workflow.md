# Add a workflow to an existing domain

Exemplars: `release-segment` (third workflow of segment-lifecycle) and
`publish-infraenv-to-pxe-server` (a short workflow on per-MCE queues). Work top-down:
contract, workflow, API, registration, tests, DOCS.

`<name>` is the kebab workflow name (`release-segment`), `<Name>` its PascalCase,
`<NAME>` its constant form, `<domain>` the snake domain folder, `<limb>` the limb whose
activities it calls (the domain itself when the domain has one limb).

## 1. Contract — `shared/`

1. `shared/models/<domain>.py`: `<Name>Input` (the smallest set of fields that
   identifies the work; derive what can be derived and cross-check it against the system
   of record), `<Name>RunArgs(BaseModel)` with `input: <Name>Input` (the ONE `run()`
   argument), `<Name>Progress` (`phase` plus whatever the page and the UI show),
   `<Name>Result`, and any new activity request/response models. No `extra="forbid"`.
2. `shared/interfaces/<limb>.py`: a stub per new activity (`@activity.defn async def`,
   docstring stating what it does, how it is idempotent, which classified errors it
   raises, then `...`). Reuse existing stubs before adding near-duplicates.
3. `shared/exceptions.py`: one `OrchestratorError` subclass per new failure. Activity
   errors say "deterministic, non-retryable" or "transient, retried"; errors the
   WORKFLOW raises start their docstring with `WORKFLOW-RAISED.`
4. `shared/consts.py`: `<NAME>_WORKFLOW_QUEUE = "<name>-workflow"`.
5. `shared/workflow_ids.py`: `def <name>_workflow_id(...) -> str`, `<name>-<scope...>`,
   every dimension that makes a run unique (the MCE for a hub-scoped pool, the type for
   a per-type allocation).

## 2. The workflow — `workflow_domains/<domain>/<name>.py`

Copy the shape of the closest exemplar (SKILL.md step 0). The skeleton every one shares:

```python
"""<name> — what it does, why it is a workflow, the numbered phases, what it never does."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import <LIMB>_ACTIVITY_QUEUE           # or <limb>_activity_queue(mce)
    from shared.exceptions import SomePermanentError, SomeWorkflowRaisedError
    from shared.interfaces.<limb> import verb_object
    from shared.models.<domain> import <Name>Progress, <Name>Result, <Name>RunArgs

_ACTIVITY_TIMEOUT = timedelta(seconds=90)                    # > the limb's HTTP timeout (60s)
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (SomePermanentError,)
_RETRY_POLICY = RetryPolicy(                                  # unbounded; classified stops only
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[e.__name__ for e in _PERMANENT_ACTIVITY_ERRORS],
)


@workflow.defn
class <Name>Workflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> <Name>Progress:
        return <Name>Progress(phase=self._phase)

    @workflow.run
    async def run(self, run_args: <Name>RunArgs) -> <Name>Result:
        wf_input = run_args.input
        # strict up-front checks
        if ...:
            raise ApplicationError("why, in operator words", type=SomeWorkflowRaisedError.__name__)
        self._phase = "doing-the-thing"
        outcome = await workflow.execute_activity(
            verb_object, <Request>(...),
            task_queue=<LIMB>_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # verify after mutate, before recording; a long machine wait is ONE heartbeating activity
        self._phase = "completed"
        return <Name>Result(...)
```

Put pure, sandbox-safe decision rules the workflow needs (and no limb does) in a policy
module beside it (`bond_selection.py`), and routing/budget helpers shared by two
workflows in a helper (`dispatch.py`); both obey the workflow import rules. A person's
decision on a running workflow is an `@workflow.update` with a validator (§5).

## 3. The API — `workflow_domains/<domain>/router.py`

ONE `POST /<name>` under the domain prefix, `status_code=202`, answering
`StartWorkflowResponse`; the request model is `<Name>Input` + `extra="forbid"` (or its
own model when one request is many runs, as add-server-to-mce's is); the id from
`<name>_workflow_id`, the queue from `<NAME>_WORKFLOW_QUEUE`;
`WorkflowAlreadyStartedError` is a 409 naming the id. Many items mean one run per item
and a per-item report, never a batch workflow. A read-only QUESTION may be a
synchronous route (`upgrade-targets`); anything that starts work is asynchronous.
Status needs nothing: `GET /workflows/runs/{workflow_id}`.

## 4. Registration

`workflow_domains/main_worker_init.py`: import the class, add
`(<NAME>_WORKFLOW_QUEUE, [<Name>Workflow])` to `_WORKER_SPECS`. No new deployment.

## 5. Activities

New ones per [activities.md](activities.md); a new credential set or a new place to
run means a new limb ([new-domain.md](new-domain.md)).

## 6. Tests

Per [testing.md](testing.md): the workflow harness, activity and API tests,
`tests/test_worker_registration.py` covering the queue the workflow dispatches on,
and a golden replay history once the workflow ships (it will have runs in flight).

## 7. Docs (SKILL.md step 3) and hand-off

- `docs/site/<domain-kebab>/<name>.html`, its CATALOGUE entry in
  `docs/site/assets/nav.js`, README's workflow and API tables, CLAUDE.md §2 and §4.
- The domain's `ui.py`, if it has one and the workflow belongs on it.
- Run the checker and pytest; walk the SKILL.md gate.
