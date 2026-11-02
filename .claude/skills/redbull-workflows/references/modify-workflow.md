# Change a workflow that may have runs in flight

Two things hold old state when the code changes: every RUNNING execution's history
(replayed by the new brain) and a limb one rollout behind (CI ships the brain first).
A change is safe only if both still decode and replay. Read `temporal-developer`'s
`references/python/determinism.md` and `versioning.md`, then apply the rules below.

## 1. Classify the change

| change | in-flight runs | do |
| --- | --- | --- |
| a new workflow, route or model nobody has started | none | build it |
| an activity's internals (HTTP path, idempotency, classification) | unaffected: activities are not replayed | change the limb; keep the signature; classify new deterministic errors |
| add/remove/reorder an activity, timer, child, signal wait or `continue_as_new`; change a timeout, interval or deadline; change an activity's input | a replay produces different commands: non-determinism, run blocked | gate with `workflow.patched("<name>-<what>")` (add-server-to-mce's `_RESERVATION_PATCH`), or drain |
| a field added to a model an activity RETURNS | absent from old payloads | read it with `"field" in model.model_fields_set` |
| a field added to workflow input / activity input | absent from old payloads | default `None` there, required only on the router's request model, the reader refuses `None` with a named error |
| a field removed or renamed on a boundary model | still in old payloads | keep the model lenient; refuse it only on the request model; drop it after retention |
| an activity argument from primitive to model | an old limb cannot decode it: retried, so DELAYED | acceptable when the old shape would make the old limb act wrongly |
| a workflow id scheme or task queue | new starts only | change the builder/constant; state that dedup against old runs no longer applies |
| a workflow RENAME | old runs replay as the OLD type on the OLD queue | see section 3 |

## 2. Patching vs draining, and the golden histories

Patch when runs are long or many (upgrade-cluster runs for weeks). Drain only when every
run is short, and say so: wait until `WorkflowType='<Name>Workflow' AND
ExecutionStatus='Running'` is empty, then ship.

Golden histories under `tests/histories/<workflow>/` replay on every test run. A golden
replay FAILING means the change needs `workflow.patched()`: never re-record to make it
pass. Re-record (`UPDATE_REPLAY_HISTORIES=1`) only while no run of the old shape can
exist, and add a new golden for the new shape.

Never fix a blocked run by resetting or terminating it from code: that is an operator's
call in the Temporal UI, after reading the history.

## 3. Renaming a workflow (the 2026-11-01 server-lifecycle precedent, CLAUDE.md §9)

Rename everything (route, ids, queue, type, models, module, tests, docs, lock holder),
with no aliases. Then keep the old world replaying:

- the OLD route answers 410 naming the new one (`_RENAMED_PATHS` in the router);
- a subclass at the bottom of the module carries the OLD type name, overrides `run`
  (the SDK refuses an inherited one) and is registered on `LEGACY_<OLD>_WORKFLOW_QUEUE`;
- the code chooses old vs new behaviour by `workflow.info().workflow_type` where it
  matters (lock holders, old child ids);
- old goldens stay, replayed by the legacy class; the new type gets its own;
- NOT renamed: patch ids, activity names, JSON field names of results;
- delete the legacy class, registration and queue once no run of the old type is RUNNING.

## 4. Cross-repo contracts

When the change alters what an external system receives or returns (the Segments
Manager, server-scan, the day1 repo, the agent service, the pipelines that call us),
state the deploy order: which side ships first so old+new keeps working, or that a
window of failed runs is accepted.

## 5. Docs and CLAUDE.md

SKILL.md step 3 applies to every behaviour change: the page's phases, failures, inputs
and re-run sections, README, the UI page's words. CLAUDE.md records a changed rule, a
removed feature ("do not re-add") or a decision taken with the operator, dated.
