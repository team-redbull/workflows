---
paths:
  - "tests/**"
---
# Tests: `tests/`

`pytest.ini` sets `pythonpath = .` and `asyncio_mode = auto`; `tests/conftest.py` switches
`.env` off and sets every settings key. Harnesses and what to pin: the `redbull-workflows`
skill's references/testing.md.

- **Workflow tests** run the REAL workflow with mock activities on one Worker per queue
  (per-MCE queues included), in the time-skipping environment, every result under
  `asyncio.wait_for(..., 60)` so a misclassified error fails instead of hanging.
- **Pin classification, not sequence**: each permanent error FAILS the run with its type
  after one attempt; a transient one is retried; each workflow-raised type; each gate.
- **Replay goldens** under `tests/histories/<workflow>/` are never re-recorded to make a
  change pass: a failing golden means `workflow.patched()`. Re-record only while no run
  of the old shape can exist; a renamed workflow's old goldens stay, replayed by its legacy class.
- **Activity tests**: technology modules with plain parameters; activities through
  `ActivityEnvironment`; `respx` for HTTP; bare git repos in a `TemporaryDirectory`.
- **API tests**: the router on a bare `FastAPI()` with a fake client; **UI tests** check
  the page's words for every phase and the fields it offers against the API.
- **A new limb** adds its keys to `conftest.py`, its pair to `test_contract.py::_DOMAINS`
  and its queue to `test_worker_registration.py::_WORKERS`.
- **The guards are never skipped, xfailed or loosened**: contract, worker registration,
  error classification, conventions, replay. A wrong guard is a rule change: CLAUDE.md,
  the rules file and the checker move together.
