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
- **A test with a LONG-LIVED or idle workflow, or with a task nobody serves, moves the
  clock by hand** (`tests/manual_time.py`; create-vm, kv-zone-lock): left to skip by
  itself the test server jumps an idle run to its ten-year timeout, and it skips no time
  at all under an outstanding task (an activity on a queue nobody polls, an unserved
  workflow task). `tests/test_manual_time.py` pins both habits.
- **Shrinking a workflow's constants** (an unsandboxed runner plus `monkeypatch`) is for
  logic only, never for a golden: a history recorded under other constants does not
  replay on the real ones.
- **A fleet is faked behind the API, not above it** (`tests/kv_fleet.py`): each fake
  cluster gets its own worker on its own queue running the REAL technology code over an
  in-memory API that behaves where the logic depends on it (what was created is there on
  the next read).
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
