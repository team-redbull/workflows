---
paths:
  - "activities/**"
---
# Activities: `activities/<limb>/`

One deployment, image, dependency set and credential set per limb. Rationale:
CLAUDE.md §1, §2, §5, §6, §7. Checker ids in brackets.

- **Three declarations by one name**: the stub in `shared/interfaces/<limb>.py`, the
  implementation in `activities/<limb>/activities.py`, the entry in that limb's
  `worker_init.py`. `test_contract.py` and `test_worker_registration.py` pin them. [AC006, IF005, EP003]
- **`activities.py` is thin**: settings, `activity.logger`, a call into a technology
  module. Each `<technology>.py` takes PLAIN parameters and holds no settings and no Temporal.
- **`async def` only**; blocking work through asyncio. [AC004, AC005]
- **`activity.logger` only**. [AC003]
- **Idempotent, always**: server-side key, check-before-create on IDENTITY, re-read and
  compare the meaning, compare-and-set for shared records. "Already done" is success.
- **Classify every failure**: deterministic answers raise a class from
  `shared/exceptions.py` that the workflow lists as non-retryable; everything else is
  transient. "Not there yet" from something polled is a typed result, not an exception.
- **Bounded error text**: an HTTP body or a dependency's message enters an exception as a
  slice or through a bounding helper, never whole — history records it verbatim, and
  Temporal fails an activity error over 2 MiB as non-retryable "Failure exceeds size
  limit.". [AC007]
- **Long machine waits** are one heartbeating activity that resumes from its heartbeat details.
- **HTTP**: an `httpx.AsyncClient` per call, inside it, `timeout=` below the activity's
  `start_to_close_timeout`. [AC001, AC002]
- **Subprocesses and secrets**: `asyncio.create_subprocess_exec` under a timeout, a fresh
  `TemporaryDirectory` per attempt, every token scrubbed from error text BEFORE the
  exception is built.
- **Settings**: `<Limb>ActivitySettings()` at module level (fail-fast). A new key goes in
  `shared/settings.py`, `.env.example`, `tests/conftest.py` and the chart, which ships first.
- **RBAC** (per-MCE limbs): a new verb or resource is a row in the chart README's table.
- **Dependencies** go in this limb's `requirements.txt` and Dockerfile, never in `shared/`.
- **Docs in the same change**: the page of every workflow that calls the activity
  (dependencies and failures sections), the chart README, CLAUDE.md §4 for a new dependency.
