# Add a domain or a limb

A LIMB is a deployment: its own image, dependency set, ServiceAccount, credentials
and activity queue, registering its activities against the already-running brain.
A DOMAIN is a subject with its own API prefix, models, workflows and docs folder;
it has one limb or several (cluster-upgrade: `upgrade-gitops` on the hub,
`cluster-upgrade` and `upgrade-agent` per MCE). Justify a new limb first (SKILL.md
step 1). Pick folder names carefully: the checker derives the kebab names, the
settings class (`<Limb>ActivitySettings`) and the chart name from them.

## A. A new limb (this repo)

1. **Contract** — `shared/interfaces/<limb>.py` (each limb its OWN interface module, so
   the contract test pairs one interface with one implementation), models in the
   DOMAIN's `shared/models/<domain>.py`, classes in `shared/exceptions.py`, and in
   `shared/consts.py` either `<LIMB>_ACTIVITY_QUEUE = "<limb>-activity"` (hub) or a
   builder `def <limb>_activity_queue(mce_cluster: str) -> str` (per MCE, like
   `server_lifecycle_activity_queue`).
2. **Settings** — `<Limb>ActivitySettings(BaseSettings)` in `shared/settings.py`
   (`env_file=".env", extra="ignore"`), keys = ConfigMap/Secret keys lowercased, a
   service prefix only for that service's own values; `.env.example`; `tests/conftest.py`.
3. **Limb** — `activities/<limb>/`: `activities.py` (the `@activity.defn` surface only:
   settings, `activity.logger`, a call into a technology module), one
   `<technology>.py` per dependency taking PLAIN parameters (no settings, no Temporal),
   `worker_init.py` (copy `activities/server_lifecycle/worker_init.py` for a per-MCE limb:
   `connect_temporal`, `graceful_shutdown_timeout`, `install_shutdown_handler`, the queue
   from `shared/consts.py`), `requirements.txt` (this limb's deps only, also pulled into
   `requirements-dev.txt` so tests can import it), and a `Dockerfile` whose CMD runs
   `activities.<limb>.worker_init`.
4. **Tests** — conftest env; add the limb to `tests/test_contract.py`'s `_DOMAINS` (a
   guard test fails until you do) and its queue to `tests/test_worker_registration.py`'s
   `_WORKERS`; activity tests per [testing.md](testing.md).
5. **CI** — `.github/workflows/build.yml`: a `build-<limb>-worker` job that `needs` the
   `test` job. With its chart in redbull-platform, use the reusable
   `ghcr-build-push.yml` in the sequential chain (`helm-values-path:
   gitops/charts/<limb>-worker/values.yaml`); until then, an IMAGE-ONLY job like
   `build-cluster-upgrade-worker` (tag `<branch-slug>-<sha>`, set by hand in the chart).
   Add a `cleanup-<limb>-worker-on-delete` job. Every limb rebuilds on any change under
   `activities/` or `shared/`; say so if that matters.
6. **Chart** — per-MCE limb: `deploy/charts/<limb>-worker/` here, with a README whose
   RBAC table is derived call by call from `activities.py`; hub limb:
   redbull-platform, per [platform-chart.md](platform-chart.md).

## B. A new domain (adds to A)

1. `workflow_domains/<domain>/` with `__init__.py` (docstring), one module per workflow
   ([new-workflow.md](new-workflow.md)), and `router.py` with
   `APIRouter(prefix="/workflows/<domain-kebab>", tags=["<domain-kebab>"])`, mounted in
   `workflow_domains/api.py`. A temporary page, if the operator needs one, is `ui.py` at
   `/ui/<domain-kebab>`, self-contained, with its own test.
2. `shared/models/<domain>.py`.
3. **Docs** — `docs/site/<domain-kebab>/<workflow>.html` per workflow, a new domain
   group in the CATALOGUE of `docs/site/assets/nav.js`, README (layout, workflows,
   API, configuration), CLAUDE.md §1 (folders), §2 (partitioning and limbs), §4 (the
   domain's dependencies), §8 (settings class), §9 (charts and routes).

## C. Hand-off

State the limb's dependency and credential set, which Secrets must exist before the
first sync, the deploy order (brain release, then Secrets, then the limb chart; the
chart after the first image exists), and the CI choice from A.5. Then walk the gate.
