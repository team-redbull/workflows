---
paths:
  - ".github/**"
  - "**/Dockerfile"
  - "**/requirements*.txt"
  - "deploy/**"
  - "docs/**"
  - ".env.example"
  - "README.md"
---
# Delivery: images, CI, charts, docs, config surface

Rationale: CLAUDE.md §1, §8, §9; redbull-platform's own CLAUDE.md for its side.
Checker ids in brackets.

- **Images, built from the repo root**: the brain (`workflow_domains/Dockerfile`, CMD
  `workflow_domains.main_worker_init`, also serving the API), one per limb
  (`activities/<limb>/Dockerfile`, CMD `activities.<limb>.worker_init`), and the docs
  site (`docs/Dockerfile`). [DL001]
- **`build.yml`**: the `test` job runs the whole suite on every push and every image job
  needs it, so a red test builds nothing and bumps nothing. `changes` decides which images
  a push touches: `scripts/image_changes.py` builds an image only when a file IT LOADS
  changed (the imports followed from its entrypoint; its Dockerfile and requirements),
  measured since the last fully successful run on the branch, and builds WIDE whenever it
  cannot tell (a data file, a deletion, `.dockerignore`). What only decides how CI runs is
  no image's input: `build.yml`, the script, the notes and their tests rebuild nothing, and
  `.github/image-builds.json` rebuilds the images whose ROW changed (compared with the
  base's, as data). So every input of an image's build stays in its row, never in
  `build.yml`. Every image is a row; `image_changes.py --first` gives
  `build-workflow-brain` the brain's and `--matrix` the `build` job every other listed
  image's — never a second decision in YAML or shell. [DL002] Every
  image is built by the reusable `ghcr-build-push.yml` (it bumps redbull-platform `main`,
  which deploys): the brain FIRST and alone, `sync-upgrade-charts` beside it, then the
  `build` matrix, one job per image, `fail-fast: false`, `max-parallel: 3` (each pushes
  to that one `main`), behind `!cancelled() && !failure()`. So a `main` build is tagged
  `vX.Y.Z`, never by commit sha: no image-only job (the operator, 2026-11-02); vendor a
  new limb's chart in redbull-platform first. A new limb adds its row (its own image
  name, Dockerfile, `chart-repo` and `gitops/charts/<limb>-worker/values.yaml`) and
  `cleanup-<limb>-worker-on-delete`. [DL002]
- **Release notes** (`release-notes`, `scripts/release_notes.py`): after the builds, on
  every branch that built something and every `main` push, never failing the run (a red
  run would rebuild what shipped). They take `changes`' list, never a second decision,
  attribute commits with `image_changes.affected_images`, and measure each release from
  the previous one (the comment its notes end with); the first is a baseline of every
  image at its current version. On `main`, `release` publishes them as the GitHub Release
  `release-<date>-<short sha>`, from a job that runs no repo code. A new image
  needs nothing there: the notes list every image `image_changes.py` finds, at the version
  of its `<chart>/vX.Y.Z` tags, so its chart directory stays named after the image.
- **Order that keeps prod up**: brain image before limbs; a chart ships a new key before
  the image needs it and drops one only after the image stopped needing it; the brain
  release installs before any hub limb.
- **Charts**: the limbs that run outside the hub (per MCE; per KV cluster:
  `vm-lifecycle-worker`) in `deploy/charts/<limb>-worker/` (README with config, RBAC and
  network tables); cluster-upgrade's two and vm-lifecycle's copied into redbull-platform by
  CI's `sync-upgrade-charts` (`scripts/sync_platform_charts.sh`: edit them here only; a
  destination of another shape is replaced, never given a `null` image); the brain and hub
  limbs in redbull-platform
  `gitops/charts/`. Each renders
  every REQUIRED settings key of its limb (a field without a default); none templates a
  Namespace. [DL005]
- **Requirements**: `shared/` adds nothing beyond temporalio + pydantic; each limb's deps in
  its own `requirements.txt`, all pulled into `requirements-dev.txt` for the tests.
- **Docs follow the code in the same change** (the skill's step 3): one page per workflow
  the API starts, `docs/site/<domain>/<workflow>.html` (copy an existing page's `<head>`
  and topbar, set `data-page`), its CATALOGUE entry in `docs/site/assets/nav.js` (`href:
  null` only while the page does not exist), children described on the parent's page,
  unbuilt designs in `docs/design/`, README's tables. [DL003]
- **`.env.example`** documents every settings key, optional ones commented out. [DL004]
