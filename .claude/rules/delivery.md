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
  a push touches. Reusable-workflow jobs (which bump redbull-platform `main`, which
  deploys) form one sequential chain guarded by `!cancelled() && !failure()`; image-only
  jobs (charts not vendored yet) stand outside it. A new limb adds `build-<limb>-worker`
  and `cleanup-<limb>-worker-on-delete`. [DL002]
- **Order that keeps prod up**: brain image before limbs; a chart ships a new key before
  the image needs it and drops one only after the image stopped needing it; the brain
  release installs before any hub limb.
- **Charts**: per-MCE limbs in `deploy/charts/<limb>-worker/` (README with config and
  RBAC tables); the brain and hub limbs in redbull-platform `gitops/charts/`. Each renders
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
