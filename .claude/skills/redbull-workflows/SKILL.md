---
name: redbull-workflows
description: Create or change Temporal workflows, activities, domains, limbs, contract models, routers, temporary UI pages, tests, docs and the per-limb Helm charts in this monorepo (workflow_domains/, activities/, shared/, deploy/, docs/). Use for any request that adds or renames a workflow, adds a domain or limb, adds or changes an activity, evolves a model crossing the workflow/activity boundary, adds configuration, or modifies a workflow that may have runs in flight. Composes with the temporal-developer skill (SDK mechanics) and CLAUDE.md (rationale); the conventions are enforced by scripts/check_conventions.py, and every change ends with its docs updated.
paths:
  - "workflow_domains/**"
  - "activities/**"
  - "shared/**"
  - "deploy/**"
  - "docs/**"
---

# redbull-workflows — how work is done in this monorepo

A Temporal-based OpenShift cluster-lifecycle orchestrator. ONE "brain" deployment runs
every workflow of every domain; each "limb" deployment runs one credential set's
activities (a domain may need several limbs: cluster-upgrade has a hub limb and two
per-MCE limbs); `shared/` is the typed contract between them. Hub limbs' charts are
vendored in the sibling **redbull-platform** repo (`gitops/charts/<service>/`, where
pushing `main` deploys); per-MCE limbs' charts live here in `deploy/charts/`.

Four layers keep the architecture true for everyone. Use them in this order:

| layer | what it is | when it loads |
| --- | --- | --- |
| `CLAUDE.md` | the RATIONALE: why each rule exists, what was removed and must not return | every session |
| `.claude/rules/*.md` | terse INVARIANTS per file kind, with checker rule ids | when a matching file is read/edited |
| this skill | the PROCEDURE: which files to touch, in which order, ending with the docs | on these paths or `/redbull-workflows` |
| `scripts/check_conventions.py` | the ENFORCEMENT: AST checks of the rules that can be checked, plus a docs reminder on every edit | every Edit/Write (hook), `pytest`, CI |

The checker is the tie-breaker: if it flags an edit, the edit is wrong, not the
checker. A DOCUMENTED exception is marked where it lives,
`# conventions: allow <RULE> — <reason>` (the reason is required), and the exception
is recorded in CLAUDE.md. If a rule itself is wrong, change CLAUDE.md, the rule file
and the checker in the same commit.

## Step 0 — orient before writing

1. Read the CLAUDE.md sections the task touches: §1 layers, §2 naming and partitioning,
   §4 the domain's dependencies (long, per domain: read the bullets of the workflow you
   touch), §5 SDK rules, §6 idempotency, §8 config, §9 deploy and routes. Most "obvious"
   simplifications were tried and reverted; §4 and §5 say which, and which removed
   features must not come back.
2. **Load `temporal-developer`** (vendored at `.agents/skills/temporal-developer/`,
   symlinked into `.claude/skills/`) for SDK mechanics, and read from
   `references/python/`: `python.md` (always), `determinism.md` and `gotchas.md` (any
   workflow change), `versioning.md` (a workflow with runs in flight), `testing.md`,
   `patterns.md` (signals, updates, children), `ai-patterns.md` (an LLM call).
3. **Precedence**: where that skill's generic advice differs from this repo, the repo
   wins. Known divergences:
   - activity retries are UNBOUNDED with classified `non_retryable_error_types`, built
     from the exception CLASSES (`[e.__name__ for e in ...]`); the one bounded retry is
     the LLM agent call (§5);
   - one Worker PER WORKFLOW queue, all in the ONE brain process; activity queues per
     LIMB, per MCE for per-MCE limbs (`*_activity_queue(mce)` builders in `shared/consts.py`);
   - connect only through `shared.temporal_client.connect_temporal` (Pydantic converter + TLS);
   - `run()` takes exactly one `<Name>RunArgs`; `@workflow.init` takes the same argument;
   - failures from workflow code are `ApplicationError(..., type=<Class>.__name__)`,
     class marked WORKFLOW-RAISED in `shared/exceptions.py`, never `non_retryable`;
   - a person's decision is an Update with a validator, never a signal; code-to-code
     stops are signals plus a flag checked before every act;
   - a long machine wait is ONE heartbeating activity, not a workflow poll loop;
   - activities are `async def` only; never cancel a sibling run, narrow the input;
   - boundary models never `extra="forbid"`; strictness lives on the router's request model;
   - charts never create a Namespace.
4. Look at the exemplars before writing (copy their shape, not their prose):

   | shape | exemplar |
   | --- | --- |
   | multi-step workflow, read-back gate, git write | `workflow_domains/segment_lifecycle/allocate_segment.py` |
   | per-MCE routing, permanent-error tuple, patches, legacy rename | `workflow_domains/server_lifecycle/add_server_to_mce.py` |
   | Updates, heartbeating watches, child driven by signals, dispatch helpers | `workflow_domains/cluster_upgrade/upgrade_cluster.py`, `dispatch.py` |
   | pure sandbox policy module | `workflow_domains/server_lifecycle/bond_selection.py` |
   | routes: single, bulk, slots, decisions, 410 for renamed paths, synchronous question | the domain `router.py` files |
   | temporary page | `workflow_domains/<domain>/ui.py` + `tests/test_<domain>_ui.py` |
   | thin activity surface over technology modules | `activities/server_lifecycle/activities.py` + `cluster_api.py` |
   | per-MCE limb entrypoint + chart | `activities/server_lifecycle/worker_init.py`, `deploy/charts/server-lifecycle-worker/` |
   | workflow tests, replay goldens | `tests/test_allocate_segment_workflow.py`, `tests/test_upgrade_cluster_replay.py` |

## Step 1 — pick the procedure

| the request | reference |
| --- | --- |
| a new workflow in an EXISTING domain | [references/new-workflow.md](references/new-workflow.md) |
| a new DOMAIN or a new LIMB (dependency set, credentials, RBAC, per-MCE reach) | [references/new-domain.md](references/new-domain.md) |
| add or change an activity (a new external call, a new classification) | [references/activities.md](references/activities.md) |
| change or RENAME a workflow that may have runs in flight, or a boundary model | [references/modify-workflow.md](references/modify-workflow.md) |
| a config key, operator knob, secret or chart | [references/platform-chart.md](references/platform-chart.md) |
| tests for any of the above | [references/testing.md](references/testing.md) |
| docs for any of the above (ALWAYS) | Step 3 below |

**Domain vs limb vs workflow**: a new LIMB is justified by a different dependency or
credential set or a different place to run (per MCE vs hub), never by "it is a
different workflow". A workflow that only needs what an existing limb has joins that
domain and reuses its queue, ConfigMap and image. A new DOMAIN is a new subject
(segments, servers, upgrades), with its own API prefix and models.

## Step 2 — vocabulary that must be spelled the same everywhere

| thing | named after | example |
| --- | --- | --- |
| API prefix, `shared/models/<domain>.py`, `workflow_domains/<domain>/`, docs folder | the DOMAIN | `/workflows/server-lifecycle`, `docs/site/server-lifecycle/` |
| activity queue, limb ConfigMap, `shared/interfaces/<limb>.py`, `activities/<limb>/` | the LIMB (= the domain when it has one) | `segment-lifecycle-activity`, `cluster-upgrade-activity-<MCE>`, `upgrade-gitops` |
| limb Deployment / ServiceAccount / chart / image / Argo app | the PROCESS: `<limb>-worker` | `segment-lifecycle-worker`, `upgrade-agent-worker` |
| workflow module, class, queue, ids, RunArgs/Progress/Result, route, docs page | the WORKFLOW | `allocate_segment.py`, `AllocateSegmentWorkflow`, `allocate-segment-workflow`, `allocate-segment-<TYPE>-<cluster>`, `POST /allocate-segment`, `allocate-segment.html` |
| brain Deployment/SA, global ConfigMap, API service | fixed | `workflows-orchestrator`, `workflows-orchestrator-config`, `workflows-orchestrator-api` |

Workflow ids carry the workflow name and every dimension that scopes a run (the MCE,
the segment type), so two legitimate runs never collide and a duplicate trigger while
running is a 409.

## Step 3 — docs follow the code, in the SAME change

Every change ends by asking: does anything a caller, an operator or a reviewer reads
now say something false or incomplete? If yes, fix it in the same change; a pure
refactor needs nothing. The checker's hook prints the pages that describe the file
you just edited on every edit, and the whole-repo run warns about workflows without
pages (DL003). Never finish with "docs to follow".

| what changed | what to update |
| --- | --- |
| a NEW workflow the API starts | `docs/site/<domain>/<workflow>.html` (copy an existing page's `<head>` + topbar verbatim, set `<body data-page="/<domain>/<workflow>.html">`); its CATALOGUE entry in `docs/site/assets/nav.js`; README's workflow and API tables; CLAUDE.md §2 (the domain's workflow list) and §4 (its dependencies) |
| a child or internal workflow | the parent's page, in its "what the run does" section |
| a route, request field, status code, or the reply's shape | the page's "how you start it and how you watch it" section; README's API table; the domain's `ui.py` page and its test, if the field is offered there |
| a phase added, renamed or removed | the page's phases list; `ui.py`'s words for phases (its test fails otherwise) |
| a failure type, its classification, a retry or a wait | the page's failures section ("outages are out-waited, real errors stop the run"); CLAUDE.md §5 if it sets a precedent |
| idempotency or re-run behaviour | the page's "run it again" section; CLAUDE.md §6 if it sets a precedent |
| a config key, secret or operator knob | `.env.example`; README's configuration table; the chart's `values.yaml` and README (`deploy/charts/<limb>-worker/README.md` for per-MCE limbs); the page's inputs/knobs section |
| an external call, RBAC verb or credential | the page's dependency section; the per-MCE chart README's RBAC table; CLAUDE.md §4 |
| a rename or removal | page renamed or deleted, CATALOGUE entry, README, CLAUDE.md (record the removal and "do not re-add" where it was a decision) |
| a decision taken with the operator, or a design not yet built | CLAUDE.md (dated, with who decided); `docs/design/<topic>.md` for the unbuilt part |

## Step 4 — definition of done (the production gate)

Do not report a change as finished until every line holds:

- [ ] `python3 scripts/check_conventions.py` reports `0 error(s)`; every warning is fixed or justified in the reply.
- [ ] `.venv/bin/python -m pytest -q` is green: the contract, worker-registration, error-classification, conventions and replay tests included; new behaviour has tests of the shape in [references/testing.md](references/testing.md).
- [ ] Every activity is declared in `shared/interfaces/<limb>.py`, implemented in `activities/<limb>/activities.py`, registered in that limb's `worker_init.py`, and dispatched on that limb's queue; every workflow is in `_WORKER_SPECS` on its own `*_WORKFLOW_QUEUE`.
- [ ] Every activity call has `start_to_close_timeout` + an unbounded, classified `RetryPolicy`; every deterministic error is a class in `shared/exceptions.py` and named through the class.
- [ ] Every activity is idempotent; every mutation whose outcome something else records is verified by read-back first.
- [ ] A change to a workflow with runs in flight is patched or its drain is stated; golden replays pass without re-recording.
- [ ] Any new setting is in `shared/settings.py`, `.env.example`, `tests/conftest.py` and the chart, which ships the key before the image that needs it.
- [ ] A new limb has its image job in `.github/workflows/build.yml` behind the `test` job, its chart, and (hub limbs) its `gitops/services/<limb>-worker/app.yaml` in redbull-platform.
- [ ] **Docs updated per Step 3**, in this same change.
- [ ] CLAUDE.md updated when a decision, dependency or rule changed, with the matching `.claude/rules/` file and the checker when it is a rule.
- [ ] Deploy order stated in the hand-off when it matters: brain before limb; chart key before image; which side of a cross-repo contract ships first.
- [ ] Nothing CLAUDE.md records as removed came back (convert-segment, the firewall flow, the DHCP scope wait in allocate-segment, `/bulk` on add-server-to-mce, a demo mode on a UI page, a sibling `cancel()`).
