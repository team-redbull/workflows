# install-server API — one route, one run per server

Status: **decided with the operator 2026-10-23, revised 2026-10-24** (a
request without `count` takes a slot too, so it is never a 409 — see
[the revision](#revised-2026-10-24-no-409-from-the-pool)). Implemented in
`workflow_domains/server_lifecycle/router.py` (`InstallServerRequest`,
`start_install_server`), tested in `tests/test_server_lifecycle_api.py`.
The workflow itself did not change.

## The problem

An operator asked install-server for a second healthy server from the same
InfraEnv while the first was still installing, and got a 409: *"An
install-server run is already in flight for InfraEnv …"*.

The 409 itself is deliberate. A request with no server named is keyed on the
pool (`install-server-<infraEnv>`), so a double-click or a retried POST cannot
install two servers. Parallel installs from one pool did exist, as
`POST install-server/bulk` with `count` (added 2026-10-20). Nobody found it:

- the name `bulk` reads as "a list of specific servers", not "N of whatever
  is healthy";
- the docs site still said parallel installs into one pool had not been built;
- the 409 did not mention it.

## The decision

ONE route, `POST /workflows/server-lifecycle/install-server`. The body says
what you want:

| body | runs | reply |
| --- | --- | --- |
| neither `count` nor `server_names` | 1, the pool picks, in the next free slot | `{workflow_id, run_id}`; sent again while it runs, a second install |
| `"count": N` (+ optional `candidate_count`) | N, the pool picks, in parallel | per-run report |
| `"server_names": "ocp-…"` | 1, that machine | `{workflow_id, run_id}`; 409 while it installs |
| `"server_names": ["ocp-…", …]` (1–20) | 1 per name, in parallel | per-run report, even for a list of one |

- **Every server is its own workflow run**, with its own id, status, failure
  and history, whether a request asks for 1 server or 20. There is never a
  batch or parent workflow.
- **`count` and `candidate_count` only go with the pool.** Either one next to
  `server_names` is a 422.
- `namespace` defaults to `inventory`. `count` and `candidate_count` are
  optional; without `count` the pool picks one.
- `install-server/bulk` is **removed** (404). The old `server_name` field is
  a 422 whose message points at `server_names`.

## Options considered

1. **Keep `/bulk` for `count` and only fix the docs and the 409 message.**
   This was the cheapest. Rejected: there would still be two places to look,
   and "bulk" would still read as a list of names.
2. **Two routes split the other way: `count` on install-server, names on
   `/bulk`.** This was built first on 2026-10-23 and never pushed. Rejected in
   favour of one route. With two, one machine could be asked for on either,
   and every caller had to learn which route takes which field. With one, the
   body is the only thing to learn.
3. **One route that always answers with a per-run report.** One reply shape
   is cleaner, but it breaks every caller that installs one server today,
   since they read `workflow_id` at the top level. It also loses the 409 that
   makes a double-click harmless.
4. **One route where `count` defaults to 1, so every request is a parallel
   draw.** Rejected on 2026-10-23 for the same reason: it loses the 409 on a
   single install. **Adopted on 2026-10-24** for the runs, keeping the single
   reply shape — see the revision below.
5. **A parent workflow that installs N servers.** Rejected. One server's
   failure would shape the status of the whole batch, and only
   one-workflow-per-id lets Temporal refuse a second run for the same
   machine. It is also the rule everywhere else in this repo
   (initialize-segment/bulk, provision-dell-server/bulk): one workflow per
   item.
6. **Chosen: one route, and the body decides.** It is option 2's behaviour in
   one place.

## Why each detail is the way it is

- **`server_names` takes a string or a list.** One name is the common case
  and reads naturally as a string. Several are a list. The reply follows the
  shape that was sent:
  - a single name keeps the simple `{workflow_id, run_id}` and the 409 (it
    is one machine, so a second run could only fail on its lock);
  - a list always gets the report, so a script that sends a list can always
    loop over `results`.
- **`count` and `candidate_count` are refused with names instead of ignored.**
  Both size a draw from the pool, and a named server is drawn alone (one
  machine, `?name=…&count=1`). `candidate_count` used to be silently ignored
  on a named install. Ignoring a field the caller sent hides a
  misunderstanding, while a 422 shows it. `vlan_id` is refused for the same
  reason.
- **A pool request is never a 409** (it was, until the revision below). A
  named machine still is: asking twice for one machine is one install.
- **`count` means "start N more", not "make sure N are running".** A second
  batch while the first installs has to add servers (commit `c487806`: the
  first version collided on `_1`..`_N`). As a result, a `count` request is
  **not retry-safe**: sent twice, it starts 2N runs.
  - An idempotency key (`request_id`, giving a retried request the same
    workflow ids) was considered and left out, because the callers are people
    using Swagger or curl, not controllers that retry automatically.
  - **Revisit this if a controller or pipeline ever calls `count`.**
- **`InstallServerRequest` is its own model, not an `InstallServerInput`
  subclass.** One request is now many runs.
  - `InstallServerInput` is decoded from Temporal history on every replay, so
    it keeps its shape: `server_name` singular, and no `count`.
  - `run_input()` builds one input per server.
  - `test_every_run_field_can_be_sent` fails if a field is added to the run
    input and not to the request.
- **`/bulk` is removed, not left as a stub.** It existed for three days, and a
  404 is clear enough.
- **The old `server_name` gets a 422 pointer, not a silent alias.** Each
  thing has one spelling.

## Revised 2026-10-24: no 409 from the pool

The operator asked what happens when a single random install is sent after
another, both from the pool. It was a 409 until the first finished, which can
take an hour: the request had the pool's id, `install-server-<infraEnv>`. The
operator's call: it should not be a 409, it should take the next slot like
`count` does.

- **Asking the pool again IS asking for one more server.** That is the normal
  reason to send it, and the 409 refused exactly that, unless the caller knew
  to add `count`.
- **The 409 was the last thing guarding a double-click.** That is the same
  trade-off already accepted for `count` (see above): the callers are people,
  and a second request from them means a second install. It no longer has
  the job it started with, keeping two runs off one machine. The install lock
  does that now, for any number of runs.
- **One way to draw from a pool.** Every pool run is on a slot now, so a new
  run's spares (`candidate_count`) count every run on the pool. A run on the
  old pool id was invisible to a `count` request.

What stayed:

- **The reply follows the request.** Without `count` it is still
  `{workflow_id, run_id}`; only `count` asks for the per-run report. Callers
  that install one server read `workflow_id` exactly as before. Only its value
  changed, from `install-server-<infraEnv>` to `install-server-<infraEnv>_<slot>`.
- **A 409 is left only for a named machine**, and for a pool whose 100 slots
  are all running.
- A start Temporal refuses is a **502 with its reason** for one run, named or
  from the pool, instead of a bare 500.

`install_server_workflow_id` became `install_server_name_workflow_id`: nothing
builds the pool id any more. Runs started on it before the change keep it, and
`GET /workflows/runs/{id}` still finds them.

## Why parallel requests cannot collide

Workflow ids follow two schemes that never overlap:

- `add-server-to-mce_<mce>_<infraEnv>_<slot>` for every run the pool picks, one or
  `count` of them (`install-server-<infraEnv>_<slot>` before 2026-10-25, and
  `install-server-<infraEnv>`, without a slot, before 2026-10-24);
- `add-server-to-mce_<mce>_<server>` for a named server, lowercased
  (`install-server-name-<server>` before 2026-10-25).

The parts are joined with `_`, which a Kubernetes name cannot contain, so no two
different (MCE, InfraEnv, slot) triples build the same id. The MCE is in the id so
a hub finds only its own runs; the machines a pool draws from are shared by every
hub, so a run sizing its spare candidates no longer counts another hub's runs
(`busy` is per hub) — the install lock, not the id, keeps two hubs off one
machine. Only the id prefix changed: the workflow type, queue and route are still
`install-server`, and runs started on the old ids keep them.

Machines are guarded by server-scan's install lock (ADR-0035 there):

- a run takes the lock before it writes anything;
- server-scan takes it by compare-and-set and refuses a live lock to any
  other workflow id;
- `/servers/available` stops listing a locked machine.

| scenario | what happens |
| --- | --- |
| one install from the pool, then another while it runs | slots `_1` and `_2`; two different machines |
| a batch of 5, then another 5 while the first installs | slots `_1`–`_5`, then `_6`–`_10`; ten different machines. If the pool has fewer than ten free, the extra runs fail with `ServerReservedError`, never a double install |
| 5 from the pool, then 5 by name | different ids. A named machine a pool run already locked fails its own run |
| 5 by name, then 5 from the pool | the locked machines are no longer listed, so the pool runs never see them |
| the same name twice, as a string and in a list | one id: the second is a 409, or `already_running` in a list |
| two runs drawing the same list at the same moment | the compare-and-set picks one. The loser skips to its next candidate, which is why `candidate_count` defaults to (slots still running) + `count` + 2, at most 20 |

## What did not change

The workflow code, `InstallServerInput` and every activity are untouched. The
ids are built only in the API, so runs already in flight replay exactly as
before, and no `workflow.patched` gate was needed.
