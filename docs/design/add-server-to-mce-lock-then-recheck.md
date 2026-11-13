# add-server-to-mce: draw one server, lock it, then live-check it

Status: **proposed with the operator 2026-11-13, not implemented.** The server-scan half is that repo's
ADR-0062 (`docs/adr/0062-lock-then-recheck-one-server.md`), which has to ship first.

## What happened

Two `POST /workflows/server-lifecycle/add-server-to-mce` requests with `count: 10` started 20 runs, and
all 20 stayed at `acquiring-server` for good. What made it happen, each from code:

1. **The router sized the draw for a sorted list.** A pool run's default `candidate_count` is
   `busy runs + new runs + 2`, capped at 20 (`router.py`, `_start_pool_installs`), on the stated premise that
   every run "takes one machine from the same top of the list". The first request's runs drew 12 candidates
   each, the second's 20. But server-scan's draw is `$match` then `$sample`, a random pick
   (`sample_available_tier`, server-scan ADR-0032 §3), so runs mostly draw *different* servers and the extra
   candidates protected against almost nothing.
2. **server-scan live-checks every drawn candidate, one after another, before it answers.**
   `_fill_from_tiers` awaits a vendor `get_one()` per candidate in a plain loop, and replaces a rejected one
   for up to 3 rounds: a draw of N costs N to 3N vendor calls. The run then installs the first candidate that
   works, so every other check was spent on a server it never touched.
3. **One check can outlast the run's patience.** A UCS Central recheck may take 240 s (server-scan
   `recheck.timeouts`). `acquire_servers` gives the whole draw 60 s (`server_scan.py`, `_HTTP_TIMEOUT`) under a
   90 s activity, and `_RETRY_POLICY` retries without limit, so a draw that does not answer in time is asked
   again from scratch, forever. 20 runs × 12-20 sequential checks (about 320) never fit.
4. **The check comes before the lock.** Two runs can check the same server, and a server checked healthy can
   be locked by another run first.

## The decision

### `candidate_count` is the number of tries, drawn one at a time

Default **3 for every pool run**, whatever else is running: the `busy + count + 2` sizing goes. The range
stays 1-20. The run asks server-scan for **one** server, tries it, and only if that try fails asks for the
next. It never draws its later tries ahead of time.

A *try* is one server the run considers, and every way a server can fail uses one:

- the run's own checks reject it (no bond, no BMC host, a bad MAC, an unusable name, a BareMetalHost already
  there);
- server-scan's live check says it does not qualify;
- server-scan cannot check it (vendor failure, below);
- no Agent registers within the hour.

**After three failed tries the run fails: the problem is no longer the servers.** The failure groups the
reasons as `_REJECTION_SUMMARY` does today, and takes a specific type when all of them agree (three vendor
failures are `VendorRecheckFailedError`, naming the manager).

**A server another run locked first does not use a try.** It says nothing about the server or this run, so
the run simply draws again.

### Why one at a time, not all three at the start

- **The draw is free now.** With `recheck=false` it is a Mongo query. Asking for spares up front saved a
  vendor round only while the draw checked every server it returned; now it saves nothing.
- **A try can take an hour** (the Agent wait). Spares drawn at the start would be up to an hour old by the
  time the run reached them: locked by another run, or claimed by a cluster. A draw made when it is needed
  sees what is free at that moment.
- **One path for every failure.** Whatever went wrong (a rejection, a failed check, no Agent, a lock lost to
  another run), the run does the same thing: draw again, with or without using a try.

### The loop

1. `resolving-vlan`, `validating-infraenv`: unchanged.
2. `acquiring-server`: draw one server (below).
3. For that server:
   - `selecting-server`: the run's own checks, on the stored document. Rejected: one try used, back to 2.
   - `reserving-server`: the install lock (unchanged). Lost to another run: back to 2, no try used.
   - `rechecking-server` (new): `POST /servers/{id}/recheck` with this run's workflow id, on the MCE's queue
     like every server-scan call. A new activity, `recheck_server`: HTTP timeout 300 s (server-scan's Route
     allows 300 s, above the slowest manager's 240), `start_to_close_timeout` 330 s, and
     `schedule_to_close_timeout` 10 min. This is the best-effort exception the rules allow, because the
     workflow catches its `ActivityError`.
     - **Does not qualify:** reason `unqualified`, carrying server-scan's reasons. Release the lock, one try
       used, back to 2.
     - **Vendor failure, or 429 still answered when the 10 min run out:** reason `recheck-failed`. Release
       the lock, one try used, back to 2. A 429 means server-scan's per-manager cap is full; the activity
       waits for its `Retry-After` and heartbeats.
   - The run's own checks **again, on the fresh document** the check returned: links and MACs may have
     changed since the collection, and the bond is built from what was just read.
   - From here everything is unchanged: the NMStateConfig check, the PXE map, the creates, the Agent wait, the
     day-long hold. No Agent: the rollback as today, one try used, back to 2.
4. Three tries used: the run fails with the grouped reasons.

### Drawing one server

`GET /servers/available?pattern=^ocp-<infraEnv>&count=<tries used + 1>&health=HEALTHY&recheck=false`, and the
run takes the first server in the answer **it has not already tried**. This means 1 on the first draw, at most
3 later.

- The draw already leaves out every locked server, so a server another run holds never comes back.
- A server this run tried and **rejected before locking it** is not locked, so the random pick can return it.
  At most *tries used* of the drawable servers are ones this run already tried, so a draw of `tries used + 1`
  always holds at least one untried server, if the pool has one. No server-scan change is needed for this.
- Nothing untried in the answer means the pool is exhausted. The run fails with the reasons it has gathered,
  or `ServerReservedError` when every server it reached was taken by other runs, as today.
- At most 10 draws lost to other runs' locks, as a safety stop.

### What it costs

Vendor checks made by server-scan:

| case | today | proposed |
| --- | --- | --- |
| 1 run, `candidate_count` 3, first server healthy | 3, before the run gets an answer | 1 |
| 3 runs × 3, a pool of exactly 9, every check fails | up to 9 per draw, 27 in all | 9 (3 tries each) |
| 3 runs × 3, a pool of exactly 9, all healthy | up to 9 | 3 |
| 20 runs (two `count: 10` requests) | about 320, sequential inside 20 requests | 20, at most 4 per manager type per server-scan pod at once |

The draws themselves are Mongo queries: one per try, plus one per lock lost to another run. With a pool of
exactly 9, overlap between runs costs a lock attempt and a draw, never a vendor call. More runs than servers is
a pool too small for the request, and the extra runs fail saying so.

## Not decided yet

- **Named installs (`server_names`).** The proposed default is the same order: the one server is locked,
  then checked, and a negative verdict fails the run with server-scan's reasons (`allow_unhealthy` waives the
  health reasons, never a safety one). Because the lock comes first, the named path could also stop going
  through `/servers/available`, whose refusal of a machine its own run had locked caused the 2026-10-31 re-run
  incident and `lookup_install_candidate`. To decide when built.
- **The cap's default** (4 per manager type per server-scan pod) against each manager's real session limit.

## Rollout, when built

1. server-scan with ADR-0062 (`?recheck=false`, `/recheck`, the cap). Its default stays `recheck=true`, so
   today's runs are unaffected.
2. This repo:
   - The workflow, behind `workflow.patched("lock-then-recheck")`. A run recorded before keeps drawing with
     the recheck, which server-scan still serves, and the goldens replay unchanged.
   - `recheck_server` in `shared/interfaces/server_lifecycle.py` and the limb.
   - `AcquireServerRequest.recheck`, and the new reasons in `_REJECTION_TYPE` and `_REJECTION_SUMMARY`
     (`unqualified`, `recheck-failed`) with their WORKFLOW-RAISED types.
   - The phase `rechecking-server` in `ui.py`'s words.
   - The router's default `candidate_count` of 3: an edge change, needing no patch.
   - The docs site page, README, CLAUDE.md §4 and §9.
3. **The brain goes before the limb** (`build.yml`). In between, a new run's `recheck_server` reaches an old
   limb that does not register it. The task is retried until the new limb is up, so runs are delayed, not
   failed. An old limb ignores `recheck` on the draw and still asks with the recheck: slower, still correct.

## Considered and rejected

- **Draw all the tries at the start.** That was this note's first version. It saves nothing once the draw is
  free, the spares go up to an hour stale during the Agent wait, and it still needs a second draw when another
  run takes one of them.
- **Keep the recheck in the draw, ask for 3, draw again only on a lost lock.** One UCS check can still outlast
  the 60 s draw, and the spare candidates are still checked before the run locks anything.
- **Parallel rechecks inside the draw.** Faster, but it still checks servers the run will not use, and 20
  draws × N parallel checks loads the managers more, not less.
- **The workflow checking the hardware itself.** The live check goes through the vendor *manager* (OME,
  OneView, UCS Central, Intersight), not the iDRAC. Doing it here would bring back the four vendor clients and
  their credentials, which is what server-scan replaced (CLAUDE.md §4). It would also copy server-scan's
  health engine. The workflow decides *when* and *which server*; server-scan does the check.
