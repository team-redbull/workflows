# provision-dell-server — handoff prompt for the next Claude Code session

Paste everything under "Prompt" into a new Claude Code session opened on the
`team-redbull/workflows` repo, branch `claude/ome-api-verification`.

---

## Prompt

You are continuing the **`provision-dell-server`** workflow in this repo (domain
`server-provisioning`). Read `CLAUDE.md` first (§4's first bullet is this
workflow), then `docs/design/dell-ome-idrac-api.md`, then
`workflow_domains/server_provisioning/provision_dell_server.py`.

### The goal (the operator's words, condensed)

Today a DC technician racks a Dell server and gives its iDRAC an IP. The DC team
then works by hand in OpenManage Enterprise (OME): they create a server profile,
turn it into a server profile template, attach it to the server and rename it to
`ocp-dell-r660-<region>-<cores>c-<mem>gb-<disk>tb-<service tag>`. Someone also
builds a RAID 1 in the BIOS.

**The workflow takes the iDRAC IP and finishes only when the server is fully
configured and visible, ready to install, in server-scan** (the inventory in
`team-redbull/server-scan`). From there the existing `install-server` workflow
draws it. Only Dell for now; Cisco, HPE and Intersight get their own workflows
later.

### Decisions the operator made (2026-09-30) — do not re-ask these

1. **Naming.** A naming service already exists inside the disconnected
   environment. It reads the server's data from OME, rounds cores/memory/disk
   itself and renames the profile. We only call it, and then verify the result.
   Its request contract is unknown: `activities/server_provisioning/server_namer.py`
   is the deliberate opening where it goes. The name is checked against the
   convention, this run's region and the iDRAC's service tag, never re-deriving
   the numbers. It is read from the **OME profile** name, which is what server-scan
   reads. Templates are shared per server type, so a per-server name cannot live
   on the template.
2. **Region** comes from the iDRAC IP prefix (a const,
   `workflow_domains/server_provisioning/regions.py`; the operator's examples are
   1.x → region1, 2.x → region2, 134.1.x → israel) or is passed explicitly. It
   must be a server-scan site code, because server-scan parses the site from the
   name.
3. **`128c`** is hyperthreaded cores (threads). **`1024gb`** is memory.
   **`10tb`** is all disks combined, rounded up by the naming service. The serial
   is the Dell service tag. The name must match the InfraEnv tokens install-server
   searches on (`^ocp-<infraEnv>`).
4. **Storage:** RAID 1 on the **BOSS** pair; every **PERC** drive **Non-RAID**.
5. **The workflow runs the OME discovery itself.**
6. **The template is per server type AND per firmware version.** It is picked from
   `DELL_TEMPLATES[<Redfish model>][<iDRAC firmware>]`. It is assumed to be the
   iDRAC firmware; the operator did not say iDRAC vs BIOS, so confirm if in doubt.
7. **Credentials.** A server arrives with root password `calvin`, the target one,
   or another factory one. **The template enforces the target root password.**
   Touch ONLY root (iDRAC user 2); never the OME user on the iDRAC, or OME loses
   the machine. Retry when a round of 3 logins fails (iDRAC9 blocks an address
   after 3 failures, so the next round waits the penalty out). **The real
   passwords belong in the worker's Secret only. Never write them into the repo**,
   tests included.
8. **No BIOS/firmware settings** beyond the template for now.
9. **server-scan discovers the server by itself** (its OME collector, every 6 h).
   Never add a refresh call; the workflow polls a read-only lookup until the
   server appears.
10. **Technicians use Swagger**, usually for **bulk** batches, hence
    `POST .../provision-dell-server/bulk` (one run per IP).

### What exists (two commits)

- `5a7f99e` (branch `claude/openshift-server-provisioning-od8539`): the domain.
  The workflow, the policy modules (`storage_plan.py`, `server_name.py`,
  `regions.py`), the router (single + bulk), the limb
  `activities/server_provisioning/` (`idrac.py`, `ome.py`, `server_namer.py`,
  `server_scan.py`), its Dockerfile, CI job and settings.
- The next commit (branch `claude/ome-api-verification`): every OME/iDRAC request
  checked against **Dell's own client code** (the `dellemc.openmanage` Ansible
  collection 9.12.3, available from the `ansible` wheel on PyPI; dell.com was
  unreachable), with 8 fixes. Also a stateful **Dell simulator** and an
  **end-to-end test**. Facts and sources: `docs/design/dell-ome-idrac-api.md`.

The workflow's shape: probe the root password → read the identity → refuse a
server server-scan says a cluster uses → OME discovery → deploy the template →
verify the target password (and rediscover in OME with it) → storage (staged,
ONE reboot, never deletes) → naming service → verify the name → wait for
server-scan.

### How to test

```bash
pip install -r requirements-dev.txt
pytest -q                                      # everything
pytest -q tests/test_server_provisioning_*.py  # policy, limb (respx), API — no Temporal needed
pytest -q tests/test_provision_dell_server_workflow.py  # workflow, mocked activities (Temporal test server)
pytest -q tests/test_provision_dell_server_e2e.py       # REAL workflow + REAL activities vs the simulator
```

- **Mocks at three levels.**
  1. `respx` mocks single HTTP calls, in `tests/test_server_provisioning_limb.py`.
     It is used mostly to check how errors are classified: a permanent error
     that is marked retryable retries forever.
  2. A stateful `Fake` of activities for the workflow tests
     (`tests/test_provision_dell_server_workflow.py`, Temporal time-skipping env).
  3. **`tests/dell_simulator.py`**: a stateful iDRAC + OME + naming service +
     server-scan served over **HTTPS**. The iDRAC is on `127.0.0.2:443` (the limb
     addresses an iDRAC by bare IP). It models iDRAC9's login block, BOSS
     OnReset vs PERC real-time jobs, NVMe RaidStatus under `DellPCIeSSD`, OME
     managing only a machine whose password it holds, the template setting
     root's password, and server-scan listing a server only after a collection.
     Time is counted in requests, never seconds, because workflow timers are
     skipped.
- The e2e test needs `openssl` and permission to bind `127.0.0.2:443`; it
  **skips** otherwise. On a host with `HTTPS_PROXY` set it adds the simulator to
  `NO_PROXY` itself, because httpx ignores CIDR entries there.
- **On macOS that skip is unavoidable, so run the e2e module in a Linux
  container.** A Mac gives neither half: 443 is privileged, and `127.0.0.2` is
  not on `lo0` unless someone aliases it. Linux needs neither — the whole of
  127/8 is local and a container's root may bind 443:

  ```bash
  podman run --rm -v "$PWD":/app:z -w /app python:3.12-slim bash -lc '
    apt-get update -qq && apt-get install -y -qq openssl
    pip install -q -r requirements-dev.txt && python -m pytest -q'
  ```

- The Temporal-based tests download Temporal's test server from
  `temporal.download` on first use. **They now run against it** (temporalio
  1.33.0): the whole suite is **367 passed, 0 skipped**, the 16 workflow cases
  and all 5 e2e scenarios included. Time skipping is what makes that take 20 s
  rather than hours: the workflow's own waits are minutes apart, up to
  `_SERVER_SCAN_POLL`'s 10 min inside an 8 h deadline. A wall-clock Temporal
  would run the same scenarios in real time, so it is not a faster substitute.
- The e2e test is not vacuous: reading RaidStatus only from `DellPhysicalDisk`
  (the real bug the API review found) fails 3 of its 5 scenarios with exactly
  the `StorageLayoutUnsupportedError` that `dell-ome-idrac-api.md` predicts.
  Re-do that check after changing the simulator.

### Open work, in order

1. ~~Run the full suite under real Temporal~~ — done, green, nothing differed.
   Open a PR from `claude/ome-api-verification` (it contains the first commit).
2. Put the naming service's real contract into `server_namer.py`.
3. Replace the placeholder prefixes in `regions.py` with the real iDRAC networks
   and server-scan site codes.
4. The **chart** `gitops/charts/server-provisioning-worker/` in
   `team-redbull/redbull-platform`: settings from `shared/settings.py`
   (`ServerProvisioningActivitySettings`; secrets are `OME_PASSWORD`,
   `IDRAC_ROOT_PASSWORD`, `IDRAC_FACTORY_PASSWORDS`, the tokens), no RBAC, and
   network access to the iDRACs, OME, the naming service and server-scan. CI's
   `build-server-provisioning-worker` job fails its image bump until the chart
   exists. Move that job back beside the other limbs once it does.
5. Confirm on the first real R660 the list at the end of
   `docs/design/dell-ome-idrac-api.md`. Above all: **the templates must not carry
   storage components** (`RAIDresetConfig=True` wipes the controller).

### Repo rules that bit this work

- Follow `CLAUDE.md` §5 exactly: one Pydantic argument per `run()`, error types
  built from the exception CLASSES, unlimited retries with only classified
  failures non-retryable, every wait on durable timers with a deadline.
- Never put a password in Temporal history. Runs name a credential by its index
  in the limb's list.
- Keep `tests/dell_simulator.py` faithful to Dell's code. Cite the collection
  file for any new behavior.
