"""provision-dell-server — from "the iDRAC has an IP" to "server-scan lists it, ready to install".

The FIRST workflow of the `server-provisioning` domain. It replaces what the DC
team did by hand for every Dell server: discover it in OpenManage Enterprise,
attach a server profile template, rename the profile to the naming convention
and build the boot RAID in the BIOS. The only manual step left is the one at the
rack — giving the iDRAC its IP.

Shape of the run:

  1. probing-idrac        — which root password does the iDRAC accept? The
                            target one first (a re-run, or a machine that
                            shipped with it), then the factory ones. A round
                            where all are rejected has tripped the iDRAC's
                            IP block, so the next round waits the penalty out.
  2. reading-identity     — service tag, model, iDRAC firmware. Must be a
                            Dell PowerEdge.
     checking-server-scan — and must not be a server a cluster is using:
                            everything after this reboots it.
  3. discovering-in-ome   — skipped when OME already has the service tag.
  4. deploying-template   — the template for (model, iDRAC firmware) from
                            DELL_TEMPLATES. It creates the server profile and
                            ENFORCES the root password.
  5. verifying-root-password — root must now accept the target password; if
                            the machine came in on a factory password, OME is
                            re-pointed at the new one (rediscovering-in-ome).
  6. configuring-storage  — RAID 1 on the BOSS, every PERC drive Non-RAID,
     applying-storage       staged together and applied by ONE reboot, then
     verifying-storage      read back. Nothing is ever deleted
                            (storage_plan.py).
  7. naming-server        — the naming service renames the OME profile, and
     verifying-name         the name is read back from OME and checked against
                            the convention, region and service tag included.
  8. awaiting-server-scan — server-scan's own collector finds the server under
                            that name. The run COMPLETES only then, because that
                            is the moment install-server can use the machine.

WHY STORAGE COMES AFTER THE TEMPLATE. The template deployment can reboot the
machine and apply BIOS attributes; configuring storage afterwards means the
layout checked in step 6 is the one the server ends up with. It also means
every storage call runs as root on the TARGET password, never a factory one.

EVERY WAIT IS THE WORKFLOW'S, bounded, on durable timers — a discovery job, a
template deployment, a reboot applying RAID, the next server-scan collection.
They are all MACHINE convergence (CLAUDE.md §5): each gets a deadline and
fails the run loudly by name when it passes. No activity sleeps.

NO PASSWORD IS EVER IN HISTORY. The run refers to a root credential by its
position in the limb's configured list (IdracRef.credential); the limb resolves
it. Temporal history is readable by anyone with UI access.

Changing any timing constant below is a non-deterministic change for in-flight
runs.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import SERVER_PROVISIONING_ACTIVITY_QUEUE
    from shared.exceptions import (
        IdracAuthError,
        IdracCredentialsMissingError,
        IdracCredentialsRejectedError,
        IdracRequestRejectedError,
        IdracUnreachableError,
        NotADellServerError,
        OmeAuthError,
        OmeDiscoveryFailedError,
        OmeRequestRejectedError,
        ProfileConflictError,
        RegionMissingError,
        ServerNameNotAppliedError,
        ServerNamerRejectedError,
        ServerAlreadyInstalledError,
        ServerScanAuthError,
        ServerScanNeverSawServerError,
        StorageJobFailedError,
        StorageLayoutUnsupportedError,
        StorageNotConvergedError,
        TemplateDeployFailedError,
        TemplateNotConfiguredError,
        TemplateNotFoundError,
        TemplatePasswordNotAppliedError,
    )
    from shared.interfaces.server_provisioning import (
        apply_staged_idrac_jobs,
        check_idrac_login,
        deploy_ome_template,
        find_in_server_scan,
        find_ome_device,
        get_idrac_jobs,
        get_ome_job,
        get_ome_profile,
        probe_idrac_credentials,
        read_idrac_identity,
        read_storage_layout,
        request_server_name,
        stage_storage_config,
        start_ome_discovery,
    )
    from shared.models.server_provisioning import (
        IDRAC_AWAITING_RESET_STATE,
        IDRAC_JOB_SUCCESS,
        IDRAC_TERMINAL_JOB_STATES,
        TARGET_CREDENTIAL,
        IdracIdentity,
        IdracJobsRef,
        IdracJobsState,
        IdracProbeResult,
        IdracRef,
        OmeDevice,
        OmeDeviceRef,
        OmeDiscoveryRequest,
        OmeJobRef,
        OmeProfileRef,
        ProvisionDellServerProgress,
        ProvisionDellServerResult,
        ProvisionDellServerRunArgs,
        ServerNameRequest,
        ServerScanLookup,
        StorageConfigRequest,
        StorageLayout,
        TemplateDeployRequest,
    )
    from workflow_domains.server_provisioning.server_name import (
        model_token,
        name_matches_convention,
    )
    from workflow_domains.server_provisioning.storage_plan import StoragePlan, plan_storage

T = TypeVar("T")

# Per-attempt budget. Generous next to the other domains' 90 s: an iDRAC walking
# its storage tree is a dozen Redfish GETs, and an embedded BMC can take several
# seconds per answer. Every HTTP call inside stays at 60 s, below this.
_ACTIVITY_TIMEOUT = timedelta(minutes=5)

# ACTIVITY-raised failures no retry can fix, as CLASSES (CLAUDE.md §5).
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    IdracAuthError,
    IdracCredentialsMissingError,
    IdracRequestRejectedError,
    OmeAuthError,
    OmeRequestRejectedError,
    TemplateNotConfiguredError,
    TemplateNotFoundError,
    ProfileConflictError,
    ServerNamerRejectedError,
    ServerScanAuthError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# --- Credential probing ------------------------------------------------------
# iDRAC9 blocks an address after 3 failed logins in its fail window, for a
# 600 s penalty by default. One round is at most three candidates, so it can
# only trip the block when ALL of them are wrong — and the next round then
# waits the penalty out, plus a minute, instead of feeding the block.
_PROBE_ROUNDS = 3
_LOCKOUT_WAIT = timedelta(minutes=11)
# An iDRAC that answers nothing: a mistyped IP, or one still being cabled.
_UNREACHABLE_DEADLINE = timedelta(minutes=30)
_UNREACHABLE_RETRY = timedelta(minutes=2)

# --- OME jobs ----------------------------------------------------------------
_DISCOVERY_DEADLINE = timedelta(minutes=30)
_DISCOVERY_POLL = timedelta(seconds=30)
# Deployment can reboot the machine and apply BIOS attributes on the way up.
_TEMPLATE_DEADLINE = timedelta(hours=2)
_TEMPLATE_POLL = timedelta(minutes=1)
# The iDRAC may restart once the template has changed its own settings.
_PASSWORD_DEADLINE = timedelta(minutes=20)
_PASSWORD_POLL = timedelta(minutes=1)

# --- Storage: a reboot through POST plus the Lifecycle Controller's apply ------
_STORAGE_DEADLINE = timedelta(minutes=90)
_STORAGE_POLL = timedelta(minutes=1)

# --- Naming ------------------------------------------------------------------
_NAME_DEADLINE = timedelta(minutes=30)
_NAME_POLL = timedelta(seconds=30)

# --- server-scan: its Dell collector runs every 6 h and may take 90 min, so the
# deadline outlasts one full cycle. The read is Mongo-only and cheap.
_SERVER_SCAN_DEADLINE = timedelta(hours=8)
_SERVER_SCAN_POLL = timedelta(minutes=10)


def _fail(message: str, error: type[Exception]) -> ApplicationError:
    """A workflow-raised failure, typed by class name (CLAUDE.md §5)."""
    return ApplicationError(message, type=error.__name__)


def _credential_label(index: int) -> str:
    return "target" if index == TARGET_CREDENTIAL else f"factory-{index}"


def _describe_jobs(state: IdracJobsState) -> str:
    return "; ".join(
        f"{job.job_id} {job.state}{f' ({job.message})' if job.message else ''}"
        for job in state.jobs
    )


@workflow.defn
class ProvisionDellServerWorkflow:
    def __init__(self) -> None:
        self._progress = ProvisionDellServerProgress(phase="pending")

    @workflow.query
    def progress(self) -> ProvisionDellServerProgress:
        """Where the run is; `waiting_on` says what a long phase is waiting for."""
        return self._progress

    def _phase(self, phase: str, waiting_on: str | None = None) -> None:
        self._progress.phase = phase
        self._progress.waiting_on = waiting_on

    @workflow.run
    async def run(self, run_args: ProvisionDellServerRunArgs) -> ProvisionDellServerResult:
        run_input = run_args.input
        idrac_ip = run_input.idrac_ip
        self._progress.idrac_ip = idrac_ip
        if not run_input.region:
            raise _fail(
                f"No region for iDRAC {idrac_ip}: start the run through the API, which "
                "resolves it from the address prefix, or pass one explicitly",
                RegionMissingError,
            )
        region = run_input.region

        # Step 1 — which root password opens this iDRAC.
        initial_credential = await self._probe(idrac_ip)
        initial = IdracRef(idrac_ip=idrac_ip, credential=initial_credential)

        # Step 2 — what the machine is.
        self._phase("reading-identity")
        identity: IdracIdentity = await self._run(read_idrac_identity, initial)
        if "dell" not in (identity.manufacturer or "dell").lower() or not identity.model.lower().startswith(
            "poweredge"
        ):
            raise _fail(
                f"{idrac_ip} is a {identity.manufacturer or 'unknown'} {identity.model!r}, not a "
                "Dell PowerEdge — this workflow provisions Dell servers only",
                NotADellServerError,
            )
        self._progress.service_tag = identity.service_tag
        self._progress.model = identity.model

        # Step 2b — never a machine a cluster is using. Everything after this
        # reboots it or rewrites its BIOS, and an IP typed one digit wrong must
        # not take a production node down with it.
        self._phase("checking-server-scan")
        known = await self._run(find_in_server_scan, ServerScanLookup(service_tag=identity.service_tag))
        if known.claimed_by:
            raise _fail(
                f"{identity.service_tag} at {idrac_ip} is {known.claimed_by} according to "
                f"server-scan (as {known.name!r}) — refusing to reprovision a server in use",
                ServerAlreadyInstalledError,
            )

        # Step 3 — into OME, unless it is already there.
        self._phase("discovering-in-ome")
        device = await self._discover(initial, identity.service_tag, purpose="discover")
        device_id = int(device.device_id or 0)
        self._progress.ome_device_id = device_id

        # Step 4 — the template: BIOS/iDRAC settings and the enforced root password.
        self._phase("deploying-template")
        deploy = await self._run(
            deploy_ome_template,
            TemplateDeployRequest(
                device_id=device_id,
                service_tag=identity.service_tag,
                model=identity.model,
                idrac_firmware=identity.idrac_firmware,
            ),
        )
        self._progress.template_name = deploy.template_name
        if deploy.job_id is not None:
            self._phase("deploying-template", waiting_on=f"OME job {deploy.job_id}")
            job, finished = await self._poll(
                lambda: self._run(get_ome_job, OmeJobRef(job_id=int(deploy.job_id or 0))),
                lambda state: state.finished,
                _TEMPLATE_DEADLINE,
                _TEMPLATE_POLL,
            )
            if not finished or not job.succeeded:
                raise _fail(
                    f"Deploying template {deploy.template_name!r} to {identity.service_tag} "
                    + (
                        f"ended {job.status} (OME job {job.job_id})"
                        if finished
                        else f"did not finish within {_TEMPLATE_DEADLINE} (OME job {job.job_id} "
                        f"is {job.status})"
                    ),
                    TemplateDeployFailedError,
                )

        # Step 5 — the template must have left root on the target password.
        target = IdracRef(idrac_ip=idrac_ip, credential=TARGET_CREDENTIAL)
        self._phase("verifying-root-password")
        _, accepted = await self._poll(
            lambda: self._run(check_idrac_login, target),
            lambda ok: ok,
            _PASSWORD_DEADLINE,
            _PASSWORD_POLL,
        )
        if not accepted:
            raise _fail(
                f"After template {deploy.template_name!r} deployed, root on {idrac_ip} still does "
                "not accept the target password — check that the template carries the root "
                "(user 2) password attribute",
                TemplatePasswordNotAppliedError,
            )
        if initial_credential != TARGET_CREDENTIAL:
            # OME discovered the machine with the factory password it came in
            # on; point it at the one root has now, or OME loses the machine.
            self._phase("rediscovering-in-ome")
            await self._discover(target, identity.service_tag, purpose="rediscover", force=True)

        # Step 6 — storage: RAID 1 on the BOSS, PERC drives Non-RAID.
        boss_created, converted = await self._configure_storage(target)

        # Step 7 — the name. The naming service computes it; OME is where it is read back.
        self._phase("naming-server")
        await self._run(
            request_server_name,
            ServerNameRequest(
                service_tag=identity.service_tag,
                ome_device_id=device_id,
                region=region,
                idrac_ip=idrac_ip,
            ),
        )
        self._phase("verifying-name", waiting_on=f"OME profile of device {device_id}")
        profile, named = await self._poll(
            lambda: self._run(get_ome_profile, OmeProfileRef(device_id=device_id)),
            lambda p: bool(
                p.found
                and p.profile_name
                and name_matches_convention(p.profile_name, identity.model, region, identity.service_tag)
            ),
            _NAME_DEADLINE,
            _NAME_POLL,
        )
        if not named:
            observed = repr(profile.profile_name) if profile.found else "missing"
            raise _fail(
                f"The OME profile of {identity.service_tag} is {observed}, not "
                f"ocp-dell-{model_token(identity.model)}-{region}-<cores>c-<mem>gb-<disk>tb-"
                f"{identity.service_tag}, {_NAME_DEADLINE} after asking the naming service",
                ServerNameNotAppliedError,
            )
        profile_name = str(profile.profile_name)
        self._progress.profile_name = profile_name

        # Step 8 — done means install-server can see it.
        self._phase("awaiting-server-scan", waiting_on="server-scan's next Dell (OME) collection")
        seen, found = await self._poll(
            lambda: self._run(
                find_in_server_scan,
                ServerScanLookup(service_tag=identity.service_tag, expected_name=profile_name),
            ),
            lambda state: state.found,
            _SERVER_SCAN_DEADLINE,
            _SERVER_SCAN_POLL,
        )
        if not found:
            raise _fail(
                f"server-scan did not list {profile_name} within {_SERVER_SCAN_DEADLINE}. Check the "
                "OPENMANAGE collector's last run and its INVENTORY_OME_NAME_PATTERN",
                ServerScanNeverSawServerError,
            )

        self._phase("completed")
        return ProvisionDellServerResult(
            idrac_ip=idrac_ip,
            region=region,
            service_tag=identity.service_tag,
            model=identity.model,
            idrac_firmware=identity.idrac_firmware,
            initial_credential=_credential_label(initial_credential),
            ome_device_id=device_id,
            template_name=deploy.template_name,
            profile_name=profile_name,
            boss_raid1_created=boss_created,
            non_raid_drives_converted=converted,
            server_scan_id=str(seen.server_id),
            server_scan_health=seen.health,
        )

    async def _probe(self, idrac_ip: str) -> int:
        """The index of the root password this iDRAC accepts.

        Two different waits, deliberately not merged: an iDRAC that answers
        nothing gets a deadline (it may still be cabled), while a round of
        rejections gets the lockout penalty waited out before the next round,
        up to _PROBE_ROUNDS rounds.
        """
        self._phase("probing-idrac")
        started = workflow.now()
        rounds = 0
        last: IdracProbeResult | None = None
        while True:
            probe: IdracProbeResult = await self._run(probe_idrac_credentials, idrac_ip)
            last = probe
            if probe.credential is not None:
                return probe.credential
            if not probe.reachable:
                if workflow.now() - started >= _UNREACHABLE_DEADLINE:
                    raise _fail(
                        f"iDRAC {idrac_ip} did not answer Redfish within {_UNREACHABLE_DEADLINE}"
                        f"{f': {probe.detail}' if probe.detail else ''}. Check the IP and the cabling",
                        IdracUnreachableError,
                    )
                self._phase("probing-idrac", waiting_on=f"iDRAC {idrac_ip} to answer")
                await workflow.sleep(_UNREACHABLE_RETRY)
                continue
            rounds += 1
            if rounds >= _PROBE_ROUNDS:
                break
            self._phase(
                "probing-idrac",
                waiting_on=f"the iDRAC's login-block penalty after round {rounds} of {_PROBE_ROUNDS}",
            )
            await workflow.sleep(_LOCKOUT_WAIT)
        raise _fail(
            f"iDRAC {idrac_ip} rejected every configured root password in {_PROBE_ROUNDS} rounds "
            f"({last.rejected if last else 0} per round), each after its lockout had expired — it "
            "has a root password this workflow does not know",
            IdracCredentialsRejectedError,
        )

    async def _discover(
        self, idrac: IdracRef, service_tag: str, purpose: str, force: bool = False
    ) -> OmeDevice:
        """The OME device for this service tag, discovering it first if needed.

        `force` runs the discovery even when OME already has the device — that
        is how OME learns root's new password.
        """
        device: OmeDevice = await self._run(find_ome_device, OmeDeviceRef(service_tag=service_tag))
        if device.found and not force:
            return device
        info = workflow.info()
        job_ref: OmeJobRef = await self._run(
            start_ome_discovery,
            OmeDiscoveryRequest(
                idrac=idrac,
                # Deterministic per run and purpose, so a retried activity finds
                # the discovery it already made.
                group_name=f"provision-{service_tag}-{purpose}-{info.run_id[:8]}",
            ),
        )
        self._progress.waiting_on = f"OME discovery job {job_ref.job_id}"
        job, finished = await self._poll(
            lambda: self._run(get_ome_job, job_ref),
            lambda state: state.finished,
            _DISCOVERY_DEADLINE,
            _DISCOVERY_POLL,
        )
        if not finished or not job.succeeded:
            raise _fail(
                f"OME discovery of {idrac.idrac_ip} ({purpose}) "
                + (f"ended {job.status}" if finished else f"did not finish within {_DISCOVERY_DEADLINE}")
                + f" (OME job {job.job_id})",
                OmeDiscoveryFailedError,
            )
        self._progress.waiting_on = None
        device = await self._run(find_ome_device, OmeDeviceRef(service_tag=service_tag))
        if not device.found or device.device_id is None:
            raise _fail(
                f"OME discovery job {job.job_id} completed but OME holds no device with service "
                f"tag {service_tag} — the discovery reached {idrac.idrac_ip} but did not onboard it",
                OmeDiscoveryFailedError,
            )
        return device

    async def _configure_storage(self, target: IdracRef) -> tuple[bool, int]:
        """Take the machine to the required storage layout; (RAID 1 created, drives converted).

        Plan, stage everything, ONE reboot, wait for the jobs, then read the
        storage back and plan again: a converged plan is the only success.
        """
        self._phase("configuring-storage")
        layout: StorageLayout = await self._run(read_storage_layout, target)
        plan = plan_storage(layout)
        if plan.problems:
            raise _fail(
                "Storage cannot be configured without destroying something: " + "; ".join(plan.problems),
                StorageLayoutUnsupportedError,
            )
        if plan.converged:
            return False, 0

        job_ids: list[str] = await self._run(
            stage_storage_config,
            StorageConfigRequest(
                idrac=target,
                boss_controller=plan.boss_controller if plan.create_boss_raid1 else None,
                boss_drives=plan.boss_drives if plan.create_boss_raid1 else [],
                non_raid_drives=plan.non_raid_drives,
            ),
        )
        jobs_ref = IdracJobsRef(idrac=target, job_ids=job_ids)
        # A PERC runs its Non-RAID conversion at once (a real-time job); the
        # reset that applies the BOSS volume must not cut it off mid-apply.
        self._phase("applying-storage", waiting_on=f"real-time iDRAC jobs among {', '.join(job_ids)}")
        running, settled = await self._poll(
            lambda: self._run(get_idrac_jobs, jobs_ref),
            lambda s: all(
                job.state in IDRAC_TERMINAL_JOB_STATES or job.state == IDRAC_AWAITING_RESET_STATE
                for job in s.jobs
            ),
            _STORAGE_DEADLINE,
            _STORAGE_POLL,
        )
        if settled:
            self._phase("applying-storage", waiting_on=f"iDRAC jobs {', '.join(job_ids)} (reboot)")
            await self._run(apply_staged_idrac_jobs, jobs_ref)
        elif any(job.state == IDRAC_AWAITING_RESET_STATE for job in running.jobs):
            # Nothing below can change: the staged job needs the reset, and the
            # reset cannot be issued while another job runs (the limb refuses,
            # or it would cut a real-time conversion off mid-apply). Waiting the
            # second deadline out would only double the time to the same answer.
            raise _fail(
                f"Storage jobs did not settle within {_STORAGE_DEADLINE}, and the reset that would "
                f"apply the staged one cannot be issued while the others run: {_describe_jobs(running)}",
                StorageJobFailedError,
            )
        # Every job is real-time and some are still going: no reset was ever
        # owed, so waiting for them to finish is the right thing to do.
        state, finished = await self._poll(
            lambda: self._run(get_idrac_jobs, jobs_ref),
            lambda s: all(job.state in IDRAC_TERMINAL_JOB_STATES for job in s.jobs),
            _STORAGE_DEADLINE,
            _STORAGE_POLL,
        )
        if not finished or any(job.state != IDRAC_JOB_SUCCESS for job in state.jobs):
            raise _fail(
                "Storage jobs "
                + ("did not all succeed" if finished else f"did not finish within {_STORAGE_DEADLINE}")
                + f": {_describe_jobs(state)}",
                StorageJobFailedError,
            )

        self._phase("verifying-storage")
        after: StoragePlan = plan_storage(await self._run(read_storage_layout, target))
        if not after.converged:
            remaining = after.problems + (["the BOSS still has no RAID 1"] if after.create_boss_raid1 else [])
            if after.non_raid_drives:
                remaining.append(f"{len(after.non_raid_drives)} PERC drive(s) still not Non-RAID")
            raise _fail(
                "Every storage job completed, but the storage read back is not the required "
                "layout: " + "; ".join(remaining),
                StorageNotConvergedError,
            )
        return plan.create_boss_raid1, len(plan.non_raid_drives)

    async def _run(self, activity_fn: Callable[..., Awaitable[T]], arg: Any) -> T:
        """Every activity of this workflow: one queue, one budget, one retry policy."""
        return await workflow.execute_activity(
            activity_fn,
            arg,
            task_queue=SERVER_PROVISIONING_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _poll(
        self,
        check: Callable[[], Awaitable[T]],
        done: Callable[[T], bool],
        deadline: timedelta,
        interval: timedelta,
    ) -> tuple[T, bool]:
        """Run `check` until `done`, on durable timers; (last value, whether done)."""
        started = workflow.now()
        while True:
            value = await check()
            if done(value):
                return value, True
            if workflow.now() - started >= deadline:
                return value, False
            await workflow.sleep(interval)
