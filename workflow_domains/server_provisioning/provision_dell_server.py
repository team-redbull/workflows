"""provision-dell-server — from "the iDRAC has an IP" to "this machine is configured".

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
     auditing-template    — the template must carry no iDRAC network settings,
                            no storage and no user accounts. Checked before the
                            run writes ANYTHING (template_policy.py).
     clearing-os-hostname — blank the factory OS hostname (`Miniwinpc`), or
                            OME shows it instead of the machine's address.
     enforcing-root-password — root goes onto the target password HERE, over
                            Redfish, while OME has never heard of the machine.
  3. discovering-in-ome   — skipped when OME already has the service tag.
                            Always with the target credential.
  4. deploying-template   — the template for (model, iDRAC firmware) from
                            DELL_TEMPLATES. It creates the server profile.
  5. verifying-root-password — nothing in the deployment may have moved root's
                            password; a guard, not the mechanism.
  6. verifying-config     — did the template's BIOS attributes ACTUALLY apply?
                            An SCP import is "continue on error", so a finished
                            deployment proves nothing. Drift is staged over
                            Redfish, never by redeploying the profile.
  7. configuring-storage  — RAID 1 on the BOSS, every PERC drive Non-RAID,
     applying-storage       staged together and applied by ONE reboot — the
     verifying-storage      same reboot that applies any BIOS drift — then read
                            back. Nothing is ever deleted (storage_plan.py).
  8. naming-server        — the naming service renames the OME profile, and
     verifying-name         the name is read back from OME and checked against
                            the convention, region and service tag included.

DONE MEANS CONFIGURED, NOT YET INVENTORIED. The run ends when the machine is
right: root on the enforced password, the template applied, the storage layout
verified and the OME profile named. It does NOT wait for server-scan to list
it — server-scan discovers by itself on a 6-hourly collection, so waiting would
add hours to every run to learn something the run cannot influence, and would
turn a slow or paused collector into a fleet of failed provisions. The early
server-scan read stays: that one is a SAFETY check (step 2), not a completion
one.

THE PASSWORD IS SET BEFORE OME EVER SEES THE MACHINE, AND THAT IS WHY THERE IS
NO REDISCOVERY. OME was once discovered with whatever password the server
arrived on, the template then enforced the target one, and OME had to be
re-pointed at it (`rediscovering-in-ome`) or it would lose the machine. That
repair was wrong in three ways: re-running a discovery over a device that
already carries a profile is not what OME's own documentation asks for (an
onboarding operation is, and OME exposes none over REST); it needed a second
discovery group per run; and the template component that carried the password,
`Users.*`, is the single most firmware-fragile thing a template can hold — Dell
KB 000326070, where iDRAC 7.20.30.00 added required SNMPv3 key attributes and
every older template began failing SYS055. Setting the password over Redfish
first removes all three at once: OME is discovered ONCE, with the password root
keeps, so a stale credential cannot exist, and the template needs no `Users.*`
at all. Reversed 2026-09-30, on the DC team's review. Do not re-add a
rediscovery — narrow the inputs instead (CLAUDE.md §5).

WHY STORAGE COMES AFTER THE TEMPLATE. The template deployment can reboot the
machine and apply BIOS attributes; configuring storage afterwards means the
layout checked in step 6 is the one the server ends up with. It also means
every storage call runs as root on the TARGET password, never a factory one.

EVERY WAIT IS THE WORKFLOW'S, bounded, on durable timers — a discovery job, a
template deployment, a reboot applying RAID. They are all MACHINE convergence
(CLAUDE.md §5): each gets a deadline and fails the run loudly by name when it
passes. No activity sleeps.

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
        ConfigurationDriftError,
        ProfileConflictError,
        RegionMissingError,
        RootPasswordNotSetError,
        ServerNameNotAppliedError,
        ServerNamerRejectedError,
        ServerAlreadyInstalledError,
        ServerScanAuthError,
        StorageJobFailedError,
        StorageLayoutUnsupportedError,
        StorageNotConvergedError,
        TemplateDeployFailedError,
        TemplateNotConfiguredError,
        TemplateNotFoundError,
        TemplatePasswordNotAppliedError,
        TemplateUnsafeError,
    )
    from shared.interfaces.server_provisioning import (
        apply_staged_idrac_jobs,
        check_idrac_login,
        clear_idrac_os_hostname,
        deploy_ome_template,
        find_in_server_scan,
        find_ome_device,
        get_idrac_jobs,
        get_ome_job,
        get_ome_profile,
        probe_idrac_credentials,
        read_idrac_identity,
        read_ome_template,
        read_storage_layout,
        stage_bios_attributes,
        verify_bios_configuration,
        request_server_name,
        set_idrac_root_password,
        stage_storage_config,
        start_ome_discovery,
    )
    from shared.models.server_provisioning import (
        IDRAC_AWAITING_RESET_STATE,
        IDRAC_JOB_SUCCESS,
        IDRAC_TERMINAL_JOB_STATES,
        TARGET_CREDENTIAL,
        BiosStageRequest,
        BiosVerifyRequest,
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
        OmeTemplateRef,
        ProvisionDellServerProgress,
        ProvisionDellServerResult,
        ProvisionDellServerRunArgs,
        ServerNameRequest,
        ServerScanLookup,
        StorageConfigRequest,
        StorageLayout,
        TemplateAttribute,
        TemplateDeployRequest,
    )
    from workflow_domains.server_provisioning.template_policy import (
        audit_template,
        bios_drift,
        bios_intent,
        describe_drift,
        describe_hazards,
        unverifiable,
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
        # How much of the template this run could check, and how much of it the
        # deployment silently failed to apply. Reported in the result because
        # it is the evidence for whether one template can serve many firmware
        # levels (docs/design/dell-scp-template-r660.md).
        self._bios_checked = 0
        self._bios_remediated = 0
        self._bios_unverified = 0

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

        # Step 2b2 — the template must be safe to deploy, checked BEFORE the run
        # writes anything at all. The worst thing a template can carry is the
        # reference server's iDRAC network configuration: deploying it moves
        # this machine's address, or resets it to DHCP, and nothing remote can
        # get it back. Failing here costs nothing — not a password change, not a
        # hostname, not an OME device.
        self._phase("auditing-template")
        template = await self._run(
            read_ome_template,
            OmeTemplateRef(model=identity.model, idrac_firmware=identity.idrac_firmware),
        )
        self._progress.template_name = template.template_name
        hazards = audit_template(template.attributes)
        if hazards:
            raise _fail(
                f"Template {template.template_name!r} carries {len(hazards)} attribute(s) this "
                f"workflow refuses to deploy — {describe_hazards(hazards)}",
                TemplateUnsafeError,
            )

        # Step 2c — blank the factory OS hostname. AFTER the in-use guard above,
        # because this writes to the machine: `Miniwinpc` is stale factory data
        # on a server being provisioned, but on a server a cluster is running it
        # would be the node's real name. Before OME sees it, because while a
        # hostname is set OME shows it beside the profile instead of the address.
        self._phase("clearing-os-hostname")
        self._progress.os_hostname_cleared = await self._run(clear_idrac_os_hostname, initial)

        # Step 2d — root goes onto the target password BEFORE OME sees the
        # machine, so OME is only ever handed the password root keeps. See the
        # module docstring: this is what makes rediscovery unnecessary.
        target = IdracRef(idrac_ip=idrac_ip, credential=TARGET_CREDENTIAL)
        if initial_credential != TARGET_CREDENTIAL:
            self._phase("enforcing-root-password")
            await self._run(set_idrac_root_password, initial)
            _, on_target = await self._poll(
                lambda: self._run(check_idrac_login, target),
                lambda ok: ok,
                _PASSWORD_DEADLINE,
                _PASSWORD_POLL,
            )
            if not on_target:
                raise _fail(
                    f"The iDRAC at {idrac_ip} accepted the change of root's password but root "
                    f"still does not accept the target one {_PASSWORD_DEADLINE} later — refusing "
                    "to discover it in OME with a credential that will not keep working",
                    RootPasswordNotSetError,
                )

        # Step 3 — into OME, unless it is already there. Always with the TARGET
        # credential: root is on it by now, whatever the machine arrived with.
        self._phase("discovering-in-ome")
        device = await self._discover(target, identity.service_tag, purpose="discover")
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

        # Step 5 — the deployment must not have moved root's password. The
        # template is audited to carry no Users.* component, so this should
        # never fire; it stays because it costs one login and the state it
        # catches — a machine OME can no longer reach — is expensive to undo.
        self._phase("verifying-root-password")
        _, accepted = await self._poll(
            lambda: self._run(check_idrac_login, target),
            lambda ok: ok,
            _PASSWORD_DEADLINE,
            _PASSWORD_POLL,
        )
        if not accepted:
            raise _fail(
                f"After template {deploy.template_name!r} deployed, root on {idrac_ip} no longer "
                "accepts the target password — the template carries a Users.* component that "
                "changed it, which OME's own credential for this machine will not survive",
                TemplatePasswordNotAppliedError,
            )

        # Steps 6 and 7 — storage and configuration drift, applied by ONE reboot.
        #
        # Storage is STAGED FIRST, deliberately. A controller that cannot take
        # the layout fails at staging, and a run that dies there must not leave
        # a pending BIOS job behind for some unrelated reset to apply weeks
        # later. So nothing else is staged until the storage jobs exist.
        plan, storage_jobs = await self._stage_storage(target)

        # Then: did the template's BIOS attributes ACTUALLY apply? A finished
        # deployment proves nothing — SCP import is "continue on error", so an
        # attribute this firmware does not know fails while the rest succeed.
        # Drift is staged as its own job, never by redeploying the profile,
        # which would re-run everything that already worked.
        bios_job = await self._verify_configuration(target, template.attributes)

        boss_created, converted = await self._apply_storage(
            target, plan, storage_jobs + ([bios_job] if bios_job else [])
        )

        # The reboot has run, so the drift must be gone. Anything left is a
        # machine that will not take the setting at all.
        if bios_job:
            await self._verify_configuration(target, template.attributes, remediating=False)

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

        # DONE. The machine is configured: root on the enforced password, the
        # template applied, the storage layout verified, the profile named.
        # server-scan's own collector picks it up on its next pass, and the run
        # deliberately does NOT wait for that — see the module docstring.
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
            os_hostname_cleared=bool(self._progress.os_hostname_cleared),
            bios_attributes_checked=self._bios_checked,
            bios_attributes_remediated=self._bios_remediated,
            bios_attributes_unverified=self._bios_unverified,
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

    async def _discover(self, idrac: IdracRef, service_tag: str, purpose: str) -> OmeDevice:
        """The OME device for this service tag, discovering it first if needed.

        There is deliberately no way to force a re-discovery. Root is already on
        the target password when this runs, so the credential OME is given never
        goes stale — see the module docstring.
        """
        device: OmeDevice = await self._run(find_ome_device, OmeDeviceRef(service_tag=service_tag))
        if device.found:
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

    async def _verify_configuration(
        self, target: IdracRef, attributes: list[TemplateAttribute], remediating: bool = True
    ) -> str | None:
        """Check the template's BIOS attributes really applied; stage the fixes.

        Returns the id of the staged BIOS job when there was drift to fix, so
        the caller can have one reboot apply it alongside the storage jobs.

        On the second pass (`remediating=False`) there is nothing left to try:
        the reboot has run, so remaining drift is a machine that will not take
        the setting, and the run fails by name rather than looping.
        """
        self._phase("verifying-config")
        intended = bios_intent(attributes)
        if not intended:
            return None
        verification = await self._run(
            verify_bios_configuration, BiosVerifyRequest(idrac=target, intended=intended)
        )
        drifted = bios_drift(verification.compared)
        unchecked = unverifiable(verification.compared)
        self._progress.bios_attributes_checked = len(verification.compared)
        self._progress.bios_attributes_drifted = len(drifted)
        self._bios_checked = len(verification.compared)
        self._bios_unverified = len(unchecked)
        if remediating:
            self._bios_remediated = len(drifted)

        if unchecked:
            # Not a failure: this is a template naming an attribute the target's
            # firmware does not have, which is Dell's documented consequence of
            # a version difference. Loud in the log, and counted in the result,
            # because it is the evidence for whether a golden template is safe.
            workflow.logger.warning(
                "%d template BIOS attribute(s) are not in %s's registry and were not verified: %s",
                len(unchecked),
                target.idrac_ip,
                ", ".join(a.display_name for a in unchecked),
            )
        if not drifted:
            return None
        if not remediating:
            raise _fail(
                f"The template deployed and the reboot ran, but {len(drifted)} BIOS attribute(s) "
                f"still do not match it: {describe_drift(drifted)}",
                ConfigurationDriftError,
            )
        workflow.logger.info(
            "Template %s reported success but %d BIOS attribute(s) did not apply; staging them "
            "over Redfish: %s",
            self._progress.template_name,
            len(drifted),
            describe_drift(drifted),
        )
        return await self._run(
            stage_bios_attributes,
            BiosStageRequest(
                idrac=target,
                attributes={str(a.attribute_name): str(a.intended) for a in drifted},
            ),
        )

    async def _stage_storage(self, target: IdracRef) -> tuple[StoragePlan, list[str]]:
        """Plan the storage and stage it; the jobs waiting for the reset.

        Staging happens BEFORE anything else is staged, and that ordering is
        load-bearing: a controller that cannot take the layout fails HERE
        (`stage_storage` checks SupportedRAIDTypes before it posts), and a run
        that dies then must not leave some other pending job behind to be
        applied by an unrelated reset weeks later.
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
            return plan, []
        return plan, await self._run(
            stage_storage_config,
            StorageConfigRequest(
                idrac=target,
                boss_controller=plan.boss_controller if plan.create_boss_raid1 else None,
                boss_drives=plan.boss_drives if plan.create_boss_raid1 else [],
                non_raid_drives=plan.non_raid_drives,
            ),
        )

    async def _apply_storage(
        self, target: IdracRef, plan: StoragePlan, job_ids: list[str]
    ) -> tuple[bool, int]:
        """ONE reboot for every staged job, then read the storage back.

        `job_ids` is the storage jobs plus anything else staged for the same
        reset — today the BIOS drift job. They are carried through the whole
        wait together, so one reboot applies everything and one poll watches it.
        A machine whose storage was already right still reboots when something
        else is staged: the job is on the iDRAC either way, and leaving it
        pending would apply it at some unrelated later reset.
        """
        if not job_ids:
            return False, 0
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
