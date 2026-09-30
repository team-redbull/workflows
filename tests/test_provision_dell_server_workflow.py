"""Workflow tests: real ProvisionDellServerWorkflow, mock activities, time-skipping env.

Same harness shape as tests/test_install_server_workflow.py. The time-skipping
environment collapses every durable timer — the iDRAC lockout wait, the
discovery / template / storage polls and the hours-long server-scan wait — to
milliseconds, so each deadline can be driven to expiry.

The mocks are one stateful fake per run: each activity reads the scripted
answers the test set up and records that it was called, so a test states the
machine's behaviour and asserts on what the workflow did about it.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    PROVISION_DELL_SERVER_WORKFLOW_QUEUE,
    SERVER_PROVISIONING_ACTIVITY_QUEUE,
)
from shared.exceptions import (
    IdracCredentialsRejectedError,
    IdracUnreachableError,
    NotADellServerError,
    ProfileConflictError,
    RootPasswordNotSetError,
    RegionMissingError,
    ServerAlreadyInstalledError,
    ServerNameNotAppliedError,
    StorageJobFailedError,
    StorageLayoutUnsupportedError,
    TemplatePasswordNotAppliedError,
)
from shared.models.server_provisioning import (
    IdracController,
    IdracDrive,
    IdracIdentity,
    IdracJobsRef,
    IdracJobsState,
    IdracJobState,
    IdracProbeResult,
    IdracRef,
    IdracVolume,
    OmeDevice,
    OmeDeviceRef,
    OmeDiscoveryRequest,
    OmeJobRef,
    OmeJobState,
    OmeProfile,
    OmeProfileRef,
    ProvisionDellServerInput,
    ProvisionDellServerRunArgs,
    RebootResult,
    ServerNameRequest,
    ServerScanLookup,
    ServerScanState,
    StorageConfigRequest,
    StorageLayout,
    TemplateDeployRequest,
    TemplateDeployResult,
)
from workflow_domains.server_provisioning.provision_dell_server import (
    ProvisionDellServerWorkflow,
)

IP = "134.1.2.1"
REGION = "israel"
TAG = "ABC1234"
NAME = f"ocp-dell-r660-{REGION}-128c-1024gb-10tb-{TAG}"
DEVICE_ID = 42
BOSS = "AHCI.SL.6-1"
PERC = "RAID.SL.3-1"


def _controllers(converged: bool) -> StorageLayout:
    boss_drives = [f"/r/Storage/{BOSS}/Drives/Disk.{n}" for n in (0, 1)]
    boss = IdracController(
        odata_id=f"/r/Storage/{BOSS}",
        id=BOSS,
        name="BOSS-N1 Monolithic",
        drives=[IdracDrive(odata_id=d) for d in boss_drives],
        volumes=(
            [IdracVolume(odata_id=f"/r/Storage/{BOSS}/Volumes/V0", raid_type="RAID1", drives=boss_drives)]
            if converged
            else []
        ),
    )
    status = "NonRAID" if converged else "Ready"
    perc = IdracController(
        odata_id=f"/r/Storage/{PERC}",
        id=PERC,
        name="PERC H755 Front",
        drives=[IdracDrive(odata_id=f"/r/Storage/{PERC}/Drives/Disk.{n}", raid_status=status) for n in (0, 1, 2)],
    )
    return StorageLayout(controllers=[boss, perc])


@dataclass
class Fake:
    """The machine, OME, the naming service and server-scan, as one script."""

    probes: list[IdracProbeResult] = field(
        default_factory=lambda: [IdracProbeResult(reachable=True, credential=1, rejected=1)]
    )
    identity: IdracIdentity = field(
        default_factory=lambda: IdracIdentity(
            service_tag=TAG, model="PowerEdge R660", manufacturer="Dell Inc.", idrac_firmware="7.10.70.00"
        )
    )
    claimed_by: str | None = None
    os_hostname_was_set: bool = True
    in_ome: bool = False
    template_job: int | None = 900
    template_error: Exception | None = None
    target_login: bool = True
    layouts: list[StorageLayout] = field(default_factory=lambda: [_controllers(False), _controllers(True)])
    storage_job_final: str = "Completed"
    # One job staged for the reset, another that never stops running: the reset
    # can never be issued, so the staged one can never apply.
    storage_never_settles: bool = False
    profile_names: list[str] = field(default_factory=lambda: ["Profile from template 00001", NAME])
    calls: list[str] = field(default_factory=list)
    discoveries: list[OmeDiscoveryRequest] = field(default_factory=list)
    staged: list[StorageConfigRequest] = field(default_factory=list)

    def activities(self) -> list:
        fake = self

        @activity.defn(name="probe_idrac_credentials")
        async def probe(idrac_ip: str) -> IdracProbeResult:
            fake.calls.append("probe")
            return fake.probes.pop(0) if len(fake.probes) > 1 else fake.probes[0]

        @activity.defn(name="check_idrac_login")
        async def check_login(ref: IdracRef) -> bool:
            fake.calls.append(f"login:{ref.credential}")
            return fake.target_login

        @activity.defn(name="read_idrac_identity")
        async def identity(ref: IdracRef) -> IdracIdentity:
            fake.calls.append("identity")
            return fake.identity

        @activity.defn(name="set_idrac_root_password")
        async def set_password(ref: IdracRef) -> bool:
            fake.calls.append(f"set-password:from-{ref.credential}")
            return True

        @activity.defn(name="clear_idrac_os_hostname")
        async def clear_hostname(ref: IdracRef) -> bool:
            fake.calls.append("clear-hostname")
            return fake.os_hostname_was_set

        @activity.defn(name="read_storage_layout")
        async def layout(ref: IdracRef) -> StorageLayout:
            fake.calls.append(f"layout:{ref.credential}")
            return fake.layouts.pop(0) if len(fake.layouts) > 1 else fake.layouts[0]

        @activity.defn(name="stage_storage_config")
        async def stage(request: StorageConfigRequest) -> list[str]:
            fake.calls.append("stage")
            fake.staged.append(request)
            return ["JID_1", "JID_2"]

        @activity.defn(name="apply_staged_idrac_jobs")
        async def apply(ref: IdracJobsRef) -> RebootResult:
            fake.calls.append("reboot")
            return RebootResult(rebooted=True, reset_type="ForceRestart")

        jobs_polls = {"n": 0}

        @activity.defn(name="get_idrac_jobs")
        async def jobs(ref: IdracJobsRef) -> IdracJobsState:
            jobs_polls["n"] += 1
            if fake.storage_never_settles:
                return IdracJobsState(
                    jobs=[
                        IdracJobState(job_id=j, state="Scheduled" if i == 0 else "Running")
                        for i, j in enumerate(ref.job_ids)
                    ]
                )
            state = "Running" if jobs_polls["n"] < 3 else fake.storage_job_final
            return IdracJobsState(jobs=[IdracJobState(job_id=j, state=state) for j in ref.job_ids])

        @activity.defn(name="find_ome_device")
        async def find_device(ref: OmeDeviceRef) -> OmeDevice:
            fake.calls.append("find_device")
            if fake.in_ome:
                return OmeDevice(found=True, device_id=DEVICE_ID)
            return OmeDevice(found=False)

        @activity.defn(name="start_ome_discovery")
        async def discover(request: OmeDiscoveryRequest) -> OmeJobRef:
            fake.calls.append(f"discover:{request.idrac.credential}")
            fake.discoveries.append(request)
            fake.in_ome = True
            return OmeJobRef(job_id=100 + len(fake.discoveries))

        ome_polls: dict[int, int] = {}

        @activity.defn(name="get_ome_job")
        async def ome_job(ref: OmeJobRef) -> OmeJobState:
            ome_polls[ref.job_id] = ome_polls.get(ref.job_id, 0) + 1
            done = ome_polls[ref.job_id] >= 2
            return OmeJobState(
                job_id=ref.job_id,
                status_id=2060 if done else 2050,
                status="Completed" if done else "Running",
                finished=done,
                succeeded=done,
            )

        @activity.defn(name="deploy_ome_template")
        async def deploy(request: TemplateDeployRequest) -> TemplateDeployResult:
            fake.calls.append("deploy")
            if fake.template_error is not None:
                raise fake.template_error
            return TemplateDeployResult(template_name="ocp-r660", template_id=7, job_id=fake.template_job)

        @activity.defn(name="get_ome_profile")
        async def profile(ref: OmeProfileRef) -> OmeProfile:
            name = fake.profile_names.pop(0) if len(fake.profile_names) > 1 else fake.profile_names[0]
            return OmeProfile(found=True, profile_id=5, profile_name=name, template_name="ocp-r660")

        @activity.defn(name="request_server_name")
        async def rename(request: ServerNameRequest) -> None:
            fake.calls.append(f"rename:{request.region}")

        @activity.defn(name="find_in_server_scan")
        async def scan(lookup: ServerScanLookup) -> ServerScanState:
            fake.calls.append("guard")
            return ServerScanState(claimed_by=fake.claimed_by, name="old-name")

        return [probe, check_login, identity, set_password, clear_hostname, layout, stage,
                apply, jobs, find_device, discover, ome_job, deploy, profile, rename, scan]


class _Harness:
    def __init__(self, fake: Fake) -> None:
        self._fake = fake

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=PROVISION_DELL_SERVER_WORKFLOW_QUEUE, workflows=[ProvisionDellServerWorkflow]),
            Worker(client, task_queue=SERVER_PROVISIONING_ACTIVITY_QUEUE, activities=self._fake.activities()),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _run(fake: Fake, region: str | None = REGION):
    async with _Harness(fake) as client:
        return await asyncio.wait_for(
            client.execute_workflow(
                ProvisionDellServerWorkflow.run,
                ProvisionDellServerRunArgs(input=ProvisionDellServerInput(idrac_ip=IP, region=region)),
                id=f"test-{uuid.uuid4()}",
                task_queue=PROVISION_DELL_SERVER_WORKFLOW_QUEUE,
            ),
            timeout=60,  # real seconds — a misclassified error would retry forever
        )


async def _failure(fake: Fake, region: str | None = REGION) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as info:
        await _run(fake, region)
    cause = info.value.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


async def test_happy_path_from_factory_password_to_a_configured_machine():
    fake = Fake()
    result = await _run(fake)

    assert result.service_tag == TAG
    assert result.profile_name == NAME
    assert result.initial_credential == "factory-1"
    assert result.boss_raid1_created and result.non_raid_drives_converted == 3
    # server-scan is read ONCE, as the in-use guard — never again to decide the
    # run is done.
    assert fake.calls.count("guard") == 1
    # Root was moved onto the target password BEFORE OME was told anything, so
    # OME is discovered exactly ONCE and with the credential it keeps. The
    # rediscovery this replaced would have shown up here as a second entry.
    assert fake.calls.index("set-password:from-1") < fake.calls.index("discover:0")
    assert [d.idrac.credential for d in fake.discoveries] == [0]
    # The guard ran before anything touched the machine — including the
    # password change and the hostname blanking, which both write to it.
    assert fake.calls.index("guard") < fake.calls.index("set-password:from-1")
    assert fake.calls.index("guard") < fake.calls.index("clear-hostname")
    # Storage ran as root on the TARGET password, after the template.
    assert fake.calls.index("deploy") < fake.calls.index("layout:0")
    assert fake.calls.count("reboot") == 1
    assert fake.staged[0].boss_controller == BOSS and len(fake.staged[0].non_raid_drives) == 3
    assert "rename:israel" in fake.calls


async def test_a_machine_already_on_the_target_password_is_not_rediscovered():
    fake = Fake(probes=[IdracProbeResult(reachable=True, credential=0)])
    result = await _run(fake)
    assert result.initial_credential == "target"
    assert len(fake.discoveries) == 1


async def test_a_machine_already_in_ome_is_not_discovered_again():
    fake = Fake(probes=[IdracProbeResult(reachable=True, credential=0)], in_ome=True)
    await _run(fake)
    assert fake.discoveries == []


async def test_converged_storage_is_left_alone():
    fake = Fake(layouts=[_controllers(True)])
    result = await _run(fake)
    assert not result.boss_raid1_created and result.non_raid_drives_converted == 0
    assert "stage" not in fake.calls and "reboot" not in fake.calls


async def test_no_region_fails_before_touching_anything():
    fake = Fake()
    error = await _failure(fake, region=None)
    assert error.type == RegionMissingError.__name__
    assert fake.calls == []


async def test_rejections_wait_out_the_lockout_then_fail_after_three_rounds():
    fake = Fake(probes=[IdracProbeResult(reachable=True, credential=None, rejected=3)])
    error = await _failure(fake)
    assert error.type == IdracCredentialsRejectedError.__name__
    assert fake.calls.count("probe") == 3


async def test_a_rejected_round_then_success_carries_on():
    fake = Fake(
        probes=[
            IdracProbeResult(reachable=True, credential=None, rejected=3),
            IdracProbeResult(reachable=True, credential=2, rejected=2),
        ]
    )
    assert (await _run(fake)).initial_credential == "factory-2"


async def test_an_idrac_that_never_answers_fails_on_the_deadline():
    fake = Fake(probes=[IdracProbeResult(reachable=False, detail="ConnectError")])
    error = await _failure(fake)
    assert error.type == IdracUnreachableError.__name__


async def test_not_a_dell_is_refused():
    fake = Fake(
        identity=IdracIdentity(service_tag=TAG, model="ProLiant DL360", manufacturer="HPE", idrac_firmware="x")
    )
    error = await _failure(fake)
    assert error.type == NotADellServerError.__name__


async def test_a_server_in_use_is_never_touched():
    fake = Fake(claimed_by="INSTALLED ocp4-prod")
    error = await _failure(fake)
    assert error.type == ServerAlreadyInstalledError.__name__
    assert not any(c.startswith("discover") for c in fake.calls)
    assert "deploy" not in fake.calls


async def test_a_profile_conflict_fails_the_run_without_retrying():
    fake = Fake(template_error=ProfileConflictError("hand-made profile"))
    error = await _failure(fake)
    assert error.type == ProfileConflictError.__name__
    assert fake.calls.count("deploy") == 1


async def test_a_password_change_that_does_not_take_stops_before_ome_is_told():
    """The safety property of setting the password first.

    OME must only ever be handed a credential that will keep working, so a
    change that did not take has to stop the run BEFORE discovery — otherwise
    OME is onboarded against a password the machine does not have, which is
    exactly the stranding the rediscovery used to paper over.
    """
    fake = Fake(target_login=False)
    error = await _failure(fake)
    assert error.type == RootPasswordNotSetError.__name__
    assert fake.discoveries == [] and "deploy" not in fake.calls


async def test_a_template_that_moves_the_password_is_still_caught():
    """A machine already on the target password skips the change, so the only
    login check left is the one after the template — which is there to catch a
    template that carries a Users.* component and moves the password itself."""
    fake = Fake(
        probes=[IdracProbeResult(reachable=True, credential=0, rejected=0)],
        target_login=False,
    )
    error = await _failure(fake)
    assert error.type == TemplatePasswordNotAppliedError.__name__
    assert "set-password:from-0" not in fake.calls
    assert fake.calls.count("deploy") == 1


async def test_storage_that_would_destroy_data_is_refused_before_staging():
    boss_only = StorageLayout(controllers=[_controllers(False).controllers[1]])  # no BOSS
    fake = Fake(layouts=[boss_only])
    error = await _failure(fake)
    assert error.type == StorageLayoutUnsupportedError.__name__
    assert "stage" not in fake.calls


async def test_a_failed_storage_job_fails_the_run():
    fake = Fake(storage_job_final="Failed")
    error = await _failure(fake)
    assert error.type == StorageJobFailedError.__name__


async def test_a_staged_job_the_reset_can_never_reach_fails_at_the_first_deadline():
    """A job stuck running blocks the reset the staged one is waiting for.

    Both waits are _STORAGE_DEADLINE long, so falling through to the second
    would spend it over again to reach the same answer — and the staged job
    could not have moved, because nothing would have rebooted the machine.
    """
    fake = Fake(storage_never_settles=True)
    error = await _failure(fake)
    assert error.type == StorageJobFailedError.__name__
    assert "cannot be issued while the others run" in error.message
    assert "reboot" not in fake.calls


async def test_a_name_that_never_matches_fails_the_run():
    fake = Fake(profile_names=["ocp-dell-r660-region1-128c-1024gb-10tb-ABC1234"])  # wrong region
    error = await _failure(fake)
    assert error.type == ServerNameNotAppliedError.__name__

