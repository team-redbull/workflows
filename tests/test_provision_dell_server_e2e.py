"""provision-dell-server END TO END: the real workflow, the REAL activities and their
real httpx calls, against tests/dell_simulator.py served over HTTPS.

Nothing between the workflow and the wire is mocked: the activities read their
settings, open their own clients, speak Redfish to an "iDRAC" on 127.0.0.2:443
and REST to an "OME", a naming service and server-scan. The simulator is built
from Dell's own client code (see its docstring), so what this proves is that
the limb and the workflow agree with each other AND with the shapes and rules
Dell's code relies on. What it cannot prove is the one thing only real hardware
can: that an actual OME and iDRAC behave as Dell's code expects.

Needs two things a laptop or CI runner usually has: permission to bind 443 on
127.0.0.2 (the limb addresses an iDRAC by bare IP, so port 443 is not
negotiable) and an `openssl` binary for the throwaway certificate. Without
either, the module is skipped rather than failed.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass

import pytest
import uvicorn
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import activities.server_provisioning.activities as limb
from shared.consts import (
    PROVISION_DELL_SERVER_WORKFLOW_QUEUE,
    SERVER_PROVISIONING_ACTIVITY_QUEUE,
)
from shared.models.server_provisioning import (
    ProvisionDellServerInput,
    ProvisionDellServerResult,
    ProvisionDellServerRunArgs,
)
from tests import dell_simulator as sim
from workflow_domains.server_provisioning.provision_dell_server import (
    ProvisionDellServerWorkflow,
)

IDRAC_IP = "127.0.0.2"
REGION = "israel"
TARGET = "Target-Pw1"
FACTORY = ["calvin", "Factory-Pw2"]

LIMB_ACTIVITIES = [
    limb.probe_idrac_credentials,
    limb.check_idrac_login,
    limb.read_idrac_identity,
    limb.clear_idrac_os_hostname,
    limb.set_idrac_root_password,
    limb.read_storage_layout,
    limb.stage_storage_config,
    limb.apply_staged_idrac_jobs,
    limb.get_idrac_jobs,
    limb.find_ome_device,
    limb.start_ome_discovery,
    limb.get_ome_job,
    limb.deploy_ome_template,
    limb.read_ome_template,
    limb.get_ome_profile,
    limb.request_server_name,
    limb.find_in_server_scan,
]


def _can_bind(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError:
            return False
    return True


pytestmark = pytest.mark.skipif(
    shutil.which("openssl") is None or not _can_bind(IDRAC_IP, 443),
    reason="needs openssl and permission to bind 127.0.0.2:443",
)


@pytest.fixture(scope="module")
def certificate(tmp_path_factory) -> tuple[str, str]:
    directory = tmp_path_factory.mktemp("tls")
    key, cert = directory / "key.pem", directory / "cert.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=dell-simulator", "-keyout", str(key), "-out", str(cert)],
        check=True,
        capture_output=True,
    )
    return str(key), str(cert)


class _Served:
    """One uvicorn server on its own thread, stopped on exit."""

    def __init__(self, app, host: str, port: int, certificate: tuple[str, str]) -> None:
        key, cert = certificate
        config = uvicorn.Config(app, host=host, port=port, ssl_keyfile=key, ssl_certfile=cert, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> _Served:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("simulator did not start")
            time.sleep(0.02)
        return self

    def __exit__(self, *exc_info) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class World:
    idrac: sim.IdracSim
    ome: sim.OmeSim
    scan: sim.ScanSim


@pytest.fixture
def world(certificate, monkeypatch) -> Iterator[World]:
    idrac = sim.IdracSim(root_password="calvin")
    ome = sim.OmeSim(idrac=idrac, idrac_ip=IDRAC_IP, template_root_password=TARGET)
    scan = sim.ScanSim(ome=ome)
    port = _free_port()
    base = f"https://127.0.0.1:{port}"

    # httpx honours HTTPS_PROXY but not CIDR entries in NO_PROXY, so on a host
    # with a proxy configured the simulator must be named explicitly.
    for variable in ("NO_PROXY", "no_proxy"):
        existing = [v for v in (os.environ.get(variable) or "").split(",") if v]
        monkeypatch.setenv(variable, ",".join([*existing, "127.0.0.1", IDRAC_IP]))

    settings = limb._settings
    monkeypatch.setattr(settings, "ome_url", base)
    monkeypatch.setattr(settings, "ome_username", ome.username)
    monkeypatch.setattr(settings, "ome_password", ome.password)
    monkeypatch.setattr(settings, "idrac_username", "root")
    monkeypatch.setattr(settings, "idrac_root_password", TARGET)
    monkeypatch.setattr(settings, "idrac_factory_passwords", FACTORY)
    monkeypatch.setattr(settings, "dell_templates", {idrac.model: {idrac.firmware: ome.template_name}})
    monkeypatch.setattr(settings, "server_namer_url", f"{base}/namer/rename")
    monkeypatch.setattr(settings, "server_scan_url", f"{base}/scan/api/v1")

    with _Served(sim.idrac_app(idrac), IDRAC_IP, 443, certificate), _Served(
        sim.services_app(ome, scan), "127.0.0.1", port, certificate
    ):
        yield World(idrac, ome, scan)


Runner = Callable[[], Awaitable[ProvisionDellServerResult]]


async def _temporal_run() -> ProvisionDellServerResult:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        config = env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        async with Worker(
            client, task_queue=PROVISION_DELL_SERVER_WORKFLOW_QUEUE, workflows=[ProvisionDellServerWorkflow]
        ), Worker(client, task_queue=SERVER_PROVISIONING_ACTIVITY_QUEUE, activities=LIMB_ACTIVITIES):
            return await asyncio.wait_for(
                client.execute_workflow(
                    ProvisionDellServerWorkflow.run,
                    ProvisionDellServerRunArgs(input=ProvisionDellServerInput(idrac_ip=IDRAC_IP, region=REGION)),
                    id=f"e2e-{uuid.uuid4()}",
                    task_queue=PROVISION_DELL_SERVER_WORKFLOW_QUEUE,
                ),
                timeout=120,
            )
    finally:
        await env.shutdown()


# Swappable so the same scenarios can be driven by another runner.
run_workflow: Runner = _temporal_run


async def _run() -> ProvisionDellServerResult:
    return await run_workflow()


async def _failure() -> ApplicationError:
    with pytest.raises((WorkflowFailureError, ApplicationError)) as info:
        await _run()
    error = info.value
    while isinstance(error, (WorkflowFailureError, ActivityError)):
        error = error.cause
    assert isinstance(error, ApplicationError)
    return error


async def test_a_factory_fresh_server_is_provisioned_end_to_end(world: World):
    result = await _run()

    assert result.service_tag == world.idrac.service_tag
    assert result.initial_credential == "factory-1"  # arrived on calvin
    # The factory OS hostname is gone, so OME shows the machine's address next
    # to its profile. Cleared before OME ever discovered it.
    assert world.idrac.os_hostname == "" and result.os_hostname_cleared
    assert result.profile_name == f"ocp-dell-r660-{REGION}-128c-1024gb-10tb-{world.idrac.service_tag}"
    # The run finished on its own configuration work. server-scan has not
    # collected the machine yet (its simulator needs 3 reads), and that is
    # deliberately not something the run waits for.
    assert world.scan.current() is None

    # Root was moved onto the target password over Redfish, BEFORE OME saw the
    # machine — so OME was discovered ONCE, with the password root keeps, and
    # its credential can never go stale. This is what replaced the rediscovery.
    assert world.idrac.root_password == TARGET
    assert world.idrac.password_writes == [TARGET]
    assert world.ome.device_credential == TARGET
    assert len(world.ome.discovery_posts) == 1
    assert world.ome.deploy_calls == 1

    # Storage: one reboot; RAID 1 over the BOSS pair; every PERC drive Non-RAID,
    # the NVMe one (DellPCIeSSD) included; the CPU-attached NVMe untouched.
    assert len(world.idrac.resets) == 1
    [volume] = world.idrac.volumes[sim.BOSS]
    assert volume["RAIDType"] == "RAID1" and len(volume["Links"]["Drives"]) == 2
    perc = [d for d in world.idrac.drives if d.controller == sim.PERC]
    assert [d.status for d in perc] == ["NonRAID"] * 4
    assert result.boss_raid1_created and result.non_raid_drives_converted == 4

    # No OME session left open behind the run.
    assert world.ome.open_sessions == set()


async def test_a_second_run_changes_nothing(world: World):
    await _run()
    resets, deploys, posts = len(world.idrac.resets), world.ome.deploy_calls, len(world.ome.discovery_posts)
    jobs = set(world.idrac.jobs)

    result = await _run()

    assert result.initial_credential == "target"
    assert not result.boss_raid1_created and result.non_raid_drives_converted == 0
    assert (len(world.idrac.resets), world.ome.deploy_calls, len(world.ome.discovery_posts)) == (
        resets,
        deploys,
        posts,
    )
    assert set(world.idrac.jobs) == jobs


async def test_an_unknown_root_password_never_trips_more_than_one_block_per_round(world: World):
    world.idrac.root_password = "nobody-knows"
    error = await _failure()
    assert error.type == "IdracCredentialsRejectedError"
    # Three rounds of at most three candidates — nothing retried behind the probe.
    assert len(world.idrac.login_attempts) <= 9
    assert world.ome.discovery_posts == []


async def test_a_server_in_use_is_never_touched(world: World):
    world.scan.claimed_by = ("INSTALLED", "ocp4-prod")
    error = await _failure()
    assert error.type == "ServerAlreadyInstalledError"
    assert world.ome.discovery_posts == [] and world.idrac.resets == []


async def test_a_boss_without_raid1_fails_before_anything_is_staged(world: World):
    world.idrac.boss_raid_types = ["RAID0"]
    error = await _failure()
    assert error.type == "IdracRequestRejectedError"
    assert world.idrac.jobs == {} and world.idrac.resets == []


async def test_a_template_carrying_the_idracs_own_address_is_refused(world: World):
    """The audit against a real OME AttributeDetails payload.

    A template captured from a reference server carries THAT server's iDRAC
    address. Deploying it would move this machine's address or reset it to
    DHCP, and no remote call could bring it back — so the run refuses before it
    writes anything at all.
    """
    world.ome.template_attribute_groups.append(
        {
            "DisplayName": "iDRAC",
            "SubAttributeGroups": [
                {
                    "DisplayName": "IPv4 Information",
                    "SubAttributeGroups": [],
                    "Attributes": [
                        {"AttributeId": 70, "DisplayName": "Address",
                         "Value": "10.9.9.9", "IsIgnored": False},
                    ],
                }
            ],
        }
    )
    error = await _failure()
    assert error.type == "TemplateUnsafeError"
    # Nothing was written: the machine is still on its factory password, still
    # carries its factory hostname, and OME never saw it.
    assert world.idrac.root_password == "calvin"
    assert world.idrac.password_writes == []
    assert world.idrac.os_hostname == "Miniwinpc"
    assert world.ome.discovery_posts == [] and world.ome.deploy_calls == 0
