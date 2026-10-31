"""The temporary server-lifecycle page (workflow_domains/server_lifecycle/ui.py) and its demo data.

demo-runs.json is what GET /workflows/runs/{id} — the real runs router — answered
for runs of the real workflows, driven by each workflow's own test harness
(tests/test_{install,remove,uninstall}_server_workflow.py and
test_publish_infraenv_boot_workflow.py), with the fixtures' names swapped for
demo ones: the repo is public. This test checks it still decodes as today's
models, so the page cannot drift from the API. `UPDATE_UI_DEMO=1` regenerates it.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle

import tests.test_install_server_workflow as install_tests
import tests.test_publish_infraenv_boot_workflow as publish_tests
import tests.test_remove_server_workflow as remove_tests
import tests.test_uninstall_server_workflow as uninstall_tests
from shared.consts import (
    INSTALL_SERVER_WORKFLOW_QUEUE,
    PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
    REMOVE_SERVER_WORKFLOW_QUEUE,
    UNINSTALL_SERVER_WORKFLOW_QUEUE,
)
from shared.models.server_lifecycle import (
    AcquiredServer,
    AgentBinding,
    BmcEndpoint,
    InstallServerInput,
    InstallServerProgress,
    InstallServerResult,
    InstallServerRunArgs,
    InventoryHostState,
    LinkState,
    PublishInfraEnvBootProgress,
    PublishInfraEnvBootResult,
    PublishInfraEnvBootRunArgs,
    RemoveServerInput,
    RemoveServerProgress,
    RemoveServerResult,
    RemoveServerRunArgs,
    ReserveServerRequest,
    ServerInterface,
    ServerReservation,
    UninstallServerProgress,
    UninstallServerResult,
    UninstallServerRunArgs,
)
from shared.workflow_ids import (
    install_server_name_workflow_id,
    install_server_slot_workflow_id,
    publish_infraenv_boot_workflow_id,
    remove_server_workflow_id,
    uninstall_server_workflow_id,
)
from workflow_domains.cluster_upgrade import ui as cluster_upgrade_ui
from workflow_domains.routers import runs as runs_router
from workflow_domains.server_lifecycle import install_server, ui
from workflow_domains.server_lifecycle.install_server import InstallServerWorkflow
from workflow_domains.server_lifecycle.publish_infraenv_boot import PublishInfraEnvBootWorkflow
from workflow_domains.server_lifecycle.remove_server import RemoveServerWorkflow
from workflow_domains.server_lifecycle.router import InstallServerRequest, RemoveServerRequest
from workflow_domains.server_lifecycle.uninstall_server import UninstallServerWorkflow

PAGE = ui.UI_DIR / "index.html"
DEMO = ui.UI_DIR / "demo-runs.json"

# The page's tabs: each workflow with the shapes its status carries.
WORKFLOWS = {
    workflow.__name__: (progress, result)
    for workflow, progress, result in (
        (InstallServerWorkflow, InstallServerProgress, InstallServerResult),
        (RemoveServerWorkflow, RemoveServerProgress, RemoveServerResult),
        (UninstallServerWorkflow, UninstallServerProgress, UninstallServerResult),
        (PublishInfraEnvBootWorkflow, PublishInfraEnvBootProgress, PublishInfraEnvBootResult),
    )
}

# What the fixtures carry that must never reach the public demo data.
_FIXTURE_NAMES = ("bat-yam", "tlv", "10.11.", "hc-01", "FCH0", "fch0", install_tests.MCE_CLUSTER)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(ui.router)
    return TestClient(app)


def test_the_page_is_served_whole_with_nothing_from_outside(client):
    response = client.get("/ui/server-lifecycle")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")
    page = response.text
    assert "<title>Server lifecycle</title>" in page
    # Air-gapped: no script, stylesheet or font from another host.
    assert "http://" not in page and "https://" not in page
    # It calls the API it is served by, on the routes the routers really have.
    for path in ("/workflows/runs", "/workflows/server-lifecycle/install-server",
                 "/workflows/server-lifecycle/remove-server", "/ui/server-lifecycle/demo-runs.json"):
        assert path in page
    # It lists each workflow by its real class name (the runs list's `workflow_type`).
    for workflow_type in WORKFLOWS:
        assert f'type: "{workflow_type}"' in page


def test_each_temporary_page_links_to_the_other():
    assert 'href="/ui/cluster-upgrade"' in PAGE.read_text()
    assert 'href="/ui/server-lifecycle"' in (cluster_upgrade_ui.UI_DIR / "index.html").read_text()


def test_the_start_forms_send_only_the_fields_offered_today():
    """Exactly these fields, each one the route's request model has.

    Pinned rather than derived: a field added to a request model later is not
    offered here until someone decides it should be.
    """
    sent: dict[str, set[str]] = {}
    for route, field in re.findall(r'data-send="([a-z-]+):([a-z_]+)"', PAGE.read_text()):
        sent.setdefault(route, set()).add(field)
    assert sent == {
        "install-server": {"infra_env", "mce_cluster", "count", "server_names"},
        "remove-server": {"server_name", "mce_cluster"},
    }
    assert sent["install-server"] <= set(InstallServerRequest.model_fields)
    assert sent["remove-server"] <= set(RemoveServerRequest.model_fields)


def test_the_skip_reasons_are_install_servers_own():
    """The page reads a failed install's candidates by these sentences: they must be the workflow's."""
    block = re.search(r'<script type="application/json" id="skip-reasons">(.*?)</script>', PAGE.read_text(), re.S)
    assert block is not None
    reasons = json.loads(block.group(1))
    assert {key: reason["summary"] for key, reason in reasons.items()} == install_server._REJECTION_SUMMARY
    assert all(reason["label"] for reason in reasons.values())


def test_the_demo_data_is_served_and_decodes_as_the_api_returns_it(client):
    response = client.get("/ui/server-lifecycle/demo-runs.json")
    assert response.status_code == 200
    demo = response.json()
    assert set(demo["lists"]) == set(WORKFLOWS)
    listed = [item["workflow_id"] for listing in demo["lists"].values() for item in listing]
    assert sorted(listed) == sorted(demo["runs"])

    statuses: dict[str, set[str]] = {}
    for workflow_type, listing in demo["lists"].items():
        progress_model, result_model = WORKFLOWS[workflow_type]
        for item in listing:
            summary = runs_router.WorkflowRunSummary.model_validate(item)
            status = runs_router.WorkflowRunStatusResponse.model_validate(demo["runs"][summary.workflow_id])
            assert status.workflow_type == summary.workflow_type == workflow_type
            assert status.status == summary.status
            if status.progress is not None:
                progress_model.model_validate(status.progress)
            if status.status == "COMPLETED":
                result_model.model_validate(status.result)
            else:
                assert status.progress is not None
                assert (status.error is not None) == (status.status == "FAILED")
            statuses.setdefault(workflow_type, set()).add(status.status)
    # The two with a start form show a run in flight, one done and one failed.
    for workflow_type in ("InstallServerWorkflow", "RemoveServerWorkflow"):
        assert statuses[workflow_type] == {"RUNNING", "COMPLETED", "FAILED"}
    # One install failed on all its candidates, which its error names under the workflow's own reasons.
    errors = [run["error"] for run in demo["runs"].values()
              if run["workflow_type"] == "InstallServerWorkflow" and run.get("error")]
    assert any(sum(reason in error for reason in install_server._REJECTION_SUMMARY.values()) >= 3 for error in errors)
    # Demo names only: nothing of the fixtures' production-looking names is left.
    text = DEMO.read_text()
    for name in _FIXTURE_NAMES:
        assert name not in text


# --- regenerating demo-runs.json (UPDATE_UI_DEMO=1) -------------------------------------
#
# Every harness runs on the same fixture MCE, renamed afterwards; the servers
# install-server draws are built here with demo names, addresses and MACs.

FIXTURE_MCE = install_tests.MCE_CLUSTER
DEMO_MCE = "ocp4-demo-mce-site1-a"
DELL_POOL = "dell-r650-site1-64c-1024gb"
CISCO_POOL = "cisco-m6-site1-64c-512gb"
NAMESPACE = "inventory"

_RENAMES = (
    (FIXTURE_MCE, DEMO_MCE),
    (FIXTURE_MCE.removeprefix("ocp4-"), DEMO_MCE.removeprefix("ocp4-")),
    ("ocp4-mce-other", "ocp4-demo-mce-site2-b"),
    (install_tests.INFRA_ENV, CISCO_POOL),
    ("FCH0001", "DEMO0111"), ("fch0001", "demo0111"),
    ("FCH0002", "DEMO0112"), ("fch0002", "demo0112"),
    ("clusters-hc-01", "clusters-demo-shop"), ("hc-01", "demo-shop"),
    ("bat-yam", "site1"),
)


def _renamed(value: Any) -> Any:
    text = json.dumps(value)
    for old, new in _RENAMES:
        text = text.replace(old, new)
    return json.loads(text)


def _server(pool: str, n: int) -> str:
    return f"ocp-{pool}-DEMO{n:04d}"


def _dell(n: int) -> AcquiredServer:
    server = install_tests.bondable_server(f"srv_demo{n:04d}", name=_server(DELL_POOL, n),
                                           mac_prefix=f"02:00:00:00:{n:02x}")
    return server.model_copy(update={
        "bmc": BmcEndpoint(host=f"192.0.2.{n}", host_is_ip=True, scheme="redfish",
                           path="/redfish/v1/Systems/System.Embedded.1"),
        "site_id": "site1",
    })


def _dell_npar(n: int) -> AcquiredServer:
    """Its only two link-up interfaces are partitions of ONE physical port: no bond."""
    mac = f"02:00:00:00:{n:02x}"
    return _dell(n).model_copy(update={"interfaces": [
        ServerInterface(name="NIC.Slot.2-1-1", mac=f"{mac}:01", location="2/1/1", link_state=LinkState.UP),
        ServerInterface(name="NIC.Slot.2-1-2", mac=f"{mac}:02", location="2/1/2", link_state=LinkState.UP),
    ]})


def _blade(n: int) -> AcquiredServer:
    """A UCS blade, driven over IPMI: it boots from the network, through the site's PXE VM."""
    server = install_tests.ucs_server(f"srv_demo{n:04d}", name=_server(CISCO_POOL, n),
                                      mac_prefix=f"02:00:00:01:{n % 256:02x}")
    return server.model_copy(update={"bmc": BmcEndpoint(host=f"192.0.2.{n}", host_is_ip=True), "site_id": "site1"})


def _timestamped_locks(server_activities: list) -> list:
    """The mock lock answers `in <ttl>s`; server-scan answers a time, so the demo shows one."""
    mock = next(a for a in server_activities if a.__name__ == "reserve_server")

    @activity.defn(name="reserve_server")
    async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
        held = await mock(request)
        expires = activity.info().started_time + timedelta(seconds=request.ttl_seconds)
        return held.model_copy(update={"expires_at": expires.isoformat(timespec="seconds")})

    return [reserve_server if a is mock else a for a in server_activities]


async def _status(client: Client, workflow_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """The run as the list and the status routes answer for it — the status through the real router."""
    app = FastAPI()
    app.include_router(runs_router.router)
    app.dependency_overrides[runs_router.get_temporal_client] = lambda: client
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://demo") as http:
        response = await http.get(f"/workflows/runs/{workflow_id}")
    assert response.status_code == 200, response.text
    described = await client.get_workflow_handle(workflow_id).describe()
    summary = runs_router.WorkflowRunSummary(
        workflow_id=described.id,
        workflow_type=described.workflow_type,
        status=described.status.name if described.status else "UNKNOWN",
        start_time=described.start_time,
        close_time=described.close_time,
    )
    return summary.model_dump(mode="json"), response.json()


async def _ended(handle: WorkflowHandle) -> None:
    """Waits for the run's end; awaiting the result is what lets the test server skip time.

    On the handle start_workflow returned: it carries the run id, as execute_workflow's does.
    """
    try:
        await asyncio.wait_for(handle.result(), timeout=60)
    except WorkflowFailureError:
        pass


async def _until(client: Client, workflow_id: str, reached: Callable[[dict[str, Any]], bool]) -> None:
    """Polls the progress query until the run is where the scenario holds it (no time is skipped)."""
    handle = client.get_workflow_handle(workflow_id)
    progress: Any = None
    for _ in range(400):
        try:
            progress = await handle.query("progress")
        except Exception:  # noqa: BLE001 — not started yet
            progress = None
        if progress and reached(progress):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{workflow_id} never got there: {progress}")


def _install(pool: str, server_name: str | None = None) -> InstallServerRunArgs:
    return InstallServerRunArgs(input=InstallServerInput(
        infra_env=pool, mce_cluster=FIXTURE_MCE, namespace=NAMESPACE, server_name=server_name))


async def _run_install(workflow_id: str, args: InstallServerRunArgs, *, hold: str | None = None, **mocks):
    _, segment_acts, server_acts = install_tests.make_mock_activities(**mocks)
    async with install_tests._Harness(segment_acts, _timestamped_locks(server_acts)) as client:
        handle = await client.start_workflow(InstallServerWorkflow.run, args, id=workflow_id,
                                             task_queue=INSTALL_SERVER_WORKFLOW_QUEUE)
        if hold:
            await _until(client, workflow_id, lambda p: p["phase"] == hold)
        else:
            await _ended(handle)
        return await _status(client, workflow_id)


async def _install_named_without_its_infraenv():
    name = _server(DELL_POOL, 21)
    return await _run_install(install_server_name_workflow_id(FIXTURE_MCE, name), _install(DELL_POOL, name),
                              candidates=[_dell(21)], infra_env_missing=True)


async def _install_first_try():
    return await _run_install(install_server_slot_workflow_id(FIXTURE_MCE, DELL_POOL, 1), _install(DELL_POOL),
                              candidates=[_dell(1)])


async def _install_after_a_rollback():
    # The first blade already has a BareMetalHost (a skip), the second never
    # registers an Agent and is rolled back, the third installs: attempt 2.
    pool = [_blade(101), _blade(102), _blade(103)]
    return await _run_install(install_server_slot_workflow_id(FIXTURE_MCE, CISCO_POOL, 1), _install(CISCO_POOL),
                              candidates=pool, already_installed={pool[0].name}, agent_never_for={pool[1].name})


async def _install_no_candidate():
    # One reason each: two partitions of one port, another MCE's lock, no Agent.
    pool = [_dell_npar(11), _dell(12), _dell(13)]
    return await _run_install(install_server_slot_workflow_id(FIXTURE_MCE, DELL_POOL, 2), _install(DELL_POOL),
                              candidates=pool, reserved_elsewhere={pool[1].name}, agent_never_for={pool[2].name})


async def _install_awaiting_the_agent():
    blade = _blade(104)
    return await _run_install(install_server_slot_workflow_id(FIXTURE_MCE, CISCO_POOL, 2), _install(CISCO_POOL),
                              hold="awaiting-agent", candidates=[blade], agent_never_for={blade.name})


async def _run_remove(server: str, *, hold: str | None = None, **mocks):
    _, activities = remove_tests.make_mocks(**mocks)
    workflow_id = remove_server_workflow_id(server)
    async with remove_tests._Harness(activities) as client:
        handle = await client.start_workflow(
            RemoveServerWorkflow.run,
            RemoveServerRunArgs(input=RemoveServerInput(server_name=server, mce_cluster=FIXTURE_MCE, namespace=NAMESPACE)),
            id=workflow_id, task_queue=REMOVE_SERVER_WORKFLOW_QUEUE,
        )
        if hold:
            await _until(client, workflow_id, lambda p: p["phase"] == hold)
        else:
            await _ended(handle)
        return await _status(client, workflow_id)


def _inventory_host(n: int, cluster_deployment: str | None = None) -> InventoryHostState:
    return InventoryHostState(
        bmh_found=True, nmstate_found=True, powered_on=True,
        agents=[AgentBinding(name=f"agent-demo{n:04d}", cluster_deployment=cluster_deployment)],
        macs=[f"02:00:00:01:{n:02x}:01", f"02:00:00:01:{n:02x}:02"],
    )


async def _remove_nothing_there():
    return await _run_remove(_server(CISCO_POOL, 34), state=InventoryHostState())


async def _remove_done():
    return await _run_remove(_server(CISCO_POOL, 31), state=_inventory_host(31))


async def _remove_bound():
    return await _run_remove(_server(CISCO_POOL, 33), state=_inventory_host(33, "clusters-demo-shop/demo-shop"))


async def _remove_powering_off():
    return await _run_remove(_server(CISCO_POOL, 32), hold="powering-off", state=_inventory_host(32),
                             never_powers_off=True)


async def _run_uninstall(server: str, *, hold: Callable[[dict[str, Any]], bool] | None = None, **mocks):
    _, activities = uninstall_tests.make_mocks(**mocks)
    workflow_id = uninstall_server_workflow_id(server)
    async with uninstall_tests._Harness(activities) as client:
        handle = await client.start_workflow(
            UninstallServerWorkflow.run,
            UninstallServerRunArgs(input=uninstall_tests.INPUT.model_copy(update={"server_name": server})),
            id=workflow_id, task_queue=UNINSTALL_SERVER_WORKFLOW_QUEUE,
        )
        if hold:
            await _until(client, workflow_id, hold)
        else:
            await _ended(handle)
        return await _status(client, workflow_id)


async def _uninstall_a_hosted_node():
    return await _run_uninstall(uninstall_tests.SERVER)


async def _uninstall_draining():
    server = uninstall_tests.SERVER.replace("FCH0001", "FCH0002")
    return await _run_uninstall(
        server,
        hold=lambda p: p["phase"] == "awaiting-machine-deletion" and p["sub_phase"] == "draining",
        membership=uninstall_tests.INSTALLED.model_copy(update={"server_name": server}),
        machines=[uninstall_tests.DELETING], agents=[uninstall_tests.AGENT_BOUND],
    )


async def _publish_the_boot_entry():
    _, segment_acts, server_acts = publish_tests.make_mocks()
    workflow_id = publish_infraenv_boot_workflow_id(FIXTURE_MCE, publish_tests.INFRA_ENV)
    async with publish_tests._Harness(segment_acts, server_acts) as client:
        handle = await client.start_workflow(
            PublishInfraEnvBootWorkflow.run, PublishInfraEnvBootRunArgs(input=publish_tests.INPUT),
            id=workflow_id, task_queue=PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE,
        )
        await _ended(handle)
        return await _status(client, workflow_id)


# Recorded in this order; each list shows its newest first, as the API does.
_SCENARIOS: list[Callable[[], Awaitable[tuple[dict[str, Any], dict[str, Any]]]]] = [
    _install_named_without_its_infraenv, _install_first_try, _install_after_a_rollback,
    _install_no_candidate, _install_awaiting_the_agent,
    _remove_nothing_there, _remove_done, _remove_bound, _remove_powering_off,
    _uninstall_a_hosted_node, _uninstall_draining,
    _publish_the_boot_entry,
]


async def _record_demo() -> dict[str, Any]:
    # One fixture MCE across the four harnesses, so one rename covers them all.
    assert remove_tests.MCE == uninstall_tests.MCE == publish_tests.MCE == FIXTURE_MCE
    lists: dict[str, list[dict[str, Any]]] = {workflow_type: [] for workflow_type in WORKFLOWS}
    runs: dict[str, dict[str, Any]] = {}
    for scenario in _SCENARIOS:
        summary, status = _renamed(await scenario())
        lists[summary["workflow_type"]].append(summary)
        runs[summary["workflow_id"]] = status
    for listing in lists.values():
        listing.sort(key=lambda item: item["start_time"] or "", reverse=True)
    return {"generated_by": "UPDATE_UI_DEMO=1 pytest tests/test_server_lifecycle_ui.py", "lists": lists, "runs": runs}


@pytest.mark.skipif(not os.environ.get("UPDATE_UI_DEMO"), reason="set UPDATE_UI_DEMO=1 to regenerate demo-runs.json")
async def test_regenerate_the_demo_data():
    DEMO.write_text(json.dumps(await _record_demo(), indent=1) + "\n")
