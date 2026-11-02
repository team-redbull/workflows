"""The temporary server-lifecycle page (workflow_domains/server_lifecycle/ui.py).

It shows only the runs the API reports, so these check the page against the
workflows and the request models: the routes it calls, the fields its start
forms send, the words it has for each workflow's phases, and the failure
message it reads a failed install's candidates out of.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import ModuleType

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import tests.test_add_server_to_mce_workflow as install_tests
from workflow_domains.cluster_upgrade import ui as cluster_upgrade_ui
from workflow_domains.server_lifecycle import (
    add_server_to_mce,
    publish_infraenv_to_pxe_server,
    remove_server_from_mce,
    ui,
    uninstall_server_from_hc,
)
from workflow_domains.server_lifecycle.router import (
    AddServerToMceRequest,
    RemoveServerFromMceRequest,
    UninstallServerFromHcRequest,
)

PAGE = ui.UI_DIR / "index.html"

# The page's tabs: each workflow by its class name (the runs list's `workflow_type`), with its module.
WORKFLOWS: dict[str, ModuleType] = {
    workflow.__name__: module
    for workflow, module in (
        (add_server_to_mce.AddServerToMceWorkflow, add_server_to_mce),
        (remove_server_from_mce.RemoveServerFromMceWorkflow, remove_server_from_mce),
        (uninstall_server_from_hc.UninstallServerFromHcWorkflow, uninstall_server_from_hc),
        (publish_infraenv_to_pxe_server.PublishInfraEnvToPxeServerWorkflow, publish_infraenv_to_pxe_server),
    )
}


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
    for path in ("/workflows/runs", "/workflows/server-lifecycle/add-server-to-mce",
                 "/workflows/server-lifecycle/remove-server-from-mce",
                 "/workflows/server-lifecycle/uninstall-server-from-hc"):
        assert path in page
    # It lists each workflow by its real class name (the runs list's `workflow_type`).
    for workflow_type in WORKFLOWS:
        assert f'type: "{workflow_type}"' in page


def test_each_tab_also_lists_its_runs_under_the_old_name():
    """Renamed 2026-11-01: a run started before is still its old type, under its old id prefix."""
    page = PAGE.read_text()
    legacy = {
        add_server_to_mce.InstallServerWorkflow: "install-server",
        remove_server_from_mce.RemoveServerWorkflow: "remove-server",
        uninstall_server_from_hc.UninstallServerWorkflow: "uninstall-server",
        publish_infraenv_to_pxe_server.PublishInfraEnvBootWorkflow: "publish-infraenv-boot",
    }
    for workflow, noun in legacy.items():
        # The class's registered name, which the runs list filters on.
        name = workflow.__temporal_workflow_definition.name
        assert f'legacy: {{ type: "{name}", noun: "{noun}" }}' in page


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
        "add-server-to-mce": {"infra_env", "mce_cluster", "count", "server_names"},
        "remove-server-from-mce": {"server_name", "mce_cluster"},
        "uninstall-server-from-hc": {"server_names", "mce_cluster", "keep_inventory"},
    }
    assert sent["add-server-to-mce"] <= set(AddServerToMceRequest.model_fields)
    assert sent["remove-server-from-mce"] <= set(RemoveServerFromMceRequest.model_fields)
    assert sent["uninstall-server-from-hc"] <= set(UninstallServerFromHcRequest.model_fields)


def test_the_skip_reasons_are_add_server_to_mces_own():
    """The page reads a failed install's candidates by these sentences: they must be the workflow's."""
    block = re.search(r'<script type="application/json" id="skip-reasons">(.*?)</script>', PAGE.read_text(), re.S)
    assert block is not None
    reasons = json.loads(block.group(1))
    assert {key: reason["summary"] for key, reason in reasons.items()} == add_server_to_mce._REJECTION_SUMMARY
    assert all(reason["label"] for reason in reasons.values())


def _page_flows() -> dict[str, dict[str, set[str]]]:
    """The page's FLOWS table by workflow type: the phases it has words for, the phases its
    steps group, and the sub-phases it has words for."""
    table = re.search(r"const FLOWS = \{(.*?)\n\};", PAGE.read_text(), re.S)
    assert table is not None
    flows: dict[str, dict[str, set[str]]] = {}
    for block in re.split(r"\n  [a-z]+: \{\n", table.group(1))[1:]:
        workflow_type = re.search(r'type: "(\w+)"', block)
        phases = re.search(r"phases: \{(.*?)\n    \},", block, re.S)
        steps = re.search(r"steps: \[(.*?)\n    \],", block, re.S)
        sub = re.search(r"sub: \{(.*?)\},", block, re.S)
        assert workflow_type and phases and steps, block
        flows[workflow_type.group(1)] = {
            "phases": set(re.findall(r'"([a-z-]+)": \[', phases.group(1))),
            "stepped": {name for group in re.findall(r", \[([^\]]*)\]\]", steps.group(1))
                        for name in re.findall(r'"([a-z-]+)"', group)},
            "sub": set(re.findall(r'([a-z-]+): "', sub.group(1))) if sub else set(),
        }
    return flows


def _assigned(module: ModuleType, attribute: str) -> set[str]:
    """Every value the workflow's code gives `self.<attribute>`: what its progress query reports."""
    assert module.__file__ is not None
    return set(re.findall(rf'self\.{attribute} = "([a-z-]+)"', Path(module.__file__).read_text()))


def test_the_page_has_words_for_every_phase_each_workflow_reports():
    """A phase the page does not know shows as its raw name and lights no step."""
    flows = _page_flows()
    assert set(flows) == set(WORKFLOWS)
    for workflow_type, module in WORKFLOWS.items():
        reported = _assigned(module, "_phase")
        assert {"pending", "completed"} <= reported, workflow_type
        assert flows[workflow_type]["phases"] == reported, workflow_type
        # Every phase between the first and the last belongs to a step.
        assert flows[workflow_type]["stepped"] == reported - {"pending", "completed"}, workflow_type
        assert flows[workflow_type]["sub"] == _assigned(module, "_sub_phase"), workflow_type


def test_a_failed_draw_names_its_candidates_the_way_the_page_reads_them():
    """The page splits "Checked N candidate(s) — <summary>: <entry>; <entry>. <summary>: …" on
    the summaries; this is add-server-to-mce's own builder of that failure."""
    servers = [install_tests.bondable_server(f"srv_{n}", name=f"ocp-pool-{n:04d}") for n in range(3)]
    reasons = ["no-bond", "reserved", "no-agent"]
    error = add_server_to_mce._no_installable_candidate(
        "pool", "inventory", servers,
        [(reason, f"{server.name} (why it was skipped)") for reason, server in zip(reasons, servers, strict=True)],
    )
    body = re.search(r"Checked (\d+) candidate\(s\) — ([\s\S]*)$", error.message)
    assert body is not None and body.group(1) == "3"
    assert body.group(2).startswith(f"{add_server_to_mce._REJECTION_SUMMARY[reasons[0]]}: ")
    for reason, server in zip(reasons, servers, strict=True):
        assert f"{add_server_to_mce._REJECTION_SUMMARY[reason]}: {server.name} (why it was skipped)" in body.group(2)
