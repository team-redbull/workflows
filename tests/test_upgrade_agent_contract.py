"""The agent contract and the upgrade agent service's vendored copy of it cannot drift apart.

The service (the k8s-agent folder, its own repo) holds a COPY of AgentRequest
and AgentVerdict, and of the tier-2 catalog rules. Two guards:

  * always: the originals here must render the snapshot both repos keep
    (tests/fixtures/upgrade_agent_contract.schema.json here, the service's
    upgrade_agent/contract.schema.json there). A change to the contract fails
    this until the snapshot is regenerated (UPDATE_AGENT_CONTRACT=1) AND the
    service gets the same classes and snapshot;
  * given UPGRADE_AGENT_SERVICE_DIR (the k8s-agent folder), the service's own
    models and catalog are compared with these directly. Skipped without it.

Descriptions are left out of the comparison: docstrings are not the wire.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

from activities.cluster_upgrade import action_catalog
from shared.exceptions import ProposedActionRefusedError
from shared.models.cluster_upgrade import AgentRequest, AgentVerdict, ProposedAction

SNAPSHOT = Path(__file__).resolve().parent / "fixtures" / "upgrade_agent_contract.schema.json"
SERVICE_DIR = os.environ.get("UPGRADE_AGENT_SERVICE_DIR", "")


def _without_descriptions(node):
    if isinstance(node, dict):
        return {k: _without_descriptions(v) for k, v in node.items() if k != "description"}
    if isinstance(node, list):
        return [_without_descriptions(v) for v in node]
    return node


def _rendered(models) -> str:
    schema = {model.__name__: _without_descriptions(model.model_json_schema()) for model in models}
    return json.dumps(schema, indent=1, sort_keys=True) + "\n"


def test_the_contract_matches_the_snapshot_the_agent_service_keeps():
    rendered = _rendered((AgentRequest, AgentVerdict))
    if os.environ.get("UPDATE_AGENT_CONTRACT") == "1":
        SNAPSHOT.write_text(rendered)
    assert SNAPSHOT.read_text() == rendered, (
        "AgentRequest/AgentVerdict changed. Regenerate the snapshot (UPDATE_AGENT_CONTRACT=1), then copy the "
        "changed classes into the upgrade agent service (k8s-agent: agent/upgrade_agent/contract.py) and this "
        "snapshot to its agent/upgrade_agent/contract.schema.json, or the service will refuse or misread requests"
    )


@pytest.fixture(scope="module")
def service():
    """The service's `upgrade_agent` package, imported from UPGRADE_AGENT_SERVICE_DIR."""
    agent = Path(SERVICE_DIR).expanduser() / "agent"
    if not SERVICE_DIR or not (agent / "upgrade_agent" / "contract.py").is_file():
        pytest.skip("set UPGRADE_AGENT_SERVICE_DIR to the k8s-agent folder to compare the service's copy")
    sys.path.insert(0, str(agent))
    try:
        contract = importlib.import_module("upgrade_agent.contract")
        catalog = importlib.import_module("upgrade_agent.catalog")
    finally:
        sys.path.remove(str(agent))
    return agent, contract, catalog


def test_the_services_models_are_these(service):
    agent, contract, _ = service
    assert _rendered((contract.AgentRequest, contract.AgentVerdict)) == _rendered((AgentRequest, AgentVerdict))
    assert (agent / "upgrade_agent" / "contract.schema.json").read_text() == SNAPSHOT.read_text()


_CASES = [
    ("delete_pod", "openshift-console/downloads-a"),
    ("delete_pod", "shop/db-0"),
    ("delete_pod", "openshift-ovn-kubernetes/ovnkube-node-x"),
    ("delete_pod", "openshift-multus/multus-x"),
    ("delete_pod", "downloads-a"),
    ("delete_pod", "openshift-console/"),
    ("delete_hcp_pod", "cluster-version-operator-7d9f"),
    ("delete_hcp_pod", "etcd-0"),
    ("delete_hcp_pod", "kube-apiserver-5f6d"),
    ("delete_hcp_pod", "ns/pod"),
    ("rerun_cronjob", "openshift-image-registry/image-pruner"),
    ("rerun_cronjob", "default/backup"),
    ("delete_failed_job", "openshift-image-registry/image-pruner-29365920"),
    ("delete_failed_job", "shop/nightly-report-1"),
    ("delete_job", "openshift-image-registry/image-pruner-1"),
    ("delete_namespace", "openshift-image-registry"),
    ("delete_pod", "Openshift-console/x"),
]


def test_the_services_catalog_accepts_and_refuses_what_the_workflows_does(service):
    _, _, catalog = service
    assert set(catalog.KINDS) == set(action_catalog.KINDS)
    for kind, target in _CASES:
        try:
            action_catalog.check(ProposedAction(action_id="a-1", kind=kind, target=target))
            ours = None
        except ProposedActionRefusedError as err:
            ours = str(err).split(": ", 1)[1]
        try:
            catalog.check(kind, target)
            theirs = None
        except catalog.Refused as err:
            theirs = str(err)
        assert theirs == ours, f"{kind} {target}: the workflow says {ours!r}, the agent service {theirs!r}"
