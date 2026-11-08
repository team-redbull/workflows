"""The KubeVirt agents' wire contract and the agents' vendored copy of it cannot drift apart.

The agents (the kv-agents repo: kv-capacity, kv-vm-api) hold a COPY of the
models in shared/models/vm_lifecycle.py's contract section. Two guards:

  * always: the originals here must render the snapshot both repos keep
    (tests/fixtures/kv_agents_contract.schema.json here, the agents'
    contract.schema.json there). A change to the contract fails this until the
    snapshot is regenerated (UPDATE_KV_AGENTS_CONTRACT=1) AND the agents get
    the same classes and snapshot;
  * given KV_AGENTS_SERVICE_DIR (the kv-agents checkout), the agents' own
    models are compared with these directly. Skipped without it.

Descriptions are left out of the comparison: docstrings are not the wire.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

from shared.models.vm_lifecycle import (
    KvApiError,
    KvCapacityReport,
    KvVmConflict,
    KvVmCreateRequest,
    KvVmCreateResponse,
    KvVmState,
)

CONTRACT = (KvCapacityReport, KvVmCreateRequest, KvVmCreateResponse, KvVmConflict, KvVmState, KvApiError)
SNAPSHOT = Path(__file__).resolve().parent / "fixtures" / "kv_agents_contract.schema.json"
SERVICE_DIR = os.environ.get("KV_AGENTS_SERVICE_DIR", "")


def _without_descriptions(node):
    if isinstance(node, dict):
        return {k: _without_descriptions(v) for k, v in node.items() if k != "description"}
    if isinstance(node, list):
        return [_without_descriptions(v) for v in node]
    return node


def _rendered(models) -> str:
    schema = {model.__name__: _without_descriptions(model.model_json_schema()) for model in models}
    return json.dumps(schema, indent=1, sort_keys=True) + "\n"


def test_the_contract_matches_the_snapshot_the_agents_keep():
    rendered = _rendered(CONTRACT)
    if os.environ.get("UPDATE_KV_AGENTS_CONTRACT") == "1":
        SNAPSHOT.write_text(rendered)
    assert SNAPSHOT.read_text() == rendered, (
        "the KubeVirt agents' contract changed. Regenerate the snapshot (UPDATE_KV_AGENTS_CONTRACT=1), then copy "
        "the changed classes into the kv-agents repo (kv_agents/contract.py) and this snapshot to its "
        "kv_agents/contract.schema.json, or the agents will refuse or misread requests"
    )


@pytest.fixture(scope="module")
def service():
    """The agents' `kv_agents` package, imported from KV_AGENTS_SERVICE_DIR."""
    root = Path(SERVICE_DIR).expanduser()
    if not SERVICE_DIR or not (root / "kv_agents" / "contract.py").is_file():
        pytest.skip("set KV_AGENTS_SERVICE_DIR to the kv-agents checkout to compare the agents' copy")
    sys.path.insert(0, str(root))
    try:
        contract = importlib.import_module("kv_agents.contract")
    finally:
        sys.path.remove(str(root))
    return root, contract


def test_the_agents_models_are_these(service):
    root, contract = service
    theirs = tuple(getattr(contract, model.__name__) for model in CONTRACT)
    assert _rendered(theirs) == _rendered(CONTRACT)
    assert (root / "kv_agents" / "contract.schema.json").read_text() == SNAPSHOT.read_text()
