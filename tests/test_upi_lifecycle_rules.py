"""upi-lifecycle's pure rules: the installer Job, the nodes-config, CSR choice, readiness, the edge."""

from __future__ import annotations

import base64

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from activities.upi_provisioning import cluster_ready, installer_job
from activities.upi_server import node_api, nodes_config
from shared.exceptions import InstallerConfigRejectedError
from shared.models.segment_lifecycle import SegmentEntry
from shared.models.server_lifecycle import BondMember
from shared.models.upi_lifecycle import InstallerStep, InstallerStepRequest, UpiNode
from workflow_domains.upi_lifecycle.router import AddServerToUpiRequest, CreateUpiClusterRequest

REQ = InstallerStepRequest(cluster="ocp4-prod-mce-tlv-a", site="tlv", step=InstallerStep.PREPARE, run_token="abcd1234",
                           segment="10.1.5.0/24", vlan_id=105, epg_name="EPG_tlv_105")


def _csr(cn: str, username: str, approved: bool = False) -> dict:
    key = ec.generate_private_key(ec.SECP256R1())
    pem = x509.CertificateSigningRequestBuilder().subject_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])).sign(key, hashes.SHA256()).public_bytes(
        serialization.Encoding.PEM)
    return {"metadata": {"name": f"csr-{cn}-{username}"},
            "spec": {"username": username, "request": base64.b64encode(pem).decode()},
            "status": {"conditions": [{"type": "Approved"}]} if approved else {}}


def test_the_cluster_yaml_carries_the_runs_keys_over_install_config():
    text = installer_job.cluster_yaml(REQ.model_copy(update={"install_config": {"compute_ip_offsets": [8]}}),
                                      vm_network="EPG_tlv_105")
    assert "segment: 10.1.5.0/24" in text and "compute_ip_offsets" in text and "terraform_dir: /work/ocp4-prod-mce-tlv-a/vsphere" in text


@pytest.mark.parametrize("key", ["segment", "cluster_name", "argocd_hub_token"])
def test_install_config_may_not_set_a_run_owned_key(key):
    with pytest.raises(InstallerConfigRejectedError):
        installer_job.cluster_yaml(REQ.model_copy(update={"install_config": {key: "x"}}), vm_network="n")


def test_the_job_names_and_scripts():
    assert installer_job.job_name(REQ.cluster, InstallerStep.INSTALL, "abcd1234") == "upi-ocp4-prod-mce-tlv-a-install-abcd1234"
    prepare = installer_job.job_script(REQ.cluster, InstallerStep.PREPARE, csr_timeout_minutes=90)
    assert "--skip-terraform --skip-argocd" in prepare and "REFUSED" in prepare and "nodes.yaml" in prepare
    install = installer_job.job_script(REQ.cluster, InstallerStep.INSTALL, csr_timeout_minutes=90)
    assert install.count("printf 'yes\\n' | python3 bootstrap.py") == 1 and "--csr-timeout 90" in install
    assert installer_job.vm_network_for("{epg_name}-{vlan_id}", request=REQ) == "EPG_tlv_105-105"


def test_the_bond_config_binds_macs_to_logical_names_with_dhcp_on_the_vlan():
    members = [BondMember(logical_name="nic1", mac="aa:00:00:00:00:01", port_key="a"),
               BondMember(logical_name="nic2", mac="aa:00:00:00:00:02", port_key="b")]
    host = nodes_config.nodes_config(hostname="w1", root_device="/dev/sda", members=members, bond_mode="802.3ad",
                                     vlan_id=105)["hosts"][0]
    assert [i["macAddress"] for i in host["interfaces"]] == ["aa:00:00:00:00:01", "aa:00:00:00:00:02"]
    ifaces = {i["name"]: i for i in host["networkConfig"]["interfaces"]}
    assert ifaces["bond0"]["link-aggregation"] == {"mode": "802.3ad", "port": ["nic1", "nic2"]}
    assert ifaces["bond0.105"]["ipv4"] == {"enabled": True, "dhcp": True}


def test_only_this_hosts_csrs_are_approved():
    assert node_api.csr_is_for_host(_csr("system:node:w1", node_api.NODE_BOOTSTRAPPER), "w1")
    assert node_api.csr_is_for_host(_csr("system:node:w1", "system:node:w1"), "w1")
    assert not node_api.csr_is_for_host(_csr("system:node:w2", node_api.NODE_BOOTSTRAPPER), "w1")
    assert not node_api.csr_is_for_host(_csr("system:node:w1", "system:serviceaccount:x:y"), "w1")


def test_a_new_clusters_node_csrs_and_readiness():
    pending = [_csr("system:node:m0", cluster_ready.NODE_BOOTSTRAPPER), _csr("x", "someone"),
               _csr("system:node:m1", "system:node:m1", approved=True)]
    assert [c["spec"]["username"] for c in cluster_ready.to_approve(pending)] == [cluster_ready.NODE_BOOTSTRAPPER]
    nodes = [UpiNode(name="m0", internal_ip="10.1.5.4", ready=True)]
    assert cluster_ready.missing(nodes, ["10.1.5.4", "10.1.5.5"], True, "4.20.1") == ["no Ready node at 10.1.5.5"]
    assert cluster_ready.missing(nodes, ["10.1.5.4"], True, "4.20.1") == []
    assert installer_job.expected_ips("control_plane_ips: [a]\ninfra_ips: [b]\ncompute_ips: []\n") == ["a", "b"]


def test_a_upi_segment_is_held_through_its_list():
    entry = SegmentEntry(segment="s", site="t", vlan_id=1, status="Allocated", type="UPI", cluster_name=["a", "b"])
    assert entry.held_by("b") and not entry.held_by("c")


def test_the_edge_refuses_what_the_run_would():
    with pytest.raises(ValueError):
        CreateUpiClusterRequest(cluster="Bad_Name", site="tlv")
    with pytest.raises(ValueError):
        CreateUpiClusterRequest(cluster="ocp4-x", site="tlv", install_config={"segment": "1"})
    with pytest.raises(ValueError):
        CreateUpiClusterRequest(cluster="ocp4-x", site="tlv", kind="standalone", mce_values="a: 1")
    assert AddServerToUpiRequest(cluster="ocp4-x", server_name="OCP-R760-1").run_input().server_name == "OCP-R760-1"
    with pytest.raises(ValueError):
        AddServerToUpiRequest(cluster="ocp4-x", server_name="s1", bond_mode="balance-rr")
