"""The VirtualMachine a KV cluster's worker builds, its MAC and its name
(activities/vm_lifecycle/vm_spec.py). docs/design/create-vm-placement.md §7."""

from __future__ import annotations

import pytest

from activities.vm_lifecycle import vm_spec
from tests.kv_fleet import REDFISH_LABEL, stopped_vm


def _build(**overrides):
    values = dict(
        namespace="inventory", flavor="c200", cpu=32, memory_gib=256, site="five", zone="az1",
        mce="ocp4-prod-mce-five-a", mac="0e:03:f2:a1:b2:c3", request_id="0196f6a2-1111-7222-8333-444455556666",
        workflow_id="create-vm-ocp4-prod-mce-five-a_c200-kv32-five-az1_1", redfish_label=REDFISH_LABEL,
        storage_class_name="cephfs", network_name="openshift-cnv/vm-external-network",
        node_selector={}, root_disk_size="120Gi",
    )
    values.update(overrides)
    return vm_spec.build(**values)


class TestTheMac:
    def test_it_is_the_clusters_prefix_and_three_drawn_bytes(self):
        assert vm_spec.draw_mac("0e:03:f2", bytes([0xA1, 0xB2, 0xC3])) == "0e:03:f2:a1:b2:c3"
        assert vm_spec.draw_mac("0E:00:02", bytes([0x5D, 0x0C, 0x9E])) == "0e:00:02:5d:0c:9e"
        assert vm_spec.draw_mac("0e:00:02", bytes(3)) == "0e:00:02:00:00:00"

    @pytest.mark.parametrize("drawn", [b"", b"\x01\x02", b"\x01\x02\x03\x04"])
    def test_exactly_three_bytes_are_drawn(self, drawn):
        with pytest.raises(ValueError):
            vm_spec.draw_mac("0e:03:f2", drawn)


class TestTheVirtualMachine:
    def test_its_name_is_the_mac_in_full_and_its_root_disk_follows(self):
        vm = _build()
        assert vm["metadata"]["name"] == "ocp-c200-kv32-five-0e03f2a1b2c3"
        assert vm["metadata"]["namespace"] == "inventory"
        assert vm_spec.root_disk_name(vm) == "ocp-c200-kv32-five-0e03f2a1b2c3-root"
        assert vm["spec"]["dataVolumeTemplates"][0]["metadata"]["name"] == "ocp-c200-kv32-five-0e03f2a1b2c3-root"
        assert vm["spec"]["template"]["metadata"]["labels"] == {"kubevirt.io/vm": "ocp-c200-kv32-five-0e03f2a1b2c3"}

    @pytest.mark.parametrize(("cpu", "label"), [(1, "1c"), (32, "32c"), (256, "256c")])
    def test_the_size_label_is_the_vcpus_and_a_c(self, cpu, label):
        """The label says the size alone; `kv<cpu>` is the NAME's field, not the label's."""
        assert vm_spec.size_label(cpu) == label

    def test_its_labels_are_the_conventions(self):
        assert _build()["metadata"]["labels"] == {
            "redfish-enabled": "true",
            "redbull.io/flavor": "c200",
            "redbull.io/size": "32c",
            "redbull.io/site": "five",
            "topology.kubernetes.io/zone": "az1",
            "redbull.io/role": "worker",
            "redbull.io/mce": "ocp4-prod-mce-five-a",
            "redbull.io/created-by": "0196f6a2-1111-7222-8333-444455556666",
        }

    def test_the_slots_workflow_id_is_an_annotation_never_a_label(self):
        # It holds `_` runs and can pass 63 characters: not a label value.
        vm = _build(workflow_id="create-vm-ocp4-prod-mce-bat-yam-a_c200n-kv32-bat-yam-az1_100" + "x" * 30)
        assert vm["metadata"]["annotations"]["redbull.io/workflow-id"].startswith("create-vm-ocp4-prod-mce-bat-yam-a_")
        assert "annotations" not in _build(workflow_id=None)["metadata"]

    def test_every_label_value_is_one_kubernetes_accepts(self):
        import re

        label_value = re.compile(r"^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$")
        for value in _build(flavor="c200n", site="bat-yam", mce="ocp4-prod-mce-bat-yam-a")["metadata"]["labels"].values():
            assert len(value) <= 63 and label_value.fullmatch(value)

    def test_it_is_halted_with_the_fixed_mac_on_the_clusters_network(self):
        vm = _build()
        assert vm["apiVersion"] == "kubevirt.io/v1" and vm["kind"] == "VirtualMachine"
        assert vm["spec"]["runStrategy"] == "Halted"
        spec = vm["spec"]["template"]["spec"]
        assert spec["domain"]["devices"]["interfaces"] == [{"name": "default", "macAddress": "0e:03:f2:a1:b2:c3", "bridge": {}}]
        assert spec["networks"] == [{"name": "default", "multus": {"networkName": "openshift-cnv/vm-external-network"}}]
        assert spec["evictionStrategy"] == "LiveMigrate"
        assert vm_spec.mac_of(vm) == "0e:03:f2:a1:b2:c3"

    @pytest.mark.parametrize(("cpu", "cores"), [(1, "1"), (8, "7"), (32, "30"), (64, "60")])
    def test_its_cpu_request_is_fifteen_sixteenths_floored_at_a_core(self, cpu, cores):
        domain = _build(cpu=cpu, memory_gib=cpu * 8)["spec"]["template"]["spec"]["domain"]
        assert domain["cpu"] == {"cores": cpu}
        assert domain["resources"] == {
            "requests": {"cpu": cores, "memory": f"{cpu * 8}Gi"},
            "limits": {"cpu": str(cpu), "memory": f"{cpu * 8}Gi"},
        }
        assert domain["memory"] == {"guest": f"{cpu * 8}Gi"}

    def test_its_root_disk_is_blank_shared_and_a_filesystem_on_the_clusters_class(self):
        storage = _build(storage_class_name="ocs-cephfs", root_disk_size="2Gi")["spec"]["dataVolumeTemplates"][0]["spec"]
        assert storage == {
            "source": {"blank": {}},
            "storage": {"storageClassName": "ocs-cephfs", "accessModes": ["ReadWriteMany"], "volumeMode": "Filesystem",
                        "resources": {"requests": {"storage": "2Gi"}}},
        }

    def test_a_node_selector_is_set_only_when_the_cluster_has_one(self):
        assert "nodeSelector" not in _build()["spec"]["template"]["spec"]
        assert _build(node_selector={"role": "vm"})["spec"]["template"]["spec"]["nodeSelector"] == {"role": "vm"}

    def test_the_network_is_always_multus_and_only_its_name_is_the_clusters(self):
        assert _build(network_name="inventory/vlan1010")["spec"]["template"]["spec"]["networks"] == [
            {"name": "default", "multus": {"networkName": "inventory/vlan1010"}}
        ]

    def test_the_body_is_the_platforms_vm_yaml(self):
        """redbull-platform's vm.yaml is the TARGET (the operator, 2026-11-21). Here it is,
        rendered for a 32-vCPU, 256 GiB VM on `basic`; the built VM must be exactly it plus
        what the workflow adds on purpose, each named below — nothing else may differ."""
        name = "ocp-c200-kv32-five-0e03f2a1b2c3"
        vm_yaml = {
            "apiVersion": "kubevirt.io/v1",
            "kind": "VirtualMachine",
            "metadata": {"name": name, "labels": {"redfish-enabled": "true"}},
            "spec": {
                "runStrategy": "Halted",
                "dataVolumeTemplates": [{
                    "metadata": {"name": f"{name}-root"},
                    "spec": {"source": {"blank": {}}, "storage": {
                        "storageClassName": "basic", "accessModes": ["ReadWriteMany"], "volumeMode": "Filesystem",
                        "resources": {"requests": {"storage": "120Gi"}}}},
                }],
                "template": {
                    "metadata": {"labels": {"kubevirt.io/vm": name}},
                    "spec": {
                        "evictionStrategy": "LiveMigrate",
                        "domain": {
                            "cpu": {"cores": 32},
                            "memory": {"guest": "256Gi"},
                            "resources": {"requests": {"cpu": "30", "memory": "256Gi"},
                                          "limits": {"cpu": "32", "memory": "256Gi"}},
                            "firmware": {"bootloader": {"efi": {"secureBoot": False}}},
                            "devices": {
                                "blockMultiQueue": True,
                                "networkInterfaceMultiqueue": True,
                                "disks": [{"name": "rootdisk", "dedicatedIOThread": True, "disk": {"bus": "virtio"}}],
                                "interfaces": [{"name": "default", "bridge": {}}],
                                "rng": {},
                            },
                        },
                        "networks": [{"name": "default", "multus": {"networkName": "openshift-cnv/vm-external-network"}}],
                        "volumes": [{"name": "rootdisk", "dataVolume": {"name": f"{name}-root"}}],
                    },
                },
            },
        }
        built = _build(storage_class_name="basic")
        # What the workflow adds on purpose: the namespace it creates in; its labels beside
        # the redfish one and the workflow id annotation (how a run finds its VM, how the
        # placement counts it); and the FIXED MAC (KubeVirt re-randomises an unset one on every
        # start, the BareMetalHost needs a fixed boot MAC, and the name is that MAC).
        assert built["metadata"].pop("namespace") == "inventory"
        assert built["metadata"].pop("annotations") == {"redbull.io/workflow-id": "create-vm-ocp4-prod-mce-five-a_c200-kv32-five-az1_1"}
        assert built["metadata"]["labels"].pop("redbull.io/created-by")
        for key in ("redbull.io/flavor", "redbull.io/size", "redbull.io/site", "topology.kubernetes.io/zone", "redbull.io/role", "redbull.io/mce"):
            built["metadata"]["labels"].pop(key)
        interface = built["spec"]["template"]["spec"]["domain"]["devices"]["interfaces"][0]
        assert interface.pop("macAddress") == "0e:03:f2:a1:b2:c3"
        assert built == vm_yaml

    def test_one_vcpu_requests_one_core_where_vm_yaml_would_request_none(self):
        """The one place the body departs from vm.yaml's arithmetic: `cpu * 15 / 16` in
        integers is 0 for one vCPU, a VM the scheduler counts as free. The reviewed plan
        floors it at one core (shared/vm_requests.py), and the placement counts the same."""
        assert _build(cpu=1, memory_gib=8)["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["cpu"] == "1"

    def test_what_the_figures_count_for_it_is_what_it_requests(self):
        # The worker that built it and the worker that counts it stopped agree by construction.
        from activities.vm_lifecycle.cluster_figures import stopped_vm_request
        from shared.vm_requests import pod_cpu_millicores, pod_memory_bytes

        assert stopped_vm_request(_build(cpu=32, memory_gib=256)) == (pod_cpu_millicores(32), pod_memory_bytes(256))
        assert stopped_vm_request(_build(cpu=1, memory_gib=8)) == (pod_cpu_millicores(1), pod_memory_bytes(8))


class TestWhatAVirtualMachineSays:
    def test_identity_and_size(self):
        vm = _build()
        assert vm_spec.created_by(vm) == "0196f6a2-1111-7222-8333-444455556666"
        assert (vm_spec.cores(vm), vm_spec.memory_gib(vm)) == (32, 256)
        assert vm_spec.macs(vm) == ["0e:03:f2:a1:b2:c3"]

    def test_a_vm_without_a_fixed_mac_or_a_label_says_none(self):
        other = stopped_vm("someone-elses", 8, 64, labels={})
        assert vm_spec.mac_of(other) is None and vm_spec.macs(other) == [] and vm_spec.created_by(other) is None

    def test_macs_are_compared_lowercase(self):
        assert vm_spec.macs(stopped_vm("x", 8, 64, mac="0E:03:F2:AA:BB:CC")) == ["0e:03:f2:aa:bb:cc"]

    @pytest.mark.parametrize(
        ("status", "admitted"),
        [("Stopped", True), ("WaitingForVolumeBinding", True), ("Running", True), ("Provisioning", False),
         ("ErrorUnschedulable", False), (None, False)],
    )
    def test_admitted_means_kubevirt_accepted_the_spec(self, status, admitted):
        vm = _build()
        vm["status"] = {"printableStatus": status} if status else {}
        assert vm_spec.admitted(vm) is admitted

    def test_the_root_disk_is_the_rootdisk_volume_else_the_first_datavolume(self):
        vm = {"spec": {"template": {"spec": {"volumes": [
            {"name": "cloudinit", "cloudInitNoCloud": {}}, {"name": "data", "dataVolume": {"name": "data-dv"}},
            {"name": "rootdisk", "dataVolume": {"name": "root-dv"}}]}}}}
        assert vm_spec.root_disk_name(vm) == "root-dv"
        del vm["spec"]["template"]["spec"]["volumes"][2]
        assert vm_spec.root_disk_name(vm) == "data-dv"
        assert vm_spec.root_disk_name({"spec": {}}) is None
