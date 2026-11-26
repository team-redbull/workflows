"""The pieces of add-server-to-mce's KubeVirt branch that need no Temporal.

  * the two objects a VM gets on the MCE, pinned field by field against the
    platform's own hand-made, WORKING ones (the operator's, 2026-11-25);
  * where a VM's BMC is: one host pattern, the KV cluster's name, the VM's name;
  * what a VM InfraEnv's labels and its MCE's name ask create-vm for;
  * the worker's settings and its answer before any VM is made.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from temporalio.testing import ActivityEnvironment

from activities.server_lifecycle import activities
from activities.server_lifecycle.bmh_resources import (
    build_baremetal_host,
    build_bmc_secret,
    build_nmstate_config,
)
from shared.bmc_address import (
    BMC_VENDORS,
    KUBEVIRT_BMC_VENDOR,
    boots_from_network,
    build_bmc_address,
    kubevirt_bmc_endpoint,
    kubevirt_bmc_host_template_problem,
)
from shared.exceptions import InvalidHostNetworkError, KubevirtBmcNotConfiguredError
from shared.infraenv_names import (
    InfraEnvKind,
    flavor_problem,
    kubevirt_size_cpu,
    kubevirt_size_problem,
    parse_infraenv,
    size_kind,
    vm_name_problem,
    zone_problem,
)
from shared.models.server_lifecycle import BmhResourceRequest, BondMember, KubevirtBmcConfigRequest
from shared.settings.server_lifecycle import ServerLifecycleActivitySettings
from shared.workflow_ids import create_vm_child_workflow_id, create_vm_slot_workflow_id
from workflow_domains.server_lifecycle import vm_install

VM = "ocp-c200-kv32-five-k7q2m9xd"
MAC = "02:b0:96:22:25:eb"
KV_CLUSTER = "ocp4-prod-kv-five-az1-a"
INFRA_ENV = "c200-kv32-az1"
NAMESPACE = "inventory"
VLAN = 1010
TEMPLATE = "kubevirt-redfish-kubevirt-redfish.apps.{kv_cluster}.example.com"
HOST = f"kubevirt-redfish-kubevirt-redfish.apps.{KV_CLUSTER}.example.com"
MCE = "ocp4-prod-mce-five-a"
LABELS = {
    "redbull.io/flavor": "c200",
    "redbull.io/size": "kv32",
    "redbull.io/site": "five",
    "topology.kubernetes.io/zone": "az1",
}


def _request(**overrides) -> BmhResourceRequest:
    """What the workflow sends for a VM: vendor KUBEVIRT, ONE member named as the guest names its NIC."""
    defaults = dict(
        server_name=VM,
        namespace=NAMESPACE,
        infra_env=INFRA_ENV,
        bmc_vendor=KUBEVIRT_BMC_VENDOR,
        bmc=kubevirt_bmc_endpoint(TEMPLATE, KV_CLUSTER, VM),
        bond_members=[BondMember(logical_name=vm_install.VM_NIC_NAME, mac=MAC, port_key=vm_install.VM_NIC_NAME)],
        vlan_id=VLAN,
    )
    defaults.update(overrides)
    return BmhResourceRequest(**defaults)


class TestTheObjectsAVmGets:
    """Against the platform's hand-made BareMetalHost and NMStateConfig for a VM."""

    def test_the_nmstateconfig_is_one_nic_with_the_vlan_on_it(self):
        # The operator's object, with $vmName, $mac and vlanId filled in.
        assert build_nmstate_config(_request())["spec"] == {
            "config": {
                "interfaces": [
                    {
                        "ipv4": {"enabled": False},
                        "ipv6": {"enabled": False},
                        "mac-address": MAC,
                        "name": "enp1s0",
                        "state": "up",
                        "type": "ethernet",
                    },
                    {
                        "ipv4": {
                            "auto-dns": True, "auto-gateway": True, "auto-routes": True,
                            "dhcp": True, "enabled": True,
                        },
                        "ipv6": {"autoconf": False, "dhcp": False, "enabled": False},
                        "name": f"enp1s0.{VLAN}",
                        "state": "up",
                        "type": "vlan",
                        "vlan": {"base-iface": "enp1s0", "id": VLAN},
                    },
                ]
            },
            "interfaces": [{"macAddress": MAC, "name": "enp1s0"}],
        }

    def test_the_nmstateconfig_is_named_after_the_vm_and_bound_to_its_infraenv(self):
        metadata = build_nmstate_config(_request())["metadata"]
        assert metadata["name"] == VM
        assert metadata["labels"] == {"infraenvs.agent-install.openshift.io": INFRA_ENV}

    def test_there_is_no_bond(self):
        kinds = [i["type"] for i in build_nmstate_config(_request())["spec"]["config"]["interfaces"]]
        assert kinds == ["ethernet", "vlan"]

    def test_the_baremetalhost_is_the_operators(self):
        bmh = build_baremetal_host(_request())
        # The operator's spec, field for field; the address in the worker's own
        # spelling of the scheme (below) and under the legacy system id.
        assert bmh["spec"] == {
            "automatedCleaningMode": "disabled",
            "bmc": {
                "address": f"redfish-virtualmedia://{HOST}/redfish/v1/Systems/{VM}",
                "credentialsName": f"kubevirt-cred-{VM}",
                "disableCertificateVerification": True,
            },
            "bootMACAddress": MAC,
            "bootMode": "UEFI",
            "customDeploy": {"method": "start_assisted_install"},
            "hardwareProfile": "empty",
            "online": True,
        }
        assert bmh["metadata"]["name"] == VM
        assert bmh["metadata"]["labels"] == {"infraenvs.agent-install.openshift.io": INFRA_ENV}
        # The operator's one annotation, plus the worker's own for every host.
        assert bmh["metadata"]["annotations"] == {
            "bmac.agent-install.openshift.io/hostname": VM,
            "inspect.metal3.io": "disabled",
        }

    def test_the_secret_is_the_one_the_baremetalhost_names(self):
        secret = build_bmc_secret(_request(), "redfish-user", "redfish-pass")
        assert secret["metadata"]["name"] == build_baremetal_host(_request())["spec"]["bmc"]["credentialsName"]

    @pytest.mark.parametrize("count", [0, 3, 4])
    def test_any_other_number_of_nics_is_refused(self, count):
        members = [
            BondMember(logical_name=f"nic{i}", mac=f"02:00:00:00:00:{i:02x}", port_key=str(i)) for i in range(count)
        ]
        with pytest.raises(InvalidHostNetworkError, match="one NIC .a VM. or a bond of 2"):
            build_nmstate_config(_request(bond_members=members))

    def test_two_members_are_still_the_bare_metal_bond(self):
        members = [
            BondMember(logical_name="nic1", mac="aa:bb:cc:dd:ee:01", port_key="1/1"),
            BondMember(logical_name="nic2", mac="aa:bb:cc:dd:ee:02", port_key="1/2"),
        ]
        names = [i["name"] for i in build_nmstate_config(_request(bond_members=members))["spec"]["config"]["interfaces"]]
        assert names == ["nic1", "nic2", "bond0", f"bond0.{VLAN}"]


class TestWhereAVmsBmcIs:
    def test_the_kv_clusters_kubevirt_redfish_under_the_vms_own_name(self):
        endpoint = kubevirt_bmc_endpoint(TEMPLATE, KV_CLUSTER, VM)
        assert (endpoint.host, endpoint.port, endpoint.path) == (HOST, None, f"/redfish/v1/Systems/{VM}")
        assert build_bmc_address(KUBEVIRT_BMC_VENDOR, endpoint) == (
            f"redfish-virtualmedia://{HOST}/redfish/v1/Systems/{VM}"
        )

    def test_a_port_in_the_pattern_is_kept(self):
        endpoint = kubevirt_bmc_endpoint("redfish.{kv_cluster}.lab:8443", KV_CLUSTER, VM)
        assert (endpoint.host, endpoint.port) == (f"redfish.{KV_CLUSTER}.lab", 8443)
        assert build_bmc_address(KUBEVIRT_BMC_VENDOR, endpoint) == (
            f"redfish-virtualmedia://redfish.{KV_CLUSTER}.lab:8443/redfish/v1/Systems/{VM}"
        )

    def test_a_vm_boots_from_virtual_media_never_the_pxe_vm(self):
        assert KUBEVIRT_BMC_VENDOR in BMC_VENDORS
        assert boots_from_network(KUBEVIRT_BMC_VENDOR) is False

    @pytest.mark.parametrize(
        ("template", "words"),
        [
            ("", "empty"),
            ("kubevirt-redfish.apps.example.com", "exactly once"),
            ("{kv_cluster}.{kv_cluster}.example.com", "exactly once"),
            ("redfish.{kv_cluster}.{site}.example.com", "nothing else in braces"),
            ("https://redfish.{kv_cluster}.example.com", "no scheme, no path"),
            ("redfish.{kv_cluster}.example.com/redfish/v1", "no scheme, no path"),
            ("redfish.{kv_clustr}.example.com", "exactly once"),
        ],
    )
    def test_a_pattern_that_is_not_one_host_naming_the_kv_cluster(self, template, words):
        assert words in kubevirt_bmc_host_template_problem(template)

    @pytest.mark.parametrize("template", [TEMPLATE, "redfish.{kv_cluster}.lab:8443", "{kv_cluster}"])
    def test_a_pattern_that_is(self, template):
        assert kubevirt_bmc_host_template_problem(template) is None

    def test_filling_it_never_raises_whatever_the_names_hold(self):
        """Workflow code fills it: a brace in a name must not be a KeyError there."""
        endpoint = kubevirt_bmc_endpoint(TEMPLATE, "{odd}", "{vm}")
        assert "{odd}" in endpoint.host and endpoint.path.endswith("{vm}")


class TestWhatTheSizeLabelSays:
    @pytest.mark.parametrize(("size", "kind"), [
        ("mtl", InfraEnvKind.METAL), ("kv32", InfraEnvKind.KUBEVIRT),
        # A mistyped VM size is still a VM InfraEnv — refused, never drawn from server-scan.
        ("kv7", InfraEnvKind.KUBEVIRT), ("kvx", InfraEnvKind.KUBEVIRT), ("kv", InfraEnvKind.KUBEVIRT),
        ("", InfraEnvKind.METAL), ("64c", InfraEnvKind.METAL),
    ])
    def test_kv_is_a_vm_anything_else_bare_metal(self, size, kind):
        assert size_kind(size) is kind

    @pytest.mark.parametrize(("size", "words"), [
        ("kv7", "an odd number"), ("kvx", "is not `kv<cpu>`"), ("kv", "is not `kv<cpu>`"),
        ("kv0", "is not `kv<cpu>`"), ("kv512", "2..256"), ("kv032", "is not `kv<cpu>`"),
    ])
    def test_a_size_no_vm_can_be_made_of(self, size, words):
        assert words in kubevirt_size_problem(size)

    @pytest.mark.parametrize("cpu", [2, 8, 32, 64, 256])
    def test_the_label_and_the_name_follow_one_rule(self, cpu):
        """The size a name's parser accepts is the size the label's check accepts."""
        size = f"kv{cpu}"
        assert kubevirt_size_problem(size) is None and kubevirt_size_cpu(size) == cpu
        assert parse_infraenv(f"c200-{size}-az1").cpu == cpu

    @pytest.mark.parametrize("size", ["kv7", "kv512"])
    def test_and_refuse_the_same_ones(self, size):
        assert kubevirt_size_problem(size) is not None
        with pytest.raises(ValueError):
            parse_infraenv(f"c200-{size}-az1")

    def test_the_other_parts(self):
        assert flavor_problem("c200n") is None and zone_problem("az12") is None
        assert "c200" in flavor_problem("C200") and "az<N>" in zone_problem("zone-a")
        assert vm_name_problem("c200", 32, "five") is None
        assert "DNS-1123" in vm_name_problem("c200", 32, "a" * 60)


class TestWhatAVmInfraEnvAsksCreateVmFor:
    def _derive(self, labels=LABELS, mce=MCE):
        return vm_install.derive(labels, mce_cluster=mce, infra_env="any-name", namespace=NAMESPACE)

    def test_the_labels_and_the_mces_name(self):
        asked = self._derive().input
        assert (asked.flavor, asked.cpu, asked.memory_gib, asked.zone) == ("c200", 32, 240, "az1")
        assert (asked.site, asked.lifecycle) == ("five", "prod")
        assert (asked.mce_cluster, asked.infra_env, asked.namespace) == (MCE, "any-name", NAMESPACE)

    def test_it_is_what_create_vms_own_route_derives_from_a_conventional_name(self):
        from workflow_domains.vm_lifecycle.router import CreateVmRequest

        from_route = CreateVmRequest(infra_env=INFRA_ENV, mce_cluster=MCE, namespace=NAMESPACE).run_input()
        from_labels = vm_install.derive(LABELS, mce_cluster=MCE, infra_env=INFRA_ENV, namespace=NAMESPACE).input
        assert from_labels == from_route

    def test_is_kubevirt(self):
        assert vm_install.is_kubevirt(LABELS)
        assert not vm_install.is_kubevirt({**LABELS, "redbull.io/size": "mtl"})
        assert not vm_install.is_kubevirt({})

    def test_a_site_of_several_words(self):
        asked = self._derive({**LABELS, "redbull.io/site": "bat-yam"}, "ocp4-prep-mce-bat-yam-b").input
        assert (asked.site, asked.lifecycle) == ("bat-yam", "prep")

    def test_every_label_problem_at_once_and_nothing_raised(self):
        wanted = self._derive({"redbull.io/size": "kv7", "redbull.io/site": "haifa"})
        assert wanted.input is None and wanted.mce_problem is None
        said = "; ".join(wanted.label_problems)
        for words in ("redbull.io/flavor is missing", "an odd number", "zone is missing", "is not its MCE's site"):
            assert words in said

    def test_an_mce_named_outside_the_convention(self):
        wanted = self._derive(mce="ocp4-mce-bat-yam-01")
        assert wanted.input is None and wanted.label_problems == []
        assert "ocp4-<lifecycle>-mce-<site>-<id>" in wanted.mce_problem

    def test_a_vm_whose_name_would_not_fit(self):
        long_site = "a" * 50
        wanted = self._derive({**LABELS, "redbull.io/site": long_site}, f"ocp4-prod-mce-{long_site}-a")
        assert wanted.input is None and "no VM of it can be named" in wanted.label_problems[0]


class TestTheChildsId:
    def test_it_is_derived_from_the_parent_and_is_never_a_slot_of_the_route(self):
        parent = f"add-server-to-mce-{MCE}_{INFRA_ENV}_3"
        child = create_vm_child_workflow_id(parent)
        assert child == f"create-vm-for-{parent}"
        assert child != create_vm_slot_workflow_id(MCE, INFRA_ENV, 3)
        # A route's slot id starts `create-vm-<mce>_`, and a canonical MCE starts `ocp4-`.
        assert not child.startswith("create-vm-ocp4-")


class TestTheWorkersSettings:
    def test_the_three_keys_are_optional(self, monkeypatch):
        for key in ("KUBEVIRT_BMC_HOST_TEMPLATE", "KUBEVIRT_BMC_USERNAME", "KUBEVIRT_BMC_PASSWORD"):
            monkeypatch.delenv(key, raising=False)
        settings = ServerLifecycleActivitySettings(_env_file=None)
        assert (settings.kubevirt_bmc_host_template, settings.kubevirt_bmc_username) == ("", "")

    @pytest.mark.parametrize("template", ["https://redfish.{kv_cluster}.x", "redfish.{kv_clustr}.x", "redfish.x"])
    def test_a_bad_pattern_fails_at_worker_start(self, monkeypatch, template):
        monkeypatch.setenv("KUBEVIRT_BMC_HOST_TEMPLATE", template)
        with pytest.raises(ValidationError, match="kubevirt_bmc_host_template"):
            ServerLifecycleActivitySettings(_env_file=None)

    def test_a_good_one_is_kept(self, monkeypatch):
        monkeypatch.setenv("KUBEVIRT_BMC_HOST_TEMPLATE", f"  {TEMPLATE}  ")
        assert ServerLifecycleActivitySettings(_env_file=None).kubevirt_bmc_host_template == TEMPLATE


class TestTheWorkerAnswersBeforeAnyVmIsMade:
    REQUEST = KubevirtBmcConfigRequest(infra_env=INFRA_ENV, namespace=NAMESPACE)

    async def test_configured_it_hands_over_the_pattern_and_no_credential(self):
        config = await ActivityEnvironment().run(activities.read_kubevirt_bmc_config, self.REQUEST)
        assert config.host_template == activities._settings.kubevirt_bmc_host_template
        assert "{kv_cluster}" in config.host_template
        assert set(config.model_dump()) == {"host_template"}

    @pytest.mark.parametrize(
        ("missing", "named"),
        [
            (("kubevirt_bmc_host_template",), ["KUBEVIRT_BMC_HOST_TEMPLATE"]),
            (("kubevirt_bmc_password",), ["KUBEVIRT_BMC_PASSWORD"]),
            (
                ("kubevirt_bmc_host_template", "kubevirt_bmc_username", "kubevirt_bmc_password"),
                ["KUBEVIRT_BMC_HOST_TEMPLATE", "KUBEVIRT_BMC_USERNAME", "KUBEVIRT_BMC_PASSWORD"],
            ),
        ],
    )
    async def test_not_configured_it_names_every_missing_key(self, monkeypatch, missing, named):
        for key in missing:
            monkeypatch.setattr(activities._settings, key, "")
        with pytest.raises(KubevirtBmcNotConfiguredError) as refused:
            await ActivityEnvironment().run(activities.read_kubevirt_bmc_config, self.REQUEST)
        for key in named:
            assert key in str(refused.value)
        assert "no VM was created" in str(refused.value)

    def test_the_secret_takes_the_kubevirt_pair(self):
        assert activities._bmc_credentials(KUBEVIRT_BMC_VENDOR) == (
            activities._settings.kubevirt_bmc_username,
            activities._settings.kubevirt_bmc_password,
        )
