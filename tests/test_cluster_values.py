"""cluster_values: what provision-cluster refuses, what it writes, and what a re-run accepts.

Pure functions over strings and a tmp_path day1 layout — no git, no Temporal.
Three contracts: validate_cluster_values collects EVERY reason into one
ClusterValuesRejectedError; assemble_cluster_file appends the allocation
block to the person's text byte for byte; same_cluster_file judges a branch
file semantically, with the day1 chain's appended mcConfigMap set aside.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from activities.cluster_lifecycle import cluster_values
from activities.cluster_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import ClusterValuesRejectedError
from workflow_domains.cluster_lifecycle.router import EXAMPLE_CLUSTER_FILE

MCE = "ocp4-prep-mce-site2-a"
PATH = f"sites/site2/mces/{MCE}/hostedClusters/ocp4-prep-e2e-site2-a.yaml"
DHCP = build_dhcp_values("10.20.90.0/24", [(1, 10), (241, 254)])

CONFIG_VALUES = "platform: agent\nhostInventory: inventory\n"
SITE_VALUES = "siteName: site2\n"
MCE_VALUES = f"mceName: {MCE}\nimagePath: registry.example/ocp\n"


def _layers(root: Path, *, config_values: str = CONFIG_VALUES, mce_values: str = MCE_VALUES) -> None:
    for relative, content in {
        "sites/configValues.yaml": config_values,
        "sites/site2/values.yaml": SITE_VALUES,
        f"sites/site2/mces/{MCE}/values.yaml": mce_values,
    }.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(content)


def _validate(root: Path, text: str, *, path: str = PATH, mce: str = MCE):
    return cluster_values.validate_cluster_values(root, path, text, mce=mce)


def _reasons(root: Path, text: str, **kwargs) -> str:
    with pytest.raises(ClusterValuesRejectedError) as caught:
        _validate(root, text, **kwargs)
    return str(caught.value)


class TestValidation:
    def test_the_swagger_example_is_accepted_and_everything_is_derived(self, tmp_path):
        _layers(tmp_path)
        validated = _validate(tmp_path, EXAMPLE_CLUSTER_FILE)
        assert validated.cluster_name == "prep-e2e-site2-a"
        assert validated.mastertag == "4.22.8"
        assert validated.config_names == [
            "nm-conf", "worker-kubeletconfig-reserved", "nic-ring-buffer", "workers-chrony-configuration",
        ]
        assert validated.day2_sigs == [] and validated.day2_env is None

    def test_every_reason_is_in_the_one_error(self, tmp_path):
        _layers(tmp_path)
        text = "nodepool: []\nvlanId: 7\nmcConfigMap: []\nclusterName: other\n"
        message = _reasons(tmp_path, text)
        for expected in ("`mastertag` is required", "`nodepool` must be a non-empty list", "`owner`",
                         "`vlanId` must not be in the file", "`mcConfigMap` must not be in the file",
                         "`clusterName` 'other' disagrees"):
            assert expected in message, expected
        assert message.startswith(PATH)

    @pytest.mark.parametrize("mastertag", ["4.22", "v4.22.8", "4.22.8.1", "latest", "4.22.8 x86"])
    def test_a_malformed_mastertag_is_refused(self, tmp_path, mastertag):
        _layers(tmp_path)
        assert "`mastertag`" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace("4.22.8", mastertag))

    @pytest.mark.parametrize("mastertag", ["4.22.8", "4.22.8-x86_64", "4.16.27-arm64"])
    def test_a_release_with_or_without_arch_is_accepted(self, tmp_path, mastertag):
        _layers(tmp_path)
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE.replace("4.22.8", mastertag)).mastertag == mastertag

    def test_a_quoted_mastertag_is_read_as_text(self, tmp_path):
        _layers(tmp_path)
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE.replace("4.22.8", '"4.22.8"')).mastertag == "4.22.8"

    @pytest.mark.parametrize(("field", "value"), [("merkaz", "unit-a"), ("anaf", "unit-b"), ("mador", "unit-c")])
    def test_each_owner_field_is_required(self, tmp_path, field, value):
        _layers(tmp_path)
        # an empty value is as missing as an absent key
        assert f"`owner.{field}` is required" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace(f'{field}: "{value}"', f'{field}: ""'))
        assert f"`owner.{field}` is required" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace(f'  {field}: "{value}"\n', ""))

    def test_owner_may_come_from_a_value_layer(self, tmp_path):
        _layers(tmp_path, mce_values=MCE_VALUES + 'owner:\n  merkaz: "m"\n  anaf: "a"\n  mador: "d"\n')
        text = EXAMPLE_CLUSTER_FILE.split("mastertag:")[1]
        assert _validate(tmp_path, "mastertag:" + text).cluster_name == "prep-e2e-site2-a"

    def test_a_kubevirt_pool_without_configs_is_legal(self, tmp_path):
        _layers(tmp_path)
        text = 'owner:\n  merkaz: "m"\n  anaf: "a"\n  mador: "d"\nmastertag: 4.20.18\nnodepool:\n- infraEnvName: x201-zone-a-8\n  replicas: 2\n'
        assert _validate(tmp_path, text).config_names == []

    @pytest.mark.parametrize("name", ["Nm-Conf", "nm conf", "-nm-conf", "nm-conf-", "nm_conf", "a" * 64])
    def test_a_config_name_that_is_not_a_dns_label_is_refused(self, tmp_path, name):
        _layers(tmp_path)
        assert "DNS-1123" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace("- name: nm-conf\n", f"- name: {name}\n"))

    def test_a_config_entry_without_a_name_is_refused(self, tmp_path):
        _layers(tmp_path)
        assert "must be `- name: <config>`" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace("- name: nm-conf\n", "- nm-conf\n"))

    def test_mc_files_wins_over_the_nodepool_configs(self, tmp_path):
        _layers(tmp_path)
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE + "mcFiles:\n- only-this\n").config_names == ["only-this"]

    def test_a_config_shared_by_two_pools_is_listed_once_in_first_seen_order(self, tmp_path):
        _layers(tmp_path)
        text = EXAMPLE_CLUSTER_FILE + "- infraEnvName: other\n  replicas: 1\n  configs:\n  - name: nic-ring-buffer\n  - name: extra\n"
        assert _validate(tmp_path, text).config_names[-2:] == ["workers-chrony-configuration", "extra"]

    @pytest.mark.parametrize("key", ["vlanId: 5", "dhcp_values:\n  network: \"10.0.0.0\"", "mcConfigMap: []", 'AutomateAllocation: "false"'])
    def test_each_key_the_run_or_the_chain_writes_is_refused(self, tmp_path, key):
        _layers(tmp_path)
        assert "must not be in the file" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE + key + "\n")

    def test_an_explicit_cluster_name_equal_to_the_derived_one_is_fine(self, tmp_path):
        _layers(tmp_path)
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE + "clusterName: prep-e2e-site2-a\n").cluster_name == "prep-e2e-site2-a"

    def test_a_cluster_file_prefix_from_config_values_is_honoured(self, tmp_path):
        _layers(tmp_path, config_values=CONFIG_VALUES + "clusterFilePrefix: ocp4-\n")
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE).cluster_name == "prep-e2e-site2-a"

    def test_a_duplicate_top_level_key_is_refused(self, tmp_path):
        _layers(tmp_path)
        assert "written twice" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE + "mastertag: 4.22.9\n")

    def test_a_broken_value_layer_is_a_reason_too(self, tmp_path):
        _layers(tmp_path, mce_values="mceName: [\n")
        assert "not valid YAML" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE)

    @pytest.mark.parametrize("text", ["", "   \n", "- a\n- b\n", "just a string\n"])
    def test_a_file_that_is_not_a_mapping_is_refused(self, tmp_path, text):
        _layers(tmp_path)
        assert "YAML mapping" in _reasons(tmp_path, text)

    def test_tabs_after_a_value_are_a_yaml_error_not_a_crash(self, tmp_path):
        _layers(tmp_path)
        assert "not valid YAML" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE.replace("replicas: 2", "replicas: 2\t"))


class TestDay2:
    def test_sigs_from_the_layers_with_the_environment_from_the_name(self, tmp_path):
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull, data-eng]\n")
        validated = _validate(tmp_path, EXAMPLE_CLUSTER_FILE)
        assert (validated.day2_sigs, validated.day2_env) == (["redbull", "data-eng"], "prep")

    def test_an_explicit_env_wins_and_skips_the_mce_cross_check(self, tmp_path):
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull]\n  env: test\n")
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE).day2_env == "test"

    def test_the_cluster_and_mce_environments_must_agree(self, tmp_path):
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull]\n")
        path = f"sites/site2/mces/{MCE}/hostedClusters/ocp4-prod-e2e-site2-a.yaml"
        assert "disagree" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE, path=path)

    def test_an_mce_folder_outside_the_convention_is_not_cross_checked(self, tmp_path):
        mce = "ocp4-mce-site2"
        for relative, content in {
            "sites/configValues.yaml": CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull]\n",
            f"sites/site2/mces/{mce}/values.yaml": "mceName: ocp4-mce-site2\n",
        }.items():
            (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
            (tmp_path / relative).write_text(content)
        path = f"sites/site2/mces/{mce}/hostedClusters/ocp4-prod-dok-site2-b.yaml"
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE, path=path, mce=mce).day2_env == "prod"

    @pytest.mark.parametrize("stem", ["ocp4-kubevirt-site2-a1", "ocp4-x"])
    def test_an_environment_outside_prod_prep_test_is_refused_only_when_there_are_sigs(self, tmp_path, stem):
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull]\n")
        path = f"sites/site2/mces/{MCE}/hostedClusters/{stem}.yaml"
        assert "set `day2.env`" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE, path=path)
        _layers(tmp_path)  # no sigs: the environment is never derived, nothing refused
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE, path=path).day2_env is None

    @pytest.mark.parametrize("sig", ["Redbull", "red bull", "-x", "a/b", 7])
    def test_a_sig_that_is_not_a_repo_name_is_refused(self, tmp_path, sig):
        _layers(tmp_path, config_values=CONFIG_VALUES + f"day2:\n  copy_to_sigs: [{sig!r}]\n")
        assert "not a sig repo name" in _reasons(tmp_path, EXAMPLE_CLUSTER_FILE)

    def test_a_duplicate_sig_is_listed_once_and_an_empty_list_means_none(self, tmp_path):
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: [redbull, redbull]\n")
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE).day2_sigs == ["redbull"]
        _layers(tmp_path, config_values=CONFIG_VALUES + "day2:\n  copy_to_sigs: []\n")
        assert _validate(tmp_path, EXAMPLE_CLUSTER_FILE).day2_sigs == []

    def test_the_cluster_file_itself_may_name_sigs(self, tmp_path):
        _layers(tmp_path)
        validated = _validate(tmp_path, EXAMPLE_CLUSTER_FILE + "day2:\n  copy_to_sigs: [redbull]\n")
        assert (validated.day2_sigs, validated.day2_env) == (["redbull"], "prep")


class TestTheAssembledFile:
    EXPECTED_BLOCK = """\
# === Added By Provision-Cluster Workflow ===
vlanId: 23

dhcp_values:
  network: "10.20.90.0"
  exclusions:
    - startAddress: "10.20.90.1"
      endAddress: "10.20.90.10"
    - startAddress: "10.20.90.241"
      endAddress: "10.20.90.254\""""

    def test_the_block_follows_the_text_after_one_blank_line(self):
        assembled = cluster_values.assemble_cluster_file(EXAMPLE_CLUSTER_FILE, 23, DHCP)
        assert assembled == EXAMPLE_CLUSTER_FILE + "\n" + self.EXPECTED_BLOCK + "\n"
        # The person's text is byte for byte at the front: comments, quotes, layout.
        assert assembled.startswith(EXAMPLE_CLUSTER_FILE)

    def test_trailing_blank_lines_collapse_to_the_one(self):
        assembled = cluster_values.assemble_cluster_file(EXAMPLE_CLUSTER_FILE + "\n\n\n", 23, DHCP)
        assert assembled == EXAMPLE_CLUSTER_FILE + "\n" + self.EXPECTED_BLOCK + "\n"

    def test_a_text_without_a_final_newline_gets_one(self):
        assembled = cluster_values.assemble_cluster_file("a: 1", 23, DHCP)
        assert assembled == "a: 1\n\n" + self.EXPECTED_BLOCK + "\n"

    def test_a_type_without_exclusions_renders_network_only(self):
        block = cluster_values.render_allocation_block(23, build_dhcp_values("10.20.90.0/24", []))
        assert block == '# === Added By Provision-Cluster Workflow ===\nvlanId: 23\n\ndhcp_values:\n  network: "10.20.90.0"'

    def test_the_assembled_file_is_what_the_validator_derived_plus_the_allocation(self, tmp_path):
        """The block must parse into exactly the two keys the chart and the chain read."""
        from activities.day1_repo import load_yaml

        document, problems = load_yaml(cluster_values.assemble_cluster_file(EXAMPLE_CLUSTER_FILE, 23, DHCP), "f")
        assert problems == []
        assert document["vlanId"] == 23
        assert document["dhcp_values"] == {
            "network": "10.20.90.0",
            "exclusions": [
                {"startAddress": "10.20.90.1", "endAddress": "10.20.90.10"},
                {"startAddress": "10.20.90.241", "endAddress": "10.20.90.254"},
            ],
        }


CHAIN_SECTION = """
# === Added by CI ===
mcConfigMap:
- name: nm-conf
  config: |
    apiVersion: machineconfiguration.openshift.io/v1
    kind: MachineConfig
"""


class TestTheRerunCheck:
    def setup_method(self):
        self.assembled = cluster_values.assemble_cluster_file(EXAMPLE_CLUSTER_FILE, 23, DHCP)

    def test_the_same_text_is_the_same_file(self):
        assert cluster_values.same_cluster_file(self.assembled, self.assembled) is None

    def test_the_chains_appended_mc_config_map_is_not_a_difference(self):
        assert cluster_values.same_cluster_file(self.assembled + CHAIN_SECTION, self.assembled) is None

    def test_the_chains_blank_line_respacing_is_not_a_difference(self):
        respaced = self.assembled.replace("\n\n", "\n").replace("nodepool:", "\nnodepool:") + "\n\n\n" + CHAIN_SECTION
        assert cluster_values.same_cluster_file(respaced, self.assembled) is None

    def test_a_comment_is_not_a_difference(self):
        assert cluster_values.same_cluster_file("# someone's note\n" + self.assembled, self.assembled) is None

    @pytest.mark.parametrize(
        ("change", "key"),
        [
            (("vlanId: 23", "vlanId: 24"), "vlanId"),
            (("replicas: 2", "replicas: 3"), "nodepool"),
            (("4.22.8", "4.22.9"), "mastertag"),
            (('"10.20.90.0"', '"10.20.91.0"'), "dhcp_values"),
        ],
    )
    def test_a_different_spec_vlan_or_network_is_a_difference_naming_the_key(self, change, key):
        reason = cluster_values.same_cluster_file(self.assembled.replace(*change), self.assembled)
        assert reason is not None and key in reason

    def test_an_extra_key_on_the_branch_is_a_difference(self):
        reason = cluster_values.same_cluster_file(self.assembled + "extra: 1\n", self.assembled)
        assert reason is not None and "extra" in reason

    def test_a_branch_file_that_does_not_parse_is_a_difference(self):
        reason = cluster_values.same_cluster_file("a: [\n", self.assembled)
        assert reason is not None and "not valid YAML" in reason

    def test_a_branch_file_that_is_not_a_mapping_is_a_difference(self):
        assert cluster_values.same_cluster_file("- a\n", self.assembled) is not None
