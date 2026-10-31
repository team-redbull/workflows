"""The hub limb's day1 work (activities/upgrade_gitops/day1_repo.py, day1_values.py, upgrade_rules.py).

Against a REAL local bare repo shaped like production day1 (stack-export's
gitops/platform-config): value layers, a production cluster file with every
NodePool pinned, a slim one whose NodePools follow mastertag, the upgrade
rules and a stand-in for day1's rules validator. No network, no Temporal: the
same approach as tests/test_values_repo.py.
"""

from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from activities.upgrade_gitops import day1_repo, day1_values
from shared.exceptions import (
    AmbiguousClusterFileError,
    ClusterFileNotFoundError,
    Day1ChangeRejectedError,
    Day1FileChangedError,
    UpgradeRulesInvalidError,
)
from shared.models.cluster_upgrade import ClusterProfile, Day1ChangeKind, Day1ChangeRequest
from tests.test_git_repo import SECRET, files_holding, serve_over_https

MCE = "ocp4-mce-a"
SITE = "site1"
PROD = "ocp4-prod-alpha-site1"
SLIM = "ocp4-lab-beta-site1"
PROD_PATH = f"sites/{SITE}/mces/{MCE}/hostedClusters/{PROD}.yaml"
SLIM_PATH = f"sites/{SITE}/mces/{MCE}/hostedClusters/{SLIM}.yaml"
AUTHOR = day1_repo.GitIdentity(name="cluster-upgrade-workflow", email="cluster-upgrade-workflow@test")

# A production file as stack-export writes them: its own clusterName, zoned
# NodePools named explicitly, each pinned with its own workertag.
PROD_FILE = """\
clusterName: prod-alpha

platform: agent

hostInventory: inventory

mastertag: 4.18.56-x86_64

dhcp_values:
  network: "192.0.2.61"

nodepool:
- name: alpha-zone-a
  replicas: 3
  nodeLabels:
    topology.kubernetes.io/zone: a
  agentLabelSelector:
    nodeLabelKey: infraenvs.agent-install.openshift.io
    nodeLabelValue: dell-r660-128c
  inPlace:
    maxUnavailable: 1
  configs:
  - name: worker-kubeletconfig-reserved
  workertag: 4.18.56-x86_64
- name: alpha-zone-b
  replicas: 3
  nodeLabels:
    topology.kubernetes.io/zone: b
  agentLabelSelector:
    nodeLabelKey: infraenvs.agent-install.openshift.io
    nodeLabelValue: dell-r660-128c
  workertag: 4.18.56-x86_64 # pinned for the upgrade

# === Added by CI ===

vlanId: 448

mcConfigMap:
- name: worker-kubeletconfig-reserved
  config: |
    apiVersion: machineconfiguration.openshift.io/v1
    kind: KubeletConfig
"""

# A lab file: no clusterName (derived from the file name), NodePools named
# after their InfraEnv, following mastertag, which the MCE layer also sets.
SLIM_FILE = """\
platform: kubevirt
mastertag: 4.18.56
upgrade:
  wave: 1
nodepool:
  - infraEnvName: kv-small
    replicas: 2
  - infraEnvName: kv-large
"""

# The sandbox demo's file, its keys as platform-day1 holds them (be95968).
DEMO_FILE = """\
platform: kubevirt
kubevirtInfra: local

mastertag: 4.19.53-x86_64

imagePath: quay.io/openshift-release-dev
releaseRepo: ocp-release
cpoRepo: ""

baseDomain: ""

nodepool:
  - name: upgrade-demo-workers
    replicas: 2
    labels:
      allowDeletion: false
    workertag: 4.19.53-x86_64
    upgradeType: InPlace
    inPlace:
      maxUnavailable: 1
    cores: 2
    memory: 8Gi

upgrade:
  profile: generic
  wave: 0
"""

LADDER = """\
schemaVersion: 1
maxWorkerSkew: 2
verifiedAgainstGraph: "2026-10-31"
release:
  imagePath: quay.io/openshift-release-dev
  repo: "{imagePath}/ocp-release"
  tag: "{version}-x86_64"
  mirrored: false
ladder:
  - { version: 4.18.56, must: true }
  - { version: 4.19.53, must: true, dataPlane: required }
adminAcks:
  - gate: ack-4.19-admissionregistration-v1beta1-api-removals-in-4.20
    source: KCS 7130599
    apis: [validatingwebhookconfigurations.v1beta1.admissionregistration.k8s.io]
apiUsageIgnoredUsers: [system:kube-controller-manager]
"""

MCE_RULES = """\
mce: { version: 2.17.3, supportedHostedVersions: ["4.20", "4.19", "4.18"] }
maxConcurrentControlPlanes: 2
"""

# Stands in for day1's scripts/validate_upgrade_rules.py: refuses the rules
# when the ladder says so, and proves it was run with --repo-root.
RULES_VALIDATOR = """\
import pathlib, sys
root = pathlib.Path(sys.argv[sys.argv.index("--repo-root") + 1])
if "REFUSE" in (root / "upgrade/ladder.yaml").read_text():
    print("upgrade/ladder.yaml: ladder[0] 4.18.56 is refused")
    sys.exit(1)
print("upgrade rules: valid")
"""

# Stands in for day1's cluster validator (DAY1_VALIDATE_COMMAND).
CLUSTER_VALIDATOR = """\
import pathlib, sys
for path in pathlib.Path("sites").glob("*/mces/*/hostedClusters/*.yaml"):
    if "4.99" in path.read_text():
        print(f"FAIL  {path.stem}: no such release")
        sys.exit(1)
print("SUCCESS: all clusters render cleanly")
"""


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _commit_all(checkout: Path, message: str) -> None:
    _git("add", "-A", cwd=checkout)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", message, cwd=checkout)
    _git("push", "-q", "origin", "HEAD:main", cwd=checkout)


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    bare = tmp_path / "day1.git"
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / "seed"
    _git("clone", "-q", str(bare), str(seed), cwd=tmp_path)
    files = {
        "sites/configValues.yaml": "dhcp_values:\n  leaseDurationDays: 8\n",
        f"sites/{SITE}/values.yaml": "imagePath: registry-internal/per-site\n",
        f"sites/{SITE}/mces/{MCE}/values.yaml": "imagePath: quay.io/openshift-release-dev\nmastertag: 4.16.27-x86_64\n",
        PROD_PATH: PROD_FILE,
        SLIM_PATH: SLIM_FILE,
        "upgrade/ladder.yaml": LADDER,
        f"upgrade/mces/{MCE}.yaml": MCE_RULES,
        "scripts/validate_upgrade_rules.py": RULES_VALIDATOR,
        "scripts/validate_all_clusters.py": CLUSTER_VALIDATOR,
    }
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _commit_all(seed, "seed day1")
    return bare


def _main_file(origin: Path, tmp_path: Path, relative: str, name: str = "check") -> str:
    checkout = tmp_path / name
    _git("clone", "-q", str(origin), str(checkout), cwd=tmp_path)
    return (checkout / relative).read_text()


def _blob(origin: Path, relative: str) -> str:
    return _git("rev-parse", f"main:{relative}", cwd=origin).strip()


def _edit_by_hand(origin: Path, tmp_path: Path, relative: str, edit, name: str = "person") -> None:
    checkout = tmp_path / name
    _git("clone", "-q", str(origin), str(checkout), cwd=tmp_path)
    path = checkout / relative
    path.write_text(edit(path.read_text()))
    _commit_all(checkout, "a person's edit")


def _request(change: Day1ChangeKind, *, path=PROD_PATH, cluster=PROD, expected_blob="", **fields) -> Day1ChangeRequest:
    return Day1ChangeRequest(
        workflow_id=f"upgrade-cluster-{cluster}", mce_cluster=MCE, cluster=cluster, path=path,
        expected_blob=expected_blob, change=change, **fields,
    )


async def _commit(origin: Path, request: Day1ChangeRequest, *, validate: str = "", blob: str | None = None):
    if request.expected_blob == "":
        request = request.model_copy(update={"expected_blob": blob or _blob(origin, request.path)})
    return await day1_repo.commit_change(
        repo_url=str(origin), token="", request=request, validate_command=validate, author=AUTHOR,
    )


def _changed_lines(before: str, after: str) -> tuple[list[str], list[str]]:
    removed = [line for line in before.splitlines() if line not in after.splitlines()]
    added = [line for line in after.splitlines() if line not in before.splitlines()]
    return removed, added


# --- read_upgrade_rules ----------------------------------------------------------------------


class TestReadingTheRules:
    async def test_the_ladder_and_the_mce_file_after_the_validator_passed_them(self, origin):
        rules = await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)
        assert rules.revision == _git("rev-parse", "main", cwd=origin).strip()
        assert [(r.version, r.must, r.data_plane_required) for r in rules.ladder] == [
            ("4.18.56", True, False), ("4.19.53", True, True),
        ]
        assert rules.max_worker_skew == 2 and rules.verified_against_graph == "2026-10-31"
        assert rules.mce_version == "2.17.3" and rules.supported_hosted_versions == ["4.20", "4.19", "4.18"]
        assert rules.max_concurrent_control_planes == 2
        assert rules.admin_acks[0].gate.startswith("ack-4.19-") and rules.admin_acks[0].source == "KCS 7130599"
        assert rules.api_usage_ignored_users == ["system:kube-controller-manager"]

    async def test_a_public_release_source_is_not_mirrored_and_names_no_cpo(self, origin):
        release = (await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)).release
        assert release.mirrored is False and release.cpo_repo is None and release.cpo_tag is None
        assert (release.image_path, release.repo, release.tag) == (
            "quay.io/openshift-release-dev", "{imagePath}/ocp-release", "{version}-x86_64",
        )

    async def test_a_mirrored_layout_by_default_with_its_cpo(self, origin, tmp_path):
        _edit_by_hand(origin, tmp_path, "upgrade/ladder.yaml", lambda text: text.replace(
            "  mirrored: false\n", '  cpoRepo: "{imagePath}/release"\n  cpoTag: "{version}-x86_64-hypershift"\n'))
        release = (await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)).release
        assert release.mirrored is True and release.cpo_repo == "{imagePath}/release"

    async def test_a_cpo_repo_without_its_tag_is_refused(self, origin, tmp_path):
        _edit_by_hand(origin, tmp_path, "upgrade/ladder.yaml",
                      lambda text: text.replace("  mirrored: false\n", '  cpoRepo: "{imagePath}/release"\n'))
        with pytest.raises(UpgradeRulesInvalidError, match="cpoRepo and cpoTag go together"):
            await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)

    async def test_the_validators_refusal_is_permanent_and_carries_its_output(self, origin, tmp_path):
        _edit_by_hand(origin, tmp_path, "upgrade/ladder.yaml", lambda text: text + "# REFUSE\n")
        with pytest.raises(UpgradeRulesInvalidError, match="ladder\\[0\\] 4.18.56 is refused"):
            await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)

    async def test_an_mce_with_no_rules_file_is_refused(self, origin):
        with pytest.raises(UpgradeRulesInvalidError, match="upgrade/mces/ocp4-other.yaml"):
            await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster="ocp4-other")

    async def test_no_validator_in_day1_is_refused(self, origin, tmp_path):
        checkout = tmp_path / "drop"
        _git("clone", "-q", str(origin), str(checkout), cwd=tmp_path)
        (checkout / "scripts/validate_upgrade_rules.py").unlink()
        _commit_all(checkout, "drop the validator")
        with pytest.raises(UpgradeRulesInvalidError, match="no scripts/validate_upgrade_rules.py"):
            await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)

    async def test_a_key_written_twice_is_refused(self, origin, tmp_path):
        _edit_by_hand(origin, tmp_path, "upgrade/ladder.yaml", lambda text: text + "maxWorkerSkew: 3\n")
        with pytest.raises(UpgradeRulesInvalidError, match="'maxWorkerSkew' is written twice"):
            await day1_repo.read_rules(repo_url=str(origin), token="", mce_cluster=MCE)


# --- read_day1_cluster ---------------------------------------------------------------------------


class TestReadingAClusterFile:
    async def test_a_production_file_as_the_chart_renders_it(self, origin):
        cluster = await day1_repo.read_cluster(repo_url=str(origin), token="", cluster=PROD)
        assert cluster.path == PROD_PATH and cluster.blob == _blob(origin, PROD_PATH)
        assert cluster.revision == _git("rev-parse", "main", cwd=origin).strip()
        # Its own clusterName names the HostedCluster, not the file name (hc.yaml:34-35).
        assert (cluster.hosted_cluster, cluster.hosted_cluster_namespace) == ("openshift-prod-alpha", "hcp-prod-alpha")
        assert cluster.mastertag == "4.18.56" and cluster.top_level_workertag is None
        # imagePath from the MCE layer, which outranks the site's.
        assert cluster.image_path == "quay.io/openshift-release-dev" and cluster.image_hc is None
        assert [(np.name, np.workertag, np.zone, np.replicas, np.max_unavailable, np.upgrade_type)
                for np in cluster.nodepools] == [
            ("alpha-zone-a", "4.18.56", "a", 3, "1", "InPlace"),
            ("alpha-zone-b", "4.18.56", "b", 3, None, "InPlace"),
        ]
        assert cluster.problems == []

    async def test_a_slim_file_names_everything_after_its_file_and_infraenvs(self, origin):
        cluster = await day1_repo.read_cluster(repo_url=str(origin), token="", cluster=SLIM)
        # File name minus .yaml and clusterFilePrefix (hcAppset.yaml:35).
        assert (cluster.hosted_cluster, cluster.hosted_cluster_namespace) == (
            "openshift-lab-beta-site1", "hcp-lab-beta-site1",
        )
        # <clusterName>-<infraEnvName> (nodepool.yaml:22); the chart's default of one replica.
        assert [(np.name, np.workertag, np.replicas) for np in cluster.nodepools] == [
            ("lab-beta-site1-kv-small", None, 2), ("lab-beta-site1-kv-large", None, 1),
        ]
        # The file's mastertag beats the MCE layer's.
        assert cluster.mastertag == "4.18.56" and cluster.upgrade.wave == 1

    def test_the_sandbox_demo_file_as_platform_day1_holds_it(self, tmp_path):
        """sites/site1/mces/ocp4-mce-workflow-test/hostedClusters/ocp4-upgrade-demo.yaml (platform-day1 be95968)."""
        path = "sites/site1/mces/ocp4-mce-workflow-test/hostedClusters/ocp4-upgrade-demo.yaml"
        (tmp_path / path).parent.mkdir(parents=True)
        (tmp_path / "sites/site1/mces/ocp4-mce-workflow-test/values.yaml").write_text("hostedCluster:\n  enabled: true\n")
        (tmp_path / path).write_text(DEMO_FILE)
        cluster = day1_values.read_cluster(tmp_path, path, revision="r", blob="b")
        assert (cluster.hosted_cluster, cluster.hosted_cluster_namespace) == ("openshift-upgrade-demo", "hcp-upgrade-demo")
        assert (cluster.image_path, cluster.release_repo, cluster.cpo_repo) == (
            "quay.io/openshift-release-dev", "ocp-release", "",
        )
        assert [(np.name, np.workertag, np.replicas, np.max_unavailable) for np in cluster.nodepools] == [
            ("upgrade-demo-workers", "4.19.53", 2, "1"),
        ]
        assert cluster.mastertag == "4.19.53" and cluster.upgrade.wave == 0 and cluster.problems == []

    def test_a_production_file_takes_the_charts_full_mirror_repositories(self, tmp_path):
        (tmp_path / PROD_PATH).parent.mkdir(parents=True)
        (tmp_path / PROD_PATH).write_text(PROD_FILE)
        cluster = day1_values.read_cluster(tmp_path, PROD_PATH, revision="r", blob="b")
        assert (cluster.release_repo, cluster.cpo_repo) == ("openshift/release-images", "openshift/release")

    def test_a_cluster_file_prefix_set_in_config_values_wins(self, tmp_path):
        (tmp_path / "sites" / SITE / "mces" / MCE / "hostedClusters").mkdir(parents=True)
        (tmp_path / "sites/configValues.yaml").write_text("clusterFilePrefix: ocp4-lab-\n")
        (tmp_path / SLIM_PATH).write_text(SLIM_FILE)
        cluster = day1_values.read_cluster(tmp_path, SLIM_PATH, revision="r", blob="b")
        assert cluster.hosted_cluster == "openshift-beta-site1"

    def test_duplicates_and_what_the_chart_refuses_are_problems_with_their_lines(self, tmp_path):
        (tmp_path / "sites" / SITE / "mces" / MCE / "hostedClusters").mkdir(parents=True)
        (tmp_path / PROD_PATH).write_text(
            "mastertag: 4.18.56\n"
            "nodepool:\n- name: dead\n  infraEnvName: x\n"
            "nodepool:\n- name: twin\n  infraEnvName: a\n- name: twin\n  infraEnvName: b\n"
            "- replicas: 2\n"
            "- name: odd\n  infraEnvName: a\n  agentLabelSelector:\n    nodeLabelValue: b\n"
            "upgrade:\n  profile: dok\n  soakMinutes: -5\n  color: blue\n"
        )
        cluster = day1_values.read_cluster(tmp_path, PROD_PATH, revision="r", blob="b")
        joined = "\n".join(cluster.problems)
        assert f"{PROD_PATH}:5: key 'nodepool' is written twice (first at line 2)" in joined
        assert "NodePool twin is named twice (nodepool[0] and [1])" in joined
        assert "nodepool[2]: neither name nor infraEnvName" in joined
        assert "infraEnvName 'a' and agentLabelSelector.nodeLabelValue 'b' disagree" in joined
        assert "upgrade.profile must be one of" in joined and "upgrade.soakMinutes" in joined
        assert "unknown key 'color'" in joined
        # Only the last nodepool block counts, as Helm reads the file.
        assert [np.name for np in cluster.nodepools] == ["twin", "twin", "odd"]

    def test_the_profile_override_is_read(self, tmp_path):
        (tmp_path / "sites" / SITE / "mces" / MCE / "hostedClusters").mkdir(parents=True)
        (tmp_path / SLIM_PATH).write_text(SLIM_FILE.replace("  wave: 1\n", "  wave: 1\n  profile: zoned\n"))
        cluster = day1_values.read_cluster(tmp_path, SLIM_PATH, revision="r", blob="b")
        assert cluster.upgrade.profile is ClusterProfile.ZONED

    def test_a_file_that_does_not_parse_is_a_problem_not_a_crash(self, tmp_path):
        (tmp_path / "sites" / SITE / "mces" / MCE / "hostedClusters").mkdir(parents=True)
        (tmp_path / PROD_PATH).write_text("mastertag: [4.18\n")
        cluster = day1_values.read_cluster(tmp_path, PROD_PATH, revision="r", blob="b")
        assert any("not valid YAML" in problem for problem in cluster.problems)

    async def test_an_unknown_cluster_is_not_found(self, origin):
        with pytest.raises(ClusterFileNotFoundError, match="no ocp4-nope.yaml"):
            await day1_repo.read_cluster(repo_url=str(origin), token="", cluster="ocp4-nope")

    async def test_a_glob_in_the_name_matches_nothing(self, origin):
        with pytest.raises(ClusterFileNotFoundError):
            await day1_repo.read_cluster(repo_url=str(origin), token="", cluster="ocp4-*")

    async def test_a_name_in_two_mces_is_ambiguous(self, origin, tmp_path):
        checkout = tmp_path / "twin"
        _git("clone", "-q", str(origin), str(checkout), cwd=tmp_path)
        twin = checkout / f"sites/site2/mces/ocp4-mce-b/hostedClusters/{PROD}.yaml"
        twin.parent.mkdir(parents=True)
        twin.write_text(PROD_FILE)
        _commit_all(checkout, "a twin")
        with pytest.raises(AmbiguousClusterFileError, match="2 files named"):
            await day1_repo.read_cluster(repo_url=str(origin), token="", cluster=PROD)


# --- commit_day1_change ----------------------------------------------------------------------------


class TestControlPlaneCommits:
    async def test_mastertag_moves_and_nothing_else_of_a_pinned_file(self, origin, tmp_path):
        before = _main_file(origin, tmp_path, PROD_PATH, "before")
        commit = await _commit(origin, _request(
            Day1ChangeKind.CONTROL_PLANE, version="4.19.53",
            nodepool_versions={"alpha-zone-a": "4.18.56", "alpha-zone-b": "4.18.56"},
        ))
        after = _main_file(origin, tmp_path, PROD_PATH, "after")
        assert commit.changed and commit.revision == _git("rev-parse", "main", cwd=origin).strip()
        assert commit.blob == _blob(origin, PROD_PATH)
        assert _changed_lines(before, after) == (["mastertag: 4.18.56-x86_64"], ["mastertag: 4.19.53-x86_64"])
        message = _git("log", "-1", "--format=%an <%ae>|%s", "main", cwd=origin).strip()
        assert message == (
            "cluster-upgrade-workflow <cluster-upgrade-workflow@test>|"
            f"upgrade({PROD}): control plane 4.18.56 → 4.19.53 [upgrade-cluster-{PROD}]"
        )

    async def test_a_repeat_is_success_with_nothing_pushed(self, origin):
        request = _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53",
                           nodepool_versions={"alpha-zone-a": "4.18.56", "alpha-zone-b": "4.18.56"})
        first = await _commit(origin, request)
        # The retry still carries the blob it was planned with: the file already
        # saying so wins over the compare-and-set, or a push that landed would
        # read as someone else's edit.
        again = await _commit(origin, request.model_copy(update={"expected_blob": request.expected_blob}),
                              blob=_blob(origin, PROD_PATH))
        assert first.changed and not again.changed
        assert again.revision == first.revision and again.blob == first.blob

    async def test_a_slim_file_pins_its_workers_with_one_top_level_workertag(self, origin, tmp_path):
        before = _main_file(origin, tmp_path, SLIM_PATH, "before")
        await _commit(origin, _request(
            Day1ChangeKind.CONTROL_PLANE, path=SLIM_PATH, cluster=SLIM, version="4.19.53",
            nodepool_versions={"lab-beta-site1-kv-small": "4.18.56", "lab-beta-site1-kv-large": "4.18.56"},
        ))
        after = _main_file(origin, tmp_path, SLIM_PATH, "after")
        assert after.splitlines()[:3] == ["platform: kubevirt", "mastertag: 4.19.53-x86_64", "workertag: 4.18.56-x86_64"]
        assert _changed_lines(before, after) == (["mastertag: 4.18.56"], [
            "mastertag: 4.19.53-x86_64", "workertag: 4.18.56-x86_64",
        ])

    async def test_someone_elses_edit_of_the_file_is_never_committed_over(self, origin, tmp_path):
        planned = _blob(origin, PROD_PATH)
        _edit_by_hand(origin, tmp_path, PROD_PATH, lambda text: text.replace("replicas: 3", "replicas: 4", 1))
        with pytest.raises(Day1FileChangedError, match="someone else changed it"):
            await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53",
                                           expected_blob=planned))

    async def test_an_edit_of_another_file_does_not_stop_the_run(self, origin, tmp_path):
        planned = _blob(origin, PROD_PATH)
        _edit_by_hand(origin, tmp_path, SLIM_PATH, lambda text: text + "# noted\n")
        commit = await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53",
                                                expected_blob=planned))
        assert commit.changed

    async def test_the_day1_validator_refusing_pushes_nothing(self, origin):
        head = _git("rev-parse", "main", cwd=origin).strip()
        with pytest.raises(Day1ChangeRejectedError, match="FAIL  ocp4-prod-alpha-site1: no such release"):
            await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.99.1"),
                          validate="python3 scripts/validate_all_clusters.py")
        assert _git("rev-parse", "main", cwd=origin).strip() == head

    async def test_the_day1_validator_passing_lets_it_through(self, origin):
        commit = await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53"),
                               validate="python3 scripts/validate_all_clusters.py")
        assert commit.changed

    async def test_a_nodepool_the_file_does_not_name_is_refused(self, origin):
        with pytest.raises(Day1ChangeRejectedError, match="lists no NodePool ghost"):
            await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53",
                                           nodepool_versions={"ghost": "4.18.56"}))

    async def test_a_rejected_push_is_retried_from_a_fresh_clone(self, origin, tmp_path, monkeypatch):
        """Someone pushes elsewhere in day1 between our clone and our push: the retry lands on top."""
        real_exec = day1_repo.git_repo.exec_git
        raced: list[bool] = []

        async def racing_exec(args, **kwargs):
            if args[0] == "push" and not raced:
                raced.append(True)
                _edit_by_hand(origin, tmp_path, SLIM_PATH, lambda text: text + "# concurrent\n", name="racer")
            return await real_exec(args, **kwargs)

        monkeypatch.setattr(day1_repo.git_repo, "exec_git", racing_exec)
        commit = await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53"))
        assert raced and commit.changed
        log = _git("log", "--format=%s", "-2", "main", cwd=origin).splitlines()
        assert log[0].startswith(f"upgrade({PROD})") and log[1] == "a person's edit"


    async def test_day1_code_run_in_the_clone_finds_no_token_and_the_push_still_lands(
        self, origin, tmp_path, monkeypatch
    ):
        """The day1 validator is day1's code: no file of the clone it runs in may hold the token."""
        serve_over_https(tmp_path, monkeypatch)
        hunter = tmp_path / "hunter.py"
        hunter.write_text(TOKEN_HUNTER)
        request = _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53", expected_blob=_blob(origin, PROD_PATH))
        commit = await day1_repo.commit_change(
            repo_url="https://git.test/day1.git", token=SECRET, request=request,
            validate_command=f"{shlex.quote(sys.executable)} {shlex.quote(str(hunter))} {SECRET}", author=AUTHOR,
        )
        assert commit.changed and commit.revision == _git("rev-parse", "main", cwd=origin).strip()
        assert files_holding(origin, SECRET) == []


# Stands in for day1 code that goes looking for a credential: it fails when any
# file of the clone it runs in (`.git/` included) holds the token.
TOKEN_HUNTER = """\
import pathlib, sys
secret = sys.argv[1].encode()
found = [str(p) for p in pathlib.Path(".").rglob("*") if p.is_file() and secret in p.read_bytes()]
print(f"the token is in {found}" if found else "no token in the clone")
sys.exit(1 if found else 0)
"""


class TestNodePoolCommits:
    async def test_a_pinned_nodepool_gets_its_own_line_set(self, origin, tmp_path):
        before = _main_file(origin, tmp_path, PROD_PATH, "before")
        await _commit(origin, _request(Day1ChangeKind.NODEPOOL, nodepool="alpha-zone-b", version="4.19.53"))
        after = _main_file(origin, tmp_path, PROD_PATH, "after")
        # Its comment and quoting survive; the other NodePool's identical line does not move.
        assert _changed_lines(before, after) == (
            ["  workertag: 4.18.56-x86_64 # pinned for the upgrade"],
            ["  workertag: 4.19.53-x86_64 # pinned for the upgrade"],
        )

    async def test_a_nodepool_that_followed_mastertag_gets_a_line_of_its_own(self, origin, tmp_path):
        await _commit(origin, _request(
            Day1ChangeKind.CONTROL_PLANE, path=SLIM_PATH, cluster=SLIM, version="4.19.53",
            nodepool_versions={"lab-beta-site1-kv-small": "4.18.56", "lab-beta-site1-kv-large": "4.18.56"},
        ))
        await _commit(origin, _request(Day1ChangeKind.NODEPOOL, path=SLIM_PATH, cluster=SLIM,
                                       nodepool="lab-beta-site1-kv-large", version="4.19.53"))
        after = _main_file(origin, tmp_path, SLIM_PATH)
        assert after.endswith("  - infraEnvName: kv-large\n    workertag: 4.19.53-x86_64\n")
        cluster = day1_values.read_cluster(tmp_path / "check", SLIM_PATH, revision="r", blob="b")
        assert {np.name: np.workertag for np in cluster.nodepools} == {
            "lab-beta-site1-kv-small": None, "lab-beta-site1-kv-large": "4.19.53",
        }

    async def test_already_at_the_version_is_success(self, origin):
        commit = await _commit(origin, _request(Day1ChangeKind.NODEPOOL, nodepool="alpha-zone-a", version="4.18.56"))
        assert not commit.changed

    async def test_an_inherited_nodepool_list_cannot_be_edited(self, origin, tmp_path):
        checkout = tmp_path / "inherit"
        _git("clone", "-q", str(origin), str(checkout), cwd=tmp_path)
        (checkout / f"sites/{SITE}/mces/{MCE}/values.yaml").write_text(
            "imagePath: quay.io/openshift-release-dev\nnodepool:\n- name: shared\n  infraEnvName: x\n"
        )
        (checkout / SLIM_PATH).write_text("mastertag: 4.18.56\n")
        _commit_all(checkout, "inherit the NodePools")
        with pytest.raises(Day1ChangeRejectedError, match="no nodepool list of its own"):
            await _commit(origin, _request(Day1ChangeKind.NODEPOOL, path=SLIM_PATH, cluster=SLIM,
                                           nodepool="shared", version="4.19.53"))


class TestPauseAndFinish:
    async def test_pause_lands_whatever_the_file_holds(self, origin, tmp_path):
        _edit_by_hand(origin, tmp_path, PROD_PATH, lambda text: text.replace("replicas: 3", "replicas: 5", 1))
        commit = await _commit(origin, _request(Day1ChangeKind.PAUSE, expected_blob=None))
        after = _main_file(origin, tmp_path, PROD_PATH)
        assert commit.changed
        assert 'mastertag: 4.18.56-x86_64\npausedUntil: "true"\n' in after
        again = await _commit(origin, _request(Day1ChangeKind.PAUSE, expected_blob=None))
        assert not again.changed

    async def test_finish_drops_the_top_level_workertag_once_every_nodepool_pins_its_own(self, origin, tmp_path):
        names = {"lab-beta-site1-kv-small": "4.18.56", "lab-beta-site1-kv-large": "4.18.56"}
        await _commit(origin, _request(Day1ChangeKind.CONTROL_PLANE, path=SLIM_PATH, cluster=SLIM,
                                       version="4.19.53", nodepool_versions=names))
        for name in names:
            await _commit(origin, _request(Day1ChangeKind.NODEPOOL, path=SLIM_PATH, cluster=SLIM,
                                           nodepool=name, version="4.19.53"))
        before = _main_file(origin, tmp_path, SLIM_PATH, "before")
        commit = await _commit(origin, _request(Day1ChangeKind.FINISH, path=SLIM_PATH, cluster=SLIM))
        after = _main_file(origin, tmp_path, SLIM_PATH, "after")
        assert commit.changed
        assert _changed_lines(before, after) == (["workertag: 4.18.56-x86_64"], [])
        assert not (await _commit(origin, _request(Day1ChangeKind.FINISH, path=SLIM_PATH, cluster=SLIM))).changed

    async def test_finish_pins_a_nodepool_the_top_level_workertag_still_holds_back(self, origin, tmp_path):
        """workers_to_target false: one NodePool stays behind, and dropping the line must not move it."""
        await _commit(origin, _request(
            Day1ChangeKind.CONTROL_PLANE, path=SLIM_PATH, cluster=SLIM, version="4.19.53",
            nodepool_versions={"lab-beta-site1-kv-small": "4.18.56", "lab-beta-site1-kv-large": "4.18.56"},
        ))
        await _commit(origin, _request(Day1ChangeKind.NODEPOOL, path=SLIM_PATH, cluster=SLIM,
                                       nodepool="lab-beta-site1-kv-large", version="4.19.53"))
        await _commit(origin, _request(Day1ChangeKind.FINISH, path=SLIM_PATH, cluster=SLIM))
        after = _main_file(origin, tmp_path, SLIM_PATH)
        assert "\nworkertag:" not in after
        cluster = day1_values.read_cluster(tmp_path / "check", SLIM_PATH, revision="r", blob="b")
        assert {np.name: np.workertag for np in cluster.nodepools} == {
            "lab-beta-site1-kv-small": "4.18.56", "lab-beta-site1-kv-large": "4.19.53",
        }


class TestTheEditRefusesWhatItCannotDoByLines:
    def _plan(self, tmp_path: Path, text: str, request: Day1ChangeRequest):
        (tmp_path / request.path).parent.mkdir(parents=True, exist_ok=True)
        return day1_values.plan_edit(tmp_path, request, text)

    def test_a_flow_style_nodepool_list(self, tmp_path):
        with pytest.raises(Day1ChangeRejectedError, match="flow style"):
            self._plan(tmp_path, "mastertag: 4.18.56\nnodepool: [{name: a, infraEnvName: x}]\n",
                       _request(Day1ChangeKind.NODEPOOL, nodepool="a", version="4.19.53"))

    def test_mastertag_written_twice(self, tmp_path):
        with pytest.raises(Day1ChangeRejectedError, match="mastertag is written 2 times"):
            self._plan(tmp_path, "mastertag: 4.18.56\nmastertag: 4.18.56\n",
                       _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53"))

    def test_a_block_scalar_where_a_version_should_be(self, tmp_path):
        with pytest.raises(Day1ChangeRejectedError, match="not a plain"):
            self._plan(tmp_path, "mastertag: |\n  4.18.56\n",
                       _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53"))

    def test_the_demos_steps_edit_its_two_version_lines_only(self, tmp_path):
        path = "sites/site1/mces/ocp4-mce-workflow-test/hostedClusters/ocp4-upgrade-demo.yaml"
        cluster = "ocp4-upgrade-demo"
        step = self._plan(tmp_path, DEMO_FILE, _request(
            Day1ChangeKind.CONTROL_PLANE, path=path, cluster=cluster, version="4.20.44",
            nodepool_versions={"upgrade-demo-workers": "4.19.53"},
        ))
        assert _changed_lines(DEMO_FILE, step.text) == (["mastertag: 4.19.53-x86_64"], ["mastertag: 4.20.44-x86_64"])
        roll = self._plan(tmp_path, step.text, _request(
            Day1ChangeKind.NODEPOOL, path=path, cluster=cluster, nodepool="upgrade-demo-workers", version="4.20.44",
        ))
        assert _changed_lines(step.text, roll.text) == (
            ["    workertag: 4.19.53-x86_64"], ["    workertag: 4.20.44-x86_64"],
        )
        assert not self._plan(tmp_path, roll.text, _request(Day1ChangeKind.FINISH, path=path, cluster=cluster)).changed

    def test_a_mastertag_only_a_layer_sets_is_written_into_the_file(self, tmp_path):
        edit = self._plan(tmp_path, "platform: kubevirt\n", _request(Day1ChangeKind.CONTROL_PLANE, version="4.19.53"))
        assert edit.changed and edit.text == "platform: kubevirt\nmastertag: 4.19.53-x86_64\n"
