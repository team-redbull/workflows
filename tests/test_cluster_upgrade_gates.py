"""Unit tests for the cluster-upgrade checks and rollout rules (workflow_domains/cluster_upgrade/gates.py).

Pure functions, so no Temporal harness: every planning and pre-flight check is
tried passing and failing, and the consequence of each failure — fail the run,
ask a person, ask an owner, wait — is pinned, because the workflow acts on it.
"""

from __future__ import annotations

import pytest

from shared.models.cluster_upgrade import (
    AdminAckRule,
    AdminAckState,
    ApiUsage,
    ArgoApplicationState,
    CheckFailure,
    CheckResult,
    ClusterProfile,
    ClusterUpgradeSettings,
    Day1Cluster,
    Day1NodePool,
    Day2LayerGap,
    Day2Layers,
    HostedClusterState,
    HostedHealth,
    MceState,
    NodePoolState,
    PinnedReleases,
    StepKind,
    StepReason,
    UpgradePlan,
    UpgradeRules,
    UpgradeStep,
)
from tests.cluster_upgrade_fakes import IMAGE_PATH, LADDER, MIRRORS, RELEASE, SUPPORTED, digest, request
from workflow_domains.cluster_upgrade import gates


def _day1(*, nodepools=None, zones=None, upgrade=None, **kwargs) -> Day1Cluster:
    names = nodepools or ["np-a", "np-b"]
    zones = zones or {}
    fields = {
        "path": "sites/s/mces/m/hostedClusters/c.yaml", "revision": "r1", "blob": "b1",
        "mastertag": "4.18.56", "image_path": IMAGE_PATH,
        "nodepools": [Day1NodePool(name=n, workertag="4.18.56", zone=zones.get(n)) for n in names],
        "upgrade": upgrade or ClusterUpgradeSettings(),
    }
    fields.update(kwargs)
    return Day1Cluster(**fields)


def _hosted(*, versions=None, upgrade_type="InPlace", **kwargs) -> HostedClusterState:
    versions = versions or {"np-a": "4.18.56", "np-b": "4.18.56"}
    fields = {
        "found": True, "control_plane_version": "4.18.56", "progressing": False,
        "conditions": {"ClusterVersionProgressing": "False", "ClusterVersionSucceeding": "True"},
        "image_content_mirrors": MIRRORS,
        "nodepools": [NodePoolState(name=n, version=v, upgrade_type=upgrade_type) for n, v in versions.items()],
    }
    fields.update(kwargs)
    return HostedClusterState(**fields)


def _rules(**kwargs) -> UpgradeRules:
    fields = {
        "revision": "r1", "ladder": LADDER, "max_worker_skew": 2, "release": RELEASE,
        "mce_version": "2.17.3", "supported_hosted_versions": SUPPORTED, "max_concurrent_control_planes": 2,
    }
    fields.update(kwargs)
    return UpgradeRules(**fields)


PLAN = UpgradePlan(
    control_plane="4.18.56", nodepools={"np-a": "4.18.56", "np-b": "4.18.56"}, target="4.20.44",
    max_worker_skew=2,
    steps=[
        UpgradeStep(kind=StepKind.CONTROL_PLANE, version="4.19.53", reason=StepReason.LADDER),
        UpgradeStep(kind=StepKind.CONTROL_PLANE, version="4.20.44", reason=StepReason.TARGET),
        UpgradeStep(kind=StepKind.WORKERS, version="4.20.44", reason=StepReason.FINAL, nodepools=["np-a", "np-b"]),
    ],
)
ALL_PINNED = PinnedReleases(pinned={"4.19.53": digest("4.19.53"), "4.20.44": digest("4.20.44")})


def _planning(**overrides) -> dict[str, CheckResult]:
    arguments = {
        "request": request(), "rules": _rules(),
        "mce": MceState(supported_versions_found=True, supported_versions=SUPPORTED),
        "day1": _day1(), "hosted": _hosted(), "plan": PLAN, "pinned": ALL_PINNED, "day2": Day2Layers(),
    }
    arguments.update(overrides)
    checks = gates.planning_checks(**arguments)
    assert all(check.on_failure is CheckFailure.FAILED for check in checks)
    return {check.name: check for check in checks}


class TestPlanningChecks:
    def test_a_sound_cluster_passes_every_check(self):
        assert gates.failed(list(_planning().values())) == []

    def test_rules_that_disagree_with_the_mce_fail(self):
        check = _planning(mce=MceState(supported_versions_found=True, supported_versions=["4.20", "4.19"]))[
            "rules-match-mce"
        ]
        assert not check.passed and "the MCE reports" in check.observed

    def test_a_missing_supported_versions_configmap_fails(self):
        check = _planning(mce=MceState(supported_versions_found=False))["rules-match-mce"]
        assert not check.passed and "not found" in check.observed

    def test_a_step_into_a_minor_the_mce_cannot_host_fails(self):
        narrow = ["4.19", "4.18"]
        check = _planning(
            rules=_rules(supported_hosted_versions=narrow),
            mce=MceState(supported_versions_found=True, supported_versions=narrow),
        )["rules-match-mce"]
        assert not check.passed and "4.20.44" in check.observed

    def test_a_release_missing_from_the_mirror_fails_with_the_snippet_that_mirrors_it(self):
        check = _planning(pinned=PinnedReleases(pinned={"4.20.44": digest("4.20.44")}, missing=["4.19.53"]))[
            "mirror-has-every-step"
        ]
        assert not check.passed
        assert "mirror 4.19.53-x86_64 first" in check.observed
        assert "- name: stable-4.19" in check.observed and "maxVersion: 4.19.53" in check.observed

    def test_an_incomplete_payload_fails(self):
        check = _planning(pinned=ALL_PINNED.model_copy(update={"problems": ["4.20.44: 3 images missing"]}))[
            "mirror-has-every-step"
        ]
        assert not check.passed and "3 images missing" in check.observed

    def test_a_release_pinning_forgot_counts_as_missing(self):
        check = _planning(pinned=PinnedReleases(pinned={"4.20.44": digest("4.20.44")}))["mirror-has-every-step"]
        assert not check.passed and "4.19.53" in check.observed

    @pytest.mark.parametrize(
        ("overrides", "needle"),
        [
            ({"day1": _day1(problems=["line 12: key nodepool written twice"])}, "written twice"),
            ({"hosted": _hosted(upgrade_type="Replace")}, "upgradeType is Replace"),
            ({"day1": _day1(image_path="registry-internal/ocp-4-18")}, "move sites/s/mces/m/hostedClusters/c.yaml to the full mirror"),
            ({"day1": _day1(image_hc="registry-internal/other")}, "imageHc is registry-internal/other"),
            ({"day1": _day1(nodepools=["np-a"])}, "the cluster has ['np-a', 'np-b']"),
        ],
    )
    def test_a_cluster_file_the_run_cannot_upgrade_as_written_fails(self, overrides, needle):
        check = _planning(**overrides)["day1-file-upgradeable"]
        assert not check.passed and needle in check.observed

    def test_git_and_the_cluster_disagreeing_fails(self):
        check = _planning(hosted=_hosted(versions={"np-a": "4.18.56", "np-b": "4.17.58"}))["git-matches-cluster"]
        assert not check.passed and "NodePool np-b: the file says 4.18.56, it runs 4.17.58" in check.observed

    def test_a_nodepool_inheriting_mastertag_reads_as_mastertag(self):
        day1 = _day1()
        day1.nodepools[0].workertag = None
        assert gates.effective_workertag(day1, "np-a") == "4.18.56"
        day1.top_level_workertag = "4.17.58"
        assert gates.effective_workertag(day1, "np-a") == "4.17.58"

    def test_a_missing_day2_layer_fails_naming_the_chart_and_sig(self):
        gap = Day2LayerGap(sig="sig-storage", chart="odf", version="4.19.53")
        check = _planning(day2=Day2Layers(missing=[gap]))["day2-layers"]
        assert not check.passed and "create ocp-versions/4.19.53/ for odf in sig-storage" in check.observed

    @pytest.mark.parametrize(("wave", "passes"), [(0, True), (1, True), (2, False), (None, False)])
    def test_auto_approve_is_for_waves_zero_and_one_only(self, wave, passes):
        check = _planning(
            request=request(auto_approve=True), day1=_day1(upgrade=ClusterUpgradeSettings(wave=wave))
        )["auto-approve-wave"]
        assert check.passed is passes


def _preflight(*, argo=None, hosted=None, health=None, mce=None, include_idms=True) -> dict[str, CheckResult]:
    checks = gates.preflight_checks(
        argo=argo or ArgoApplicationState(found=True, sync_status="Synced", health_status="Healthy"),
        hosted=hosted or _hosted(),
        health=health or HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS),
        mce=mce or MceState(supported_versions_found=True, idms_mirrors=MIRRORS),
        release=RELEASE,
        include_idms=include_idms,
    )
    return {check.name: check for check in checks}


class TestPreflightChecks:
    def test_a_healthy_cluster_passes_every_check(self):
        assert gates.worst(list(_preflight().values())) is None

    @pytest.mark.parametrize(
        ("kwargs", "name", "consequence"),
        [
            ({"argo": ArgoApplicationState(found=True, sync_status="OutOfSync", health_status="Healthy",
                                           out_of_sync=["ConfigMap/x"])}, "argo-app-clean", CheckFailure.NEEDS_HUMAN),
            ({"health": HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
                                     operators_unhealthy=["ingress"])}, "operators-healthy", CheckFailure.NEEDS_HUMAN),
            ({"health": HostedHealth(reachable=True, cluster_version_upgradeable=False, idms_mirrors=MIRRORS,
                                     upgradeable_reason="ClusterOperatorsNotUpgradeable",
                                     upgradeable_message="Cluster operator storage should not be upgraded")},
             "cluster-version-upgradeable", CheckFailure.NEEDS_HUMAN),
            ({"hosted": _hosted(progressing=True)}, "control-plane-idle", CheckFailure.WAIT),
            ({"health": HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
                                     nodes_not_ready=["w-1"], pending_csrs=2)}, "nodes-healthy", CheckFailure.NEEDS_HUMAN),
            ({"health": HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
                                     blocking_pdbs=["db/pg"])}, "pdbs-allow-disruption", CheckFailure.NOTIFY_OWNER),
            ({"health": HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
                                     dead_webhooks=["kyverno"])}, "webhooks-have-endpoints", CheckFailure.NEEDS_HUMAN),
            ({"mce": MceState(supported_versions_found=True, idms_mirrors=MIRRORS[:1])},
             "idms-covers-release", CheckFailure.FAILED),
        ],
    )
    def test_each_failure_has_its_own_consequence(self, kwargs, name, consequence):
        checks = _preflight(**kwargs)
        assert [check.name for check in gates.failed(list(checks.values()))] == [name]
        assert checks[name].on_failure is consequence

    def test_a_rollout_the_cvo_has_taken_is_not_idle(self):
        # What a 4.17 -> 4.18 upgrade looked like on a live HostedCluster ten
        # minutes in: Progressing already False, the CVO still rolling.
        mid_rollout = _hosted(
            progressing=False, control_plane_version=None,
            conditions={"ClusterVersionProgressing": "True", "ClusterVersionSucceeding": "True"},
        )
        check = _preflight(hosted=mid_rollout)["control-plane-idle"]
        assert not check.passed and check.on_failure is CheckFailure.WAIT
        assert "not Completed" in check.observed and "ClusterVersionProgressing=True" in check.observed

    def test_an_update_still_running_waits_instead_of_asking_a_person(self):
        # A 4.19 CVO says Upgradeable=False, UpdateInProgress, for its whole
        # rollout and up to two minutes after it.
        checks = _preflight(health=HostedHealth(
            reachable=True, cluster_version_upgradeable=False, idms_mirrors=MIRRORS,
            upgradeable_reason="UpdateInProgress",
            upgradeable_message="An update is already in progress and the details are in the Progressing condition",
        ))
        check = checks["cluster-version-upgradeable"]
        assert not check.passed and check.on_failure is CheckFailure.WAIT
        assert check.observed.startswith("UpdateInProgress: ")
        assert gates.worst(list(checks.values())) is CheckFailure.WAIT

    def test_a_missing_admin_ack_alone_stops_no_preflight(self):
        # What the 4.19.53 lab cluster said (CONTEXT §8.11). The admin-ack step
        # answers it before the next control-plane minor step, and HyperShift
        # rolls NodePools regardless (§8.13), so neither waits on a person here.
        check = _preflight(health=HostedHealth(
            reachable=True, cluster_version_upgradeable=False, idms_mirrors=MIRRORS,
            upgradeable_reason="AdminAckRequired",
            upgradeable_message="The admissionregistration.k8s.io/v1beta1 group version is deprecated in 4.19",
        ))["cluster-version-upgradeable"]
        assert check.passed
        assert check.observed.startswith("AdminAckRequired: ") and "before the next control-plane minor step" in check.observed

    def test_an_admin_ack_during_a_running_update_waits(self):
        # Two checks failing: the CVO says MultipleReasons on Upgradeable and
        # keeps one condition per reason, which the reader lists.
        check = _preflight(health=HostedHealth(
            reachable=True, cluster_version_upgradeable=False, idms_mirrors=MIRRORS,
            upgradeable_reason="MultipleReasons", upgradeable_reasons=["AdminAckRequired", "UpdateInProgress"],
        ))["cluster-version-upgradeable"]
        assert not check.passed and check.on_failure is CheckFailure.WAIT

    def test_an_admin_ack_beside_another_reason_asks_a_person(self):
        check = _preflight(health=HostedHealth(
            reachable=True, cluster_version_upgradeable=False, idms_mirrors=MIRRORS,
            upgradeable_reason="MultipleReasons",
            upgradeable_reasons=["AdminAckRequired", "ClusterOperatorsNotUpgradeable"],
        ))["cluster-version-upgradeable"]
        assert not check.passed and check.on_failure is CheckFailure.NEEDS_HUMAN

    def test_missing_cvo_conditions_are_not_idle(self):
        check = _preflight(hosted=_hosted(conditions={}))["control-plane-idle"]
        assert not check.passed

    def test_an_unreachable_hosted_api_passes_nothing_it_cannot_read(self):
        checks = _preflight(health=HostedHealth(reachable=False, idms_mirrors=MIRRORS))
        assert {c.name for c in gates.failed(list(checks.values()))} == {
            "operators-healthy", "cluster-version-upgradeable",
        }
        assert "unreachable" in checks["operators-healthy"].observed

    def test_the_idms_check_names_every_gap(self):
        check = _preflight(hosted=_hosted(image_content_mirrors=[]))["idms-covers-release"]
        assert check.observed.count("HostedCluster imageContentSources lacks") == 2

    def test_the_idms_check_runs_only_when_asked(self):
        assert "idms-covers-release" not in _preflight(include_idms=False)

    def test_the_worst_failure_decides(self):
        checks = list(_preflight(
            hosted=_hosted(progressing=True),
            health=HostedHealth(reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
                                blocking_pdbs=["db/pg"]),
            mce=MceState(supported_versions_found=True),
        ).values())
        assert gates.worst(checks) is CheckFailure.FAILED
        assert "idms-covers-release" in gates.describe(checks)


class TestProfileAndOrder:
    def test_zone_labels_make_a_cluster_zoned_unless_the_file_says_otherwise(self):
        assert gates.cluster_profile(_day1()) is ClusterProfile.GENERIC
        assert gates.cluster_profile(_day1(zones={"np-a": "a"})) is ClusterProfile.ZONED
        overridden = _day1(zones={"np-a": "a"}, upgrade=ClusterUpgradeSettings(profile=ClusterProfile.GENERIC))
        assert gates.cluster_profile(overridden) is ClusterProfile.GENERIC

    def test_soak_defaults_by_profile_and_the_file_overrides_it(self):
        assert gates.soak_minutes(ClusterProfile.ZONED, _day1()) == 30
        assert gates.soak_minutes(ClusterProfile.GENERIC, _day1()) == 0
        assert gates.soak_minutes(ClusterProfile.ZONED, _day1(upgrade=ClusterUpgradeSettings(soak_minutes=5))) == 5

    def test_the_request_s_order_wins_then_the_file_s_then_the_zones(self):
        names = ["np-c", "np-a", "np-b"]
        zoned = _day1(nodepools=names, zones={"np-a": "b", "np-b": "c", "np-c": "a"})
        assert gates.nodepool_order(requested=["np-b", "np-c", "np-a"], day1=zoned, names=names) == ["np-b", "np-c", "np-a"]
        by_file = zoned.model_copy(update={"upgrade": ClusterUpgradeSettings(nodepool_order=["np-a", "np-b", "np-c"])})
        assert gates.nodepool_order(requested=None, day1=by_file, names=names) == ["np-a", "np-b", "np-c"]
        assert gates.nodepool_order(requested=None, day1=zoned, names=names) == ["np-c", "np-a", "np-b"]

    def test_without_a_zone_on_every_nodepool_the_file_order_stands(self):
        names = ["np-c", "np-a"]
        assert gates.nodepool_order(requested=None, day1=_day1(nodepools=names, zones={"np-a": "a"}), names=names) == names

    @pytest.mark.parametrize("order", [["np-a"], ["np-a", "np-a", "np-b"], ["np-a", "np-b", "np-z"]])
    def test_an_order_must_name_each_nodepool_exactly_once(self, order):
        with pytest.raises(ValueError, match="exactly once"):
            gates.nodepool_order(requested=order, day1=_day1(), names=["np-a", "np-b"])


class TestRollDeadline:
    @pytest.mark.parametrize(
        ("replicas", "max_unavailable", "batches"),
        [
            (3, None, 3),  # unset is 1 at a time
            (3, "0", 3),  # 0 is raised to 1
            (3, "33%", 3),  # 0.99 rounds down to 0, then raised to 1
            (5, "50%", 3),  # 2 at a time
            (4, "100%", 1),
            (6, "2", 3),
            (None, None, 1),
        ],
    )
    def test_one_budget_per_batch_hypershift_takes_down(self, replicas, max_unavailable, batches):
        assert gates.nodepool_roll_deadline_minutes(replicas, max_unavailable) == batches * gates.PER_BATCH_MINUTES


def test_release_templates_expand_to_the_full_mirror():
    assert gates.release_repositories(RELEASE) == [
        f"{IMAGE_PATH}/openshift/release-images", f"{IMAGE_PATH}/openshift/release",
    ]
    assert gates.expand(RELEASE.tag, image_path=IMAGE_PATH, version="4.20.44") == "4.20.44-x86_64"


def test_step_versions_are_each_release_once_in_plan_order():
    assert gates.step_versions(PLAN) == ["4.19.53", "4.20.44"]


GATE = "ack-4.19-admissionregistration-v1beta1-api-removals-in-4.20"
VWC = "validatingwebhookconfigurations.v1beta1.admissionregistration.k8s.io"
VAP = "validatingadmissionpolicies.v1beta1.admissionregistration.k8s.io"
RULE = AdminAckRule(gate=GATE, apis=[VWC, VAP], source="KCS 7130599")
WALKER = "system:serviceaccount:kube-system:generic-garbage-collector"


class TestAdminAcks:
    """D16: which gates the CVO enforces now, and whether the run may ack them itself."""

    @pytest.mark.parametrize(
        ("gates_found", "acks", "pending"),
        [
            ({GATE: "removed in 4.20"}, {}, [GATE]),
            ({GATE: "removed in 4.20"}, {GATE: "true"}, []),
            # Only the exact string "true" acks a gate, as the CVO reads it.
            ({GATE: "removed in 4.20"}, {GATE: "True"}, [GATE]),
            # A gate of another minor does not apply to a 4.19 cluster.
            ({"ack-4.18-kube-1.32-api-removals-in-4.19": "x", GATE: "y"}, {}, [GATE]),
            # A name the CVO cannot parse is its error to report, a person's to read.
            ({"ack-v1beta1-removals": "x"}, {}, ["ack-v1beta1-removals"]),
        ],
    )
    def test_pending_gates_follow_the_cvo(self, gates_found, acks, pending):
        state = AdminAckState(reachable=True, gates=gates_found, acks=acks)
        assert list(gates.pending_admin_gates(state, "4.19.53")) == pending

    def test_an_unused_listed_gate_is_the_run_s_to_ack(self):
        [review] = gates.review_admin_gates({GATE: "m"}, [RULE], [], [WALKER])
        assert review.listed and review.in_use == []

    def test_only_callers_that_are_not_ignored_count(self):
        usage = [
            ApiUsage(api=VWC, username=WALKER, verbs=["list", "watch"], requests=40),
            ApiUsage(api=VAP, username="system:serviceaccount:policy:kyverno", user_agent="kyverno/1.11",
                     verbs=["get"], requests=3),
            ApiUsage(api="ingresses.v1beta1.networking.k8s.io", username="bob", requests=9),  # another API
            ApiUsage(api=VWC, username="alice", requests=0),
        ]
        [review] = gates.review_admin_gates({GATE: "m"}, [RULE], usage, [WALKER])
        assert [use.username for use in review.in_use] == ["system:serviceaccount:policy:kyverno"]
        assert "kyverno/1.11" in gates.describe_api_usage(review.in_use)

    def test_a_gate_the_rules_do_not_list_is_a_person_s(self):
        [review] = gates.review_admin_gates({"ack-4.19-something-else": "m"}, [RULE], [], [])
        assert not review.listed

    def test_every_api_is_asked_about_once(self):
        other = AdminAckRule(gate="ack-4.21-x", apis=[VAP, "flowschemas.v1beta3.flowcontrol.apiserver.k8s.io"])
        assert gates.admin_ack_apis([RULE, other]) == [VWC, VAP, "flowschemas.v1beta3.flowcontrol.apiserver.k8s.io"]

    def test_the_approver_sees_the_acks_on_the_way(self):
        assert gates.admin_acks_on_path(PLAN, [RULE]) == [f"{GATE} before 4.20.44"]
        before_419 = AdminAckRule(gate="ack-4.18-kube-1.32-api-removals-in-4.19", apis=[VAP])
        assert gates.admin_acks_on_path(PLAN, [before_419]) == [
            "ack-4.18-kube-1.32-api-removals-in-4.19 before 4.19.53"
        ]

    def test_a_z_stream_step_needs_no_ack(self):
        z_stream = PLAN.model_copy(update={
            "control_plane": "4.19.20", "target": "4.19.53",
            "steps": [UpgradeStep(kind=StepKind.CONTROL_PLANE, version="4.19.53", reason=StepReason.TARGET)],
        })
        assert gates.admin_acks_on_path(z_stream, [RULE]) == []


@pytest.mark.parametrize(("path", "mce"), [
    ("sites/site1/mces/ocp4-prod-mce-site1-a/hostedClusters/ocp4-prod-herzi-site1.yaml", "ocp4-prod-mce-site1-a"),
    ("sites/s/mces/m/hostedClusters/c.yml", "m"),
    ("clusters/c.yaml", None),
    ("sites/s/mces/m/version.yaml", None),
    ("sites/s/mces/m/hostedClusters/sub/c.yaml", None),
])
def test_the_mce_is_the_folder_day1_keeps_the_cluster_file_in(path, mce):
    assert gates.mce_of_day1_path(path) == mce

