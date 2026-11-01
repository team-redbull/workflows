"""Unit tests for health_waiver: which of server-scan's refusals allow_unhealthy may waive.

Pure functions, no Temporal. The split they encode is the whole safety of the
hidden option (CLAUDE.md §4): the server's own HEALTH may be waived on request,
and nothing about who else has the machine ever is.
"""

from __future__ import annotations

import pytest

from shared.models.server_lifecycle import (
    AcquiredServer,
    BmcEndpoint,
    InstallCandidateRecord,
    LinkState,
    ServerInterface,
    ServerLockRecord,
    ServerMembership,
)
from workflow_domains.server_lifecycle.bond_selection import select_bond_members
from workflow_domains.server_lifecycle.health_waiver import (
    bond_link_waiver,
    health_findings,
    holds_own_lock,
    safety_refusal,
)

RUN = "add-server-to-mce-ocp4-mce-a_ocp-x"


def _server(*links: LinkState) -> AcquiredServer:
    return AcquiredServer(
        id="srv_x",
        name="ocp-x",
        vendor="dell",
        bmc_vendor="DELL",
        bmc=BmcEndpoint(host="10.0.0.5", host_is_ip=True),
        interfaces=[
            ServerInterface(
                name=f"NIC.Integrated.1-{i}-1",
                mac=f"aa:bb:cc:dd:ee:0{i}",
                location=f"1/{i}/1",
                link_state=link,
            )
            for i, link in enumerate(links, start=1)
        ],
        health_overall="HEALTHY",
    )


def _record(
    *,
    health: dict[str, str] | None = None,
    reachable: bool | None = True,
    lock: ServerLockRecord | None = None,
    **membership,
) -> InstallCandidateRecord:
    return InstallCandidateRecord(
        membership=ServerMembership(
            found=True,
            server_id="srv_x",
            server_name="ocp-x",
            **{"lifecycle_state": "AVAILABLE", **membership},
        ),
        server=_server(LinkState.UP, LinkState.UP),
        health=health if health is not None else {"overall": "HEALTHY", "network": "HEALTHY"},
        reachable=reachable,
        lock=lock or ServerLockRecord(),
    )


class TestSafetyIsNeverWaived:
    def test_a_clean_record_has_no_safety_reason(self):
        assert safety_refusal(_record(), RUN) is None

    def test_a_name_server_scan_does_not_know(self):
        refusal = safety_refusal(InstallCandidateRecord(), RUN)
        assert refusal is not None and "no server of that exact name" in refusal.reason

    @pytest.mark.parametrize("state", ["INSTALLED", "INSTALLED_TO_INVENTORY", "SOMETHING_NEW"])
    def test_any_claim_but_available(self, state):
        # A state server-scan adds later is a claim too, not a pass.
        refusal = safety_refusal(_record(lifecycle_state=state, mce_name="ocp4-b"), RUN)
        assert refusal is not None
        assert f"server-scan reads it {state} (MCE ocp4-b)" in refusal.reason
        assert refusal.another_runs_lock is False

    def test_a_contest(self):
        refusal = safety_refusal(_record(contested_with="hc-b"), RUN)
        assert refusal is not None and "contest" in refusal.reason

    def test_maintenance(self):
        refusal = safety_refusal(_record(maintenance=True, maintenance_reason="DIMM swap"), RUN)
        assert refusal is not None and refusal.reason.endswith(": DIMM swap")

    def test_another_runs_live_lock(self):
        lock = ServerLockRecord(
            live=True, holder="add-server-to-mce", workflow_id="other", mce_cluster="ocp4-b",
            expires_at="2026-10-31T12:00:00Z",
        )
        refusal = safety_refusal(_record(lock=lock), RUN)
        assert refusal is not None and refusal.another_runs_lock is True
        assert "'other'" in refusal.reason and "ocp4-b" in refusal.reason

    def test_its_own_live_lock_is_no_reason(self):
        own = ServerLockRecord(live=True, holder="add-server-to-mce", workflow_id=RUN)
        assert safety_refusal(_record(lock=own), RUN) is None
        assert holds_own_lock(_record(lock=own), RUN) is True

    def test_an_expired_lock_is_no_reason(self):
        stale = ServerLockRecord(live=False, holder="add-server-to-mce", workflow_id="other")
        assert safety_refusal(_record(lock=stale), RUN) is None
        assert holds_own_lock(_record(lock=stale), RUN) is False

    def test_a_claim_is_reported_before_unhealthy_health(self):
        """Health is never even looked at when a safety reason exists."""
        sick = {"overall": "CRITICAL", "network": "MAJOR"}
        assert safety_refusal(_record(health=sick, lifecycle_state="INSTALLED"), RUN) is not None


class TestHealthFindings:
    def test_a_healthy_record_has_none(self):
        assert health_findings(_record()) == []

    def test_the_tier_lists_only_categories_worse_than_healthy(self):
        record = _record(
            health={
                "overall": "MAJOR", "storage": "MAJOR", "power": "WARNING",
                "cpu": "HEALTHY", "gpu": "UNKNOWN", "network": "HEALTHY",
            }
        )
        [finding] = health_findings(record)
        assert finding.check == "server-scan-health"
        assert finding.observed == "overall MAJOR (storage=MAJOR, power=WARNING)"

    def test_a_network_category_that_is_not_healthy(self):
        record = _record(health={"overall": "HEALTHY", "network": "MAJOR"})
        assert [(f.check, f.observed) for f in health_findings(record)] == [
            ("network-health", "network MAJOR: server-scan reads fewer than two link-up NICs")
        ]

    def test_links_never_read(self):
        # UNKNOWN ranks below HEALTHY in the roll-up, so `overall` alone would
        # pass it — the network category is its own check, as in server-scan.
        record = _record(health={"overall": "HEALTHY", "network": "UNKNOWN"})
        assert [f.observed for f in health_findings(record)] == [
            "network UNKNOWN: server-scan never read its links"
        ]

    def test_no_health_at_all_is_two_findings(self):
        assert [f.check for f in health_findings(_record(health={}))] == [
            "server-scan-health",
            "network-health",
        ]

    def test_an_unreachable_bmc(self):
        assert [f.check for f in health_findings(_record(reachable=False))] == ["bmc-reachable"]

    def test_reachability_server_scan_does_not_report_is_not_a_finding(self):
        assert health_findings(_record(reachable=None)) == []


class TestTheBondWaiver:
    def test_both_members_up_need_no_waiver(self):
        server = _server(LinkState.UP, LinkState.UP)
        assert bond_link_waiver(server, select_bond_members(server) or []) is None

    def test_a_member_down_is_named_with_its_link(self):
        server = _server(LinkState.UP, LinkState.DOWN)
        members = select_bond_members(server, allow_link_down=True) or []
        waiver = bond_link_waiver(server, members)
        assert waiver is not None and waiver.check == "bond-link-up"
        assert waiver.observed == (
            "1 of 2 bond members link-UP: nic1 aa:bb:cc:dd:ee:01 [1/1] UP, "
            "nic2 aa:bb:cc:dd:ee:02 [1/2] DOWN"
        )
