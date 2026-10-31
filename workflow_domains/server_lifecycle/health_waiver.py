"""Why server-scan refused a NAMED server: what may be waived, and what never is — pure.

`/servers/available` refuses a named machine with one 404 whatever the reason,
and its detail names neither the network category nor the install lock. Once
it has, install-server reads the machine's stored record
(`lookup_install_candidate`) and sorts the reasons here, with no I/O, so the
sort is tested without Temporal and replays identically (bond_selection.py is
the precedent).

SAFETY — about who else has the machine, or a human's say-so. NEVER waived,
whatever the request asks:

  * server-scan does not know the name (nothing to lock, nothing to trust);
  * a cluster claims it (`lifecycle_state` not AVAILABLE);
  * two claims contest it (`contested_with`);
  * it is in maintenance (an operator's explicit "do not touch");
  * ANOTHER run's install lock is live on it.

HEALTH — the server's own condition. Waived only for a named install that
asked (`allow_unhealthy`, CLAUDE.md §4), and every waiver is recorded with
what was observed:

  * `server-scan-health` — its health tier is not HEALTHY (RAID, disk,
    storage, power, … — the categories worse than HEALTHY are listed);
  * `network-health` — its network category is not HEALTHY (fewer than two
    link-up NICs actually read);
  * `bmc-reachable` — server-scan cannot reach the BMC (from where it runs;
    Ironic on the MCE may);
  * `server-scan-recheck` — none of the above in the record, yet refused: its
    live recheck did it (the manager no longer finds the machine);
  * `bond-link-up` — a bond member whose link is not UP (bond_selection).

The run's OWN live lock — left by an earlier run on the same workflow id — is
neither: re-taking it is an extension (server-scan's own rule), so it is no
reason to refuse.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_lifecycle import (
        AcquiredServer,
        BondMember,
        InstallCandidateRecord,
        WaivedHealthCheck,
    )
    from workflow_domains.server_lifecycle.bond_selection import (
        link_states,
        members_not_up,
    )

_HEALTHY = "HEALTHY"
_AVAILABLE = "AVAILABLE"
# Worse than HEALTHY. UNKNOWN (never evaluated) ranks BELOW HEALTHY in
# server-scan's roll-up, so listing it beside the overall tier would only add
# noise for categories a machine does not have (a gpu, say).
_WORSE_THAN_HEALTHY = ("WARNING", "MAJOR", "CRITICAL")


@dataclass(frozen=True)
class SafetyRefusal:
    """Why the machine must not be installed, whatever the request asked.

    `another_runs_lock` picks the failure type: ServerReservedError for a lock
    another run holds — what a pool candidate in the same spot is skipped
    with — and ServerNotAvailableError, the type server-scan's refusal already
    had, for everything else.
    """

    reason: str
    another_runs_lock: bool = False


def safety_refusal(record: InstallCandidateRecord, workflow_id: str) -> SafetyRefusal | None:
    """The SAFETY reason the record shows, or None when there is none."""
    membership = record.membership
    if not membership.found or record.server is None:
        return SafetyRefusal("server-scan has no server of that exact name")
    if membership.lifecycle_state not in (None, _AVAILABLE):
        where = ", ".join(
            part
            for part in (
                f"cluster {membership.cluster_name}" if membership.cluster_name else "",
                f"MCE {membership.mce_name}" if membership.mce_name else "",
            )
            if part
        )
        return SafetyRefusal(
            f"a cluster claims it: server-scan reads it {membership.lifecycle_state}"
            + (f" ({where})" if where else "")
        )
    if membership.contested_with:
        return SafetyRefusal(
            f"two claims contest it in server-scan (with {membership.contested_with})"
        )
    if membership.maintenance:
        return SafetyRefusal(
            "it is in maintenance in server-scan"
            + (f": {membership.maintenance_reason}" if membership.maintenance_reason else "")
        )
    lock = record.lock
    if lock.live and lock.workflow_id != workflow_id:
        return SafetyRefusal(
            f"another run holds its install lock: {lock.holder!r} for MCE "
            f"{lock.mce_cluster!r} (run {lock.workflow_id!r}) until {lock.expires_at}",
            another_runs_lock=True,
        )
    return None


def holds_own_lock(record: InstallCandidateRecord, workflow_id: str) -> bool:
    """The record's live install lock is this workflow id's — an earlier run's of the same id."""
    return record.lock.live and record.lock.workflow_id == workflow_id


def health_findings(record: InstallCandidateRecord) -> list[WaivedHealthCheck]:
    """Every HEALTH reason the record shows why `/servers/available` refused it."""
    findings: list[WaivedHealthCheck] = []
    health = record.health
    overall = health.get("overall", "UNKNOWN")
    if overall != _HEALTHY:
        worse = [
            f"{category}={severity}"
            for category, severity in health.items()
            if category != "overall" and severity in _WORSE_THAN_HEALTHY
        ]
        findings.append(
            WaivedHealthCheck(
                check="server-scan-health",
                observed=f"overall {overall}" + (f" ({', '.join(worse)})" if worse else ""),
            )
        )
    network = health.get("network", "UNKNOWN")
    if network != _HEALTHY:
        findings.append(
            WaivedHealthCheck(
                check="network-health",
                observed=(
                    f"network {network}: server-scan reads fewer than two link-up NICs"
                    if network != "UNKNOWN"
                    else "network UNKNOWN: server-scan never read its links"
                ),
            )
        )
    if record.reachable is False:
        findings.append(
            WaivedHealthCheck(check="bmc-reachable", observed="server-scan cannot reach its BMC")
        )
    return findings


def bond_link_waiver(
    server: AcquiredServer, members: list[BondMember]
) -> WaivedHealthCheck | None:
    """The waiver a bond with a member whose link is not UP needs, or None when both are up."""
    down = members_not_up(server, members)
    if not down:
        return None
    return WaivedHealthCheck(
        check="bond-link-up",
        observed=(
            f"{len(members) - down} of {len(members)} bond members link-UP: "
            + ", ".join(link_states(server, members))
        ),
    )
