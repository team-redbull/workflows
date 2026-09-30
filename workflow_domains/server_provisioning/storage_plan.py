"""Which storage changes a Dell server needs — pure, deterministic.

The required layout, decided with the operator on 2026-09-30:

  * the BOSS card (the pair of M.2 boot drives) carries ONE RAID 1 volume over
    both of its drives — the OS disk;
  * every drive behind a PERC controller is Non-RAID, so the OS sees each one
    directly (ODF / local storage use them as whole disks).

Anything else behind the machine's controllers — CPU-attached NVMe, an embedded
SATA controller — has no RAID state to set and is left alone.

The plan NEVER DESTROYS DATA. A drive that is already a volume member, a BOSS
volume that is not exactly that RAID 1, a foreign configuration — each is a
PROBLEM that fails the run, never something deleted to make room. A new
server arrives with none of them; one that has them is either not new or was
configured by hand, and either way that is a person's call.

Kept out of the workflow file so it is testable with no Temporal environment,
and out of the limb because it is policy: the limb reports what exists
(StorageLayout), this decides what to change, and the workflow runs it and
checks the result against the same plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_provisioning import IdracController, StorageLayout

BOSS_DRIVE_COUNT = 2
_RAID1 = "RAID1"

# Dell `DellPhysicalDisk.RaidStatus` values, as the iDRAC reports them.
_NON_RAID = "NonRAID"
_READY = "Ready"


@dataclass(frozen=True)
class StoragePlan:
    """What to stage, or why nothing can be.

    `problems` non-empty means the layout cannot be reached without destroying
    something, and nothing is staged at all.
    """

    boss_controller: str | None = None
    boss_drives: list[str] = field(default_factory=list)
    create_boss_raid1: bool = False
    non_raid_drives: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def converged(self) -> bool:
        """True when the machine already has exactly the required layout."""
        return not self.problems and not self.create_boss_raid1 and not self.non_raid_drives


def is_boss(controller: IdracController) -> bool:
    """A BOSS card names itself BOSS (`BOSS-N1 Monolithic`, `BOSS-S2`, ...)."""
    return "BOSS" in controller.name.upper() or "BOSS" in controller.id.upper()


def is_perc(controller: IdracController) -> bool:
    """A PERC is a `RAID.*` controller (`RAID.SL.3-1`, `RAID.Integrated.1-1`)."""
    return not is_boss(controller) and (
        controller.id.upper().startswith("RAID.") or "PERC" in controller.name.upper()
    )


def _drive_name(odata_id: str) -> str:
    return odata_id.rstrip("/").rsplit("/", 1)[-1]


def plan_storage(layout: StorageLayout) -> StoragePlan:
    """The changes that take `layout` to the required one, or the problems preventing it."""
    problems: list[str] = []

    bosses = [c for c in layout.controllers if is_boss(c)]
    if len(bosses) != 1:
        found = ", ".join(c.id for c in bosses) or "none"
        problems.append(f"expected exactly one BOSS controller, found {found}")
        boss = None
    else:
        boss = bosses[0]

    create_boss_raid1 = False
    boss_drives: list[str] = []
    if boss is not None:
        boss_drives = sorted(d.odata_id for d in boss.drives)
        if len(boss_drives) != BOSS_DRIVE_COUNT:
            problems.append(
                f"BOSS {boss.id} has {len(boss_drives)} drive(s), a RAID 1 needs exactly "
                f"{BOSS_DRIVE_COUNT}"
            )
        mirrors = [
            v
            for v in boss.volumes
            if (v.raid_type or "").upper() == _RAID1 and sorted(v.drives) == boss_drives
        ]
        others = [v for v in boss.volumes if v not in mirrors]
        if others:
            problems.append(
                f"BOSS {boss.id} already holds volume(s) that are not the RAID 1 over both "
                f"drives: {', '.join(_drive_name(v.odata_id) for v in others)} — delete by hand "
                "if it is really unused"
            )
        create_boss_raid1 = not mirrors and not others

    non_raid_drives: list[str] = []
    for controller in (c for c in layout.controllers if is_perc(c)):
        if controller.volumes:
            problems.append(
                f"PERC {controller.id} already holds volume(s) "
                f"{', '.join(_drive_name(v.odata_id) for v in controller.volumes)} — its "
                "drives must all be Non-RAID; delete by hand if really unused"
            )
        for drive in controller.drives:
            if drive.raid_status == _NON_RAID:
                continue
            if drive.raid_status == _READY:
                non_raid_drives.append(drive.odata_id)
            elif not controller.volumes:
                problems.append(
                    f"PERC drive {_drive_name(drive.odata_id)} is {drive.raid_status or 'of unknown state'}, "
                    "not Ready or Non-RAID"
                )

    return StoragePlan(
        boss_controller=boss.id if boss is not None else None,
        boss_drives=boss_drives,
        create_boss_raid1=create_boss_raid1 and not problems,
        non_raid_drives=non_raid_drives if not problems else [],
        problems=problems,
    )
