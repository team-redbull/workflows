"""Which OME template attributes this workflow refuses to deploy — pure, deterministic.

Three groups, each fatal for its own reason. Decided with the DC team on
2026-09-30; the evidence is in `docs/design/dell-scp-template-r660.md`.

  * IDRAC NETWORK — the address the run reached the machine on. A template
    captured from a reference server carries THAT server's address, so
    deploying it moves every target onto one address, or resets it to DHCP.
    The machine is then unreachable and the only way back is the rack. This is
    the highest-consequence group on the page and the reason the audit exists.
  * STORAGE — a Clone or Replace export silently sets `RAIDresetConfig=True`
    and `RAIDforeignConfig=Clear`, so every deploy wipes the controller before
    applying; this workflow never destroys data (`storage_plan.py`). And on OME
    4.5.x it does not even work: KB 000384312 drops every
    `IncludedPhysicalDiskID` after the first, and our BOSS mirror spans two.
    RAID is built over Redfish instead.
  * USERS — root's password is set over Redfish before OME sees the machine, so
    the template does not need it, and carrying `Users.*` is what makes a
    template break across a firmware upgrade (KB 000326070). Any slot other
    than root is worse: the OME account lives on the iDRAC too.

MATCHING IS ON DISPLAY NAMES, AND THAT IS A COMPROMISE. OME's AttributeDetails
reports what the GUI shows — group "iDRAC,IPv4 Information", attribute "Address"
— not the SCP attribute names (`IPv4Static.1#Address`). There is no stable id to
key on, so the rules below are token matches over the group path and the display
name, ANCHORED ON THE GROUP. Both kinds of mistake are expensive and they pull
in opposite directions: a false negative strands a machine at the rack, while a
false positive fails every run until someone edits the template. The group
anchor is what keeps the second from happening on ordinary BIOS settings — see
the comment above the token lists for the ones that would otherwise trip it.

Kept out of the workflow file so it is testable with no Temporal environment,
and out of the limb because it is policy: the limb reports what the template
contains, this decides what is unsafe, and the workflow refuses.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_provisioning import BiosComparison, TemplateAttribute

# --- the three groups --------------------------------------------------------
NETWORK = "idrac-network"
STORAGE = "storage"
USERS = "user-accounts"

_WHY = {
    NETWORK: (
        "would rewrite the iDRAC's own address — every target would land on the reference "
        "server's address, or be reset to DHCP, and be reachable only at the rack"
    ),
    STORAGE: (
        "would wipe the controller (Clone/Replace exports set RAIDresetConfig=True), and on "
        "OME 4.5.x cannot create a two-disk mirror at all (KB 000384312). Storage is built "
        "over Redfish instead"
    ),
    USERS: (
        "would change an iDRAC local account. Root's password is set over Redfish before OME "
        "sees the machine, and a Users.* component is what breaks a template across a firmware "
        "upgrade (KB 000326070)"
    ),
}

# EVERY RULE IS ANCHORED ON THE GROUP, and that is what keeps the audit usable.
# Matching a bare word anywhere would refuse templates that are perfectly safe,
# and since an unsafe template FAILS the run, a false positive stops
# provisioning entirely. The BIOS in particular is full of words that look like
# hazards out of context:
#
#   "BIOS,System Security"      -> "System Password", "Setup Password"
#   "BIOS,Boot Settings"        -> "Hard-Disk Drive Sequence"
#   "BIOS,SATA Settings"        -> "Embedded SATA" (whose value can be RAID Mode)
#   "NIC,Integrated 1 Port 1"   -> "MAC Address"
#
# None of those is what this audit is for: the hazards are the iDRAC's OWN
# network, the storage components a Clone/Replace export injects, and iDRAC
# LOCAL USER accounts. So each rule first establishes the group, then looks.
_NETWORK_TOKENS = (
    "ipv4", "ipv6", "dns", "vlan", "netmask", "gateway", "dhcp",
    "static ip", "ip address", "mac address",
)
# Unambiguous even on their own: no BIOS attribute is called any of these.
_STORAGE_TOKENS = ("virtual disk", "raidreset", "raidaction", "raidforeign", "raidinit", "disk.virtual")
_STORAGE_GROUPS = ("storage", "raid")
_USER_GROUPS = ("user",)
# The one group name shared by both halves of this module: the audit must NOT
# treat BIOS boot/SATA settings as storage, and the verification checks only
# BIOS attributes.
_BIOS_GROUP = "bios"


@dataclass(frozen=True)
class Hazard:
    """One attribute the template must not carry, and why."""

    group: str
    attribute: str
    reason: str

    def describe(self) -> str:
        return f"{self.attribute} [{self.group}]"


def _haystacks(attribute: TemplateAttribute) -> tuple[str, str]:
    return attribute.group.lower(), attribute.name.lower()


def _is_idrac_network(attribute: TemplateAttribute) -> bool:
    group, name = _haystacks(attribute)
    if "idrac" not in group:
        return False
    return any(token in group or token in name for token in _NETWORK_TOKENS)


def _is_storage(attribute: TemplateAttribute) -> bool:
    group, name = _haystacks(attribute)
    # The BIOS names disks all over its boot and SATA settings, and none of that
    # configures a controller — "Hard-Disk Drive Sequence" is a boot order.
    if _BIOS_GROUP in group:
        return False
    if any(token in group for token in _STORAGE_GROUPS):
        return True
    return any(token in name for token in _STORAGE_TOKENS)


def _is_user_account(attribute: TemplateAttribute) -> bool:
    group, _ = _haystacks(attribute)
    # Group only. "Password" alone would refuse the BIOS System and Setup
    # passwords, which are a legitimate thing for a template to carry — the
    # hazard is an iDRAC LOCAL USER, and those live in a Users group.
    return any(token in group for token in _USER_GROUPS)


_RULES = ((NETWORK, _is_idrac_network), (STORAGE, _is_storage), (USERS, _is_user_account))


def audit_template(attributes: list[TemplateAttribute]) -> list[Hazard]:
    """Every attribute this template must not deploy, in the order OME reports them.

    An attribute marked `IsIgnored` is not deployed at all, so it is not a
    hazard — that flag is exactly how an operator keeps a captured attribute in
    a template without applying it, and refusing it would make the audit
    impossible to satisfy from the OME GUI.
    """
    hazards: list[Hazard] = []
    for attribute in attributes:
        if attribute.is_ignored:
            continue
        for group, matches in _RULES:
            if matches(attribute):
                hazards.append(
                    Hazard(group=group, attribute=attribute.describe(), reason=_WHY[group])
                )
                break
    return hazards


# --- what the template MEANT to set, and whether it took ---------------------
# A deployment reporting Completed proves nothing: SCP Import is a "continue on
# error" operation, so one attribute the firmware does not know fails while the
# rest apply (SCP-RG §2.5). Dell's own reference client does not trust the job
# state either — it string-searches the message for failure words.
#
# Only BIOS attributes are verified. That is a deliberate limit, not an
# oversight: the BIOS attribute registry maps OME's display names to the names
# Redfish accepts, which is what makes a BIOS attribute both checkable and
# fixable. The iDRAC's own attributes have no such published bridge, so they are
# reported as unverified rather than guessed at.


def bios_intent(attributes: list[TemplateAttribute]) -> dict[str, str | None]:
    """The BIOS attributes a template would deploy, keyed by display name.

    Ignored attributes are skipped — OME does not deploy them, so the machine
    is under no obligation to match them.
    """
    return {
        attribute.name: attribute.value
        for attribute in attributes
        if not attribute.is_ignored and _BIOS_GROUP in attribute.group.lower()
    }


def bios_drift(compared: list[BiosComparison]) -> list[BiosComparison]:
    """Every attribute the template set that the machine does not actually have."""
    return [attribute for attribute in compared if attribute.drifted]


def unverifiable(compared: list[BiosComparison]) -> list[BiosComparison]:
    """Attributes that could not be checked at all — the honest coverage gap.

    Almost always a template naming something this firmware does not have, which
    is exactly what Dell warns of when a template and a target differ in
    version. Reported, never silently passed.
    """
    return [
        attribute
        for attribute in compared
        if not attribute.read_only and attribute.intended is not None and not attribute.attribute_name
    ]


# A template deployed onto a firmware that does not match it can fail dozens of
# attributes at once, and the whole list would go into a workflow failure
# message — recorded verbatim in Temporal history and shown in the UI. Enough
# to diagnose, not the entire registry.
_MAX_LISTED = 10


def _listed(items: list[str]) -> str:
    shown = "; ".join(items[:_MAX_LISTED])
    return shown if len(items) <= _MAX_LISTED else f"{shown}; and {len(items) - _MAX_LISTED} more"


def describe_drift(drifted: list[BiosComparison]) -> str:
    return _listed(
        [
            f"{a.display_name} ({a.attribute_name}) is {a.actual!r}, template says {a.intended!r}"
            for a in drifted
        ]
    )


def describe_hazards(hazards: list[Hazard]) -> str:
    """One sentence per forbidden group, naming every attribute that fell in it.

    Grouped rather than listed flat because the groups are different problems
    with different fixes, and a template usually trips one of them many times
    over — a flat list of forty iDRAC network attributes buries the one line an
    operator needs.
    """
    by_group: dict[str, list[str]] = {}
    reasons: dict[str, str] = {}
    for hazard in hazards:
        by_group.setdefault(hazard.group, []).append(hazard.attribute)
        reasons[hazard.group] = hazard.reason
    return ". ".join(
        f"{group} ({len(found)}): {reasons[group]} — {_listed(sorted(found))}"
        for group, found in by_group.items()
    )
