"""Which two NICs of a candidate server carry the bond — pure, deterministic.

Split out of add_server_to_mce.py because it is POLICY, not orchestration: it makes
no activity call, reads no clock and touches no I/O, so it is the one part of
that workflow which can be unit-tested with no Temporal environment at all. The
workflow file is then only the shape of the run.

It stays in the DOMAIN folder rather than in `shared/`. `shared/` is the
contract between brain and limbs (`shared/bmc_address.py` is there because both
sides use it — the activity puts the address on the BareMetalHost and the
workflow reports it), and nothing on the limb side selects a bond. Keeping it
here also keeps it obviously subject to the determinism rule: it runs inside the
workflow sandbox on every replay, so it must never grow a clock, a random or a
set-iteration order.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_lifecycle import AcquiredServer, BondMember, LinkState

# Two link-up NICs on two DISTINCT physical ports, and nothing less. The
# provisioning network is delivered over an 802.3ad bond0 and the switch ports
# are configured for it, so a machine that cannot form that bond must not enter
# the inventory at all: half a bond is not a degraded install, it is a host the
# fabric is not set up to carry.
#
# This is DELIBERATELY stricter than the operator being replaced. bmhgen fell
# back to a non-bonded layout, VLAN straight on the NIC, when it found exactly
# one interface (yaml_generators.py, "Single interface — preserve the original
# non-bonded layout"). That path is NOT ported, and its absence is a decision,
# not an oversight — do not restore it without the fabric changing first.
#
# No NIC count is asked of server-scan: /servers/available already requires the
# network health category HEALTHY, i.e. at least two links observed up.
#
# `allow_link_down` (add-server-to-mce's hidden `allow_unhealthy`, CLAUDE.md §4)
# relaxes ONLY the link state. The bond keeps its two members on two distinct
# physical ports — a bond with one member DOWN, which the fabric's LAG carries
# on the remaining link — and never becomes bmhgen's non-bonded layout above.
BOND_MEMBER_COUNT = 2

# The order members are taken in when the link-up rule is waived: link-UP
# first, so nic1 — the BareMetalHost's boot MAC and an IPMI host's only PXE
# mapping — is a live link whenever the server has one; then UNKNOWN (not
# reported, may well be up), DOWN, and DISABLED last.
_WAIVED_LINK_ORDER = {
    LinkState.UP: 0,
    LinkState.UNKNOWN: 1,
    LinkState.DOWN: 2,
    LinkState.DISABLED: 3,
}


def select_bond_members(
    server: AcquiredServer, *, allow_link_down: bool = False
) -> list[BondMember] | None:
    """Pick two link-up NICs on two DISTINCT physical ports, or None.

    Link state is required to be UP strictly, never merely "not DOWN". That is
    a deliberate, accepted trade: HPE OneView reports no link state at all (its
    portMap carries none, so server-scan stores UNKNOWN unconditionally) and
    Intersight vNICs usually report none either, so servers from those
    collectors cannot satisfy this and fail in the caller with an explicit
    message — rather than being silently passed over.

    Distinctness is by physical port, not by interface: two NPAR partitions of
    one port are two interfaces with two MACs on ONE wire, and bonding them
    yields no redundancy at all.

    ORDER IS SERVER-SCAN'S, and it is load-bearing: the first link-up NIC with
    a MAC that server-scan lists is always `nic1`, and `nic1` is the
    BareMetalHost's `bootMACAddress` — and, for an IPMI host, the only MAC
    mapped on the site's PXE VM. Port distinctness only ever rejects a LATER
    interface, so it can never displace the first.

    `allow_link_down` — only ever set for a named install that asked for
    `allow_unhealthy` — keeps every rule but link-UP: the interfaces are taken
    link-UP first and then by _WAIVED_LINK_ORDER, each group in server-scan's
    order (a stable sort). With two link-up NICs that is exactly the strict
    choice; with one, it is nic1 and the next interface on another port whose
    link is not up. None still means fewer than two physical ports with a MAC:
    nothing to bond, link state or not.

    Returns None rather than raising, because "this candidate is no use" is a
    normal answer the caller acts on by trying the next one — and because a bare
    exception raised in workflow code fails the workflow task and retries
    forever.
    """
    interfaces = server.interfaces
    if allow_link_down:
        interfaces = sorted(
            interfaces, key=lambda i: _WAIVED_LINK_ORDER.get(i.link_state, len(_WAIVED_LINK_ORDER))
        )
    members: list[BondMember] = []
    seen_ports: set[str] = set()
    for interface in interfaces:
        if interface.mac is None:
            continue
        if not allow_link_down and interface.link_state != LinkState.UP:
            continue
        port_key = interface.physical_port_key()
        if port_key in seen_ports:
            continue
        seen_ports.add(port_key)
        members.append(
            BondMember(
                logical_name=f"nic{len(members) + 1}",
                mac=interface.mac,
                port_key=port_key,
            )
        )
        if len(members) == BOND_MEMBER_COUNT:
            return members
    return None


def link_states(server: AcquiredServer, members: list[BondMember]) -> list[str]:
    """Each bond member's link as server-scan reported it: `nic1 <mac> [<port>] UP`."""
    by_mac = {i.mac: i.link_state for i in server.interfaces if i.mac is not None}
    return [
        f"{m.logical_name} {m.mac} [{m.port_key}] {by_mac.get(m.mac, LinkState.UNKNOWN).value}"
        for m in members
    ]


def members_not_up(server: AcquiredServer, members: list[BondMember]) -> int:
    """How many bond members server-scan does not report link-UP."""
    up = {i.mac for i in server.interfaces if i.mac is not None and i.link_state == LinkState.UP}
    return sum(1 for m in members if m.mac not in up)


def describe_candidate(server: AcquiredServer) -> str:
    """One candidate's interfaces, for the no-usable-candidate failure message.

    Names the provider and every link state, because the usual cause is that
    the collector cannot report link state at all rather than that the hardware
    is down — and those two look identical from the workflow's side.
    """
    if not server.interfaces:
        interfaces = "no interfaces reported"
    else:
        interfaces = ", ".join(
            f"{i.name}[{i.physical_port_key()}]={i.link_state.value}"
            for i in server.interfaces
        )
    return f"{server.name} (provider={server.source_provider}): {interfaces}"
