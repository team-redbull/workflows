"""The nodes-config.yaml `oc adm node-image create` reads, for ONE host — pure.

Plain parameters, no I/O (CLAUDE.md §1). The shape is the agent-based installer's
`hosts[]` (the same agent code node-image runs), as in the operator's runbook
("Creation MCE 4.20", page 2) and cluster-expender's template — with the bond the
platform's fabric is wired for:

  * `interfaces[]` binds each bond member's MAC to a LOGICAL name (`nic1`, `nic2`,
    from select_bond_members): the agent renames the NIC to match before applying
    `networkConfig`, so nothing here has to know what the kernel calls it (the
    NMStateConfig precedent of add-server-to-mce).
  * `networkConfig`: both members up with no address, `bond0` over them in
    `bond_mode` (802.3ad — the switch ports are configured for it), and
    `bond0.<vlan>` taking its address by DHCP. The VLAN is the cluster's node
    segment's.
  * `rootDeviceHints.deviceName`: the install disk.

The hostname is the Node's name. The hash of the rendered text is what the ISO
store keys a build on, so a retry with the same config builds nothing twice.
"""

from __future__ import annotations

import hashlib
from typing import Any

import yaml

from shared.models.server_lifecycle import BondMember

BOND_NAME = "bond0"


def nodes_config(
    *, hostname: str, root_device: str, members: list[BondMember], bond_mode: str, vlan_id: int
) -> dict[str, Any]:
    if len(members) != 2:
        raise ValueError(f"a bond takes exactly two members, got {len(members)}")
    ports = [m.logical_name for m in members]
    ethernet = [
        {
            "name": m.logical_name,
            "type": "ethernet",
            "state": "up",
            "mac-address": m.mac,
            "ipv4": {"enabled": False},
            "ipv6": {"enabled": False},
        }
        for m in members
    ]
    bond = {
        "name": BOND_NAME,
        "type": "bond",
        "state": "up",
        "ipv4": {"enabled": False},
        "ipv6": {"enabled": False},
        "link-aggregation": {"mode": bond_mode, "port": ports},
    }
    vlan = {
        "name": f"{BOND_NAME}.{vlan_id}",
        "type": "vlan",
        "state": "up",
        "vlan": {"base-iface": BOND_NAME, "id": vlan_id},
        "ipv4": {"enabled": True, "dhcp": True},
        "ipv6": {"enabled": False},
    }
    return {
        "hosts": [
            {
                "hostname": hostname,
                "rootDeviceHints": {"deviceName": root_device},
                "interfaces": [{"name": m.logical_name, "macAddress": m.mac} for m in members],
                "networkConfig": {"interfaces": [*ethernet, bond, vlan]},
            }
        ]
    }


def render(config: dict[str, Any]) -> str:
    return yaml.safe_dump(config, sort_keys=False, default_flow_style=False)


def config_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()
