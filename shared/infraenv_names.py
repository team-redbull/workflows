"""InfraEnv and VM names under the platform naming convention — pure, stdlib only, read on both sides.

An InfraEnv's name states what the hosts in it are, where, and how big:

    c200-kv32-five-az1
     │    │    │    └─ zone, `az<N>`
     │    │    └────── site (one field or several: `five`, `bat-yam`)
     │    └─────────── `kv<cpu>`: VirtualMachines of <cpu> vCPUs; `mtl`: bare metal
     └──────────────── flavor, with its optional variant letter (c200, c200n, c200d…)

  * `<flavor>-mtl-<site>-<zone>`       — bare metal: add-server-to-mce draws
    physical servers for it;
  * `<flavor>-kv<cpu>-<site>-<zone>`   — KubeVirt: its hosts are VirtualMachines
    of `<cpu>` vCPUs and `<cpu> × MEMORY_GIB_PER_CPU` GiB, created by create-vm
    on one of the zone's KV clusters.

A VM of a KubeVirt InfraEnv is `ocp-<flavor>-kv<cpu>-<site>-<its MAC, 12 hex>`
(`ocp-c200-kv32-five-0e03f2a1b2c3`): the full MAC makes the name unique
wherever the MAC is, and the zone is NOT in it (a label carries it).

This replaced `<conf>[-<opt>]-zone-<zone>-<cpu>` / `…-metal` and the
`ocp-<infraenv>-<6 hex>` VM name on 2026-11-20 (no VM of that shape was ever
created in production: the agents it needed were never deployed).

Lives in `shared/` because both sides apply the rule: the API edge derives
the flavor, site, zone and VM size from the name (the run carries them, never
re-deriving on replay — the provision-dell-server region precedent), and the
worker inside the KV cluster names the VM. No I/O, no third-party import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

# The ONE place the convention's memory ratio lives: a KubeVirt InfraEnv of
# `<cpu>` vCPUs holds VMs of `<cpu> × 8` GiB. The name carries the vCPUs only.
MEMORY_GIB_PER_CPU = 8
# vCPUs an InfraEnv name may ask for. The upper bound keeps a typo from
# sizing a VM no node could hold.
MIN_CPU = 1
MAX_CPU = 256

_SITE = r"[a-z0-9]+(?:-[a-z0-9]+)*"
_INFRAENV = re.compile(
    rf"^(?P<flavor>[a-z][0-9a-z]*)-(?P<size>mtl|kv[1-9][0-9]*)-(?P<site>{_SITE})-(?P<zone>az[1-9][0-9]*)$"
)
_FLAVOR = re.compile(r"^[a-z][0-9a-z]*$")
_SITE_TOKEN = re.compile(rf"^{_SITE}$")
# A VirtualMachine's name is a DNS-1123 label: the VM, its DataVolume
# (`<name>-root`) and its launcher pod's prefix all need one.
_DNS_1123 = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DNS_1123_MAX = 63
_MAC = re.compile(r"^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$")
VM_NAME_PREFIX = "ocp-"
BARE_METAL_SIZE = "mtl"
KUBEVIRT_SIZE_PREFIX = "kv"
# The longest suffix a VM's name gets on another object: its root DataVolume.
ROOT_DISK_SUFFIX = "-root"


class InfraEnvNameError(ValueError):
    """The name is not one the convention describes. A ValueError too, so a
    Pydantic validator turns it into a 422 at the API edge."""


class InfraEnvKind(str, Enum):
    METAL = "metal"
    KUBEVIRT = "kubevirt"


@dataclass(frozen=True)
class ParsedInfraEnv:
    """What an InfraEnv name says. `cpu`/`memory_gib` are None for bare metal."""

    name: str
    kind: InfraEnvKind
    flavor: str
    site: str
    zone: str
    cpu: int | None
    memory_gib: int | None


def parse_infraenv(name: str) -> ParsedInfraEnv:
    """`c200n-kv32-bat-yam-az2` -> flavor c200n, 32 vCPUs / 256 GiB, site bat-yam, zone az2."""
    match = _INFRAENV.fullmatch(name)
    if match is None:
        raise InfraEnvNameError(
            f"InfraEnv {name!r} is not named by the convention `<flavor>-kv<cpu>-<site>-<zone>` "
            "(`c200-kv32-five-az1`) or `<flavor>-mtl-<site>-<zone>`, the zone being `az<N>`"
        )
    flavor, size, site, zone = match.group("flavor"), match.group("size"), match.group("site"), match.group("zone")
    if size == BARE_METAL_SIZE:
        return ParsedInfraEnv(
            name=name, kind=InfraEnvKind.METAL, flavor=flavor, site=site, zone=zone, cpu=None, memory_gib=None
        )
    cpu = int(size.removeprefix(KUBEVIRT_SIZE_PREFIX))
    if not MIN_CPU <= cpu <= MAX_CPU:
        raise InfraEnvNameError(f"InfraEnv {name!r} asks for {cpu} vCPUs; the convention allows {MIN_CPU}..{MAX_CPU}")
    return ParsedInfraEnv(
        name=name, kind=InfraEnvKind.KUBEVIRT, flavor=flavor, site=site, zone=zone,
        cpu=cpu, memory_gib=cpu * MEMORY_GIB_PER_CPU,
    )


def is_kubevirt(name: str) -> bool:
    """True for a KubeVirt InfraEnv; raises InfraEnvNameError for a name outside the convention."""
    return parse_infraenv(name).kind is InfraEnvKind.KUBEVIRT


def vm_size(cpu: int) -> str:
    """`kv<cpu>`: the size field of an InfraEnv's and a VM's name, and the VM's `redbull.io/size` label."""
    return f"{KUBEVIRT_SIZE_PREFIX}{cpu}"


def vm_name(flavor: str, cpu: int, site: str, mac: str) -> str:
    """`ocp-<flavor>-kv<cpu>-<site>-<MAC, 12 lowercase hex>`: the VM, and the server name server-scan lists.

    Built by the worker inside the KV cluster from the MAC it drew, so name
    and MAC agree by construction. Checked as a DNS-1123 label short enough
    for the root DataVolume's `<name>-root` too.
    """
    mac = mac.lower()
    if not _MAC.fullmatch(mac):
        raise InfraEnvNameError(f"MAC {mac!r} is not six lowercase hex octets joined by colons")
    if not _FLAVOR.fullmatch(flavor):
        raise InfraEnvNameError(f"flavor {flavor!r} is not a letter followed by letters and digits")
    if not _SITE_TOKEN.fullmatch(site):
        raise InfraEnvNameError(f"site {site!r} must be lowercase letters, digits and single dashes")
    if not MIN_CPU <= cpu <= MAX_CPU:
        raise InfraEnvNameError(f"a VM of {cpu} vCPUs is outside the convention's {MIN_CPU}..{MAX_CPU}")
    name = f"{VM_NAME_PREFIX}{flavor}-{vm_size(cpu)}-{site}-{mac.replace(':', '')}"
    if len(name) + len(ROOT_DISK_SUFFIX) > _DNS_1123_MAX or not _DNS_1123.fullmatch(name):
        raise InfraEnvNameError(
            f"VM name {name!r} is not a DNS-1123 label of at most {_DNS_1123_MAX - len(ROOT_DISK_SUFFIX)} "
            f"characters (its root disk is `<name>{ROOT_DISK_SUFFIX}`)"
        )
    return name
