"""InfraEnv and VM names under the platform naming convention — pure, stdlib only, read on both sides.

The convention is kubevirt-workflows' docs/naming-convention.md (the
`naming-convention` skill fetches it). An InfraEnv's name states what the hosts
in it are, how big, and in which zone:

    c200-kv32-az1
     │    │    └─ zone, `az<N>`
     │    └────── `kv<cpu>`: VirtualMachines of <cpu> vCPUs; `mtl`: bare metal
     └─────────── flavor, with its optional variant letter (c200, c200n, c200d…)

  * `<flavor>-mtl-<zone>`       — bare metal: add-server-to-mce draws
    physical servers for it;
  * `<flavor>-kv<cpu>-<zone>`   — KubeVirt: its hosts are VirtualMachines
    of `<cpu>` vCPUs and `<cpu> × 7.5` GiB (`vm_memory_gib`), created by
    create-vm on one of the zone's KV clusters. `<cpu>` is EVEN, so the memory
    is a whole number of GiB.

The SITE is not in the name: an InfraEnv belongs to one MCE, and so to that
MCE's site (`shared/cluster_names.site_of_mce_cluster`).

A VM of a KubeVirt InfraEnv is `ocp-<flavor>-kv<cpu>-<site>-<its MAC, 12 hex>`
(`ocp-c200-kv32-five-0e03f2a1b2c3`): the full MAC makes the name unique
wherever the MAC is, and the zone is NOT in it (a label carries it).

This replaced `<conf>[-<opt>]-zone-<zone>-<cpu>` / `…-metal` and the
`ocp-<infraenv>-<6 hex>` VM name on 2026-11-20 (no VM of that shape was ever
created in production: the agents it needed were never deployed), and the
InfraEnv lost its site (`c200-kv32-five-az1`) on 2026-11-22, when the
convention left draft.

Lives in `shared/` because both sides apply the rule: the API edge derives
the flavor, zone and VM size from the name (and the site from the MCE's) (the run carries them, never
re-deriving on replay — the provision-dell-server region precedent), and the
worker inside the KV cluster names the VM. No I/O, no third-party import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

# The ONE place the memory ratio lives: a KubeVirt InfraEnv of `<cpu>` vCPUs
# holds VMs of `<cpu> × 7.5` GiB. The name carries the vCPUs only, and the
# naming convention defines no memory: the ratio is this repo's. A FRACTION, so
# the arithmetic stays in integers (`cpu × 15 // 2`).
#
# 7.5 and not 8 (the operator, 2026-11-24; docs/design/create-vm-placement.md
# §6.1.1): every KV node has 8 GiB of RAM per core, and a node gives VMs less
# than its RAM (the kernel's own share, the kubelet reserve, the node's pods,
# each VM's overhead), so at 8 the last VM a node's CPUs allowed never fitted
# on memory. Computed for the node types in use (128 and 192 CPUs); ANOTHER
# node type needs that section's table recomputed before it joins — some sizes
# stay one VM short at 7.5 (tests/test_vm_lifecycle_placement.py lists them).
MEMORY_GIB_PER_CPU = (15, 2)
# vCPUs an InfraEnv name may ask for: EVEN (7.5 GiB each is then a whole number
# of GiB). The upper bound keeps a typo from sizing a VM no node could hold.
MIN_CPU = 2
MAX_CPU = 256
# A VM's NAME carries any size from one vCPU up: names of VMs that already
# exist are read, and the convention's `kv<cpu>` states no parity.
_MIN_NAMED_CPU = 1

_SITE = r"[a-z0-9]+(?:-[a-z0-9]+)*"
_INFRAENV = re.compile(r"^(?P<flavor>[a-z][0-9a-z]*)-(?P<size>mtl|kv[1-9][0-9]*)-(?P<zone>az[1-9][0-9]*)$")
_FLAVOR = re.compile(r"^[a-z][0-9a-z]*$")
_SITE_TOKEN = re.compile(rf"^{_SITE}$")
# A VirtualMachine's name is a DNS-1123 label: the VM, its DataVolume
# (`<name>-root`) and its launcher pod's prefix all need one.
_DNS_1123 = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DNS_1123_MAX = 63
# A VM's serial: what tells two VMs of one flavor, size and site apart in their name, and
# their SMBIOS serial number too (a bare-metal node's name carries its serial the same way).
VM_SERIAL_LENGTH = 8
VM_SERIAL_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
_VM_SERIAL = re.compile(rf"^[a-z0-9]{{{VM_SERIAL_LENGTH}}}$")
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
    """What an InfraEnv name says — never the site, which is its MCE's. `cpu`/`memory_gib`
    are None for bare metal."""

    name: str
    kind: InfraEnvKind
    flavor: str
    zone: str
    cpu: int | None
    memory_gib: int | None


def parse_infraenv(name: str) -> ParsedInfraEnv:
    """`c200n-kv32-az2` -> flavor c200n, 32 vCPUs / 240 GiB, zone az2."""
    match = _INFRAENV.fullmatch(name)
    if match is None:
        raise InfraEnvNameError(
            f"InfraEnv {name!r} is not named by the convention `<flavor>-kv<cpu>-<zone>` "
            "(`c200-kv32-az1`) or `<flavor>-mtl-<zone>`, the zone being `az<N>` (no site: it is the MCE's)"
        )
    flavor, size, zone = match.group("flavor"), match.group("size"), match.group("zone")
    if size == BARE_METAL_SIZE:
        return ParsedInfraEnv(name=name, kind=InfraEnvKind.METAL, flavor=flavor, zone=zone, cpu=None, memory_gib=None)
    cpu = int(size.removeprefix(KUBEVIRT_SIZE_PREFIX))
    if not MIN_CPU <= cpu <= MAX_CPU:
        raise InfraEnvNameError(f"InfraEnv {name!r} asks for {cpu} vCPUs; a KubeVirt InfraEnv has {MIN_CPU}..{MAX_CPU}")
    if cpu % MEMORY_GIB_PER_CPU[1]:
        raise InfraEnvNameError(
            f"InfraEnv {name!r} asks for {cpu} vCPUs, an odd number: a VM's memory is 7.5 GiB per vCPU, which "
            f"would not be a whole number of GiB. Use an even size (`kv{cpu + 1}`)"
        )
    return ParsedInfraEnv(
        name=name, kind=InfraEnvKind.KUBEVIRT, flavor=flavor, zone=zone, cpu=cpu, memory_gib=vm_memory_gib(cpu),
    )


def vm_memory_gib(cpu: int) -> int:
    """The guest memory of a `cpu`-vCPU VM, in GiB: `cpu × 7.5`, for an even `cpu` (8 -> 60, 32 -> 240)."""
    numerator, denominator = MEMORY_GIB_PER_CPU
    if cpu % denominator:
        raise InfraEnvNameError(f"{cpu} vCPUs × 7.5 GiB is not a whole number of GiB: the vCPU count must be even")
    return cpu * numerator // denominator


def is_kubevirt(name: str) -> bool:
    """True for a KubeVirt InfraEnv; raises InfraEnvNameError for a name outside the convention."""
    return parse_infraenv(name).kind is InfraEnvKind.KUBEVIRT


def vm_size(cpu: int) -> str:
    """`kv<cpu>`: the size field of an InfraEnv's and a VM's name, and the `redbull.io/size` label's value."""
    return f"{KUBEVIRT_SIZE_PREFIX}{cpu}"


def vm_name(flavor: str, cpu: int, site: str, serial: str) -> str:
    """`ocp-<flavor>-kv<cpu>-<site>-<serial, 8 lowercase letters and digits>`: the VM, and the
    server name server-scan lists.

    The serial is drawn by the worker inside the KV cluster and is also the VM's SMBIOS serial
    (the operator, 2026-11-25: KubeMacPool picks the MAC, so the name no longer carries it).
    Checked as a DNS-1123 label short enough for the root DataVolume's `<name>-root` too.
    """
    if not _VM_SERIAL.fullmatch(serial):
        raise InfraEnvNameError(
            f"serial {serial!r} is not {VM_SERIAL_LENGTH} lowercase letters and digits"
        )
    if not _FLAVOR.fullmatch(flavor):
        raise InfraEnvNameError(f"flavor {flavor!r} is not a letter followed by letters and digits")
    if not _SITE_TOKEN.fullmatch(site):
        raise InfraEnvNameError(f"site {site!r} must be lowercase letters, digits and single dashes")
    if not _MIN_NAMED_CPU <= cpu <= MAX_CPU:
        raise InfraEnvNameError(f"a VM of {cpu} vCPUs is outside the convention's {_MIN_NAMED_CPU}..{MAX_CPU}")
    name = f"{VM_NAME_PREFIX}{flavor}-{vm_size(cpu)}-{site}-{serial}"
    if len(name) + len(ROOT_DISK_SUFFIX) > _DNS_1123_MAX or not _DNS_1123.fullmatch(name):
        raise InfraEnvNameError(
            f"VM name {name!r} is not a DNS-1123 label of at most {_DNS_1123_MAX - len(ROOT_DISK_SUFFIX)} "
            f"characters (its root disk is `<name>{ROOT_DISK_SUFFIX}`)"
        )
    return name
