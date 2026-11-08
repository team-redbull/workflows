"""InfraEnv names under the naming convention — pure, stdlib only, read on both sides.

An InfraEnv's name states what the hosts in it are:

  * `<conf>[-<opt>]-zone-<zone>-metal`  — bare metal (`x201-zone-a-metal`,
    `x201-r-zone-b-metal`): add-server-to-mce draws physical servers for it;
  * `<conf>[-<opt>]-zone-<zone>-<cpu>`  — KubeVirt (`x201-zone-a-8`): its hosts
    are VirtualMachines of `<cpu>` vCPUs and `<cpu> × MEMORY_GIB_PER_CPU` GiB,
    created by create-vm on one of the zone's KubeVirt hosted clusters.

`<conf>` is the server configuration the InfraEnv is for and `<opt>` one
optional letter of variant; `<zone>` is the zone in the data centre, which
with the MCE's site (shared/cluster_names.py) names the seed cluster whose
day1 folder lists the zone's KubeVirt clusters.

Lives in `shared/` because both sides apply the rule: the API edge derives
the zone and the VM size from the name (the run carries them, never
re-deriving on replay — the provision-dell-server region precedent) and the
limb names the VM after the InfraEnv. No I/O, no third-party import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

# The ONE place the convention's memory ratio lives: a KubeVirt InfraEnv of
# `<cpu>` vCPUs holds VMs of `<cpu> × 8` GiB.
MEMORY_GIB_PER_CPU = 8
# vCPUs an InfraEnv name may ask for. The upper bound keeps a typo from
# sizing a VM no node could hold; the lower one is the smallest VM the
# convention names (and vm.yaml's `cpu × 15 / 16` request rounds a 1-vCPU VM
# to a ZERO request, which the placement rule could never see).
MIN_CPU = 1
MAX_CPU = 256

_INFRAENV = re.compile(
    r"^(?P<conf>[a-z][a-z0-9]*)(?:-(?P<opt>[a-z]))?-zone-(?P<zone>[a-z0-9]+)-(?P<tail>metal|[1-9][0-9]*)$"
)
# A VirtualMachine name is a DNS-1123 subdomain label-ish name: `ocp-<infraenv>-<6 hex>`.
_DNS_1123 = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DNS_1123_MAX = 63
_SUFFIX = re.compile(r"^[0-9a-f]{6}$")
VM_NAME_PREFIX = "ocp-"


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
    server_conf: str
    option: str | None
    zone: str
    cpu: int | None
    memory_gib: int | None


def parse_infraenv(name: str) -> ParsedInfraEnv:
    """`x201-r-zone-b-8` -> conf x201, option r, zone b, 8 vCPUs, 64 GiB. Refuses anything else."""
    match = _INFRAENV.fullmatch(name)
    if match is None:
        raise InfraEnvNameError(
            f"InfraEnv {name!r} is not named by the convention "
            "`<conf>[-<letter>]-zone-<zone>-metal` or `<conf>[-<letter>]-zone-<zone>-<cpu>`"
        )
    tail = match.group("tail")
    if tail == "metal":
        return ParsedInfraEnv(
            name=name, kind=InfraEnvKind.METAL, server_conf=match.group("conf"),
            option=match.group("opt"), zone=match.group("zone"), cpu=None, memory_gib=None,
        )
    cpu = int(tail)
    if not MIN_CPU <= cpu <= MAX_CPU:
        raise InfraEnvNameError(f"InfraEnv {name!r} asks for {cpu} vCPUs; the convention allows {MIN_CPU}..{MAX_CPU}")
    return ParsedInfraEnv(
        name=name, kind=InfraEnvKind.KUBEVIRT, server_conf=match.group("conf"),
        option=match.group("opt"), zone=match.group("zone"), cpu=cpu, memory_gib=cpu * MEMORY_GIB_PER_CPU,
    )


def is_kubevirt(name: str) -> bool:
    """True for a KubeVirt InfraEnv; raises InfraEnvNameError for a name outside the convention."""
    return parse_infraenv(name).kind is InfraEnvKind.KUBEVIRT


def vm_name(infra_env: str, suffix_hex: str) -> str:
    """`ocp-<infraenv>-<6 lowercase hex>`: the VM, and the server name server-scan will list.

    The suffix is the run's draw, and the limb composes the VM's MAC from the
    same six hex digits (its last three octets), so name and MAC agree by
    construction. Checked as a DNS-1123 label, which every Kubernetes name
    the VM becomes (VM, DataVolume, launcher pod prefix) requires.
    """
    if not _SUFFIX.fullmatch(suffix_hex):
        raise InfraEnvNameError(f"VM suffix {suffix_hex!r} is not 6 lowercase hex digits")
    parse_infraenv(infra_env)
    name = f"{VM_NAME_PREFIX}{infra_env}-{suffix_hex}"
    if len(name) > _DNS_1123_MAX or not _DNS_1123.fullmatch(name):
        raise InfraEnvNameError(f"VM name {name!r} is not a DNS-1123 label of at most {_DNS_1123_MAX} characters")
    return name
