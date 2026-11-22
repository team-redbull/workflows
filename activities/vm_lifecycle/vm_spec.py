"""The VirtualMachine a KV cluster's worker builds, and what a VirtualMachine says — pure.

docs/design/create-vm-placement.md §7 is the design. No settings, no Temporal,
no I/O: vm_operations.py hands this the request and the cluster's own values.

  * THE MAC is half the cluster's, half random: the cluster's `VM_MAC_PREFIX`
    (`0e` + its VLAN ID, three octets) and three random bytes drawn by the
    worker — 16.7 million MACs per cluster, and never the same prefix on two
    clusters that share a network (the Segments Manager never gives one VLAN
    to two clusters of a site).
  * THE NAME is the MAC in full, `ocp-<flavor>-kv<cpu>-<site>-<12 hex>`
    (shared/infraenv_names.vm_name), so name and MAC agree by construction and
    the name is unique wherever the MAC is. The zone is a label, never in it.
  * THE BODY is the platform's vm.yaml (redbull-platform, rendered until
    2026-11-20 by the kv-vm-api agent from `kv-agents/templates/vm.yaml.j2`),
    built here as a plain object: Halted, a blank root disk on a DataVolume
    template (ReadWriteMany + Filesystem), one bridged interface with the
    fixed MAC on the cluster's Multus network (vm.yaml's `openshift-cnv/vm-external-network`
    unless the cluster names another), the CPU request `vCPUs × 15/16`
    floored at one core. KubeVirt re-randomises an unset MAC on every start
    and the BareMetalHost needs a fixed boot MAC, hence `macAddress`.
"""

from __future__ import annotations

from typing import Any

from activities.vm_lifecycle.cluster_figures import get, guest_memory_bytes, vcpus
from shared.infraenv_names import ROOT_DISK_SUFFIX, vm_name, vm_size
from shared.upgrade_layout import ZONE_LABEL
from shared.vm_requests import GIB, request_cores

KUBEVIRT_API_VERSION = "kubevirt.io/v1"
# How a run finds the VM it created (a retried create, a rollback): the RUN's
# id, a UUID, as a label — a slot's workflow id is reused by later runs and
# holds characters a label value cannot.
CREATED_BY_LABEL = "redbull.io/created-by"
WORKFLOW_ID_ANNOTATION = "redbull.io/workflow-id"
FLAVOR_LABEL = "redbull.io/flavor"
SIZE_LABEL = "redbull.io/size"
SITE_LABEL = "redbull.io/site"
ROLE_LABEL = "redbull.io/role"
MCE_LABEL = "redbull.io/mce"
# A tenant warm-pool VM (the naming convention's §4.b).
WORKER_ROLE = "worker"
ROOT_DISK_VOLUME = "rootdisk"
RANDOM_OCTETS = 3
# printableStatus values KubeVirt reaches only after it accepted the spec.
_ADMITTED_STATUSES = frozenset(
    {"Stopped", "Running", "Starting", "Stopping", "Paused", "Migrating", "WaitingForVolumeBinding"}
)


def draw_mac(prefix: str, random_bytes: bytes) -> str:
    """`0e:03:f2` + three bytes -> `0e:03:f2:a1:b2:c3`, lowercase."""
    if len(random_bytes) != RANDOM_OCTETS:
        raise ValueError(f"a VM MAC takes exactly {RANDOM_OCTETS} random bytes, got {len(random_bytes)}")
    return f"{prefix.lower()}:" + ":".join(f"{byte:02x}" for byte in random_bytes)


def size_label(cpu: int) -> str:
    """The `redbull.io/size` label's value: `kv<cpu>` (`kv32`), the name's size field —
    the naming convention's §4 value, the same as its InfraEnv's Agents carry (the
    operator, 2026-11-22; from 2026-11-21 it was `<cpu>c`)."""
    return vm_size(cpu)


def labels(
    *, flavor: str, cpu: int, site: str, zone: str, mce: str, request_id: str, redfish_label: tuple[str, str]
) -> dict[str, str]:
    """The VM's labels (design §7.4). The redfish label is what kubevirt-redfish exposes
    it by AND what the capacity figures count a stopped VM by."""
    return {
        redfish_label[0]: redfish_label[1],
        FLAVOR_LABEL: flavor,
        SIZE_LABEL: size_label(cpu),
        SITE_LABEL: site,
        ZONE_LABEL: zone,
        ROLE_LABEL: WORKER_ROLE,
        MCE_LABEL: mce,
        CREATED_BY_LABEL: request_id,
    }


def build(
    *,
    namespace: str,
    flavor: str,
    cpu: int,
    memory_gib: int,
    site: str,
    zone: str,
    mce: str,
    mac: str,
    request_id: str,
    workflow_id: str | None,
    redfish_label: tuple[str, str],
    storage_class_name: str,
    network_name: str,
    node_selector: dict[str, str],
    root_disk_size: str,
) -> dict[str, Any]:
    """The VirtualMachine for one request with one drawn MAC, as the object to create."""
    name = vm_name(flavor, cpu, site, mac)
    root = f"{name}{ROOT_DISK_SUFFIX}"
    memory = f"{memory_gib}Gi"
    template_spec: dict[str, Any] = {
        "evictionStrategy": "LiveMigrate",
        "domain": {
            "cpu": {"cores": cpu},
            "memory": {"guest": memory},
            "resources": {
                "requests": {"cpu": str(request_cores(cpu)), "memory": memory},
                "limits": {"cpu": str(cpu), "memory": memory},
            },
            "firmware": {"bootloader": {"efi": {"secureBoot": False}}},
            "devices": {
                "blockMultiQueue": True,
                "networkInterfaceMultiqueue": True,
                "disks": [{"name": ROOT_DISK_VOLUME, "dedicatedIOThread": True, "disk": {"bus": "virtio"}}],
                "interfaces": [{"name": "default", "macAddress": mac.lower(), "bridge": {}}],
                "rng": {},
            },
        },
        "networks": [{"name": "default", "multus": {"networkName": network_name}}],
        "volumes": [{"name": ROOT_DISK_VOLUME, "dataVolume": {"name": root}}],
    }
    if node_selector:
        template_spec["nodeSelector"] = dict(node_selector)
    metadata: dict[str, Any] = {
        "name": name,
        "namespace": namespace,
        "labels": labels(
            flavor=flavor, cpu=cpu, site=site, zone=zone, mce=mce, request_id=request_id, redfish_label=redfish_label
        ),
    }
    if workflow_id:
        metadata["annotations"] = {WORKFLOW_ID_ANNOTATION: workflow_id}
    return {
        "apiVersion": KUBEVIRT_API_VERSION,
        "kind": "VirtualMachine",
        "metadata": metadata,
        "spec": {
            "runStrategy": "Halted",
            "dataVolumeTemplates": [
                {
                    "metadata": {"name": root},
                    "spec": {
                        "source": {"blank": {}},
                        "storage": {
                            "storageClassName": storage_class_name,
                            "accessModes": ["ReadWriteMany"],
                            "volumeMode": "Filesystem",
                            "resources": {"requests": {"storage": root_disk_size}},
                        },
                    },
                }
            ],
            "template": {"metadata": {"labels": {"kubevirt.io/vm": name}}, "spec": template_spec},
        },
    }


# --- what a VirtualMachine says ------------------------------------------------------------


def name_of(vm: dict) -> str:
    return str(get(vm, "metadata", "name", default=""))


def namespace_of(vm: dict) -> str:
    return str(get(vm, "metadata", "namespace", default=""))


def uid_of(vm: dict) -> str | None:
    return get(vm, "metadata", "uid")


def macs(vm: dict) -> list[str]:
    """Every fixed MAC of the VM's interfaces, lowercased."""
    interfaces = get(vm, "spec", "template", "spec", "domain", "devices", "interfaces", default=[]) or []
    return [
        str(interface["macAddress"]).lower()
        for interface in interfaces
        if isinstance(interface, dict) and interface.get("macAddress")
    ]


def mac_of(vm: dict) -> str | None:
    """The FIRST interface's fixed MAC, lowercased; None when none is set."""
    found = macs(vm)
    return found[0] if found else None


def created_by(vm: dict) -> str | None:
    """The request id the VM was created for (its `redbull.io/created-by` label)."""
    return (get(vm, "metadata", "labels", default={}) or {}).get(CREATED_BY_LABEL)


def cores(vm: dict) -> int:
    return vcpus(vm)


def memory_gib(vm: dict) -> int:
    return guest_memory_bytes(vm) // GIB


def admitted(vm: dict) -> bool:
    return get(vm, "status", "printableStatus") in _ADMITTED_STATUSES


def root_disk_name(vm: dict) -> str | None:
    """The DataVolume of the `rootdisk` volume, else the first dataVolume volume."""
    volumes = get(vm, "spec", "template", "spec", "volumes", default=[]) or []
    for volume in volumes:
        if isinstance(volume, dict) and volume.get("name") == ROOT_DISK_VOLUME and volume.get("dataVolume"):
            return volume["dataVolume"].get("name")
    for volume in volumes:
        if isinstance(volume, dict) and volume.get("dataVolume"):
            return volume["dataVolume"].get("name")
    return None
