"""The VirtualMachine a KV cluster's worker builds, and what a VirtualMachine says — pure.

docs/design/create-vm-placement.md §7 is the design. No settings, no Temporal,
no I/O: vm_operations.py hands this the request and the cluster's own values.

  * THE MAC is KubeMacPool's: the VM is created with none and KubeMacPool
    assigns one from its pool at admission, keeping it unique on the cluster
    and fixed across restarts (the operator, 2026-11-25). A requested MAC it
    does NOT guard — on cluster-qcl8n it admitted three VMs requesting one —
    so this worker never requests one. Each cluster's range is set in its
    HyperConverged object, later, so clusters on one network do not overlap.
  * THE NAME carries a SERIAL, `ocp-<flavor>-kv<cpu>-<site>-<serial>`
    (shared/infraenv_names.vm_name): 8 random lowercase letters and digits,
    drawn by the worker, also the VM's SMBIOS serial (`firmware.serial`), as a
    bare-metal node's name carries its serial. The zone is a label, never in it.
  * THE BODY is the platform's vm.yaml (redbull-platform, rendered until
    2026-11-20 by the kv-vm-api agent from `kv-agents/templates/vm.yaml.j2`),
    built here as a plain object: Halted, a blank root disk on a DataVolume
    template (ReadWriteMany + Filesystem), one bridged interface on the
    cluster's Multus network (vm.yaml's `openshift-cnv/vm-external-network`
    unless the cluster names another) whose MAC KubeMacPool writes in, the CPU
    request `vCPUs × 15/16` floored at one core.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from activities.vm_lifecycle.cluster_figures import get, guest_memory_bytes, vcpus
from shared.infraenv_names import (
    CREATED_BY_LABEL,
    KV_CLUSTER_LABEL,
    ROOT_DISK_SUFFIX,
    VM_SERIAL_ALPHABET,
    VM_SERIAL_LENGTH,
    vm_name,
    vm_size,
)
from shared.upgrade_layout import ZONE_LABEL
from shared.vm_requests import GIB, request_cores

KUBEVIRT_API_VERSION = "kubevirt.io/v1"
# How a run finds the VM it created (a retried create, a rollback): the RUN's
# id, a UUID, as a label — a slot's workflow id is reused by later runs and
# holds characters a label value cannot. CREATED_BY_LABEL, from shared/infraenv_names.py.
WORKFLOW_ID_ANNOTATION = "redbull.io/workflow-id"
FLAVOR_LABEL = "redbull.io/flavor"
SIZE_LABEL = "redbull.io/size"
SITE_LABEL = "redbull.io/site"
ROLE_LABEL = "redbull.io/role"
MCE_LABEL = "redbull.io/mce"
# The KV cluster the VM lives on, its day1 file name (the naming convention's §4, since
# 2026-11-26): an inventory gathered from many KV clusters says where each VM is, and
# delete-vm refuses a VM that names another cluster than its worker's own.
# KV_CLUSTER_LABEL, from shared/infraenv_names.py.
# A tenant warm-pool VM (the naming convention's §4.b).
WORKER_ROLE = "worker"
ROOT_DISK_VOLUME = "rootdisk"
# printableStatus values KubeVirt reaches only after it accepted the spec.
_ADMITTED_STATUSES = frozenset(
    {"Stopped", "Running", "Starting", "Stopping", "Paused", "Migrating", "WaitingForVolumeBinding"}
)


def draw_serial(random_bytes: Callable[[int], bytes]) -> str:
    """VM_SERIAL_LENGTH characters of VM_SERIAL_ALPHABET, uniform: a byte past the largest
    multiple of the alphabet's size is passed over, so no character is likelier than another."""
    size = len(VM_SERIAL_ALPHABET)
    limit = 256 - 256 % size
    serial: list[str] = []
    while len(serial) < VM_SERIAL_LENGTH:
        for byte in random_bytes(VM_SERIAL_LENGTH):
            if byte < limit and len(serial) < VM_SERIAL_LENGTH:
                serial.append(VM_SERIAL_ALPHABET[byte % size])
    return "".join(serial)


def size_label(cpu: int) -> str:
    """The `redbull.io/size` label's value: `kv<cpu>` (`kv32`), the name's size field —
    the naming convention's §4 value, the same as its InfraEnv's Agents carry (the
    operator, 2026-11-22; from 2026-11-21 it was `<cpu>c`)."""
    return vm_size(cpu)


def labels(
    *, flavor: str, cpu: int, site: str, zone: str, mce: str, request_id: str, kv_cluster: str,
    redfish_label: tuple[str, str],
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
        KV_CLUSTER_LABEL: kv_cluster,
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
    serial: str,
    request_id: str,
    workflow_id: str | None,
    kv_cluster: str,
    redfish_label: tuple[str, str],
    storage_class_name: str,
    network_name: str,
    node_selector: dict[str, str],
    root_disk_size: str,
) -> dict[str, Any]:
    """The VirtualMachine for one request with one drawn serial, as the object to create.
    No MAC: KubeMacPool assigns one at admission."""
    name = vm_name(flavor, cpu, site, serial)
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
            "firmware": {"serial": serial, "bootloader": {"efi": {"secureBoot": False}}},
            "devices": {
                "blockMultiQueue": True,
                "networkInterfaceMultiqueue": True,
                "disks": [{"name": ROOT_DISK_VOLUME, "dedicatedIOThread": True, "disk": {"bus": "virtio"}}],
                "interfaces": [{"name": "default", "bridge": {}}],
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
            flavor=flavor, cpu=cpu, site=site, zone=zone, mce=mce, request_id=request_id, kv_cluster=kv_cluster,
            redfish_label=redfish_label,
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
