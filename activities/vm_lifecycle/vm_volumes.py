"""Which volumes are ONE VM's — the rule delete-vm's three activities share. Pure.

No settings, no Temporal, no I/O: vm_operations.py hands this the objects it
read (the API server's own JSON) and gets back the rows delete-vm records.

A VM's storage outlives the VirtualMachine object in several ways, so the rule
must still find it once the VM — and then its claims — are gone:

  * THE ROOT DISK is a DataVolume `<vm>-root` from the VM's own template
    (vm_spec.py), which CDI turns into a claim of the same name.
  * THE BOOT ISO is kubevirt-redfish's: a BARE claim nothing owns, named
    `<vm>-bootiso-<unix seconds>-<6 digits>`, referenced from the VM's volumes
    while the media is inserted and deleted by kubevirt-redfish on eject —
    which metal3 does while it releases the host — so it is a leftover only
    when that did not happen. Read in the team's kubevirt-redfish fork,
    `pkg/kubevirt/client.go` (commit 9f98767): `generateUniquePVCName`, and
    `sanitizeResourceName`, which cuts any name over 63 characters to its first
    49 plus a 5-character hash and the word `truncated`. On the CDI import path
    (Block storage) the claim carries the label `vm.redfish: <vm>` and a
    `dataSourceRef` to a VolumeImportSource, and CDI's `prime-…` claim is owned
    by it; on the helper-pod path (Filesystem) it carries no label at all.

In the VM's namespace a claim is the VM's when ANY of:
  spec       the VirtualMachine names it (`dataVolumeTemplates`,
             `volumes[].dataVolume`, `volumes[].persistentVolumeClaim`);
  root-disk  its name is `<vm>-root`;
  boot-iso   its name is the boot ISO's, whole — or cut, while the cut still
             keeps all of `<vm>-bootiso-` (a VM name of at most 40 characters);
  label      it carries `vm.redfish: <vm>`;
  owned      an owner reference names the VirtualMachine, another of the VM's
             claims, or one of its DataVolumes.
A claim whose CUT name keeps only part of `<vm>-bootiso-` no longer says whose
it is (two long names differing in their last characters cut to the same 49):
by name alone it is a `cut-name` LOOK-ALIKE, reported and never deleted.

A PersistentVolume is the VM's when one of those claims is bound to it, or its
`claimRef` names one of them — or names a claim the NAME rules recognise,
which is how a re-run finds a volume after the VM and its claims are gone. A
VolumeSnapshot is the VM's when its source claim is, by the same two ways.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from activities.vm_lifecycle.cluster_figures import get
from shared.infraenv_names import ROOT_DISK_SUFFIX
from shared.models.vm_lifecycle import VmClaim, VmSnapshot, VmVolume

# kubevirt-redfish's own labels on a boot ISO claim it fills through CDI (and on CDI's
# prime claim, which inherits them): the VM's name, and the CD-ROM device.
REDFISH_VM_LABEL = "vm.redfish"
BOOT_ISO_INFIX = "-bootiso"
# sanitizeResourceName: a name over this is cut to its first _KEPT characters, a
# 5-character base-36 hash and this word.
MAX_NAME = 63
_KEPT = 49
_CUT_TAIL = re.compile(r"^[0-9a-z]{5}truncated$")
_ISO_TAIL = re.compile(r"^[0-9]+-[0-9]{6}$")
_CDI_GROUP = "cdi.kubevirt.io"

SPEC, ROOT_DISK, BOOT_ISO, LABEL, OWNED, CUT_NAME = "spec", "root-disk", "boot-iso", "label", "owned", "cut-name"
# The states a VM is in while a guest of it exists (KubeVirt's printableStatus).
_RUNNING_STATUSES = frozenset({"Running", "Starting", "Stopping", "Paused", "Migrating", "Terminating"})


def boot_iso_rule(vm_name: str, claim_name: str) -> str | None:
    """BOOT_ISO when `claim_name` is this VM's boot ISO claim for certain, CUT_NAME when it
    only may be (see the module docstring), None when it is not."""
    prefix = f"{vm_name}{BOOT_ISO_INFIX}"
    if claim_name == prefix:  # the name kubevirt-redfish falls back to on eject
        return BOOT_ISO
    prefix += "-"
    if claim_name.startswith(prefix) and len(claim_name) <= MAX_NAME and _ISO_TAIL.match(claim_name[len(prefix) :]):
        return BOOT_ISO
    if len(claim_name) != MAX_NAME or not _CUT_TAIL.match(claim_name[_KEPT:]):
        return None
    kept = claim_name[:_KEPT]
    if len(prefix) <= _KEPT:  # the cut kept the whole prefix, and some of the timestamp after it
        return BOOT_ISO if kept.startswith(prefix) and re.fullmatch(r"[0-9-]*", kept[len(prefix) :]) else None
    return CUT_NAME if kept == prefix[:_KEPT] else None


def name_rule(vm_name: str, claim_name: str) -> str | None:
    """The rule that makes a claim the VM's BY ITS NAME ALONE, certain ones only: ROOT_DISK,
    BOOT_ISO or None. What still recognises a claim once the claim itself is gone."""
    if claim_name == f"{vm_name}{ROOT_DISK_SUFFIX}":
        return ROOT_DISK
    rule = boot_iso_rule(vm_name, claim_name)
    return rule if rule == BOOT_ISO else None


def spec_names(vm: dict | None) -> tuple[list[str], list[str]]:
    """(DataVolumes, claims) the VirtualMachine names in its spec; a DataVolume's claim has its name."""
    if not vm:
        return [], []
    datavolumes: list[str] = []
    claims: list[str] = []
    for template in get(vm, "spec", "dataVolumeTemplates", default=[]) or []:
        name = get(template, "metadata", "name")
        if name:
            datavolumes.append(str(name))
    for volume in get(vm, "spec", "template", "spec", "volumes", default=[]) or []:
        if not isinstance(volume, dict):
            continue
        datavolume = get(volume, "dataVolume", "name")
        if datavolume:
            datavolumes.append(str(datavolume))
        claim = get(volume, "persistentVolumeClaim", "claimName")
        if claim:
            claims.append(str(claim))
    return sorted(set(datavolumes)), sorted(set(claims))


def is_running(vm: dict | None) -> bool:
    """A guest of the VM exists (or is on its way up or down): its delete also shuts one down."""
    if not vm:
        return False
    return bool(get(vm, "status", "created")) or get(vm, "status", "printableStatus") in _RUNNING_STATUSES


def pod_claims(namespace: str) -> Callable[[dict], tuple[str, list[str]] | None]:
    """A reducer for the unfinished-pods list: (pod name, the claims it mounts) for a pod of
    `namespace` that mounts any, else None — so a ten-thousand-pod list keeps a handful."""

    def keep(pod: dict) -> tuple[str, list[str]] | None:
        if get(pod, "metadata", "namespace") != namespace:
            return None
        claims = [
            str(volume["persistentVolumeClaim"]["claimName"])
            for volume in get(pod, "spec", "volumes", default=[]) or []
            if isinstance(volume, dict) and get(volume, "persistentVolumeClaim", "claimName")
        ]
        return (str(get(pod, "metadata", "name", default="")), claims) if claims else None

    return keep


def _owners(obj: dict) -> list[tuple[str, str]]:
    return [
        (str(owner.get("kind", "")), str(owner.get("name", "")))
        for owner in get(obj, "metadata", "ownerReferences", default=[]) or []
        if isinstance(owner, dict)
    ]


def _import_source(claim: dict) -> str | None:
    """The CDI VolumeImportSource a claim is filled from — read from the claim, never built:
    kubevirt-redfish names it `<claim>-populator` and cuts THAT too when it is over 63."""
    ref = get(claim, "spec", "dataSourceRef", default={}) or {}
    if ref.get("kind") == "VolumeImportSource" and str(ref.get("apiGroup") or "") == _CDI_GROUP and ref.get("name"):
        return str(ref["name"])
    return None


def _claim_row(claim: dict, rule: str, used_by: list[str]) -> VmClaim:
    return VmClaim(
        name=str(get(claim, "metadata", "name", default="")),
        rule=rule,
        uid=get(claim, "metadata", "uid"),
        phase=get(claim, "status", "phase"),
        volume=get(claim, "spec", "volumeName"),
        storage_class=get(claim, "spec", "storageClassName"),
        deleting=bool(get(claim, "metadata", "deletionTimestamp")),
        finalizers=[str(item) for item in get(claim, "metadata", "finalizers", default=[]) or []],
        import_source=_import_source(claim),
        used_by=sorted(used_by),
    )


def volume_row(volume: dict) -> VmVolume:
    """A PersistentVolume as delete-vm records it."""
    return VmVolume(
        name=str(get(volume, "metadata", "name", default="")),
        claim=get(volume, "spec", "claimRef", "name"),
        reclaim_policy=get(volume, "spec", "persistentVolumeReclaimPolicy"),
        phase=get(volume, "status", "phase"),
        storage_class=get(volume, "spec", "storageClassName"),
        driver=get(volume, "spec", "csi", "driver"),
        volume_handle=get(volume, "spec", "csi", "volumeHandle"),
        deleting=bool(get(volume, "metadata", "deletionTimestamp")),
        finalizers=[str(item) for item in get(volume, "metadata", "finalizers", default=[]) or []],
    )


@dataclass(frozen=True)
class Claims:
    """The VM's claims among a namespace's, the look-alikes, and the DataVolumes to look for."""

    datavolumes: list[str] = field(default_factory=list)
    claims: list[VmClaim] = field(default_factory=list)
    look_alikes: list[VmClaim] = field(default_factory=list)


def claims_of(
    vm_name: str,
    vm: dict | None,
    pvcs: list[dict],
    pods: list[tuple[str, list[str]]] | None = None,
    known: list[str] | None = None,
) -> Claims:
    """The rule of the module docstring over one namespace's claims.

    `vm` is the VirtualMachine when it still exists. `known` are claim names a
    caller already holds as the VM's (the wait's, from the run's history) and
    takes as SPEC. `pods` are `pod_claims` rows, for `used_by`.
    """
    spec_datavolumes, spec_claims = spec_names(vm)
    datavolumes = set(spec_datavolumes) | {f"{vm_name}{ROOT_DISK_SUFFIX}"}
    named = set(spec_claims) | set(spec_datavolumes) | set(known or [])
    by_name = {str(get(claim, "metadata", "name", default="")): claim for claim in pvcs}
    rules: dict[str, str] = {}
    look_alikes: dict[str, str] = {}
    for name, claim in by_name.items():
        if name in named:
            rules[name] = SPEC
        elif name_rule(vm_name, name):
            rules[name] = name_rule(vm_name, name) or ""
        elif (get(claim, "metadata", "labels", default={}) or {}).get(REDFISH_VM_LABEL) == vm_name:
            rules[name] = LABEL
        elif boot_iso_rule(vm_name, name) == CUT_NAME:
            look_alikes[name] = CUT_NAME
    # Owner references, to a fixed point: a claim owned by one of the VM's claims or
    # DataVolumes (CDI's prime claim; a DataVolume's own claim) is the VM's, and a
    # DataVolume that owns one of the VM's claims is one to look for.
    changed = True
    while changed:
        changed = False
        for name, claim in by_name.items():
            owners = _owners(claim)
            if name not in rules and any(
                (kind == "VirtualMachine" and owner == vm_name)
                or (kind == "PersistentVolumeClaim" and owner in rules)
                or (kind == "DataVolume" and owner in datavolumes)
                for kind, owner in owners
            ):
                rules[name] = OWNED
                look_alikes.pop(name, None)
                changed = True
            if name in rules:
                for kind, owner in owners:
                    if kind == "DataVolume" and owner not in datavolumes:
                        datavolumes.add(owner)
                        changed = True
    users: dict[str, list[str]] = {}
    for pod, mounted in pods or []:
        for claim_name in mounted:
            users.setdefault(claim_name, []).append(pod)
    return Claims(
        datavolumes=sorted(datavolumes),
        claims=[_claim_row(by_name[name], rules[name], users.get(name, [])) for name in sorted(rules)],
        look_alikes=[_claim_row(by_name[name], CUT_NAME, users.get(name, [])) for name in sorted(look_alikes)],
    )


def _is_claim_of(vm_name: str, claim_name: str | None, claims: dict[str, VmClaim]) -> bool:
    return bool(claim_name) and (claim_name in claims or name_rule(vm_name, str(claim_name)) is not None)


def volumes_of(vm_name: str, namespace: str, claims: list[VmClaim], pvs: list[dict]) -> list[VmVolume]:
    """The PersistentVolumes behind the VM's claims: bound to one, or reserved for one by
    `claimRef` — a claim that exists (the same object, by UID) or one the NAME rules
    recognise (the claim is gone and its volume is not)."""
    by_name = {claim.name: claim for claim in claims}
    bound = {claim.volume for claim in claims if claim.volume}
    found: list[VmVolume] = []
    for volume in pvs:
        name = str(get(volume, "metadata", "name", default=""))
        ref = get(volume, "spec", "claimRef", default={}) or {}
        mine = name in bound
        if not mine and ref.get("namespace") == namespace and _is_claim_of(vm_name, ref.get("name"), by_name):
            claim = by_name.get(str(ref.get("name")))
            # A claimRef to a claim that exists must be to THAT claim, not an earlier one of its name.
            mine = claim is None or not claim.uid or not ref.get("uid") or claim.uid == ref.get("uid")
        if mine:
            found.append(volume_row(volume))
    return sorted(found, key=lambda row: row.name)


def snapshots_of(vm_name: str, claims: list[VmClaim], snapshots: list[dict]) -> list[VmSnapshot]:
    """The VolumeSnapshots whose source is one of the VM's claims, existing or recognised by name."""
    by_name = {claim.name: claim for claim in claims}
    found: list[VmSnapshot] = []
    for snapshot in snapshots:
        source = get(snapshot, "spec", "source", "persistentVolumeClaimName")
        if _is_claim_of(vm_name, source, by_name):
            found.append(
                VmSnapshot(
                    name=str(get(snapshot, "metadata", "name", default="")),
                    claim=str(source),
                    ready=get(snapshot, "status", "readyToUse"),
                    content=get(snapshot, "status", "boundVolumeSnapshotContentName"),
                )
            )
    return sorted(found, key=lambda row: row.name)
