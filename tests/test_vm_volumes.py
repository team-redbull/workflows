"""Which volumes are ONE VM's (activities/vm_lifecycle/vm_volumes.py): the rule delete-vm's
three activities share. The boot ISO's names are kubevirt-redfish's, read in the team's
fork (`generateUniquePVCName`, `sanitizeResourceName`)."""

from __future__ import annotations

import hashlib

import pytest

from activities.vm_lifecycle import vm_spec, vm_volumes
from activities.vm_lifecycle.vm_volumes import BOOT_ISO, CUT_NAME, LABEL, OWNED, ROOT_DISK, SPEC
from tests.kv_fleet import REDFISH_LABEL

VM = "ocp-c200-kv32-five-k7q2m9xd"
NS = "inventory"


def sanitize(name: str) -> str:
    """kubevirt-redfish's sanitizeResourceName, as its Go code computes it."""
    if len(name) <= 63:
        return name
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    number, text = int.from_bytes(hashlib.sha256(name.encode()).digest(), "big"), ""
    while number:
        number, digit = divmod(number, 36)
        text = digits[digit] + text
    return name[:49] + text[:5] + "truncated"


def boot_iso(vm: str, timestamp: int = 1795000000, suffix: str = "012345") -> str:
    return sanitize(f"{vm}-bootiso-{timestamp}-{suffix}")


def named(length: int, serial: str = "k7q2m9xd") -> str:
    """A VM name of exactly `length` characters, ending in a serial as every VM's does."""
    head = "ocp-" + "f" * (length - len("ocp--kv32-five-") - len(serial)) + "-kv32-five-"
    return head + serial


def claim(name: str, *, uid: str | None = None, volume: str | None = None, labels: dict | None = None,
          owners: list[tuple[str, str]] | None = None, source: str | None = None, deleting: bool = False) -> dict:
    spec: dict = {"storageClassName": "cephfs"}
    if volume:
        spec["volumeName"] = volume
    if source:
        spec["dataSourceRef"] = {"apiGroup": "cdi.kubevirt.io", "kind": "VolumeImportSource", "name": source}
    metadata: dict = {"name": name, "namespace": NS, "uid": uid or f"uid-{name}", "finalizers": ["kubernetes.io/pvc-protection"]}
    if labels:
        metadata["labels"] = labels
    if owners:
        metadata["ownerReferences"] = [{"kind": kind, "name": owner} for kind, owner in owners]
    if deleting:
        metadata["deletionTimestamp"] = "2026-11-26T10:00:00Z"
    return {"metadata": metadata, "spec": spec, "status": {"phase": "Bound" if volume else "Pending"}}


def volume(name: str, claim_name: str, *, uid: str | None = None, namespace: str = NS, policy: str = "Delete",
           phase: str = "Bound") -> dict:
    return {
        "metadata": {"name": name},
        "spec": {
            "persistentVolumeReclaimPolicy": policy, "storageClassName": "cephfs",
            "claimRef": {"namespace": namespace, "name": claim_name, "uid": uid or f"uid-{claim_name}"},
            "csi": {"driver": "openshift-storage.cephfs.csi.ceph.com", "volumeHandle": f"handle-{name}"},
        },
        "status": {"phase": phase},
    }


def the_vm(**volumes_by_kind) -> dict:
    vm = vm_spec.build(
        namespace=NS, flavor="c200", cpu=32, memory_gib=240, site="five", zone="az1", mce="ocp4-prod-mce-five-a",
        serial="k7q2m9xd", request_id="run-1", workflow_id=None, kv_cluster="ocp4-prod-kv-five-az1-a",
        redfish_label=REDFISH_LABEL, storage_class_name="cephfs", network_name="openshift-cnv/vm-external-network",
        node_selector={}, root_disk_size="120Gi",
    )
    for name in volumes_by_kind.get("claims", []):
        vm["spec"]["template"]["spec"]["volumes"].append({"name": "cdrom0", "persistentVolumeClaim": {"claimName": name}})
    return vm


class TestTheBootIsoName:
    def test_the_name_kubevirt_redfish_gives_is_recognised_whole(self):
        assert len(VM) == 27 and len(boot_iso(VM)) == 53
        assert vm_volumes.boot_iso_rule(VM, boot_iso(VM)) == BOOT_ISO
        assert vm_volumes.boot_iso_rule(VM, f"{VM}-bootiso") == BOOT_ISO  # its fallback name on eject

    @pytest.mark.parametrize("other", [
        "ocp-c200-kv32-five-k7q2m9xe-bootiso-1795000000-012345",   # another VM's
        "ocp-c200-kv32-five-k7q2m9xd-bootiso-backup",               # not a timestamp and six digits
        "ocp-c200-kv32-five-k7q2m9xd-bootiso-1795000000-01234",
        "ocp-c200-kv32-five-k7q2m9xd-bootiso-1795000000-012345-prime",
        "xocp-c200-kv32-five-k7q2m9xd-bootiso-1795000000-012345",
        "ocp-c200-kv32-five-k7q2m9xd-data",
    ])
    def test_anything_else_is_not_it(self, other):
        assert vm_volumes.boot_iso_rule(VM, other) is None
        assert vm_volumes.name_rule(VM, other) is None

    @pytest.mark.parametrize("length", [38, 39, 40])
    def test_a_cut_name_that_keeps_the_whole_prefix_is_still_certain(self, length):
        """Over 37 characters the full name passes 63 and is cut to 49 plus a hash — the
        cut still holds all of `<vm>-bootiso-` while the VM's name is at most 40."""
        vm = named(length)
        cut = boot_iso(vm)
        assert len(vm) == length and len(cut) == 63 and cut.endswith("truncated")
        assert vm_volumes.boot_iso_rule(vm, cut) == BOOT_ISO
        assert vm_volumes.name_rule(vm, cut) == BOOT_ISO
        assert vm_volumes.boot_iso_rule(named(length, "zzzzzzzz"), cut) is None  # another VM of that length

    @pytest.mark.parametrize("length", [41, 45, 49, 50, 58])
    def test_a_cut_name_that_lost_part_of_the_prefix_is_only_a_look_alike(self, length):
        vm = named(length)
        cut = boot_iso(vm)
        assert vm_volumes.boot_iso_rule(vm, cut) == CUT_NAME
        assert vm_volumes.name_rule(vm, cut) is None  # never the VM's by its name alone

    def test_two_long_names_that_differ_in_their_last_characters_cut_to_the_same_prefix(self):
        """Why a cut name is not enough: past 49 characters the serial's tail is gone."""
        one, other = named(53, "k7q2m9xd"), named(53, "k7q2zzzz")
        cut = boot_iso(other)
        assert one[:49] == other[:49] and one != other
        assert vm_volumes.boot_iso_rule(one, cut) == CUT_NAME  # it LOOKS like one's and is other's

    def test_the_root_disk_is_recognised_by_its_name(self):
        assert vm_volumes.name_rule(VM, f"{VM}-root") == ROOT_DISK
        assert vm_volumes.name_rule(VM, f"{VM}-root2") is None


class TestWhichClaimsAreTheVms:
    def test_every_rule_names_itself(self):
        iso, cdi_iso = boot_iso(VM), boot_iso(VM, suffix="999999")
        vm = the_vm(claims=["hand-attached-disk"])
        pvcs = [
            claim(f"{VM}-root", owners=[("DataVolume", f"{VM}-root")], volume="pv-root"),
            claim("hand-attached-disk"),                              # the VM's spec names it
            claim(iso, volume="pv-iso"),                              # a bare claim, by its name
            claim("oddly-named", labels={"vm.redfish": VM}),          # kubevirt-redfish's label
            claim("prime-1234", owners=[("PersistentVolumeClaim", cdi_iso)]),  # CDI's, owned by a claim of the VM
            claim(cdi_iso, labels={"vm.redfish": VM}, source=f"{cdi_iso}-populator"),
            claim("someone-elses"),
            claim(f"{VM[:-1]}e-root"),
        ]
        found = vm_volumes.claims_of(VM, vm, pvcs)
        assert {row.name: row.rule for row in found.claims} == {
            f"{VM}-root": SPEC, "hand-attached-disk": SPEC, iso: BOOT_ISO, "oddly-named": LABEL,
            "prime-1234": OWNED, cdi_iso: BOOT_ISO,
        }
        assert found.look_alikes == []
        assert found.datavolumes == [f"{VM}-root"]

    def test_with_the_vm_gone_its_claims_are_still_found_by_name_label_and_owner(self):
        iso = boot_iso(VM)
        pvcs = [claim(f"{VM}-root"), claim(iso), claim("prime-1", owners=[("PersistentVolumeClaim", iso)]),
                claim("labelled", labels={"vm.redfish": VM}), claim("unrelated")]
        found = vm_volumes.claims_of(VM, None, pvcs)
        assert {row.name: row.rule for row in found.claims} == {
            f"{VM}-root": ROOT_DISK, iso: BOOT_ISO, "prime-1": OWNED, "labelled": LABEL,
        }

    def test_a_row_says_what_a_person_and_the_delete_need(self):
        iso = boot_iso(VM)
        pvcs = [claim(iso, uid="u-1", volume="pv-1", source="the-populator", deleting=True)]
        pods = [("copy-iso-pod", [iso]), ("other-pod", ["other-claim"])]
        [row] = vm_volumes.claims_of(VM, None, pvcs, pods).claims
        assert row.model_dump() == {
            "name": iso, "rule": BOOT_ISO, "uid": "u-1", "phase": "Bound", "volume": "pv-1", "storage_class": "cephfs",
            "deleting": True, "finalizers": ["kubernetes.io/pvc-protection"], "import_source": "the-populator",
            "used_by": ["copy-iso-pod"],
        }

    def test_a_data_source_that_is_not_a_volume_import_source_is_not_one(self):
        other = claim("x", labels={"vm.redfish": VM})
        other["spec"]["dataSourceRef"] = {"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": "snap"}
        assert vm_volumes.claims_of(VM, None, [other]).claims[0].import_source is None

    def test_a_cut_name_is_a_look_alike_until_something_else_says_whose_it_is(self):
        vm_name = named(53)
        cut = boot_iso(vm_name)
        bare = vm_volumes.claims_of(vm_name, None, [claim(cut)])
        assert bare.claims == [] and [row.rule for row in bare.look_alikes] == [CUT_NAME]
        labelled = vm_volumes.claims_of(vm_name, None, [claim(cut, labels={"vm.redfish": vm_name})])
        assert [row.rule for row in labelled.claims] == [LABEL] and labelled.look_alikes == []
        inserted = {"metadata": {"name": vm_name}, "spec": {"template": {"spec": {"volumes": [
            {"name": "cdrom0", "persistentVolumeClaim": {"claimName": cut}}]}}}}
        named_by_the_vm = vm_volumes.claims_of(vm_name, inserted, [claim(cut)])
        assert [row.rule for row in named_by_the_vm.claims] == [SPEC] and named_by_the_vm.look_alikes == []

    def test_a_datavolume_that_owns_one_of_the_vms_claims_is_one_to_look_for(self):
        pvcs = [claim("imported-disk", labels={"vm.redfish": VM}, owners=[("DataVolume", "imported-disk-dv")])]
        assert vm_volumes.claims_of(VM, None, pvcs).datavolumes == ["imported-disk-dv", f"{VM}-root"]

    def test_the_claims_a_caller_already_holds_are_taken_as_the_vms(self):
        """The wait's: a claim the VM's spec named is no longer recognisable once the VM is gone."""
        found = vm_volumes.claims_of(VM, None, [claim("hand-attached-disk"), claim("unrelated")], known=["hand-attached-disk"])
        assert [row.name for row in found.claims] == ["hand-attached-disk"]

    def test_the_pods_of_another_namespace_are_not_kept(self):
        keep = vm_volumes.pod_claims(NS)
        mounting = {"metadata": {"name": "p", "namespace": NS}, "spec": {"volumes": [
            {"name": "a", "persistentVolumeClaim": {"claimName": "c1"}}, {"name": "b", "emptyDir": {}}]}}
        assert keep(mounting) == ("p", ["c1"])
        assert keep({"metadata": {"name": "p", "namespace": "other"}, "spec": mounting["spec"]}) is None
        assert keep({"metadata": {"name": "q", "namespace": NS}, "spec": {"volumes": [{"name": "b", "emptyDir": {}}]}}) is None


class TestWhichVolumesAndSnapshots:
    def test_a_volume_is_the_vms_by_its_bound_claim_or_its_claim_ref(self):
        rows = vm_volumes.claims_of(VM, None, [claim(f"{VM}-root", volume="pv-root")]).claims
        pvs = [volume("pv-root", f"{VM}-root"), volume("pv-other", "unrelated"),
               volume("pv-elsewhere", f"{VM}-root", namespace="other-namespace")]
        [found] = vm_volumes.volumes_of(VM, NS, rows, pvs)
        assert found.model_dump() == {
            "name": "pv-root", "claim": f"{VM}-root", "reclaim_policy": "Delete", "phase": "Bound",
            "storage_class": "cephfs", "driver": "openshift-storage.cephfs.csi.ceph.com", "volume_handle": "handle-pv-root",
            "deleting": False, "finalizers": [],
        }

    def test_with_the_claims_gone_a_volume_is_still_found_by_the_name_its_claim_ref_carries(self):
        """A re-run: the VM and its claims are gone, a Released `Retain` volume is not."""
        iso = boot_iso(VM)
        pvs = [volume("pv-root", f"{VM}-root", policy="Retain", phase="Released"), volume("pv-iso", iso, phase="Released"),
               volume("pv-other", "unrelated", phase="Released")]
        assert [row.name for row in vm_volumes.volumes_of(VM, NS, [], pvs)] == ["pv-iso", "pv-root"]

    def test_a_claim_ref_to_an_earlier_claim_of_the_same_name_is_not_this_claims_volume(self):
        rows = vm_volumes.claims_of("some-vm", None, [claim("some-vm-root", uid="new-uid")]).claims
        stale = volume("pv-old", "some-vm-root", uid="old-uid", phase="Released")
        assert vm_volumes.volumes_of("some-vm", NS, rows, [stale]) == []

    def test_a_look_alikes_volume_is_not_the_vms(self):
        vm_name = named(53)
        cut = boot_iso(vm_name)
        assert vm_volumes.volumes_of(vm_name, NS, [], [volume("pv-cut", cut)]) == []

    def test_a_snapshot_is_the_vms_when_its_source_claim_is(self):
        rows = vm_volumes.claims_of(VM, None, [claim("labelled", labels={"vm.redfish": VM})]).claims
        snapshots = [
            {"metadata": {"name": "of-root"}, "spec": {"source": {"persistentVolumeClaimName": f"{VM}-root"}},
             "status": {"readyToUse": True, "boundVolumeSnapshotContentName": "snapcontent-1"}},
            {"metadata": {"name": "of-labelled"}, "spec": {"source": {"persistentVolumeClaimName": "labelled"}}},
            {"metadata": {"name": "of-another"}, "spec": {"source": {"persistentVolumeClaimName": "unrelated"}}},
            {"metadata": {"name": "pre-provisioned"}, "spec": {"source": {"volumeSnapshotContentName": "x"}}},
        ]
        found = vm_volumes.snapshots_of(VM, rows, snapshots)
        assert [(row.name, row.claim, row.ready, row.content) for row in found] == [
            ("of-labelled", "labelled", None, None), ("of-root", f"{VM}-root", True, "snapcontent-1"),
        ]


class TestWhatAVmSays:
    def test_the_spec_names_its_datavolumes_and_claims(self):
        assert vm_volumes.spec_names(the_vm(claims=["iso-claim"])) == ([f"{VM}-root"], ["iso-claim"])
        assert vm_volumes.spec_names(None) == ([], [])

    @pytest.mark.parametrize(("status", "running"), [
        ({"printableStatus": "Stopped"}, False), ({"printableStatus": "Provisioning"}, False),
        ({"printableStatus": "Running", "created": True}, True), ({"printableStatus": "Starting"}, True),
        ({"printableStatus": "Stopping"}, True), ({"printableStatus": "Stopped", "created": True}, True),
        ({}, False),
    ])
    def test_a_vm_with_a_guest_is_running(self, status, running):
        assert vm_volumes.is_running({"status": status}) is running
        assert vm_volumes.is_running(None) is False
