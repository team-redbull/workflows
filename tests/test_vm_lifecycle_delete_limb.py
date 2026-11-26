"""delete-vm on the limb side: the three operations (vm_operations.py) on a fake KV cluster
whose storage behaves — a VM's delete takes its DataVolumes, a DataVolume its claim, a claim
its PersistentVolume — the new calls of the Kubernetes client wrapper (kubevirt_api.py) on
fake SDK clients, and the thin activities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from temporalio.testing import ActivityEnvironment

from activities.vm_lifecycle import activities as limb
from activities.vm_lifecycle import kubevirt_api, vm_operations
from activities.vm_lifecycle.kubevirt_api import KubevirtApi
from shared.exceptions import (
    KvClusterAccessError,
    KvClusterApiError,
    KvClusterMismatchError,
    VmIdentityConflictError,
    VmRequestRejectedError,
)
from shared.models.vm_lifecycle import VmCreated, VmRemovalRequest
from tests.kv_fleet import FakeKvCluster, stopped_vm
from tests.test_vm_lifecycle_limb import KV, NOW, RUN, FakeCore, FakeCustom, _Raw, api_error, cluster, create, create_request

MCE = "ocp4-prod-mce-five-a"
NS = "inventory"


async def made(kv: FakeKvCluster, **overrides) -> VmCreated:
    """A VM as a completed create-vm run leaves it: Halted, its root disk imported and bound."""
    created = await create(kv, create_request(**overrides))
    kv.bind_now(NS, created.name)
    return created


def removal(name: str, **overrides) -> VmRemovalRequest:
    values = dict(kv_cluster=KV, namespace=NS, name=name, mce=MCE)
    values.update(overrides)
    return VmRemovalRequest(**values)


def watching(name: str, state, **overrides) -> VmRemovalRequest:
    """The wait's request, as the workflow builds it from what it saw."""
    return removal(
        name, vm_uid=state.vm_uid, datavolumes=state.datavolumes, claims=[claim.name for claim in state.claims],
        volumes=[volume.name for volume in state.volumes], **overrides,
    )


async def inspect(kv: FakeKvCluster, request: VmRemovalRequest):
    return await vm_operations.inspect_for_deletion(kv, kv.values, request)


async def delete(kv: FakeKvCluster, request: VmRemovalRequest):
    return await vm_operations.delete_with_volumes(kv, kv.values, request)


async def wait_gone(kv: FakeKvCluster, request: VmRemovalRequest, *, waited_seconds: float = 0.0, polls: int = 0,
                    beats=None, clock=None):
    clock = clock if clock is not None else {"elapsed": 0.0}

    async def sleep(seconds: float) -> None:
        clock["elapsed"] += seconds

    return await vm_operations.wait_gone(
        kv, kv.values, request, waited_seconds=waited_seconds, polls=polls,
        now=lambda: NOW + timedelta(seconds=clock["elapsed"]),
        heartbeat=lambda waited, count: (beats if beats is not None else []).append((waited, count)), sleep=sleep,
    )


def writes(kv: FakeKvCluster) -> list[str]:
    return [call for call in kv.calls if call.startswith("delete_")]


# --- inspect_for_deletion ----------------------------------------------------------------------


class TestInspect:
    async def test_a_vm_as_create_vm_left_it(self):
        kv = cluster()
        created = await made(kv)
        kv.calls.clear()
        state = await inspect(kv, removal(created.name, created_by=RUN, mac=created.mac))
        assert state.found and state.vm_found and not state.was_running
        assert state.vm_uid == kv.vms[(NS, created.name)]["metadata"]["uid"]
        assert (state.mac, state.created_by) == (created.mac, RUN)
        assert state.datavolumes == [f"{created.name}-root"]
        [claim] = state.claims
        assert (claim.name, claim.rule, claim.phase) == (f"{created.name}-root", "spec", "Bound")
        [volume] = state.volumes
        assert volume.name == claim.volume and volume.claim == claim.name
        assert (volume.reclaim_policy, volume.phase, volume.driver) == ("Delete", "Bound", "fake.csi.redbull.io")
        assert volume.volume_handle and state.snapshots == [] and state.look_alikes == []
        assert writes(kv) == []  # observe-only

    async def test_nothing_there_is_a_normal_answer(self):
        kv = cluster()
        other = await made(kv)
        state = await inspect(kv, removal("ocp-c200-kv32-five-notthere"))
        assert not state.found and not state.vm_found and state.claims == [] and state.volumes == []
        assert (NS, other.name) in kv.vms

    async def test_a_boot_iso_a_snapshot_and_a_pod_are_all_reported(self):
        kv = cluster()
        created = await made(kv)
        iso = kv.add_bootiso(NS, created.name, cdi=True)
        kv.add_snapshot(NS, "nightly", f"{created.name}-root")
        kv.add_snapshot(NS, "of-another-disk", "unrelated-claim")
        kv.add_claim_pod(NS, "copy-iso-helper", iso)
        state = await inspect(kv, removal(created.name))
        by_name = {claim.name: claim for claim in state.claims}
        assert set(by_name) == {f"{created.name}-root", iso}
        assert by_name[iso].import_source == f"{iso}-populator" and by_name[iso].used_by == ["copy-iso-helper"]
        assert len(state.volumes) == 2
        assert [(snapshot.name, snapshot.claim) for snapshot in state.snapshots] == [("nightly", f"{created.name}-root")]

    async def test_a_running_vm_is_said_to_be_running(self):
        kv = cluster()
        created = await made(kv)
        kv.start(NS, created.name, "n1")
        state = await inspect(kv, removal(created.name))
        assert state.was_running
        assert state.claims[0].used_by == [f"virt-launcher-{created.name}"]

    async def test_with_the_vm_gone_what_it_left_is_still_found(self):
        """A re-run: the VirtualMachine and its claims are gone, a `Retain` volume is Released."""
        kv = cluster()
        kv.reclaim_policy = "Retain"
        created = await made(kv)
        await delete(kv, removal(created.name))
        state = await inspect(kv, removal(created.name))
        assert state.found and not state.vm_found and state.claims == [] and state.datavolumes == []
        [volume] = state.volumes
        assert (volume.phase, volume.reclaim_policy, volume.claim) == ("Released", "Retain", f"{created.name}-root")

    @pytest.mark.parametrize(("labels", "request_fields", "said"), [
        ({"redbull.io/created-by": None}, {}, "create-vm did not make it"),
        ({"redbull.io/mce": "ocp4-prod-mce-five-b"}, {}, "redbull.io/mce is ocp4-prod-mce-five-b"),
        ({}, {"created_by": "another-run"}, "redbull.io/created-by is"),
        ({}, {"mac": "02:b0:96:ff:ff:ff"}, "its MAC is 02:b0:96:00:00:00"),
        ({"redbull.io/kv-cluster": "ocp4-prod-kv-five-az1-b"}, {}, "redbull.io/kv-cluster is ocp4-prod-kv-five-az1-b"),
    ])
    async def test_a_vm_that_is_not_the_one_asked_for_is_refused_and_left_alone(self, labels, request_fields, said):
        kv = cluster()
        created = await made(kv)
        vm_labels = kv.vms[(NS, created.name)]["metadata"]["labels"]
        for key, value in labels.items():
            vm_labels.pop(key) if value is None else vm_labels.__setitem__(key, value)
        kv.calls.clear()
        for operation in (inspect, delete):
            with pytest.raises(VmIdentityConflictError, match=said) as refused:
                await operation(kv, removal(created.name, **request_fields))
            assert "Nothing was deleted" in str(refused.value)
        assert writes(kv) == [] and (NS, created.name) in kv.vms and len(kv.pvcs) == 1
        assert kv.calls == ["get_vm", "get_vm"]  # refused before anything else is even read

    async def test_a_hand_made_vm_of_that_name_is_never_deleted(self):
        kv = cluster()
        kv.add_vm(stopped_vm("someones-vm", 8, 60))
        with pytest.raises(VmIdentityConflictError, match="create-vm did not make it"):
            await delete(kv, removal("someones-vm"))
        assert (NS, "someones-vm") in kv.vms

    async def test_a_vm_made_before_the_kv_cluster_label_is_still_the_one(self):
        kv = cluster()
        created = await made(kv)
        kv.vms[(NS, created.name)]["metadata"]["labels"].pop("redbull.io/kv-cluster")
        assert (await inspect(kv, removal(created.name))).vm_found

    async def test_the_mac_is_compared_without_its_case(self):
        kv = cluster()
        created = await made(kv)
        assert (await inspect(kv, removal(created.name, mac=created.mac.upper()))).vm_found

    @pytest.mark.parametrize("operation", [inspect, delete, wait_gone])
    async def test_a_request_for_another_cluster_is_refused(self, operation):
        kv = cluster()
        with pytest.raises(KvClusterMismatchError):
            await operation(kv, removal("x", kv_cluster="ocp4-prod-kv-five-az1-b"))
        assert kv.calls == []


# --- delete_with_volumes -----------------------------------------------------------------------


class TestDelete:
    async def test_the_vm_its_datavolume_and_its_claim_go_and_the_volume_follows(self):
        kv = cluster()
        created = await made(kv)
        uid = kv.vms[(NS, created.name)]["metadata"]["uid"]
        deleted = await delete(kv, removal(created.name))
        assert deleted.vm_deleted and deleted.seen.vm_uid == uid
        # The DataVolume went with its VM (the garbage collector's cascade), so this attempt's
        # own delete of it found nothing: what was THERE is in `seen`.
        assert deleted.seen.datavolumes == [f"{created.name}-root"] and kv.datavolumes == {}
        assert deleted.claims_deleted == [f"{created.name}-root"] and deleted.import_sources_deleted == []
        assert [volume.phase for volume in deleted.seen.volumes] == ["Bound"]  # as it was right before
        gone = await wait_gone(kv, watching(created.name, deleted.seen))
        assert gone.result == "done" and kv.vms == {} and kv.datavolumes == {} and kv.pvcs == {} and kv.pvs == {}

    async def test_an_import_source_goes_before_its_claim(self):
        """Its name is only on the claim: the other order would lose it to a worker that died in between."""
        kv = cluster()
        created = await made(kv)
        iso = kv.add_bootiso(NS, created.name, cdi=True)
        kv.calls.clear()
        deleted = await delete(kv, removal(created.name))
        assert deleted.import_sources_deleted == [f"{iso}-populator"] and kv.import_sources == set()
        assert sorted(deleted.claims_deleted) == sorted([iso, f"{created.name}-root"])
        order = [call for call in kv.calls if call in {"delete_volume_import_source", "delete_pvc"}]
        assert order.index("delete_volume_import_source") < len(order) - 1
        assert order[order.index("delete_volume_import_source") + 1] == "delete_pvc"
        assert writes(kv)[:2] == ["delete_vm", "delete_datavolume"]

    async def test_a_boot_iso_left_behind_is_deleted_whether_inserted_or_not(self):
        kv = cluster()
        created = await made(kv)
        inserted, ejected_but_left = kv.add_bootiso(NS, created.name), kv.add_bootiso(NS, created.name, inserted=False)
        deleted = await delete(kv, removal(created.name))
        assert {inserted, ejected_but_left} <= set(deleted.claims_deleted)

    async def test_no_volume_no_snapshot_no_pod_and_no_other_vms_claim_is_ever_deleted(self):
        kv = cluster()
        created, neighbour = await made(kv), await made(kv, request_id="another-run")
        kv.add_snapshot(NS, "nightly", f"{created.name}-root")
        kv.add_claim_pod(NS, "holder", f"{created.name}-root")
        neighbours_iso = kv.add_bootiso(NS, neighbour.name)
        await delete(kv, removal(created.name))
        await kv.list_pvcs(NS)
        assert set(writes(kv)) == {"delete_vm", "delete_datavolume", "delete_pvc"}
        assert not any(hasattr(KubevirtApi, name) for name in ("delete_pv", "delete_volume_snapshot", "delete_pod"))
        assert list(kv.snapshots) == [(NS, "nightly")] and len(kv.pvs) == 3
        assert [pod["metadata"]["name"] for pod in kv.pods] == ["holder"]
        assert (NS, neighbour.name) in kv.vms and {(NS, f"{neighbour.name}-root"), (NS, neighbours_iso)} <= set(kv.pvcs)

    async def test_a_look_alike_is_reported_and_left(self):
        """A VM name over 40 characters: a cut boot ISO name no longer says whose the claim is."""
        kv = cluster()
        long_flavor = "c" + "2" * 30
        created = await made(kv, flavor=long_flavor)
        assert len(created.name) > 40
        cut = (f"{created.name}-bootiso-1795000000-012345")[:49] + "ab12ctruncated"
        kv.add_bootiso(NS, created.name, inserted=False, name=cut)
        deleted = await delete(kv, removal(created.name))
        assert [claim.name for claim in deleted.seen.look_alikes] == [cut] and cut not in deleted.claims_deleted
        await kv.list_pvcs(NS)
        assert list(kv.pvcs) == [(NS, cut)]

    async def test_a_second_delete_finds_nothing_to_do(self):
        kv = cluster()
        created = await made(kv)
        await delete(kv, removal(created.name))
        again = await delete(kv, removal(created.name))
        assert not again.vm_deleted and again.datavolumes_deleted == [] and again.claims_deleted == []
        assert not again.seen.vm_found

    async def test_an_attempt_cut_off_half_way_is_finished_by_the_next(self):
        kv = cluster()
        created = await made(kv)
        iso = kv.add_bootiso(NS, created.name)
        kv.fail("delete_pvc", KvClusterApiError("this cluster's API failed while deleting (503): the test's outage"))
        with pytest.raises(KvClusterApiError):
            await delete(kv, removal(created.name))
        assert kv.vms == {}  # the VM went; a claim did not
        retried = await delete(kv, removal(created.name))
        assert not retried.vm_deleted and not retried.seen.vm_found
        state = await inspect(kv, removal(created.name))
        assert all(claim.deleting for claim in state.claims) or state.claims == []
        gone = await wait_gone(kv, watching(created.name, retried.seen))
        assert gone.result == "done" and kv.pvcs == {} and iso not in {name for _, name in kv.pvcs}

    async def test_a_vm_replaced_by_a_namesake_since_the_read_is_not_deleted(self):
        """The delete carries the UID it read: a VM made under that name in between stays."""
        kv = cluster()
        created = await made(kv)
        real_delete = kv.delete_vm

        async def replaced_first(namespace, name, uid=None):
            kv.vms[(namespace, name)]["metadata"]["uid"] = "a-newer-vm"
            return await real_delete(namespace, name, uid)

        kv.delete_vm = replaced_first
        deleted = await delete(kv, removal(created.name))
        assert not deleted.vm_deleted and (NS, created.name) in kv.vms


# --- wait_gone ---------------------------------------------------------------------------------


class TestWaitGone:
    async def test_done_only_when_the_volume_is_gone_too(self):
        kv = cluster()
        kv.volume_release_polls = 4
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        beats: list = []
        gone = await wait_gone(kv, watching(created.name, deleted.seen), beats=beats)
        assert (gone.result, gone.polls) == ("done", 5) and kv.pvs == {}
        # Each heartbeat carries the seconds of WATCHING so far: eight between two polls.
        assert beats == [(0.0, 1), (8.0, 2), (16.0, 3), (24.0, 4), (32.0, 5)]
        assert not gone.vm and gone.claims == [] and gone.volumes == [] and gone.datavolumes == []

    async def test_what_it_watches_is_what_the_run_recorded_not_what_it_can_still_find(self):
        """An attempt that starts after the claims are gone sees no claim to follow to a volume:
        the volume's name must come with the request."""
        kv = cluster()
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        [volume] = deleted.seen.volumes
        kv.stuck_volumes.add(volume.name)
        await kv.list_pvcs(NS)
        assert kv.pvcs == {} and volume.name in kv.pvs  # the claim went, its volume did not
        told = await wait_gone(kv, watching(created.name, deleted.seen))
        assert told.result == "deadline" and [row.name for row in told.volumes] == [volume.name]
        assert told.volumes[0].phase == "Released" and volume.volume_handle in told.detail
        not_told = await wait_gone(kv, removal(created.name))  # no record: nothing left to find it by
        assert not_told.result == "done"

    async def test_a_retain_volume_never_goes_so_the_wait_says_left_at_once(self):
        kv = cluster()
        kv.reclaim_policy = "Retain"
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        gone = await wait_gone(kv, watching(created.name, deleted.seen))
        assert (gone.result, gone.polls) == ("left", 1)
        [volume] = gone.volumes
        assert (volume.reclaim_policy, volume.phase) == ("Retain", "Released")
        assert "reclaim policy Retain" in gone.detail and volume.volume_handle in gone.detail

    async def test_a_retain_volume_beside_one_still_being_deleted_is_waited_for(self):
        kv = cluster()
        created = await made(kv)
        kv.reclaim_policy = "Retain"
        kv.add_bootiso(NS, created.name)
        kv.volume_release_polls = 3
        deleted = await delete(kv, removal(created.name))
        gone = await wait_gone(kv, watching(created.name, deleted.seen))
        assert (gone.result, gone.polls) == ("left", 4) and len(gone.volumes) == 1

    async def test_the_deadline_names_the_claim_and_the_pod_holding_it(self):
        kv = cluster()
        created = await made(kv)
        kv.add_claim_pod(NS, "backup-job-7", f"{created.name}-root")
        deleted = await delete(kv, removal(created.name))
        gone = await wait_gone(kv, watching(created.name, deleted.seen, deadline_minutes=15))
        assert gone.result == "deadline" and gone.polls == 114  # a poll every 8 s: the 114th is past 15 min
        [claim] = gone.claims
        assert claim.deleting and claim.used_by == ["backup-job-7"]
        assert "Terminating" in gone.detail and "backup-job-7" in gone.detail and "kubernetes.io/pvc-protection" in gone.detail
        assert [volume.phase for volume in gone.volumes] == ["Bound"]

    async def test_a_running_vm_is_waited_for_while_its_guest_shuts_down(self):
        kv = cluster()
        kv.vm_shutdown_polls = 3
        created = await made(kv)
        kv.start(NS, created.name, "n1")
        deleted = await delete(kv, removal(created.name))
        assert deleted.vm_deleted and deleted.seen.was_running and (NS, created.name) in kv.vms
        gone = await wait_gone(kv, watching(created.name, deleted.seen))
        assert gone.result == "done" and gone.polls >= 4 and kv.vms == {} and kv.pvs == {} and kv.pods == []

    async def test_a_vm_that_never_shuts_down_is_named_at_the_deadline(self):
        kv = cluster()
        kv.vm_shutdown_polls = 10_000
        created = await made(kv)
        kv.start(NS, created.name, "n1")
        deleted = await delete(kv, removal(created.name))
        gone = await wait_gone(kv, watching(created.name, deleted.seen, deadline_minutes=1))
        assert gone.result == "deadline" and gone.vm
        assert f"VirtualMachine {NS}/{created.name}" in gone.detail and "virtualMachineControllerFinalize" in gone.detail

    async def test_a_namesake_made_since_is_not_the_vm_waited_for(self):
        kv = cluster()
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        kv.add_vm(stopped_vm(created.name, 32, 240))  # another object, another UID
        gone = await wait_gone(kv, watching(created.name, deleted.seen))
        assert gone.result == "done" and not gone.vm

    async def test_a_claim_that_turns_up_after_the_delete_is_still_watched(self):
        kv = cluster()
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        late = kv.add_bootiso(NS, created.name, inserted=False)
        gone = await wait_gone(kv, watching(created.name, deleted.seen, deadline_minutes=1))
        assert gone.result == "deadline" and [claim.name for claim in gone.claims] == [late]

    async def test_a_resumed_wait_goes_on_from_the_watching_already_done(self):
        kv = cluster()
        created = await made(kv)
        kv.add_claim_pod(NS, "holder", f"{created.name}-root")
        deleted = await delete(kv, removal(created.name))
        beats: list = []
        gone = await wait_gone(kv, watching(created.name, deleted.seen), waited_seconds=14 * 60, polls=105, beats=beats)
        # One more minute of watching is left: eight polls' worth, the ninth is past it.
        assert (gone.result, gone.polls) == ("deadline", 105 + 9)
        assert beats[0] == (14 * 60, 106) and beats[-1][0] == 14 * 60 + 64

    async def test_an_outage_does_not_use_the_deadline_up(self):
        """Found on cluster-qcl8n (2026-11-26): the cluster was away for twenty minutes of a
        fifteen-minute wait, and the run failed the moment it answered again. A poll that fails
        ends the attempt; the time until the next attempt's first answer counts for nothing."""
        kv = cluster()
        kv.volume_release_polls = 6
        created = await made(kv)
        deleted = await delete(kv, removal(created.name))
        request, clock, beats = watching(created.name, deleted.seen), {"elapsed": 0.0}, []
        # The first attempt watches for two polls, then the cluster stops answering.
        real_get_pv, calls = kv.get_pv, {"count": 0}

        async def answers_twice(name):
            calls["count"] += 1
            if calls["count"] > 2:
                raise KvClusterApiError("this cluster's API could not be reached while reading: the test's outage")
            return await real_get_pv(name)

        kv.get_pv = answers_twice
        with pytest.raises(KvClusterApiError):
            await wait_gone(kv, request, clock=clock, beats=beats)
        assert beats == [(0.0, 1), (8.0, 2)]
        # Twenty minutes later the cluster is back and Temporal's next attempt starts.
        clock["elapsed"] += 20 * 60
        kv.get_pv = real_get_pv
        gone = await wait_gone(kv, request, waited_seconds=beats[-1][0], polls=beats[-1][1], clock=clock, beats=beats)
        assert gone.result == "done" and kv.pvs == {}  # not `deadline`: it was given the time to finish
        assert beats[2] == (8.0, 3)  # the outage added nothing

    async def test_it_deletes_nothing(self):
        kv = cluster()
        created = await made(kv)
        state = await inspect(kv, removal(created.name))
        kv.calls.clear()
        gone = await wait_gone(kv, watching(created.name, state, deadline_minutes=1))
        assert gone.result == "deadline" and gone.vm and writes(kv) == []


# --- kubevirt_api: the calls delete-vm added ----------------------------------------------------


class VolumeCore(FakeCore):
    def list_namespaced_persistent_volume_claim(self, namespace, **kwargs):
        return self._page("list_pvc", {"namespace": namespace, **kwargs})

    def delete_namespaced_persistent_volume_claim(self, name, namespace, **kwargs):
        self._call("delete_pvc", {"name": name, "namespace": namespace, **kwargs})
        return _Raw({})

    def list_persistent_volume(self, **kwargs):
        return self._page("list_pv", kwargs)

    def read_persistent_volume(self, name, **kwargs):
        self._call("read_pv", {"name": name, **kwargs})
        return _Raw({"metadata": {"name": name}})


class VolumeCustom(FakeCustom):
    def list_namespaced_custom_object(self, group, version, namespace, plural, **kwargs):
        self._call("list_namespaced", (group, version, namespace, plural), kwargs)
        pages = self.pages.get(plural, [[]])
        index = int(kwargs.get("_continue") or 0)
        return {"items": pages[index], "metadata": {"continue": str(index + 1)} if index + 1 < len(pages) else {}}


@pytest.fixture
def kube():
    core, custom = VolumeCore(), VolumeCustom()
    return KubevirtApi(core=core, custom=custom), core, custom


class TestKubevirtApiVolumes:
    async def test_the_claims_of_one_namespace_are_listed_page_by_page(self, kube):
        api, core, _ = kube
        core.pages["list_pvc"] = [[{"metadata": {"name": "a"}}], [{"metadata": {"name": "b"}}]]
        assert [claim["metadata"]["name"] for claim in await api.list_pvcs(NS)] == ["a", "b"]
        assert [kwargs["namespace"] for _, kwargs in core.calls] == [NS, NS]
        assert all(kwargs["_request_timeout"] == kubevirt_api.REQUEST_TIMEOUT for _, kwargs in core.calls)

    async def test_a_claim_is_deleted_only_while_it_is_the_object_read(self, kube):
        api, core, _ = kube
        assert await api.delete_pvc(NS, "vm-a-root", "uid-1") is True
        name, kwargs = core.calls[0]
        assert (name, kwargs["name"], kwargs["namespace"]) == ("delete_pvc", "vm-a-root", NS)
        assert kwargs["body"].preconditions.uid == "uid-1"
        core.errors["delete_pvc"] = [api_error(404, "not found", "NotFound"), api_error(409, "Precondition failed", "Conflict")]
        assert await api.delete_pvc(NS, "vm-a-root", "uid-1") is False  # already gone
        assert await api.delete_pvc(NS, "vm-a-root", "uid-1") is False  # no longer that object

    async def test_volumes_are_listed_cluster_wide_and_read_by_name(self, kube):
        api, core, _ = kube
        core.pages["list_pv"] = [[{"metadata": {"name": "pv-1"}}], [{"metadata": {"name": "pv-2"}}]]
        assert [volume["metadata"]["name"] for volume in await api.list_pvs()] == ["pv-1", "pv-2"]
        assert (await api.get_pv("pv-1"))["metadata"]["name"] == "pv-1"
        core.errors["read_pv"] = [api_error(404, "not found", "NotFound")]
        assert await api.get_pv("pv-1") is None

    async def test_a_cluster_that_serves_no_snapshots_has_none(self, kube):
        api, _, custom = kube
        custom.pages["volumesnapshots"] = [[{"metadata": {"name": "nightly"}}]]
        assert [snapshot["metadata"]["name"] for snapshot in await api.list_volume_snapshots(NS)] == ["nightly"]
        assert custom.calls[0][1] == ("snapshot.storage.k8s.io", "v1", NS, "volumesnapshots")
        custom.errors["list_namespaced"] = [api_error(404, "the server could not find the requested resource", "NotFound")]
        assert await api.list_volume_snapshots(NS) == []

    async def test_an_import_source_delete_is_idempotent(self, kube):
        api, _, custom = kube
        custom.objects[("volumeimportsources", NS, "iso-populator")] = {}
        assert await api.delete_volume_import_source(NS, "iso-populator") is True
        assert await api.delete_volume_import_source(NS, "iso-populator") is False
        assert custom.calls[0][1] == ("cdi.kubevirt.io", "v1beta1", NS, "volumeimportsources", "iso-populator")

    @pytest.mark.parametrize(("call", "fake", "method"), [
        (lambda api: api.list_pvcs(NS), "core", "list_pvc"),
        (lambda api: api.delete_pvc(NS, "c", "u"), "core", "delete_pvc"),
        (lambda api: api.list_pvs(), "core", "list_pv"),
        (lambda api: api.get_pv("pv"), "core", "read_pv"),
        (lambda api: api.list_volume_snapshots(NS), "custom", "list_namespaced"),
        (lambda api: api.delete_volume_import_source(NS, "s"), "custom", "delete"),
    ])
    async def test_every_new_call_is_classified(self, kube, call, fake, method):
        """403: the chart's ClusterRole predates delete-vm (permanent). 5xx: retried. 422: permanent."""
        api, core, custom = kube
        errors = (core if fake == "core" else custom).errors
        errors[method] = [api_error(403, "forbidden: User cannot do this")]
        with pytest.raises(KvClusterAccessError, match="ClusterRole"):
            await call(api)
        errors[method] = [api_error(503, "etcd is unavailable")]
        with pytest.raises(KvClusterApiError):
            await call(api)
        errors[method] = [api_error(422, "invalid")]
        with pytest.raises(VmRequestRejectedError):
            await call(api)


# --- the thin activities -----------------------------------------------------------------------


@pytest.fixture
def bound(monkeypatch):
    """The real activities, bound to a fake cluster instead of the pod's own API."""
    kv = cluster()
    monkeypatch.setattr(limb, "_api", kv)
    monkeypatch.setattr(limb, "_cluster", kv.values)
    monkeypatch.setattr(vm_operations, "GONE_POLL_SECONDS", 0.001)  # the wait really sleeps between polls
    return kv


class TestTheActivities:
    async def test_inspect_delete_and_wait_end_to_end(self, bound):
        env = ActivityEnvironment()
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        bound.bind_now(NS, created.name)
        bound.volume_release_polls = 2
        request = removal(created.name, created_by=RUN, mac=created.mac)
        state = await env.run(limb.inspect_vm_for_deletion, request)
        assert state.vm_found and len(state.claims) == 1 and len(state.volumes) == 1
        deleted = await env.run(limb.delete_vm_and_volumes, request)
        assert deleted.vm_deleted
        beats: list = []
        env.on_heartbeat = lambda *details: beats.append(details)
        gone = await env.run(limb.await_vm_gone, watching(created.name, deleted.seen))
        assert (gone.result, gone.polls) == ("done", 3) and [beat[0]["polls"] for beat in beats] == [1, 2, 3]
        assert bound.vms == {} and bound.pvcs == {} and bound.pvs == {}
        assert not (await env.run(limb.inspect_vm_for_deletion, request)).found

    async def test_the_wait_resumes_its_watching_from_the_heartbeat(self, bound):
        env = ActivityEnvironment()
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        bound.bind_now(NS, created.name)
        bound.add_claim_pod(NS, "holder", f"{created.name}-root")
        deleted = await env.run(limb.delete_vm_and_volumes, removal(created.name))
        # The attempts before this one watched the cluster for the whole fifteen minutes.
        env.info = env.info.__class__(**{**env.info.__dict__, "heartbeat_details": [
            {"waited_seconds": 15 * 60, "polls": 110}]})
        gone = await env.run(limb.await_vm_gone, watching(created.name, deleted.seen))
        assert (gone.result, gone.polls) == ("deadline", 111)

    async def test_an_unreadable_heartbeat_detail_starts_the_watch_afresh(self, bound):
        env = ActivityEnvironment()
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        bound.bind_now(NS, created.name)
        deleted = await env.run(limb.delete_vm_and_volumes, removal(created.name))
        env.info = env.info.__class__(**{**env.info.__dict__, "heartbeat_details": [{"waited_seconds": "not a number"}]})
        gone = await env.run(limb.await_vm_gone, watching(created.name, deleted.seen))
        assert (gone.result, gone.polls) == ("done", 1)

    def test_the_waits_bounds_sit_under_the_workflows_timeouts(self):
        from workflow_domains.vm_lifecycle import delete_vm

        one_call = sum(kubevirt_api.REQUEST_TIMEOUT)
        # One poll of the wait (its reads go out together) and its sleep, well inside a heartbeat.
        assert vm_operations.GONE_POLL_SECONDS + 2 * one_call < delete_vm._GONE_HEARTBEAT_TIMEOUT.total_seconds()
        assert delete_vm._GONE_DEADLINE_MINUTES * 60 + one_call < delete_vm._GONE_TIMEOUT.total_seconds()
        # The inspection: one read, then four at once, then the DataVolumes at once.
        assert 3 * one_call < delete_vm._INSPECT_TIMEOUT.total_seconds()
        # The wait's request defaults to the workflow's own deadline.
        assert VmRemovalRequest.model_fields["deadline_minutes"].default == delete_vm._GONE_DEADLINE_MINUTES
