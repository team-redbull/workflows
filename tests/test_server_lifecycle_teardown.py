"""teardown_bmh_resources — the rollback, and why its ORDER is what it is.

Every step here was established against a live cluster, and the test names say
what was measured rather than what was intended:

  * metal3's `baremetalhost.metal3.io` finalizer blocks the delete for as long
    as it is trying to deprovision through the BMC. On an unreachable BMC that
    is forever — still stuck past four minutes when measured, in state
    `deleting`. A rollback happens precisely BECAUSE that BMC never answered,
    so this is the normal path here, not the exceptional one.
  * the `detached` annotation only helps if it is applied BEFORE the delete.
    Applied afterwards, with `deletionTimestamp` already set, it changed
    nothing for four minutes.
  * BMAC's own `bmac.agent-install.openshift.io/deprovision` finalizer released
    inside one second, so it is deliberately left alone.
  * the BMC Secret really does carry an ownerReference to the BareMetalHost, so
    it cascades — but only once the host is genuinely gone, which is after the
    finalizer clears, not when delete is called.

Driven through the activity rather than the cluster layer, because the ORDER is
the thing under test and only the activity decides it.
"""

from __future__ import annotations

import pytest

from activities.server_lifecycle import activities, cluster_api
from activities.server_lifecycle.bmh_resources import (
    BMH_DEPROVISION_FINALIZERS,
    DETACHED_ANNOTATION,
)
from shared.exceptions import BmhTeardownError
from shared.models.server_lifecycle import BmhRef

SERVER = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"
NAMESPACE = "multicluster-engine"
SECRET = "dell-cred-ocp-dell-r650-tlv-64c-1024gb-del0000485"
REF = BmhRef(server_name=SERVER, namespace=NAMESPACE)


class _FakeCluster:
    """The target cluster's API, recording the ORDER it is called in.

    `survives` models the finalizer: while true, the BareMetalHost keeps being
    readable after its delete, exactly as a host metal3 cannot deprovision does.
    """

    def __init__(self, *, present: bool = True, survives: bool = False) -> None:
        self.calls: list[str] = []
        self.present = present
        self.survives = survives
        self.secret_present = present
        self.annotations: dict[str, str] = {}

    async def read_custom_object(self, *, kind, name, **_):
        self.calls.append(f"read:{kind}")
        if not self.present:
            return None
        return {
            "metadata": {"name": name},
            "spec": {"bmc": {"credentialsName": SECRET}},
        }

    async def annotate_custom_object(self, *, kind, annotations, **_):
        self.calls.append(f"annotate:{kind}:{','.join(annotations)}")
        self.annotations.update(annotations)
        return self.present

    async def delete_custom_object(self, *, kind, **_):
        self.calls.append(f"delete:{kind}")
        existed = self.present
        if kind == "BareMetalHost" and not self.survives:
            self.present = False
        return existed

    async def clear_custom_object_finalizers(self, *, kind, finalizers, **_):
        self.calls.append(f"finalizers:{kind}:{','.join(sorted(finalizers))}")
        self.survives = False
        self.present = False
        return True

    async def secret_exists(self, namespace, name):
        self.calls.append(f"secret_exists:{name}")
        return self.secret_present

    async def delete_secret_if_present(self, namespace, name):
        self.calls.append(f"delete:Secret:{name}")
        existed = self.secret_present
        self.secret_present = False
        return existed


@pytest.fixture
def fake(monkeypatch):
    """Swap the cluster layer, and make the bounded wait instant."""

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr(activities.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(activities, "_TEARDOWN_GONE_TIMEOUT", 6.0)

    def _install(cluster: _FakeCluster) -> _FakeCluster:
        for name in (
            "read_custom_object",
            "annotate_custom_object",
            "delete_custom_object",
            "clear_custom_object_finalizers",
            "secret_exists",
            "delete_secret_if_present",
        ):
            monkeypatch.setattr(cluster_api, name, getattr(cluster, name))
        return cluster

    return _install


async def test_the_host_is_detached_before_it_is_deleted(fake):
    """The ordering the whole rollback depends on.

    Applied after `deletionTimestamp` is set, the annotation is inert — measured
    doing nothing for four minutes on a live cluster. So "annotate, then delete"
    is not tidiness, it is the only order that works.
    """
    cluster = fake(_FakeCluster())
    await activities.teardown_bmh_resources(REF)

    annotate = next(
        i for i, c in enumerate(cluster.calls) if c.startswith("annotate:BareMetalHost")
    )
    delete = next(
        i for i, c in enumerate(cluster.calls) if c == "delete:BareMetalHost"
    )
    assert annotate < delete
    assert cluster.annotations == {DETACHED_ANNOTATION: ""}


async def test_a_cooperative_host_needs_no_finalizer_surgery(fake):
    cluster = fake(_FakeCluster())
    result = await activities.teardown_bmh_resources(REF)

    assert result.finalizers_cleared is False
    assert not any(c.startswith("finalizers:") for c in cluster.calls)
    assert f"BareMetalHost/{SERVER.lower()}" in result.removed


async def test_a_host_metal3_will_not_release_has_its_finalizer_dropped(fake):
    """The expected path on an unreachable BMC, not an exceptional one."""
    cluster = fake(_FakeCluster(survives=True))
    result = await activities.teardown_bmh_resources(REF)

    assert result.finalizers_cleared is True
    dropped = next(c for c in cluster.calls if c.startswith("finalizers:"))
    # ONLY metal3's own, never BMAC's: that one released in under a second, so
    # taking it would rob a controller already doing its job.
    assert dropped.endswith(":".join(["", ",".join(sorted(BMH_DEPROVISION_FINALIZERS))]))
    assert "bmac.agent-install.openshift.io/deprovision" not in dropped


async def test_the_nmstateconfig_is_deleted_explicitly(fake):
    """It has no finalizer and no ownerReference, so nothing removes it for us."""
    cluster = fake(_FakeCluster())
    result = await activities.teardown_bmh_resources(REF)

    assert "delete:NMStateConfig" in cluster.calls
    assert any(r.startswith("NMStateConfig/") for r in result.removed)


async def test_the_secret_name_is_read_off_the_host_not_rebuilt(fake):
    """A BmhRef carries no BMC vendor, and the host is authoritative anyway.

    `spec.bmc.credentialsName` stays correct for a Secret an operator renamed by
    hand, where rebuilding `{vendor}-cred-{server}` would miss it and leave a
    BMC credential behind on every rollback.
    """
    cluster = fake(_FakeCluster())
    await activities.teardown_bmh_resources(REF)

    assert f"secret_exists:{SECRET}" in cluster.calls


async def test_a_cascaded_secret_is_not_deleted_again(fake):
    """The ownerReference normally does this for us once the host really goes."""
    cluster = _FakeCluster()
    cluster.secret_present = False
    fake(cluster)
    result = await activities.teardown_bmh_resources(REF)

    assert not any(c.startswith("delete:Secret") for c in cluster.calls)
    assert not any(r.startswith("Secret/") for r in result.removed)


async def test_teardown_of_an_already_clean_namespace_is_success(fake):
    """Idempotent, which is what lets Temporal retry it."""
    fake(_FakeCluster(present=False))
    result = await activities.teardown_bmh_resources(REF)

    assert result.removed == []
    assert result.finalizers_cleared is False


async def test_a_host_that_survives_everything_refuses_to_report_success(fake):
    """The server must NOT go back to the inventory while a host points at it.

    Reporting success here would hand the machine to another MCE while this one
    still holds a BareMetalHost for it — and server-scan cannot see that, since
    nothing changes a server's lifecycle state until a cluster reports the node.
    """

    class _Immortal(_FakeCluster):
        async def clear_custom_object_finalizers(self, **kwargs):
            self.calls.append("finalizers:BareMetalHost:refused")
            return False  # the patch landed and the object still stands

    fake(_Immortal(survives=True))
    with pytest.raises(BmhTeardownError) as excinfo:
        await activities.teardown_bmh_resources(REF)

    message = str(excinfo.value)
    assert "still on the cluster" in message
    assert "NOT" in message  # ... not being returned to the inventory
