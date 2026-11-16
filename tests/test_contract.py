"""Guard against interface/implementation drift.

Activity names + signatures are deliberately declared twice — as typed stubs in
shared/interfaces/ (what workflows call) and as real implementations in
activities/ (what the worker registers). If they drift, the failure mode at
runtime is silent payload-conversion weirdness, not a type error — so this test
makes drift a loud CI failure instead.
"""

from __future__ import annotations

import pathlib
import typing

import pytest

import activities.cluster_lifecycle.activities as cluster_lifecycle_impl
import activities.cluster_upgrade.activities as cluster_upgrade_impl
import activities.segment_lifecycle.activities as segment_impl
import activities.server_lifecycle.activities as server_impl
import activities.server_provisioning.activities as provisioning_impl
import activities.upgrade_agent.activities as upgrade_agent_impl
import activities.upgrade_gitops.activities as upgrade_gitops_impl
import activities.vm_lifecycle.activities as vm_lifecycle_impl
import shared.interfaces.cluster_lifecycle as cluster_lifecycle_interface
import shared.interfaces.cluster_upgrade as cluster_upgrade_interface
import shared.interfaces.segment_lifecycle as segment_interface
import shared.interfaces.server_lifecycle as server_interface
import shared.interfaces.server_provisioning as provisioning_interface
import shared.interfaces.upgrade_agent as upgrade_agent_interface
import shared.interfaces.upgrade_gitops as upgrade_gitops_interface
import shared.interfaces.vm_lifecycle as vm_lifecycle_interface

# Every domain, not just the first one written: an activity whose stub and
# implementation disagree converts payloads silently rather than raising.
_DOMAINS = [
    pytest.param(segment_interface, segment_impl, id="segment_lifecycle"),
    pytest.param(server_interface, server_impl, id="server_lifecycle"),
    pytest.param(provisioning_interface, provisioning_impl, id="server_provisioning"),
    pytest.param(cluster_upgrade_interface, cluster_upgrade_impl, id="cluster_upgrade"),
    pytest.param(upgrade_agent_interface, upgrade_agent_impl, id="upgrade_agent"),
    pytest.param(upgrade_gitops_interface, upgrade_gitops_impl, id="upgrade_gitops"),
    pytest.param(vm_lifecycle_interface, vm_lifecycle_impl, id="vm_lifecycle"),
    pytest.param(cluster_lifecycle_interface, cluster_lifecycle_impl, id="cluster_lifecycle"),
]


def _activity_definitions(module) -> dict[str, object]:
    """Map registered activity name -> function for a module's @activity.defn fns."""
    definitions: dict[str, object] = {}
    for attr in vars(module).values():
        defn = getattr(attr, "__temporal_activity_definition", None)
        if defn is not None:
            definitions[defn.name] = attr
    return definitions


@pytest.mark.parametrize(("interface_module", "impl_module"), _DOMAINS)
def test_interfaces_and_implementations_declare_the_same_activities(
    interface_module, impl_module
):
    interfaces = _activity_definitions(interface_module)
    implementations = _activity_definitions(impl_module)
    assert interfaces, "no @activity.defn stubs found in shared/interfaces"
    assert set(interfaces) == set(implementations)


@pytest.mark.parametrize(("interface_module", "impl_module"), _DOMAINS)
def test_interface_signatures_match_implementations(interface_module, impl_module):
    interfaces = _activity_definitions(interface_module)
    implementations = _activity_definitions(impl_module)
    for name, stub in interfaces.items():
        impl = implementations[name]
        assert typing.get_type_hints(stub) == typing.get_type_hints(impl), (
            f"activity {name!r}: interface and implementation signatures differ"
        )


def test_every_limb_is_in_domains():
    """A limb missing from _DOMAINS is never contract-checked: a new
    activities/<limb>/activities.py must be added above in the same change."""
    repo = pathlib.Path(__file__).resolve().parent.parent
    limbs = {p.parent.name for p in (repo / "activities").glob("*/activities.py")}
    listed = {param.id for param in _DOMAINS}
    assert limbs == listed, f"limbs not in _DOMAINS: {sorted(limbs - listed)}; stale: {sorted(listed - limbs)}"
