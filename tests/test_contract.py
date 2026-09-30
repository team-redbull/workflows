"""Guard against interface/implementation drift.

Activity names + signatures are deliberately declared twice — as typed stubs in
shared/interfaces/ (what workflows call) and as real implementations in
activities/ (what the worker registers). If they drift, the failure mode at
runtime is silent payload-conversion weirdness, not a type error — so this test
makes drift a loud CI failure instead.
"""

from __future__ import annotations

import typing

import pytest

import activities.segment_lifecycle.activities as segment_impl
import activities.server_lifecycle.activities as server_impl
import activities.server_provisioning.activities as provisioning_impl
import shared.interfaces.segment_lifecycle as segment_interface
import shared.interfaces.server_lifecycle as server_interface
import shared.interfaces.server_provisioning as provisioning_interface

# Every domain, not just the first one written: an activity whose stub and
# implementation disagree converts payloads silently rather than raising.
_DOMAINS = [
    pytest.param(segment_interface, segment_impl, id="segment_lifecycle"),
    pytest.param(server_interface, server_impl, id="server_lifecycle"),
    pytest.param(provisioning_interface, provisioning_impl, id="server_provisioning"),
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
