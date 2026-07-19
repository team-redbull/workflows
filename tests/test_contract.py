"""Guard against interface/implementation drift.

Activity names + signatures are deliberately declared twice — as typed stubs in
shared/interfaces/ (what workflows call) and as real implementations in
activities/ (what the worker registers). If they drift, the failure mode at
runtime is silent payload-conversion weirdness, not a type error — so this test
makes drift a loud CI failure instead.
"""

from __future__ import annotations

import typing

import activities.segment_connectivity.activities as impl_module
import shared.interfaces.segment_connectivity as interface_module


def _activity_definitions(module) -> dict[str, object]:
    """Map registered activity name -> function for a module's @activity.defn fns."""
    definitions: dict[str, object] = {}
    for attr in vars(module).values():
        defn = getattr(attr, "__temporal_activity_definition", None)
        if defn is not None:
            definitions[defn.name] = attr
    return definitions


def test_interfaces_and_implementations_declare_the_same_activities():
    interfaces = _activity_definitions(interface_module)
    implementations = _activity_definitions(impl_module)
    assert interfaces, "no @activity.defn stubs found in shared/interfaces"
    assert set(interfaces) == set(implementations)


def test_interface_signatures_match_implementations():
    interfaces = _activity_definitions(interface_module)
    implementations = _activity_definitions(impl_module)
    for name, stub in interfaces.items():
        impl = implementations[name]
        assert typing.get_type_hints(stub) == typing.get_type_hints(impl), (
            f"activity {name!r}: interface and implementation signatures differ"
        )
