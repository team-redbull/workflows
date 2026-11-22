"""What a correctly named Dell server looks like — pure, deterministic.

The name follows the platform naming convention (Idan-Maimon/kubevirt-workflows,
docs/naming-convention.md, fetched by the `naming-convention` skill), §1 for a
bare-metal node:

    ocp-<flavor>-mtl-<site>-<serial>        ocp-c200-mtl-five-7xk2m33

all lowercase, at most 32 characters (the limit Cisco UCS sets on a server's
name, applied to every bare-metal name). For a Dell the serial is its SERVICE
TAG. The flavor is the server configuration with an optional variant letter
(`c200`, `c200n`, `c200d`); the convention defines no Dell flavor, and the
naming service is the author of the name, so only the flavor's SHAPE is checked
here (the operator, 2026-11-22). What the workflow knows for certain is checked
exactly: `mtl`, the SITE the run was given and the service tag the iDRAC
reported. A name that gets any of those wrong would put the machine in the
wrong server-scan site or pool, or name it after another machine.

The naming service writes the name, so the length is checked when it is read
back, not before it is written (the convention's "before writing it to OME" is
the service's to honour).

Until 2026-11-22 the name was `ocp-dell-<model>-<region>-<cores>c-<mem>gb-<disk>tb-<tag>`.
"""

from __future__ import annotations

import re

MAX_NAME_LENGTH = 32
BARE_METAL_SIZE = "mtl"
# A flavor: a letter, then letters and digits — its variant letter included.
_FLAVOR = r"[a-z][0-9a-z]*"


def expected_shape(site: str, service_tag: str) -> str:
    """`ocp-<flavor>-mtl-five-7xk2m33`: the name this machine must carry, for messages."""
    return f"ocp-<flavor>-{BARE_METAL_SIZE}-{site}-{service_tag.lower()}"


def name_problem(name: str, site: str, service_tag: str) -> str | None:
    """Why `name` is not this machine's name under the convention; None when it is."""
    if name != name.lower():
        return f"{name!r} is not all lowercase"
    if len(name) > MAX_NAME_LENGTH:
        return f"{name!r} is {len(name)} characters, over the {MAX_NAME_LENGTH} a bare-metal name may have"
    pattern = rf"ocp-{_FLAVOR}-{BARE_METAL_SIZE}-{re.escape(site)}-{re.escape(service_tag.lower())}"
    if re.fullmatch(pattern, name) is None:
        return f"{name!r} is not {expected_shape(site, service_tag)}"
    return None


def name_matches_convention(name: str, site: str, service_tag: str) -> bool:
    """Whether `name` is this machine's name under the convention."""
    return name_problem(name, site, service_tag) is None
