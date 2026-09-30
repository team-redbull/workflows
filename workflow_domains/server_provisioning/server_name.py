"""What a correctly named Dell server looks like — pure, deterministic.

The naming service is the author of the name: it reads cores, memory and disks
from OME and rounds them its own way. This module does NOT re-derive those
numbers (the service is the source of record for them, and two roundings would
eventually disagree). It checks what the workflow itself knows for certain:
the shape of the convention, the vendor and model, the REGION the run was given
and the SERVICE TAG the iDRAC reported. A name that gets any of those wrong
would put the machine in the wrong server-scan site or pool, or name it after
another machine.

    ocp-dell-r660-<region>-128c-1024gb-10tb-<service tag>

The model/region/cores/memory prefix is also what install-server matches an
InfraEnv name against (`^ocp-<infraEnv>`), so this shape is load-bearing.
"""

from __future__ import annotations

import re


def model_token(model: str) -> str:
    """`PowerEdge R660` -> `r660`: the last word of the Redfish model, lowercased."""
    return model.split()[-1].lower() if model.split() else ""


def name_matches_convention(name: str, model: str, region: str, service_tag: str) -> bool:
    """Whether `name` is this machine's name under the convention.

    Case-insensitive, because the service tag is upper case in some names and
    lower case in others; every token is still required to be exactly this
    machine's.
    """
    pattern = (
        rf"ocp-dell-{re.escape(model_token(model))}-{re.escape(region)}"
        rf"-\d+c-\d+gb-\d+tb-{re.escape(service_tag)}"
    )
    return re.fullmatch(pattern, name, flags=re.IGNORECASE) is not None
