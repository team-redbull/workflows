"""What a KubeVirt InfraEnv asks create-vm for — add-server-to-mce's VM branch, pure and deterministic.

An InfraEnv whose agentLabels say `redbull.io/size: kv<cpu>` is filled with
VirtualMachines: add-server-to-mce has ONE made (create-vm, as its child) and
installs exactly that one, with no draw from server-scan (the naming
convention's "A VM needs no draw", AD29; built 2026-11-25).

This module is the POLICY of that branch, split out for the reason
bond_selection.py is: no activity, no clock, no I/O, so it is tested with no
Temporal environment, and it runs in the workflow sandbox on every replay.

WHY THE LABELS, NOT THE NAME. create-vm's own route parses the InfraEnv's NAME
at the API edge. add-server-to-mce cannot: its route takes any InfraEnv, and
what an InfraEnv is FOR is stated by the labels its Agents receive — the same
three the bare-metal pool draw is built from, plus the zone. So the flavor,
the vCPU count and the zone come from the labels, each judged by the rule the
name's parser applies to that part (shared/infraenv_names.py — one copy of
each rule), and the site and the lifecycle from the MCE's name, as create-vm's
route takes them. The InfraEnv's own name need not follow the convention.

REPLAY. Nothing here is carried in the run's input: it is recomputed, from an
activity result (the labels) and the run's input (the MCE), both recorded, so
a replay computes what the run computed. Changing what this module REFUSES
changes which runs start a child, and is a `workflow.patched` change; changing
only a derived value (the memory ratio) is not — a child's input is not
compared on replay.

Nothing here raises: the parsers it leans on raise ValueError, and an
exception in workflow code hangs a run instead of failing it. Every problem is
returned as words.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import MceNameError, parse_mce_cluster
    from shared.infraenv_names import (
        InfraEnvKind,
        flavor_problem,
        kubevirt_size_cpu,
        kubevirt_size_problem,
        size_kind,
        vm_memory_gib,
        vm_name_problem,
        zone_problem,
    )
    from shared.models.vm_lifecycle import CreateVmInput
    from shared.upgrade_layout import ZONE_LABEL

# The InfraEnv's agentLabels that state what its hosts are (the convention's §4.a).
FLAVOR_LABEL = "redbull.io/flavor"
SIZE_LABEL = "redbull.io/size"
SITE_LABEL = "redbull.io/site"

# The one NIC of a VM, as the guest names it. The VirtualMachine create-vm's
# worker builds (the platform's vm.yaml) has ONE interface, a virtio NIC on the
# q35 machine's first PCIe root port, which RHCOS names `enp1s0`. Written as the
# real name, not a placeholder like a bare-metal bond's `nic1`: the NMStateConfig
# puts the VLAN straight on it, and this is the layout the platform's own
# hand-made VM hosts run on (the operator's, 2026-11-25). The MAC -> name
# binding in `spec.interfaces` still matches the NIC by its MAC.
VM_NIC_NAME = "enp1s0"


def is_kubevirt(agent_labels: Mapping[str, str]) -> bool:
    """Whether the InfraEnv is filled with VMs: its size label says so (`kv…`)."""
    return size_kind(agent_labels.get(SIZE_LABEL) or "") is InfraEnvKind.KUBEVIRT


@dataclass(frozen=True)
class VmRequest:
    """What to ask create-vm for, or why it cannot be asked.

    `label_problems` are the InfraEnv's (one line per label at fault);
    `mce_problem` is the MCE's name not saying its site and lifecycle. `input`
    is set exactly when both are empty.
    """

    input: CreateVmInput | None = None
    label_problems: list[str] = field(default_factory=list)
    mce_problem: str | None = None


def _label(agent_labels: Mapping[str, str], key: str, problems: list[str], judge=None) -> str | None:
    """One label's value, or None with its problem recorded."""
    value = agent_labels.get(key)
    if not value:
        problems.append(f"{key} is missing")
        return None
    why = judge(value) if judge is not None else None
    if why is not None:
        problems.append(f"{key}: {why}")
        return None
    return value


def derive(
    agent_labels: Mapping[str, str], *, mce_cluster: str, infra_env: str, namespace: str
) -> VmRequest:
    """The create-vm input a KubeVirt InfraEnv's labels and its MCE's name state — every problem at once."""
    label_problems: list[str] = []
    flavor = _label(agent_labels, FLAVOR_LABEL, label_problems, flavor_problem)
    size = _label(agent_labels, SIZE_LABEL, label_problems, kubevirt_size_problem)
    zone = _label(agent_labels, ZONE_LABEL, label_problems, zone_problem)
    site = _label(agent_labels, SITE_LABEL, label_problems)

    mce_problem: str | None = None
    lifecycle: str | None = None
    try:
        mce = parse_mce_cluster(mce_cluster)
    except MceNameError as err:
        mce_problem = str(err)
    else:
        lifecycle = mce.lifecycle
        if site is not None and site != mce.site:
            label_problems.append(
                f"{SITE_LABEL}={site!r} is not its MCE's site ({mce.site!r}, from {mce_cluster}): "
                "a VM is made on a KV cluster of the MCE's site"
            )

    if flavor is not None and size is not None and site is not None and not label_problems:
        unnameable = vm_name_problem(flavor, kubevirt_size_cpu(size), site)
        if unnameable is not None:
            label_problems.append(f"no VM of it can be named: {unnameable}")

    if label_problems or mce_problem is not None:
        return VmRequest(label_problems=label_problems, mce_problem=mce_problem)

    cpu = kubevirt_size_cpu(size)
    return VmRequest(
        input=CreateVmInput(
            mce_cluster=mce_cluster,
            infra_env=infra_env,
            namespace=namespace,
            flavor=flavor,
            site=site,
            zone=zone,
            cpu=cpu,
            memory_gib=vm_memory_gib(cpu),
            lifecycle=lifecycle,
        )
    )
