"""VmLifecycleActivitySettings — the vm-lifecycle worker inside ONE KV cluster: which cluster it is, its MAC prefix, and the values of the VirtualMachine it builds.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

import re

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.cluster_names import parse_kv_cluster

_MAC_PREFIX = re.compile(r"^[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}$")
_QUANTITY = re.compile(r"^[1-9][0-9]*(Mi|Gi|Ti)$")
_LABEL = re.compile(r"^(?P<key>[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?)=(?P<value>[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)$")
# The first octet every VM MAC of ours starts with: locally administered and
# unicast, and deliberately not `02` (KubeMacPool's range) nor `0a` (OVN-Kubernetes').
MAC_FIRST_OCTET = "0e"
# `<namespace>/<name>` of a NetworkAttachmentDefinition: what Multus resolves and what
# OVN-Kubernetes requires a NAD to name itself as.
_NETWORK_NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?/[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")
# The platform's vm.yaml (redbull-platform): every VM boots on this network.
DEFAULT_VM_NETWORK_NAME = "openshift-cnv/vm-external-network"


class VmLifecycleActivitySettings(BaseSettings):
    """Config for the `vm-lifecycle-worker` inside ONE KV cluster.

    Everything here is the CLUSTER's own, rendered into the chart's ConfigMap
    when the KV cluster is created and never typed in by hand
    (deploy/charts/vm-lifecycle-worker/). The worker holds no Secret: its
    ServiceAccount is its whole identity. The placement buffer is NOT here —
    it is one value for every cluster, on the hub (shared/settings/cluster_lifecycle.py).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # The KV cluster this worker runs in: EXACTLY the day1 file's name
    # (`ocp4-prod-kv-five-a`), because it builds the queue the worker polls and
    # the hub builds the same queue from the day1 list. NEVER canonicalised.
    # Parsed at startup as `ocp4-<lifecycle>-kv-<site>-<id>`: the site is read
    # from it, so there is no separate SITE to keep in step.
    kv_cluster: str

    # The first three octets of every MAC this cluster's VMs get: `0e` + the
    # cluster's VLAN ID (12 bits over the next two octets), computed when the
    # cluster is created from the Segments Manager's allocation. REQUIRED, no
    # default: the Segments Manager never gives one VLAN to two clusters of a
    # site, so two clusters that share a network never share a prefix — and a
    # worker without its prefix must not start. The ONE place it is set.
    vm_mac_prefix: str

    # --- the VirtualMachine this cluster's worker builds ----------------------
    # The root disk's storage class. It must bind IMMEDIATELY and allow a
    # ReadWriteMany + Filesystem volume: a WaitForFirstConsumer class never
    # binds a Halted VM's disk, and every run would end in the rollback.
    vm_storage_class_name: str
    # The Multus network every VM of the cluster boots on, `<namespace>/<name>` of
    # its NetworkAttachmentDefinition: the VM's `networks[0]` is ALWAYS
    # `{multus: {networkName: <this>}}`, as in the platform's vm.yaml, whose
    # network is the default. Only the name is a value — never the network's
    # type: a pod-network VM is not one the platform runs.
    vm_network_name: str = DEFAULT_VM_NETWORK_NAME
    # The VM template's nodeSelector, JSON; `{}` for none.
    vm_node_selector: dict[str, str] = {}
    # `key=value`: the label kubevirt-redfish exposes a VM by, and the one the
    # capacity figures count stopped VMs by.
    vm_redfish_label: str = "redfish-enabled=true"
    # The root disk's size, a Kubernetes quantity.
    vm_root_disk_size: str = "120Gi"

    @field_validator("kv_cluster")
    @classmethod
    def _kv_cluster(cls, value: str) -> str:
        parse_kv_cluster(value)  # ClusterNameError is a ValueError: the worker does not start
        return value

    @field_validator("vm_mac_prefix")
    @classmethod
    def _mac_prefix(cls, value: str) -> str:
        prefix = value.strip().lower()
        if not _MAC_PREFIX.fullmatch(prefix):
            raise ValueError(f"vm_mac_prefix must be three hex octets `xx:xx:xx` (got {value!r})")
        if not prefix.startswith(f"{MAC_FIRST_OCTET}:"):
            raise ValueError(
                f"vm_mac_prefix {prefix!r} must start with `{MAC_FIRST_OCTET}`: locally administered, unicast, "
                "and neither KubeMacPool's `02` nor OVN-Kubernetes' `0a`; the other two octets are the cluster's VLAN ID"
            )
        return prefix

    @field_validator("vm_storage_class_name")
    @classmethod
    def _storage_class(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("vm_storage_class_name must name the root disk's storage class")
        return value.strip()

    @field_validator("vm_network_name")
    @classmethod
    def _network_name(cls, value: str) -> str:
        if not _NETWORK_NAME.fullmatch(value.strip()):
            raise ValueError(
                f"vm_network_name must be `<namespace>/<name>` of the VMs' NetworkAttachmentDefinition (got {value!r})"
            )
        return value.strip()

    @field_validator("vm_redfish_label")
    @classmethod
    def _redfish_label(cls, value: str) -> str:
        if not _LABEL.fullmatch(value.strip()):
            raise ValueError(f"vm_redfish_label must be one `key=value` label (got {value!r})")
        return value.strip()

    @field_validator("vm_root_disk_size")
    @classmethod
    def _root_disk_size(cls, value: str) -> str:
        if not _QUANTITY.fullmatch(value.strip()):
            raise ValueError(f"vm_root_disk_size must be a whole number of Mi, Gi or Ti (got {value!r})")
        return value.strip()

    @property
    def site(self) -> str:
        """The site this cluster is at, read from its name."""
        return parse_kv_cluster(self.kv_cluster).site

    @property
    def redfish_label(self) -> tuple[str, str]:
        """(key, value) of `vm_redfish_label`."""
        key, _, value = self.vm_redfish_label.partition("=")
        return key, value
