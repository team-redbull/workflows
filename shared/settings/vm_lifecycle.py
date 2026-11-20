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
_NETWORK_TYPES = frozenset({"multus", "pod"})


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
    # The VM's `networks[0]` body, JSON, exactly ONE network type (`multus` or
    # `pod`): the network every VM of the cluster boots on (the site's
    # inventory network). REQUIRED, no default: KubeVirt admits a Halted VM on
    # a network that does not exist, so a default naming another platform's
    # network would make VMs that never boot, with nothing saying why.
    vm_network: dict
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

    @field_validator("vm_network")
    @classmethod
    def _network(cls, value: dict) -> dict:
        if not value:
            raise ValueError('vm_network must be the VM\'s `networks[0]` body, e.g. {"multus": {"networkName": "ns/name"}}')
        # KubeVirt's admission refuses a network of two types ("should have only one network
        # type") — which is what a chart default merged with an override produces. Refused
        # here, at worker start, not on the first run of a zone.
        if len(value) != 1 or not set(value) <= _NETWORK_TYPES:
            raise ValueError(
                f"vm_network must name exactly ONE network type of {sorted(_NETWORK_TYPES)} (got {sorted(value)}): "
                "KubeVirt refuses a network of two"
            )
        if "multus" in value and not (isinstance(value["multus"], dict) and value["multus"].get("networkName")):
            raise ValueError("vm_network.multus must name its NetworkAttachmentDefinition: {\"multus\": {\"networkName\": \"ns/name\"}}")
        return value

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
