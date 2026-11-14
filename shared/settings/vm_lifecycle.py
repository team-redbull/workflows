"""VmLifecycleActivitySettings — The vm-lifecycle hub limb only: the day1 repo, the KubeVirt clusters' agents and create-vm's placement policy.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

import re
import string

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.models.vm_lifecycle import KvClusterRef


# What an agent URL template may name (VmLifecycleActivitySettings).
_KV_URL_PLACEHOLDERS = frozenset({"cluster", "name", "hosted_cluster", "site", "zone"})


class VmLifecycleActivitySettings(BaseSettings):
    """Config for the vm-lifecycle HUB limb (`vm-lifecycle-worker`) only.

    The limb holds no Kubernetes credential. It reaches every KubeVirt cluster
    through that cluster's two agents behind Routes, with one fleet bearer
    token each, and reads the zone's cluster list from the day1 repo. Keep
    aligned with redbull-platform's gitops/charts/vm-lifecycle-worker/
    (vm-lifecycle-config and the Secrets it names).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- day1: the zone's KubeVirt clusters ----------------------------------
    # The same repo and keys as segment-lifecycle's and upgrade-gitops's
    # (DAY1_REPO_URL, DAY1_GIT_TOKEN): the chart fills DAY1_GIT_TOKEN from the shared
    # `redbull-git-token` Secret. Read at `main` only, never pushed.
    day1_repo_url: str
    day1_git_token: str

    # --- the agents on each KubeVirt cluster ---------------------------------
    # Rendered per cluster from the day1 entry: `{cluster}` is the chart's
    # clusterName (`kubevirt-bat-yam-a1`), `{name}` the day1 file's stem
    # (`ocp4-kubevirt-bat-yam-a1`), `{hosted_cluster}` the HostedCluster
    # (`openshift-<clusterName>`), `{site}` and `{zone}` the run's. Whether a
    # cluster's apps domain is `apps.<clusterName>.<base>` or
    # `apps.openshift-<clusterName>.<base>` is the template's to say
    # (`https://kv-capacity.apps.{cluster}.example.net`).
    kv_capacity_url_template: str
    kv_vm_api_url_template: str
    # One fleet bearer token per agent; empty sends no Authorization header.
    kv_capacity_token: str = ""
    kv_vm_api_token: str = ""
    # A PEM file the agents' Route certificates are verified against. EMPTY =
    # NOT VERIFIED — the operator's decision (2026-11-04): the Routes carry the
    # ingress wildcard certificate, whose CA the limb image cannot carry.
    # The one place in the repo TLS verification is off by default.
    kv_agent_ca: str = ""

    # --- our placement policy (no service prefix: it is ours, CLAUDE.md §3) --
    # The share of every KubeVirt cluster's schedulable allocatable that must
    # stay unallocated AFTER placing the VM, on CPU and on memory (per
    # cluster, never per node): docs/design/kubevirt-vm-placement.md.
    headroom_buffer_percent: int
    # The first three octets of every VM's MAC: `xx:xx:xx`, locally
    # administered and unicast (the first byte's low bits `10`), so a drawn
    # MAC can never collide with a vendor's. The run draws the other three.
    vm_mac_prefix: str

    @field_validator("day1_repo_url")
    @classmethod
    def _require_https_day1_repo_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url)."""
        if not url.startswith("https://"):
            raise ValueError(
                "day1_repo_url must be an https:// URL — the token is only "
                f"injected into https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url

    @field_validator("kv_capacity_url_template", "kv_vm_api_url_template")
    @classmethod
    def _url_template(cls, template: str) -> str:
        """http(s), and only the placeholders kv_capacity_url/kv_vm_api_url fill — a
        typo (`{clustr}`) fails at worker start, not as a KeyError on the first run."""
        if not template.startswith(("http://", "https://")):
            raise ValueError(f"agent URL templates must be http:// or https:// (got {template!r})")
        try:
            fields = {name for _, name, _, _ in string.Formatter().parse(template) if name is not None}
        except ValueError as err:
            raise ValueError(f"agent URL template {template!r} is not a format string: {err}") from None
        unknown = sorted(fields - _KV_URL_PLACEHOLDERS)
        if unknown or "" in fields:
            raise ValueError(
                f"agent URL template {template!r} uses {unknown or ['{}']}; the placeholders are "
                f"{sorted(_KV_URL_PLACEHOLDERS)}"
            )
        return template.rstrip("/")

    @field_validator("headroom_buffer_percent")
    @classmethod
    def _buffer(cls, value: int) -> int:
        if not 0 <= value <= 90:
            raise ValueError(f"headroom_buffer_percent must be 0..90 (got {value})")
        return value

    @field_validator("vm_mac_prefix")
    @classmethod
    def _mac_prefix(cls, value: str) -> str:
        prefix = value.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}", prefix):
            raise ValueError(f"vm_mac_prefix must be three hex octets `xx:xx:xx` (got {value!r})")
        first = int(prefix[:2], 16)
        if first & 0b11 != 0b10:
            raise ValueError(
                f"vm_mac_prefix {prefix!r} must be locally administered and unicast: the first "
                "octet's low two bits are `10` (02, 06, 0a, 0e, 12, … as the first octet)"
            )
        return prefix

    def kv_capacity_url(self, ref: KvClusterRef) -> str:
        return self.kv_capacity_url_template.format(**_url_fields(ref))

    def kv_vm_api_url(self, ref: KvClusterRef) -> str:
        return self.kv_vm_api_url_template.format(**_url_fields(ref))


def _url_fields(ref: KvClusterRef) -> dict[str, str]:
    return {
        "cluster": ref.cluster_name, "name": ref.name, "hosted_cluster": ref.hosted_cluster,
        "site": ref.site, "zone": ref.zone,
    }
