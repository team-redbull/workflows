"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Two settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (both workers and api.py).
  - SegmentLifecycleActivitySettings: needed only by the segment-lifecycle activity
    worker/tasks (Segments Manager + next API URLs/credentials + port policy).
    The workflow worker has no business holding these.

Field names deliberately equal the Helm ConfigMap/Secret keys (lowercased) —
pydantic-settings matches env vars case-insensitively, so SEGMENTS_MANAGER_URL
populates segments_manager_url.

Note: which ConfigMap a key lives in (an ops grouping) is INDEPENDENT of which
settings class declares it (a code grouping). pydantic reads the flat process
env, so it never sees the ConfigMap boundary. DOMAIN and SEGMENTS_MANAGER_URL
live in the shared `workflows-orchestrator-config` ConfigMap (so future workflows reuse
them without duplication), yet stay fields on SegmentLifecycleActivitySettings —
only the activity worker requires them, and it mounts workflows-orchestrator-config +
segment-lifecycle-config together. Keep the files aligned:
helm-charts-workflows-orchestrator/templates/config.yaml   (workflows-orchestrator-config: temporal + domain + segments-manager url)
helm-charts-segment-lifecycle-worker/templates/config.yaml (segment-lifecycle-config: next URIs + ports; + the token Secret)

Do NOT import this module from inside a workflow definition (it runs in the
sandbox) — only from worker entrypoints, api.py, and activity
implementations.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# "9000" or "30000-32767"
_PORT_ENTRY_RE = re.compile(r"^(\d{1,5})(?:-(\d{1,5}))?$")
_SUPPORTED_PROTOCOLS = ("tcp", "udp")


class SiteNetworks(BaseModel):
    """One site's networks, from the shared SITE_NETWORKS topology.

    This structure is defined ONCE — redbull-platform gitops/values/<env>.yaml,
    key `siteNetworks` — and rendered into BOTH this chart's ConfigMap and the
    Segments Manager's. That is what guarantees the two services agree on the
    site list; it used to be enforced only by a comment.

    extra="ignore" is load-bearing: Segments Manager owns `pool` (the range its
    segments must fall inside) and this service must never depend on it, nor
    break when another consumer adds a sub-key.

    Out-of-band management sits on a different /16 per server hardware vendor,
    so a site has ONE BMC network per vendor it actually has hardware from —
    both, or only Dell, or only Cisco (see BmcVendor). An MCE segment opens
    rules to whichever are configured. The keys are hyphenated (`dell-bmc`,
    `cisco-bmc`) because they are operator-facing config, matching the Helm
    values verbatim; the aliases below map them onto valid Python field names,
    and populate_by_name keeps construction by field name (tests, call sites)
    working.

    AT LEAST ONE is required. A site with neither is a config gap, not a
    single-vendor site — an MCE there would open no BMC rules at all and look
    healthy — so it crash-loops the worker at startup instead of failing a
    workflow hours in. Which vendors a site has is real topology; having none
    of them is not.

    That relaxation costs the typo guard the both-required shape used to give
    for free (`dell-bcm` next to a valid `cisco-bmc` would now read as a
    legitimately Cisco-only site), so _reject_bmc_typos puts it back: an
    unrecognised key that LOOKS like a BMC key is rejected, while `pool` and
    any future consumer's sub-keys stay ignored.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    dell_bmc: str | None = Field(default=None, alias="dell-bmc")
    cisco_bmc: str | None = Field(default=None, alias="cisco-bmc")

    @model_validator(mode="before")
    @classmethod
    def _reject_bmc_typos(cls, data: Any) -> Any:
        """Reject an unknown sub-key that looks like a misspelt BMC key.

        extra="ignore" is load-bearing (`pool` is the Segments Manager's, and
        another consumer may add its own), so this cannot be a blanket
        extra="forbid": only keys carrying a BMC/vendor token are judged. It
        catches `dell-bcm`, `dellbmc`, `cisco_bmc` and the pre-vendor-split
        `bmc` — each of which would otherwise be silently dropped and read as
        a single-vendor (or unconfigured) site.
        """
        if not isinstance(data, dict):
            return data
        # The field names are known too: populate_by_name accepts them, so
        # rejecting them here would break a construction path the model
        # otherwise supports (tests, call sites).
        known = {"dell-bmc", "cisco-bmc", "dell_bmc", "cisco_bmc"}
        for key in data:
            if not isinstance(key, str) or key in known:
                continue
            lowered = key.lower()
            if any(token in lowered for token in ("bmc", "bcm", "dell", "cisco")):
                raise ValueError(
                    f"unrecognised BMC key {key!r} — expected 'dell-bmc' and/or "
                    "'cisco-bmc'"
                )
        return data

    @field_validator("dell_bmc", "cisco_bmc")
    @classmethod
    def _validate_bmc(cls, cidr: str | None) -> str | None:
        if cidr is None:
            return None
        try:
            ipaddress.ip_network(cidr, strict=True)
        except ValueError as exc:
            raise ValueError(f"invalid BMC CIDR {cidr!r}: {exc}") from exc
        return cidr

    @model_validator(mode="after")
    def _require_a_bmc_network(self) -> "SiteNetworks":
        """A site may have one vendor or both, never neither."""
        if self.dell_bmc is None and self.cisco_bmc is None:
            raise ValueError(
                "at least one of 'dell-bmc' / 'cisco-bmc' must be set for every site"
            )
        return self


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_host: str
    temporal_namespace: str = "default"


class SegmentLifecycleActivitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Segments Manager (GETs are public; mutating calls need the token) ---
    segments_manager_url: str
    segments_manager_api_token: str

    # --- our payload policy (NOT next's config — no NEXT_ prefix) ---
    domain: str

    # --- the next (connectivity) service itself ---
    next_url: str
    # URI paths are configurable because the air-gapped prod paths may differ
    # from the placeholders the local mock serves.
    next_token_renewal_uri: str = "/token-renewal-uri"
    next_open_rules_uri: str = "/open-rules-uri"
    next_check_status_uri: str = "/check-request-status"

    # The AD group next attributes every open-rules request to (payload
    # `ad_groups`). It names a group in NEXT's own directory, so it is theirs to
    # define and differs per environment — no code default, an operator must set
    # it in each environment's ConfigMap.
    next_group: str

    # Seconds between polls of a submitted next request's status. Differs
    # sharply by environment (fast in local/dev against the mock, slow in prod
    # against the real human-approved service), so no code default — an
    # operator must set it explicitly in each environment's ConfigMap.
    next_checking_request_interval_seconds: int

    # --- port policy per direction (REQUIRED — populated from the ConfigMap;
    # no code defaults, so a missing/typo'd key fails the worker at startup,
    # never mid-workflow). JSON per protocol, e.g.:
    #   PORTS_HC_TO_MCE={"tcp": ["30000-32767"], "udp": ["9000"]}
    # The activity layer expands this to the next API's ports structure.
    ports_hc_to_mce: dict[str, list[str]]
    ports_mce_to_hc: dict[str, list[str]]
    ports_inventory_to_mce: dict[str, list[str]]
    ports_mce_to_inventory: dict[str, list[str]]
    ports_pxe_to_mce: dict[str, list[str]]
    ports_mce_to_pxe: dict[str, list[str]]

    # --- The shared site topology (SITE_NETWORKS). This service reads only
    # each site's `dell-bmc` and `cisco-bmc`: every MCE segment opens a
    # one-directional rule to BOTH of its site's static BMC networks (server
    # BMCs live on a different /16 per hardware vendor). BMC is NOT a
    # Segments-Manager-tracked segment type, so those CIDRs are
    # operator-configured rather than queried at runtime. The Segments Manager
    # reads `pool` out of the same structure. ---
    site_networks: dict[str, SiteNetworks]
    # ONE port profile for both vendors: an MCE reaches a Dell BMC and a
    # Cisco BMC over the same IPMI ports, and two knobs that must be kept
    # equal are a drift source, not a feature.
    ports_mce_to_bmc: dict[str, list[str]]

    # --- allocate-segment: the day1 values repo -----------------------------
    # Where cluster values files live (sites/<site>/mces/<mce>/hostedClusters/
    # <cluster>.yaml). The workflow appends the vlanId + dhcp_values block
    # there and pushes; Argo CD + Crossplane take it from git to a live DHCP
    # scope. The token authenticates the push (and the clone, for a private
    # repo) — it is injected into the clone URL in memory only and scrubbed
    # from every log line and error message.
    day1_repo_url: str
    day1_branch: str = "main"
    day1_clusters_root: str = "sites"
    day1_git_user_name: str
    day1_git_user_email: str
    day1_git_token: str

    # --- allocate-segment: DHCP scope policy + the DHCP API -----------------
    # The ONE DHCP policy knob: last-octet ranges excluded from distribution,
    # e.g. [[1, 10], [241, 254]]. startRange/endRange are DERIVED (first/last
    # non-excluded host octet), so the range and the exclusions can never
    # contradict each other. /24 segments only — build_dhcp_values asserts the
    # prefix and rejects anything else as UnsupportedSegmentPrefix.
    dhcp_exclusion_octet_ranges: list[tuple[int, int]]
    # The DHCP scope API (read-only here: the workflow only ever GETs a scope
    # to observe Crossplane's convergence — it never creates one itself).
    #
    # No token setting: that API leaves its scope GETs unauthenticated precisely
    # so this poll needs no credential. Secrets are namespace-scoped and envFrom
    # resolves per-pod, so authenticating here meant a copy of the API's token
    # living in redbull-workflows that had to rotate in step with the original.
    # Writes there are still authenticated — this worker just never makes one.
    dhcp_api_url: str

    @field_validator(
        "ports_hc_to_mce",
        "ports_mce_to_hc",
        "ports_inventory_to_mce",
        "ports_mce_to_inventory",
        "ports_pxe_to_mce",
        "ports_mce_to_pxe",
        "ports_mce_to_bmc",
    )
    @classmethod
    def _validate_port_profile(cls, profile: dict[str, list[str]]) -> dict[str, list[str]]:
        """Strict, fail-fast validation of the ConfigMap port syntax."""
        if not profile:
            raise ValueError("port profile must not be empty")
        for protocol, entries in profile.items():
            if protocol.lower() not in _SUPPORTED_PROTOCOLS:
                raise ValueError(
                    f"unsupported protocol {protocol!r} (expected one of {_SUPPORTED_PROTOCOLS})"
                )
            if not entries:
                raise ValueError(f"protocol {protocol!r} has no port entries")
            for entry in entries:
                match = _PORT_ENTRY_RE.match(entry)
                if not match:
                    raise ValueError(
                        f"invalid port entry {entry!r} for {protocol!r} "
                        "(expected 'PORT' or 'START-END')"
                    )
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else start
                if not (1 <= start <= 65535 and 1 <= end <= 65535):
                    raise ValueError(f"port out of range in entry {entry!r} for {protocol!r}")
                if start > end:
                    raise ValueError(f"inverted range in entry {entry!r} for {protocol!r}")
        return profile

    @field_validator("site_networks")
    @classmethod
    def _validate_site_networks(cls, sites: dict[str, SiteNetworks]) -> dict[str, SiteNetworks]:
        """Fail fast on an empty topology. Per-site CIDRs are validated by SiteNetworks."""
        if not sites:
            raise ValueError("site_networks must not be empty")
        return sites

    @field_validator("day1_clusters_root")
    @classmethod
    def _validate_day1_clusters_root(cls, root: str) -> str:
        """A bare relative directory name — path building assumes no slashes
        to strip and no absolute escape out of the clone."""
        if not root or root != root.strip("/").strip():
            raise ValueError(
                f"day1_clusters_root must be a bare relative path (got {root!r})"
            )
        return root

    @field_validator("dhcp_exclusion_octet_ranges")
    @classmethod
    def _validate_dhcp_exclusion_octet_ranges(
        cls, ranges: list[tuple[int, int]]
    ) -> list[tuple[int, int]]:
        """Strict, fail-fast validation of the one DHCP policy knob: every
        octet a valid /24 host octet, pairs ordered, strictly ascending and
        non-overlapping, and at least one octet left to distribute."""
        if not ranges:
            raise ValueError("dhcp_exclusion_octet_ranges must not be empty")
        previous_end = 0
        for start, end in ranges:
            if not (1 <= start <= 254 and 1 <= end <= 254):
                raise ValueError(
                    f"exclusion octets must be in 1..254 (got [{start}, {end}])"
                )
            if start > end:
                raise ValueError(f"inverted exclusion range [{start}, {end}]")
            if start <= previous_end:
                raise ValueError(
                    "exclusion ranges must be strictly ascending and "
                    f"non-overlapping (got [{start}, {end}] after octet {previous_end})"
                )
            previous_end = end
        excluded = {
            octet for start, end in ranges for octet in range(start, end + 1)
        }
        if len(excluded) >= 254:
            raise ValueError(
                "dhcp_exclusion_octet_ranges excludes every host octet — "
                "nothing left to distribute"
            )
        return ranges
