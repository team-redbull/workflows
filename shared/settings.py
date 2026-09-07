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

from shared.models.segment_lifecycle import SegmentType

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

    # Credentials next's token-renewal endpoint authenticates with (POSTed as
    # the renewal body; the access token it returns is what the open-rules and
    # status calls carry). No code defaults — they live in the
    # `next-api-credentials` Secret, so a missing one must crash the worker at
    # startup rather than surface as a 401 mid-workflow.
    next_client_id: str
    next_password: str

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
    # No PORTS_*_PXE / PORTS_PXE_*: PXE is a real Segments Manager type, but
    # connectivity is DELIBERATELY not opened for it (see _SUPPORTED_TYPES in
    # workflow_domains/segment_lifecycle/open_segment_rules.py). These profiles
    # ARE the peering topology, so leaving a placeholder profile here would make
    # every MCE run discover same-site PXE segments and open MCE<->PXE rules.

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
    #
    # The clusters root and the committer identity are NOT config: they are
    # hardcoded in activities/segment_lifecycle/values_repo.py (CLUSTERS_ROOT,
    # GIT_USER_NAME, GIT_USER_EMAIL). The root is the day1 repo's own layout —
    # a wrong value simply finds no cluster file — and the identity names THIS
    # workflow, so neither is something an operator tunes per environment.
    day1_repo_url: str
    day1_branch: str = "main"
    day1_git_token: str

    # --- allocate-segment: DHCP scope policy + the DHCP API -----------------
    # The ONE DHCP policy knob, PER SEGMENT TYPE: last-octet ranges excluded
    # from distribution, e.g. {"HC": [[1, 10], [241, 254]]}. Together with the
    # network they are the WHOLE written block: no startRange/endRange, so the
    # DHCP API derives .1-.253 and these exclusions carve the ends out of it —
    # one derivation, in the service that owns it. /24 segments only —
    # build_dhcp_values asserts the prefix and rejects anything else as
    # UnsupportedSegmentPrefix. A type's list may be EMPTY (excludes nothing).
    #
    # Keyed by TYPE because the policy genuinely differs per type (a PXE
    # segment reserves a different slice of its /24 than an HC one), and the
    # lookup is dynamic (the allocation's type) — the same reason site_networks
    # is a map while the statically-referenced PORTS_* directions are flat keys.
    #
    # A type that is NOT LISTED excludes nothing — a type earns an entry by
    # reserving part of its /24, and no operator should have to write an empty
    # list to say "nothing". HC is the one REQUIRED key: it is the only type
    # allocate-segment allocates today, and a forgotten HC policy would quietly
    # hand out the addresses production reserves rather than fail. Types listed
    # ahead of the code that allocates them are validated the same way now.
    dhcp_exclusion_octet_ranges: dict[SegmentType, list[tuple[int, int]]]
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

    @field_validator("dhcp_exclusion_octet_ranges")
    @classmethod
    def _validate_dhcp_exclusion_octet_ranges(
        cls, policy: dict[SegmentType, list[tuple[int, int]]]
    ) -> dict[SegmentType, list[tuple[int, int]]]:
        """Strict, fail-fast validation of the one DHCP policy knob: a policy
        for HC at minimum, and within EVERY type's ranges each octet a valid
        /24 host octet, pairs ordered, strictly ascending and non-overlapping,
        and at least one distributable octet left. A type's list may be EMPTY
        (that type excludes nothing); the map itself may not be."""
        if not policy:
            raise ValueError("dhcp_exclusion_octet_ranges must not be empty")
        if SegmentType.HC not in policy:
            raise ValueError(
                "dhcp_exclusion_octet_ranges must carry a policy for type "
                f"{SegmentType.HC.value!r} (the only type allocate-segment "
                "supports); got "
                f"{sorted(segment_type.value for segment_type in policy)}"
            )
        for segment_type, ranges in policy.items():
            label = segment_type.value
            # An EMPTY list is legal and meaningful: that type excludes
            # nothing, so its block carries a network and no exclusions and the
            # scope distributes the DHCP API's whole derived .1-.253. Written
            # explicitly, it says an operator decided — unlike a missing key.
            previous_end = 0
            for start, end in ranges:
                if not (1 <= start <= 254 and 1 <= end <= 254):
                    raise ValueError(
                        f"exclusion octets must be in 1..254 "
                        f"(got [{start}, {end}] for {label})"
                    )
                if start > end:
                    raise ValueError(
                        f"inverted exclusion range [{start}, {end}] for {label}"
                    )
                if start <= previous_end:
                    raise ValueError(
                        "exclusion ranges must be strictly ascending and "
                        f"non-overlapping (got [{start}, {end}] after octet "
                        f"{previous_end} for {label})"
                    )
                previous_end = end
            # The DHCP API distributes .1-.253 (it stops short of the .254
            # gateway it derives), so that is what the exclusions eat into — a
            # policy covering all of it leaves nothing to lease.
            excluded = {
                octet for start, end in ranges for octet in range(start, end + 1)
            }
            if excluded >= set(range(1, 254)):
                raise ValueError(
                    f"dhcp_exclusion_octet_ranges[{label}] excludes every "
                    "distributable octet (.1-.253) — nothing left to distribute"
                )
        return policy
