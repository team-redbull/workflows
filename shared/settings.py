"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Two settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (both workers and api.py).
  - SegmentLifecycleActivitySettings: needed only by the segment-lifecycle
    activity worker/tasks (the Segments Manager, the day1 values repo and the
    DHCP exclusion policy). The workflow worker has no business holding these.

Field names deliberately equal the Helm ConfigMap/Secret keys (lowercased) —
pydantic-settings matches env vars case-insensitively, so SEGMENTS_MANAGER_URL
populates segments_manager_url.

Note: which ConfigMap a key lives in (an ops grouping) is INDEPENDENT of which
settings class declares it (a code grouping). pydantic reads the flat process
env, so it never sees the ConfigMap boundary. SEGMENTS_MANAGER_URL lives in the
shared `workflows-orchestrator-config` ConfigMap (so future workflows reuse it
without duplication), yet stays a field on SegmentLifecycleActivitySettings —
only the activity worker requires it, and it mounts workflows-orchestrator-config
+ segment-lifecycle-config together. Keep the files aligned with the vendored
charts in the Argo CD repo:
redbull-platform/gitops/charts/workflows-orchestrator/templates/config.yaml
    (workflows-orchestrator-config: temporal + segments-manager url)
redbull-platform/gitops/charts/segment-lifecycle-worker/templates/config.yaml
    (segment-lifecycle-config: the day1 repo URL + the DHCP policy; + the
    day1-git-token Secret)

Do NOT import this module from inside a workflow definition (it runs in the
sandbox) — only from worker entrypoints, api.py, and activity
implementations.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.models.segment_lifecycle import SegmentType


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_host: str
    temporal_namespace: str = "default"


class SegmentLifecycleActivitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Segments Manager (GETs are public; mutating calls need the token) ---
    segments_manager_url: str
    segments_manager_api_token: str

    # --- allocate-segment: the day1 values repo -----------------------------
    # Where cluster values files live (sites/<site>/mces/<mce>/hostedClusters/
    # <cluster>.yaml). The workflow appends the vlanId + dhcp_values block
    # there and pushes. The token authenticates the push (and the clone, for a
    # private repo) — it is injected into the clone URL in memory only and
    # scrubbed from every log line and error message.
    #
    # The BRANCH is deliberately NOT a setting: every run names it
    # (`values_branch`), because the day1 pipeline that triggers allocate-segment
    # runs on a temporary branch only it knows, and a new cluster's file exists
    # only there. The block reaches Argo CD (which reads main only) when a human
    # merges that branch — after the run, outside it.
    #
    # The clusters root and the committer identity are NOT config: they are
    # hardcoded in activities/segment_lifecycle/values_repo.py (CLUSTERS_ROOT,
    # GIT_USER_NAME, GIT_USER_EMAIL). The root is the day1 repo's own layout —
    # a wrong value simply finds no cluster file — and the identity names THIS
    # workflow, so neither is something an operator tunes per environment.
    day1_repo_url: str
    day1_git_token: str

    # --- allocate-segment: DHCP scope policy --------------------------------
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
    # lookup is DYNAMIC — the activity selects the ranges for the type actually
    # allocated, which a flat per-type key could not express.
    #
    # A type that is NOT LISTED excludes nothing — a type earns an entry by
    # reserving part of its /24, and no operator should have to write an empty
    # list to say "nothing". HC is the one REQUIRED key: it is the only type
    # allocate-segment allocates today, and a forgotten HC policy would quietly
    # hand out the addresses production reserves rather than fail. Types listed
    # ahead of the code that allocates them are validated the same way now.
    dhcp_exclusion_octet_ranges: dict[SegmentType, list[tuple[int, int]]]

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
