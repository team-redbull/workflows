"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (every worker and api.py).
  - SegmentLifecycleActivitySettings: needed only by the segment-lifecycle
    activity worker/tasks (the Segments Manager, the day1 values repo and the
    DHCP exclusion policy). The workflow worker has no business holding these.
  - ServerLifecycleActivitySettings / ServerProvisioningActivitySettings: the
    same, for those domains' activity workers.

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

    @field_validator("day1_repo_url")
    @classmethod
    def _require_https_day1_repo_url(cls, url: str) -> str:
        """The push token is injected into https:// URLs ONLY
        (values_repo.authenticated_url). Any other scheme would silently drop
        it: every ls-remote would then fail authentication, which is the
        retryable ValuesRepoGitError, so each run would retry forever. Fail at
        worker startup instead, where the fix is one config edit."""
        if not url.startswith("https://"):
            raise ValueError(
                "day1_repo_url must be an https:// URL — the push token is only "
                f"injected into https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url

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


class ServerLifecycleActivitySettings(BaseSettings):
    """Config for the server-lifecycle activity worker only.

    The brain never reads these (it holds no credentials), and neither does the
    segment-lifecycle limb — except SEGMENTS_MANAGER_URL, which install-server
    reaches through a segment-lifecycle activity on THAT limb rather than by
    holding a second copy of its token here.

    Keep aligned with redbull-platform's
    gitops/charts/server-lifecycle-worker/templates/config.yaml
    (server-lifecycle-config) and its server-lifecycle-credentials Secret.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- which MCE cluster this worker serves --------------------------------
    # Names the activity queue this worker polls
    # (`server-lifecycle-activity-<mce_cluster>`), so it must match the
    # `mce_cluster` callers put in an install-server request — the same string
    # the Segments Manager holds as that segment's `cluster_name`.
    #
    # Required, with no default, because a wrong value here is undetectable at
    # runtime: the worker would serve installs meant for a different MCE and
    # create their BareMetalHosts on this cluster, with both halves succeeding.
    # ONE WORKER SERVES ONE MCE; this is the field that says which.
    mce_cluster: str

    # --- server-scan: the inventory platform --------------------------------
    # Base URL INCLUDING the /api/v1 prefix. The token needs server-scan's
    # ADMIN role: besides the /servers/available GET, install-server takes and
    # releases the install lock (POST/DELETE /servers/{id}/reservation, ADR-0035),
    # and those are mutations. It may be empty — server-scan's auth is disabled
    # by default, which auto-admits every caller.
    server_scan_url: str
    server_scan_api_token: str = ""

    # --- BMC credentials, per vendor ----------------------------------------
    # server-scan deliberately never holds these: it returns inventory data
    # only, so the credentials Ironic will use to drive the BMC come from this
    # worker's own Secret, exactly as the operator's did. A vendor with no pair
    # configured fails that server's install with BmcCredentialsMissingError
    # rather than silently writing an unusable Secret.
    #
    # No defaults, deliberately — bmhgen defaulted Dell to root/calvin, which
    # is the factory password: a forgotten Secret then produced a BareMetalHost
    # that failed at Ironic instead of failing here with a clear reason.
    hp_bmc_username: str = ""
    hp_bmc_password: str = ""
    dell_bmc_username: str = ""
    dell_bmc_password: str = ""
    cisco_bmc_username: str = ""
    cisco_bmc_password: str = ""
    intersight_bmc_username: str = ""
    intersight_bmc_password: str = ""


class ServerProvisioningActivitySettings(BaseSettings):
    """Config for the server-provisioning activity worker only.

    Keep aligned with redbull-platform's
    gitops/charts/server-provisioning-worker/templates/config.yaml
    (server-provisioning-config) and its server-provisioning-credentials Secret.
    The passwords below all belong in that Secret.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- OpenManage Enterprise ----------------------------------------------
    # Base URL of the appliance, e.g. https://ome.example; the client appends
    # /api. The account discovers devices and deploys templates, so it needs
    # OME's device-manager rights over the Dell estate.
    ome_url: str
    ome_username: str
    ome_password: str

    # --- iDRAC root credentials ---------------------------------------------
    # The ONLY iDRAC account this workflow touches (user slot 2). The OME
    # account on the iDRAC is never read or changed — doing so would cut OME
    # off from the machine.
    #
    # IDRAC_ROOT_PASSWORD is the password the Dell template ENFORCES, and is
    # always tried first: a machine provisioned before (or re-run) already
    # has it. IDRAC_FACTORY_PASSWORDS are the ones a server may arrive with,
    # tried next in the order given — a JSON list, e.g. ["calvin", "<other>"].
    # Keep that list short: iDRAC9 blocks an address after 3 failed logins
    # within its fail window, so the target plus two factory passwords is one
    # round that can never trip the block before the right password is tried.
    idrac_username: str = "root"
    idrac_root_password: str
    idrac_factory_passwords: list[str]

    # --- Dell templates -----------------------------------------------------
    # Which OME template provisions which machine: Redfish model -> iDRAC
    # firmware version -> template name, e.g.
    # {"PowerEdge R660": {"7.10.70.00": "ocp-r660-idrac-7.10.70.00"}}.
    # A machine whose (model, firmware) is not listed fails its run with
    # TemplateNotConfiguredError rather than getting a near-miss template.
    dell_templates: dict[str, dict[str, str]]

    # --- the naming service -------------------------------------------------
    # Renames the OME server profile to ocp-dell-<model>-<region>-<cores>c-
    # <mem>gb-<disk>tb-<service tag>. See
    # activities/server_provisioning/server_namer.py for the request it gets.
    server_namer_url: str
    server_namer_api_token: str = ""

    # --- server-scan --------------------------------------------------------
    # Base URL INCLUDING /api/v1; a VIEWER token is enough (GET /servers only).
    server_scan_url: str
    server_scan_api_token: str = ""

    @field_validator("ome_url")
    @classmethod
    def _require_https_ome_url(cls, url: str) -> str:
        """OME serves its API over HTTPS only; fail at startup, not per run."""
        if not url.startswith("https://"):
            raise ValueError(f"ome_url must be an https:// URL (got {url!r})")
        return url.rstrip("/")

    @field_validator("idrac_factory_passwords")
    @classmethod
    def _require_factory_passwords(cls, passwords: list[str]) -> list[str]:
        """At least one, none empty, and few enough that one round cannot lock
        the iDRAC out before the last candidate is tried."""
        if not passwords or any(not p for p in passwords):
            raise ValueError("idrac_factory_passwords must be a non-empty list of non-empty passwords")
        if len(passwords) > 2:
            raise ValueError(
                "idrac_factory_passwords holds at most 2 entries: with the target "
                "password tried first, a third would be a third failed login, "
                "which iDRAC9 answers by blocking this worker's address"
            )
        return passwords

    @field_validator("dell_templates")
    @classmethod
    def _require_templates(cls, templates: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
        """Every model maps at least one firmware to a non-empty template name."""
        if not templates:
            raise ValueError("dell_templates must list at least one model")
        for model, by_firmware in templates.items():
            if not by_firmware or any(not name for name in by_firmware.values()):
                raise ValueError(f"dell_templates[{model!r}] must map firmware versions to template names")
        return templates

    @property
    def idrac_root_passwords(self) -> list[str]:
        """The candidate list a run's `IdracRef.credential` indexes into.

        Target first (TARGET_CREDENTIAL = 0), then each factory password in
        order, without repeating the target.
        """
        return [self.idrac_root_password] + [
            p for p in self.idrac_factory_passwords if p != self.idrac_root_password
        ]
