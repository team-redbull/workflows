"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (every worker and api.py).
  - SegmentLifecycleActivitySettings: needed only by the segment-lifecycle
    activity worker/tasks (the Segments Manager and the DHCP scope API). The
    workflow worker has no business holding these.
  - ClusterLifecycleActivitySettings: the cluster-lifecycle hub limb — the
    day1 repo (the same keys as upgrade-gitops's and vm-lifecycle's) plus its
    git provider, the DHCP exclusion policy the allocation block is written
    with (moved here from segment-lifecycle on 2026-11-11), and the day2 sig
    repos the cluster is registered in.
  - ServerLifecycleActivitySettings / ServerProvisioningActivitySettings: the
    same, for those domains' activity workers.
  - UpgradeGitopsActivitySettings, ClusterUpgradeActivitySettings and
    UpgradeAgentActivitySettings: the cluster-upgrade domain's three limbs,
    one class per credential set (CLAUDE.md §2) — the hub's day1/day2/Brain
    repos and release registry, the per-MCE cluster limb's Argo CD location,
    the per-MCE agent limb's upgrade agent endpoint. Each read by that limb only.
  - VmLifecycleActivitySettings: the vm-lifecycle hub limb — the day1 repo
    (the same keys as above), the KubeVirt clusters' agent URL templates and
    tokens, and create-vm's placement policy.

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
    (segment-lifecycle-config: the DHCP API URL; + the dhcp-api-token Secret)
redbull-platform/gitops/charts/cluster-lifecycle-worker/templates/config.yaml
    (cluster-lifecycle-config: the day1 repo URL and provider, the DHCP policy,
    the day2 sig templates; + the redbull-git-token Secret)

Do NOT import this module from inside a workflow definition (it runs in the
sandbox) — only from worker entrypoints, api.py, and activity
implementations.
"""

from __future__ import annotations

import re
import string

from typing import Annotated

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.cluster_names import canonical_mce_cluster
from shared.models.segment_lifecycle import SegmentType
from shared.models.vm_lifecycle import KvClusterRef


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_host: str
    temporal_namespace: str = "default"

    # --- TLS to the frontend (optional) -------------------------------------
    # Unset = plain gRPC, which is what every client INSIDE the hub uses against
    # the temporal-frontend Service. A worker on ANOTHER cluster reaches the
    # frontend through a passthrough Route, which routes by the TLS SNI and so
    # carries TLS only: it sets the CA that signed the endpoint's certificate
    # (a PEM file path) and, when the dialled host differs from the name on the
    # certificate, the name to verify and send as SNI. Verification is never
    # switched off — a CA is the whole configuration.
    temporal_tls_ca: str = ""
    temporal_tls_server_name: str = ""


class SegmentLifecycleActivitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Segments Manager (GETs are public; mutating calls need the token) ---
    segments_manager_url: str
    segments_manager_api_token: str

    # The day1 repo and the DHCP exclusion policy are NOT here any more: since
    # provision-cluster owns the cluster file (2026-11-11), allocate-segment
    # allocates and verifies only, and DAY1_REPO_URL, DAY1_GIT_TOKEN and
    # DHCP_EXCLUSION_OCTET_RANGES moved to ClusterLifecycleActivitySettings.
    # The segment-lifecycle chart keeps rendering them until this image is the
    # deployed one (CLAUDE.md §8); `extra="ignore"` lets the limb ignore them.

    # --- release-segment: the DHCP scope API (dhcp_scope_manager) -----------
    # release-segment checks whether a released cluster's scope survived the
    # Argo CD cascade (GET, anonymous) and deletes it if so (DELETE, bearer
    # token). It is the ONLY write this worker makes there, and only a DELETE:
    # Crossplane stays the only creator/updater of scopes (CLAUDE.md §4).
    #
    # The token is a COPY of the DHCP API's own token (redbull-platform:
    # gitops/charts/dhcp-scope-manager/charts/dhcp-api-token/values.yaml),
    # rendered into this worker's namespace by the segment-lifecycle-worker
    # chart. The two must rotate together — a stale copy fails every release
    # with DhcpApiAuthError (non-retryable), never silently.
    dhcp_api_url: str
    dhcp_api_token: str


class ServerLifecycleActivitySettings(BaseSettings):
    """Config for the server-lifecycle activity worker only.

    The brain never reads these (it holds no credentials), and neither does the
    segment-lifecycle limb — except SEGMENTS_MANAGER_URL, which add-server-to-mce
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
    # `mce_cluster` callers put in an add-server-to-mce request — the same string
    # the Segments Manager holds as that segment's `cluster_name`.
    #
    # Required, with no default, because a wrong value here is undetectable at
    # runtime: the worker would serve installs meant for a different MCE and
    # create their BareMetalHosts on this cluster, with both halves succeeding.
    # ONE WORKER SERVES ONE MCE; this is the field that says which.
    #
    # Canonicalised like the API's `mce_cluster` (the `ocp4-` prefix added when
    # left out), so `prod-mce-bat-yam` here and `ocp4-prod-mce-bat-yam` in a
    # request still meet on one queue.
    mce_cluster: str

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)

    # --- server-scan: the inventory platform --------------------------------
    # Base URL INCLUDING the /api/v1 prefix. The token needs server-scan's
    # ADMIN role: besides the /servers/available GET, add-server-to-mce takes and
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

    # --- PXE map: the network-boot path for IPMI servers --------------------
    # An IPMI BMC (a UCS-managed Cisco blade) has no virtual media, so the host
    # PXE-boots and the site's PXE VM serves it the InfraEnv's iPXE script by
    # MAC. Site -> that VM's base URL, e.g. {"bat-yam": "http://10.20.90.5:8080"};
    # the site is the MCE inventory segment's, as the Segments Manager names it.
    #
    # Defaulted EMPTY, unlike most keys: only an IPMI install needs it, and a
    # required key would crash-loop every MCE's worker until its chart shipped
    # one. An IPMI candidate at a site that is not listed is skipped with
    # PxeSiteNotConfiguredError (add-server-to-mce; publish-infraenv-to-pxe-server
    # fails on it, having nothing else to do); Redfish candidates never read this.
    pxe_map_urls: dict[str, str] = {}
    pxe_map_token: str = ""

    @field_validator("pxe_map_urls")
    @classmethod
    def _validate_pxe_map_urls(cls, urls: dict[str, str]) -> dict[str, str]:
        """Every entry an http(s) base URL — a typo fails at startup, not mid-install."""
        for site, url in urls.items():
            if not site or not url.startswith(("http://", "https://")):
                raise ValueError(
                    f"pxe_map_urls[{site!r}] must be an http:// or https:// base "
                    f"URL (got {url!r})"
                )
        return {site: url.rstrip("/") for site, url in urls.items()}


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


# --- cluster-upgrade: three limbs (CLAUDE.md §2) -------------------------------------
# One class per limb, each filled in by that limb's code. Field names = Helm
# ConfigMap/Secret keys lowercased, as for the classes above.


class UpgradeGitopsActivitySettings(BaseSettings):
    """Config for the cluster-upgrade HUB limb (`upgrade-gitops-worker`) only.

    The day1 and day2 repos, the release registry, Brain: everything the
    upgrade reads or writes in git or a registry. The brain never reads these.

    Keep aligned with redbull-platform's gitops/charts/upgrade-gitops-worker/
    (upgrade-gitops-config and the Secrets it names).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- day1: the rules, the cluster files, the upgrade's commits ------------
    # The same repo and the same keys as segment-lifecycle's (DAY1_REPO_URL,
    # DAY1_GIT_TOKEN); the chart fills DAY1_GIT_TOKEN from the shared
    # `redbull-git-token` Secret. This limb reads and
    # pushes `main` only: Argo CD renders main, and nothing else (CONTEXT §6.1).
    day1_repo_url: str
    day1_git_token: str
    # Run in the clone before every push, e.g. `python3 scripts/validate_all_clusters.py`
    # (D3: day1 CI does not run on main). Split like a shell line, run without
    # a shell. Empty = no validator, which every commit logs.
    day1_validate_command: str = ""

    # --- day2: version layers (check 4) ----------------------------------------
    # Empty = not checked: check_day2_layers reports nothing missing and says why.
    day2_repo_url: str = ""
    day2_git_token: str = ""

    # --- Brain: where an agent verdict is recorded (D10) -----------------------
    # Empty = not configured: write_incident records a note in the run's
    # history and pushes nothing, so no run ever waits on Brain.
    brain_repo_url: str = ""
    brain_git_token: str = ""

    # --- the release registry ---------------------------------------------------
    # A container auth file (`.dockerconfigjson` shape) for the release, CPO and
    # payload repositories, passed to `oc adm release info` as well. Empty =
    # anonymous, which quay.io's public ocp-release allows; its payload
    # repository (ocp-v4.0-art-dev) needs the pull secret.
    registry_auth_file: str = ""
    # Off only for a registry whose CA cannot be mounted (the internal mirror).
    registry_tls_verify: bool = True

    # Who the upgrade's commits are by is NOT config: activities/git_repo.py's
    # UPGRADE_AUTHOR, hard-coded like segment-lifecycle's — it identifies the
    # writer, so it must not vary per environment.

    @field_validator("day1_repo_url", "day2_repo_url", "brain_repo_url")
    @classmethod
    def _require_https_repo_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url).

        Another scheme would silently drop it, and every clone would then fail
        as the retryable ValuesRepoGitError, forever. Fail at worker start
        instead. An empty optional repo stays empty (not configured).
        """
        if url and not url.startswith("https://"):
            raise ValueError(
                "repo URLs must be https:// — the token is only injected into "
                f"https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url


class ClusterUpgradeActivitySettings(BaseSettings):
    """Config for the per-MCE cluster-upgrade limb (`cluster-upgrade-worker`) only.

    Polls `cluster-upgrade-activity-<mce_cluster>`; reads that MCE's
    HostedClusters and NodePools and syncs their Argo CD Applications as its
    own ServiceAccount. It never writes a HostedCluster or NodePool (DESIGN
    D1): Argo CD applies the day1 commit. Keep aligned with
    deploy/charts/cluster-upgrade-worker/templates/configmap.yaml.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Which MCE this worker serves: names its queue. Canonical, `ocp4-` prefixed.
    mce_cluster: str

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)

    # --- Argo CD on this MCE --------------------------------------------------
    # Where the hosted clusters' Applications live: the namespace of the
    # hcAppset that renders them, the shared openshift-gitops instance in
    # production and on the sandbox. The limb reads and syncs the Application
    # named after the cluster file there, and its chart's Role is in it.
    argo_namespace: str = "openshift-gitops"
    # Which source of a MULTI-SOURCE Application holds the day1 values: the one
    # with this `ref`. The hcAppset renders two sources, the hostedclusters-setup
    # chart and the day1 repo as `ref: values`, read by the chart's valueFiles
    # as `$values/...`; a day1 commit is synced in that source while the chart's
    # keeps its revision. A single-source Application never reads this.
    argo_values_ref: str = "values"

    @field_validator("argo_namespace")
    @classmethod
    def _namespace_name(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", value):
            raise ValueError(f"argo_namespace must be a Kubernetes namespace name (got {value!r})")
        return value

    @field_validator("argo_values_ref")
    @classmethod
    def _ref_key(cls, value: str) -> str:
        # Argo CD's own rule for a source's `ref` (GetRefSources, util/argo/argo.go).
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", value):
            raise ValueError(f"argo_values_ref must be an Argo CD source ref: letters, digits, _ and - (got {value!r})")
        return value


class UpgradeAgentActivitySettings(BaseSettings):
    """Config for the per-MCE upgrade-agent limb (`upgrade-agent-worker`) only.

    Holds no Kubernetes credential: it calls the upgrade agent's endpoint.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Which MCE this worker serves: names its queue. Canonical, `ocp4-` prefixed.
    mce_cluster: str

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)

    # The upgrade agent service in this MCE: an agent-sandbox `Sandbox` behind
    # a Service (the k8s-agent chart), e.g. http://upgrade-agent.<ns>.svc:8080.
    # The token, when the service asks for one, comes from a Secret.
    upgrade_agent_url: str
    upgrade_agent_token: str = ""

    @field_validator("upgrade_agent_url")
    @classmethod
    def _http_upgrade_agent_url(cls, url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"upgrade_agent_url must be an http:// or https:// URL (got {url!r})")
        return url.rstrip("/")


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


# The git providers the cluster-lifecycle limb knows how to open a merge request on.
GIT_PROVIDERS = ("gitlab", "github")


class ClusterLifecycleActivitySettings(BaseSettings):
    """Config for the cluster-lifecycle HUB limb (`cluster-lifecycle-worker`) only.

    The day1 values repo (the cluster file, its branch, the merge request on
    the repo's git provider), the DHCP exclusion policy the allocation block is
    written with, and the day2 sig repos the cluster is registered in. No
    Kubernetes credential. Keep aligned with redbull-platform's
    gitops/charts/cluster-lifecycle-worker/ (cluster-lifecycle-config and the
    Secrets it names).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- day1: the cluster file, its branch, the merge request ----------------
    # The same repo as upgrade-gitops's and vm-lifecycle's (DAY1_REPO_URL). The
    # token is REDBULL_GIT_TOKEN, the key of the shared `redbull-git-token` Secret
    # (the air-gap's name, the operator 2026-11-11), so the chart passes the Secret
    # through unrenamed. DAY1_GIT_TOKEN is still accepted, for a chart that maps a
    # `day1-git-token` Secret (the dev cluster): REDBULL_GIT_TOKEN wins when both
    # are set. This limb READS `main` (where the cluster goes)
    # and PUSHES a new branch per cluster, `provision-cluster/<cluster>`, which a
    # person merges through the merge request the limb opens. The token must
    # therefore reach the provider's API too: a GitLab project/personal token
    # with `api` + `write_repository`, or a GitHub token with `repo`.
    #
    # The clusters root, the branch scheme and the committer identity are NOT
    # config: they are the day1 repo's own layout and this workflow's identity
    # (activities/cluster_lifecycle/day1_writer.py).
    day1_repo_url: str
    redbull_git_token: Annotated[str, Field(validation_alias=AliasChoices("REDBULL_GIT_TOKEN", "DAY1_GIT_TOKEN"))]
    # Which API opens the merge request: production is the air-gapped GitLab
    # (a merge request with `remove_source_branch`), dev is GitHub (a pull
    # request; delete-on-merge is a repository setting there). No default: the
    # two APIs share nothing, and a wrong guess fails every run at phase 4.
    day1_git_provider: str

    # --- the DHCP exclusion policy, written into every cluster file ------------
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
    # provision-cluster allocates today, and a forgotten HC policy would quietly
    # hand out the addresses production reserves rather than fail. Types listed
    # ahead of the code that allocates them are validated the same way now.
    # (Moved here from SegmentLifecycleActivitySettings on 2026-11-11, with the
    # cluster file; the policy predates the service-prefix rule, CLAUDE.md §3.)
    dhcp_exclusion_octet_ranges: dict[SegmentType, list[tuple[int, int]]]

    # --- day2: the sig repos the cluster is registered in -----------------------
    # Each sig is a repo (production: one GitLab project per sig, the tree at
    # its root) or a folder of one repo (dev: `sigs/<sig>` of one GitHub repo).
    # `{sig}` must appear in the URL template or the path template, or every
    # sig would be registered in one place. The path template is the prefix
    # before `sites/`; "" for the tree at the repo root. The push goes to the
    # sig repo's default branch with DAY2_GIT_TOKEN — REDBULL_GIT_TOKEN when
    # empty, the shared `redbull-git-token` covering both.
    day2_sig_repo_url_template: str
    day2_sig_path_template: str = ""
    day2_git_token: str = ""

    @field_validator("day1_repo_url")
    @classmethod
    def _require_https_day1_repo_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url).
        Any other scheme would silently drop it: every ls-remote would then fail
        authentication, which is the retryable ValuesRepoGitError, so each run
        would retry forever. Fail at worker startup instead."""
        if not url.startswith("https://"):
            raise ValueError(
                "day1_repo_url must be an https:// URL — the token is only "
                f"injected into https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url.rstrip("/")

    @field_validator("day1_git_provider")
    @classmethod
    def _known_git_provider(cls, value: str) -> str:
        provider = value.strip().lower()
        if provider not in GIT_PROVIDERS:
            raise ValueError(f"day1_git_provider must be one of {list(GIT_PROVIDERS)} (got {value!r})")
        return provider

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
                f"{SegmentType.HC.value!r} (the only type provision-cluster "
                "allocates); got "
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

    @field_validator("day2_sig_path_template")
    @classmethod
    def _relative_sig_path_template(cls, template: str) -> str:
        """A path inside the repo: leading and trailing slashes dropped, `..` refused."""
        template = template.strip().strip("/")
        if ".." in template.split("/"):
            raise ValueError(f"day2_sig_path_template {template!r} must stay inside the repo (no `..`)")
        return template

    @field_validator("day2_sig_repo_url_template")
    @classmethod
    def _https_sig_url_template(cls, template: str) -> str:
        if not template.startswith("https://"):
            raise ValueError(
                "day2_sig_repo_url_template must be an https:// URL — the token is only "
                f"injected into https clone URLs (got {template.split('@')[-1]!r})"
            )
        return template.rstrip("/")

    @model_validator(mode="after")
    def _sig_is_somewhere_in_the_templates(self) -> ClusterLifecycleActivitySettings:
        """`{sig}` in the URL or the path template, and nothing else in braces: a typo
        (`{sgi}`) fails at worker start, not as a KeyError on the first run."""
        placeholders: set[str] = set()
        for name, template in (
            ("day2_sig_repo_url_template", self.day2_sig_repo_url_template),
            ("day2_sig_path_template", self.day2_sig_path_template),
        ):
            try:
                fields = {field for _, field, _, _ in string.Formatter().parse(template) if field is not None}
            except ValueError as err:
                raise ValueError(f"{name} {template!r} is not a format string: {err}") from None
            if fields - {"sig"}:
                raise ValueError(f"{name} {template!r} may name only {{sig}} (got {sorted(fields - {'sig'})})")
            placeholders |= fields
        if "sig" not in placeholders:
            raise ValueError(
                "neither day2_sig_repo_url_template nor day2_sig_path_template names {sig}: every sig "
                "would be registered in one place"
            )
        return self

    @property
    def day2_token(self) -> str:
        """The token the sig repos are pushed with: DAY2_GIT_TOKEN, else REDBULL_GIT_TOKEN."""
        return self.day2_git_token or self.redbull_git_token

    def day2_sig_repo_url(self, sig: str) -> str:
        return self.day2_sig_repo_url_template.format(sig=sig)

    def day2_sig_path_prefix(self, sig: str) -> str:
        return self.day2_sig_path_template.format(sig=sig).strip("/")
