"""ServerLifecycleActivitySettings — The server-lifecycle activity worker only (one deployment per MCE).

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.bmc_address import kubevirt_bmc_host_template_problem
from shared.cluster_names import canonical_mce_cluster


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

    # --- VMs: a KubeVirt InfraEnv's hosts, behind kubevirt-redfish ------------
    # add-server-to-mce has a VM made for an InfraEnv whose agentLabels say
    # `redbull.io/size: kv<cpu>`, and its BareMetalHost drives the VM through
    # the kubevirt-redfish of the KV cluster it landed on. That Route's host is
    # ONE pattern in which only the KV cluster's name changes (the operator,
    # 2026-11-25): `host[:port]`, no scheme, no path, naming {kv_cluster} — the
    # cluster's day1 file name, `ocp4-prod-kv-five-az1-a` — e.g.
    # `kubevirt-redfish-kubevirt-redfish.apps.{kv_cluster}.example.com`. The
    # Redfish user is the same on every KV cluster.
    #
    # All three OPTIONAL (the day1 keys' reasoning, below): a worker without
    # them serves every bare-metal install, and a VM install fails at its first
    # step with KubevirtBmcNotConfiguredError, before any VM is made.
    kubevirt_bmc_host_template: str = ""
    kubevirt_bmc_username: str = ""
    kubevirt_bmc_password: str = ""

    @field_validator("kubevirt_bmc_host_template")
    @classmethod
    def _kubevirt_host_names_the_kv_cluster(cls, template: str) -> str:
        """A typo (`{kv_clustr}`, a scheme, a path) fails at worker start, not mid-install."""
        template = template.strip()
        if not template:
            return ""
        problem = kubevirt_bmc_host_template_problem(template)
        if problem is not None:
            raise ValueError(f"kubevirt_bmc_host_template {template!r}: {problem}")
        return template

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

    # --- day1: update-nodepool's cluster file and its replicas commit --------
    # The same repo and keys as the segment-lifecycle and upgrade-gitops limbs
    # (one Secret may serve all). This limb reads a cluster file at `main` and
    # commits only `nodepool[].replicas` there (read_day1_nodepools,
    # commit_day1_replicas). OPTIONAL, unlike theirs: only update-nodepool
    # needs them, so a worker without them serves every other workflow, and
    # update-nodepool fails at once with Day1NotConfiguredError.
    day1_repo_url: str = ""
    day1_git_token: str = ""
    # Run in the clone before the push, split like a shell line; empty = none.
    day1_validate_command: str = ""

    @field_validator("day1_repo_url")
    @classmethod
    def _require_https_day1_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url): fail at start, not per clone."""
        if url and not url.startswith("https://"):
            raise ValueError(f"day1_repo_url must be https:// (got {url.split('@')[-1]!r})")
        return url

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
