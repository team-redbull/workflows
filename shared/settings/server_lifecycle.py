"""ServerLifecycleActivitySettings — The server-lifecycle activity worker only (one deployment per MCE).

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

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
