"""ServerProvisioningActivitySettings — The server-provisioning activity worker only.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
