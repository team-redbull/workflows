"""UpiServerActivitySettings — the upi-server activity worker only (one deployment INSIDE each UPI cluster).

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class UpiServerActivitySettings(BaseSettings):
    """Config for the upi-server worker of ONE UPI cluster.

    Keep aligned with deploy/charts/upi-server-worker (ConfigMap
    `upi-server-config`, Secret `upi-server-credentials`).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- which cluster this worker serves -------------------------------------
    # EXACTLY the name create-upi-cluster was given: the queue is
    # `upi-server-activity-<UPI_CLUSTER>`, built the same way on the hub.
    upi_cluster: str

    # --- server-scan (ADMIN token: the allocation locks the server) -------------
    server_scan_url: str
    server_scan_api_token: str

    # --- BMC credentials, per vendor (Redfish virtual media) -------------------
    hp_bmc_username: str = ""
    hp_bmc_password: str = ""
    dell_bmc_username: str = ""
    dell_bmc_password: str = ""
    intersight_bmc_username: str = ""
    intersight_bmc_password: str = ""

    # --- the ISOs ---------------------------------------------------------------
    # Where the worker writes them (the chart mounts a volume, shared with the
    # nginx sidecar that serves it) and the URL the BMCs fetch them from (the
    # chart's Route or NodePort in front of that sidecar), without a trailing `/`.
    iso_dir: str = "/isos"
    iso_base_url: str
    # `oc adm node-image create`: an optional registry auth file (the runbook's
    # `--registry-config`) and `--insecure` for a mirror with its own CA.
    node_image_registry_config: str = ""
    node_image_insecure: bool = False
    # The install disk when a request names none (nodes-config rootDeviceHints).
    default_root_device: str = "/dev/sda"
