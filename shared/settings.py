"""Typed, fail-fast configuration via pydantic-settings.

Values are read from the process environment and, if present, a .env file
(see .env.example) — never hardcoded endpoints, per the deployment-agnostic rule.

Two settings groups, matching the deployment boundary:
  - TemporalSettings: needed by anything that connects a Temporal Client
    (both workers and api.py).
  - ConnectivityActivitySettings: needed only by the connectivity activity
    worker/tasks (Segments Manager + next API URLs/credentials + port policy).
    The workflow worker has no business holding these.

Field names deliberately equal the Helm ConfigMap/Secret keys (lowercased) —
pydantic-settings matches env vars case-insensitively, so SEGMENTS_MANAGER_URL
populates segments_manager_url.

Note: which ConfigMap a key lives in (an ops grouping) is INDEPENDENT of which
settings class declares it (a code grouping). pydantic reads the flat process
env, so it never sees the ConfigMap boundary. DOMAIN and SEGMENTS_MANAGER_URL
live in the shared `orchestrator-config` ConfigMap (so future workflows reuse
them without duplication), yet stay fields on ConnectivityActivitySettings —
only the activity worker requires them, and it mounts orchestrator-config +
connectivity-config together. Keep the files aligned:
helm/workflow-worker/templates/config.yaml   (orchestrator-config: temporal + domain + segments-manager url)
helm/connectivity/templates/config.yaml      (connectivity-config: next URIs + ports; + the token Secret)

Do NOT import this module from inside a workflow definition (it runs in the
sandbox) — only from worker entrypoints, api.py, and activity implementations.
"""

from __future__ import annotations

import re

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# "9000" or "30000-32767"
_PORT_ENTRY_RE = re.compile(r"^(\d{1,5})(?:-(\d{1,5}))?$")
_SUPPORTED_PROTOCOLS = ("tcp", "udp")


class TemporalSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_host: str
    # Temporal's tenant-isolation unit; override only if the server hosts this
    # project in a dedicated namespace.
    temporal_namespace: str = "default"


class ConnectivityActivitySettings(BaseSettings):
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

    @field_validator(
        "ports_hc_to_mce",
        "ports_mce_to_hc",
        "ports_inventory_to_mce",
        "ports_mce_to_inventory",
        "ports_pxe_to_mce",
        "ports_mce_to_pxe",
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
