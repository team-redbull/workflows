"""TemporalSettings — What anything that connects a Temporal Client needs: every worker and api.py.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


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
