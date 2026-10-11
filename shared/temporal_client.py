"""The one way every process here opens its Temporal client.

Each worker entrypoint and api.py connects through `connect_temporal`, so the
two things every connection must carry are written once rather than five
times: the Pydantic data converter (without it Pydantic payloads cannot cross
the boundary — CLAUDE.md §5) and, when configured, TLS to the frontend.

Not imported by workflow code: it reads files and builds a client, neither of
which belongs in the sandbox.
"""

from __future__ import annotations

from pathlib import Path

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import TLSConfig

from shared.settings import TemporalSettings


def tls_config(settings: TemporalSettings) -> TLSConfig | None:
    """TLS for the frontend, or None for plain gRPC.

    Read from a file the pod mounts, so a CA rotation is a ConfigMap change and
    a rollout, never an image rebuild. A server name without a CA is refused
    rather than silently connecting in plain text: it can only mean a CA
    mount went missing.
    """
    if not settings.temporal_tls_ca:
        if settings.temporal_tls_server_name:
            raise ValueError(
                "TEMPORAL_TLS_SERVER_NAME is set but TEMPORAL_TLS_CA is not: "
                "TLS needs the CA that signed the frontend's certificate"
            )
        return None
    return TLSConfig(
        server_root_ca_cert=Path(settings.temporal_tls_ca).read_bytes(),
        domain=settings.temporal_tls_server_name or None,
    )


async def connect_temporal(settings: TemporalSettings) -> Client:
    """Connect with the Pydantic data converter, over TLS when configured."""
    tls = tls_config(settings)
    return await Client.connect(
        settings.temporal_host,
        namespace=settings.temporal_namespace,
        data_converter=pydantic_data_converter,
        tls=tls if tls is not None else False,
    )
