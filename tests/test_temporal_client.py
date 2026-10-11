"""shared/temporal_client.py — when a connection is TLS, and with what.

Plain gRPC stays the default because every client inside the hub talks to the
temporal-frontend Service without TLS; only a worker on another cluster,
coming in through a passthrough Route, sets a CA.
"""

from __future__ import annotations

import pytest

from shared.settings import TemporalSettings
from shared.temporal_client import tls_config

CA_PEM = b"-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"


def _settings(monkeypatch, **env: str) -> TemporalSettings:
    for key in ("TEMPORAL_TLS_CA", "TEMPORAL_TLS_SERVER_NAME"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return TemporalSettings()


def test_no_ca_means_plain_grpc(monkeypatch):
    assert tls_config(_settings(monkeypatch)) is None


def test_a_ca_turns_tls_on_and_verifies_against_it(monkeypatch, tmp_path):
    ca = tmp_path / "ca.crt"
    ca.write_bytes(CA_PEM)
    tls = tls_config(
        _settings(
            monkeypatch,
            TEMPORAL_TLS_CA=str(ca),
            TEMPORAL_TLS_SERVER_NAME="temporal.apps.hub.example",
        )
    )
    assert tls is not None
    assert tls.server_root_ca_cert == CA_PEM
    # The name the Route serves and the certificate carries — also the SNI.
    assert tls.domain == "temporal.apps.hub.example"


def test_without_a_server_name_the_dialled_host_is_verified(monkeypatch, tmp_path):
    ca = tmp_path / "ca.crt"
    ca.write_bytes(CA_PEM)
    tls = tls_config(_settings(monkeypatch, TEMPORAL_TLS_CA=str(ca)))
    assert tls is not None and tls.domain is None


def test_a_server_name_without_a_ca_is_refused_not_sent_in_plain_text(monkeypatch):
    with pytest.raises(ValueError, match="TEMPORAL_TLS_CA"):
        tls_config(
            _settings(monkeypatch, TEMPORAL_TLS_SERVER_NAME="temporal.apps.hub.example")
        )


def test_a_missing_ca_file_fails_at_startup(monkeypatch, tmp_path):
    with pytest.raises(FileNotFoundError):
        tls_config(_settings(monkeypatch, TEMPORAL_TLS_CA=str(tmp_path / "absent.crt")))
