"""Test environment bootstrap.

activities/segment_connectivity/activities.py instantiates SegmentConnectivityActivitySettings
at import time (fail-fast by design), so the full activity config must be in
the environment BEFORE any test module imports it. Real env vars take
precedence over the repo's .env, keeping tests deterministic everywhere.
"""

from __future__ import annotations

import os

os.environ.update(
    {
        "TEMPORAL_HOST": "localhost:7233",
        "SEGMENTS_MANAGER_URL": "http://segments-manager.test",
        "SEGMENTS_MANAGER_API_TOKEN": "test-token",
        "DOMAIN": "test-domain",
        "NEXT_URL": "http://next.test",
        "NEXT_CHECKING_REQUEST_INTERVAL_SECONDS": "15",
        "PORTS_HC_TO_MCE": '{"tcp": ["30000-32767"], "udp": ["9000"]}',
        "PORTS_MCE_TO_HC": '{"tcp": ["6443", "30000-32767"]}',
        "PORTS_INVENTORY_TO_MCE": '{"tcp": ["30000-32767"]}',
        "PORTS_MCE_TO_INVENTORY": '{"tcp": ["6443"]}',
        "PORTS_PXE_TO_MCE": '{"udp": ["69"]}',
        "PORTS_MCE_TO_PXE": '{"tcp": ["6443"]}',
    }
)
