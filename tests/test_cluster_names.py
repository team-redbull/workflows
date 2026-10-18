"""One canonical MCE name, with or without the `ocp4-` prefix a caller typed.

The name finds the VLAN in the Segments Manager and builds the queue only that
MCE's worker polls, so the API and the worker must canonicalise it the same way.
"""

from __future__ import annotations

import pytest

from shared.cluster_names import canonical_mce_cluster
from shared.settings import ServerLifecycleActivitySettings


@pytest.mark.parametrize(
    ("typed", "canonical"),
    [
        ("prod-mce-bat-yam", "ocp4-prod-mce-bat-yam"),
        ("ocp4-prod-mce-bat-yam", "ocp4-prod-mce-bat-yam"),
        ("  prod-mce-bat-yam  ", "ocp4-prod-mce-bat-yam"),
    ],
)
def test_both_spellings_meet_on_one_name(typed, canonical):
    assert canonical_mce_cluster(typed) == canonical


def test_canonicalising_twice_changes_nothing():
    once = canonical_mce_cluster("prod-mce-bat-yam")
    assert canonical_mce_cluster(once) == once


def test_the_worker_setting_is_canonicalised_too(monkeypatch):
    # A worker configured without the prefix must still poll the queue the
    # API's runs dispatch to.
    monkeypatch.setenv("MCE_CLUSTER", "prod-mce-bat-yam")
    assert ServerLifecycleActivitySettings().mce_cluster == "ocp4-prod-mce-bat-yam"
