"""MCE cluster names — one canonical spelling, whatever a caller typed.

Every MCE is named `ocp4-<rest>` (`ocp4-prod-mce-bat-yam`), and callers often
drop the prefix (`prod-mce-bat-yam`). The name is load-bearing: it is the
Segments Manager's `cluster_name` the VLAN is found by, and it builds the
activity queue only that MCE's worker polls. So both spellings are accepted
and turned into the canonical one, here, before anything uses it.

Applied at the API EDGE and to the worker's own MCE_CLUSTER setting — never to
a model decoded from Temporal history, where changing the name of a recorded
run would change the queue it replays against. Pure, so workflow code may
import it too.
"""

from __future__ import annotations

MCE_CLUSTER_PREFIX = "ocp4-"


def canonical_mce_cluster(name: str) -> str:
    """`prod-mce-bat-yam` -> `ocp4-prod-mce-bat-yam`; a prefixed name is unchanged."""
    name = name.strip()
    if name.startswith(MCE_CLUSTER_PREFIX):
        return name
    return f"{MCE_CLUSTER_PREFIX}{name}"
