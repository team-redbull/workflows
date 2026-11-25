"""Waiting for metal3 to let a deleted BareMetalHost go — never forcing it.

The one policy behind remove-server-from-mce's delete and add-server-to-mce's
rollbacks (the operator, 2026-11-24, after 2026-11-23). metal3 deprovisions a
deleted host — powers it off through its BMC, deletes its ironic node — and
only then releases its `baremetalhost.metal3.io` finalizer. Dropping that
finalizer early orphans the ironic node, whose power sync then forces the
machine to a stale power state every minute: twelve servers nobody could
power on. So both workflows delete with no `detached` annotation and no
finalizer touched (`delete_host_resources`), read the BareMetalHost on a
durable timer until it is gone, and fail loudly at the deadline
(BmhDeletionTimeoutError) instead of forcing it.

Pure and sandbox-safe: the deadline, the poll and the words for metal3's
state. Each workflow makes its own calls, under its own activity options.
Changing a constant here is a non-deterministic change for runs of BOTH.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_lifecycle import BmhState

# Machine convergence, so a real deadline (CLAUDE.md §5): 30-45 s measured with
# a BMC that answers; fifteen minutes covers a slow one, and past it metal3
# cannot reach the BMC.
BMH_DELETION_DEADLINE = timedelta(minutes=15)
BMH_DELETION_POLL_INTERVAL = timedelta(seconds=15)
# metal3's errorMessage goes into a failure as its first characters only.
_METAL3_ERROR_CHARS = 300


def describe_metal3(bmh: BmhState) -> str:
    """metal3's view of a host it has not let go, for the failure: state, status, error."""
    parts = [
        f"{label} {value!r}"
        for label, value in (
            ("state", bmh.provisioning_state),
            ("operationalStatus", bmh.operational_status),
            ("error", (bmh.error_message or "")[:_METAL3_ERROR_CHARS]),
        )
        if value
    ]
    return ", ".join(parts) or "no status reported"
