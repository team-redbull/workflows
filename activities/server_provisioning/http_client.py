"""What this limb's four service modules do IDENTICALLY — and nothing else.

idrac.py, ome.py, server_namer.py and server_scan.py each own one dependency.
What they share is only plumbing: the same per-request budget, the same TLS
trade, and — for the two Dell services — the same error envelope.

What deliberately does NOT live here is each service's error vocabulary: which
statuses mean "the service refused this request" and which exception names that
refusal. Those differ per service (an iDRAC's 405 and OME's 404 are both
refusals; server-scan's 404 is not), and the workflow's
`non_retryable_error_types` is built from exactly those class names, so
collapsing them into one generic classifier would make a retry policy out of a
lowest common denominator.
"""

from __future__ import annotations

import httpx

# Under the activities' 5-minute start_to_close_timeout, so a network hang
# fails the call and frees the worker before Temporal reaps the activity
# (CLAUDE.md §5). Per request, not per activity: an iDRAC walking its storage
# tree makes a dozen of them.
TIMEOUT = httpx.Timeout(60.0)

# Every endpoint this limb speaks to presents a certificate nothing here can
# verify: an iDRAC ships a factory self-signed one, the OME appliance ships its
# own, and this estate runs no CA that would issue either a real one. The same
# trade the other limbs make (see activities/server_lifecycle/server_scan.py).
VERIFY_TLS = False


def extended_info(resp: httpx.Response) -> str:
    """A Dell error body's own explanation of itself.

    The iDRAC and OME share one envelope — `error.@Message.ExtendedInfo[]`,
    each entry carrying a `Message` — because OME's REST API is Redfish
    shaped. Falls back to the body, then to the raw text, so a failure is
    never reported as an empty string.
    """
    try:
        body = resp.json()
    except ValueError:
        return resp.text[:500]
    error = body.get("error", {}) if isinstance(body, dict) else {}
    infos = error.get("@Message.ExtendedInfo") or []
    messages = [str(i.get("Message")) for i in infos if isinstance(i, dict) and i.get("Message")]
    return "; ".join(messages) or str(error.get("message") or body)[:500]
