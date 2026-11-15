"""The shortcuts service (go-links): a hosted cluster's console shortcut.

Plain parameters, no settings and no Temporal (CLAUDE.md §1): the activity
hands in the service's base URL, its bearer token, the owner every shortcut is
created with, the name and the URL. Two endpoints:

  * `GET  <url>/check/<name>` → `{"available": bool, "owner": str | null}`
  * `POST <url>/create` `{"shortcut", "username", "url"}` → a list holding the
    created shortcut (`name`, `url`, `username`, `id`, `hits`)

`ensure` is find-before-create, then a READ-BACK (CLAUDE.md §6): the check is
asked again after the create, and only `available: false` with OUR owner is
success. A name taken by our owner is a re-run or a retry → success, nothing
posted (the check does not report the URL, so it cannot be compared). Taken by
anyone else → ShortcutTakenError, the one deterministic answer. EVERY other
failure — unreachable, any non-2xx including 401/403/400/422, a body that is
not what the API documents — is the retryable ShortcutsApiError: the run is
past its merge and cannot be re-run, so it waits until the token or the config
is fixed (the operator, 2026-11-15). A lost race on create needs no special
case: the retry's check finds the name and judges its owner.

Each call opens its own httpx.AsyncClient with a timeout well below the
activity's start_to_close (three calls × 20 s < 90 s). The service's own text
goes into an error BOUNDED (AC007), and the token never does. TLS verification
is OFF, as for GitLab and the Segments Manager: the air-gapped services serve
an internal CA the image does not carry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from shared.exceptions import ShortcutsApiError, ShortcutTakenError
from shared.models.cluster_lifecycle import ClusterShortcut

HTTP_TIMEOUT_SECONDS = 20.0
TLS_VERIFY = False

_MAX_DETAIL_CHARS = 500


def _bounded(text: str) -> str:
    """The start of `text` an operator reads, saying how much was cut."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return f"{text[:_MAX_DETAIL_CHARS]}… ({len(text) - _MAX_DETAIL_CHARS} more characters cut)"


@dataclass(frozen=True)
class Availability:
    available: bool
    owner: str | None


async def ensure(*, base_url: str, token: str, owner: str, shortcut: str, url: str) -> ClusterShortcut:
    """The shortcut `shortcut` → `url`, owned by `owner`: found, else created and read back."""
    found = await check(base_url=base_url, token=token, shortcut=shortcut)
    if not found.available:
        _require_ours(found, owner, shortcut)
        return ClusterShortcut(shortcut=shortcut, url=url, owner=owner, created=False)

    await create(base_url=base_url, token=token, owner=owner, shortcut=shortcut, url=url)

    after = await check(base_url=base_url, token=token, shortcut=shortcut)
    if after.available:
        raise ShortcutsApiError(
            f"created shortcut {shortcut!r}, yet the service's check still reports it available — "
            "not trusted until the check confirms it; retrying"
        )
    _require_ours(after, owner, shortcut)
    return ClusterShortcut(shortcut=shortcut, url=url, owner=owner, created=True)


async def check(*, base_url: str, token: str, shortcut: str) -> Availability:
    async with _client(base_url, token) as client:
        resp = await _send(client, "GET", f"/check/{quote(shortcut, safe='')}")
    _classify(f"check shortcut {shortcut!r}", resp)
    body = _json(resp)
    if not isinstance(body, dict) or not isinstance(body.get("available"), bool):
        raise ShortcutsApiError(
            f"check shortcut {shortcut!r}: the service answered {_bounded(resp.text)!r}, not "
            "{\"available\": bool, \"owner\": ...}"
        )
    owner = body.get("owner")
    if not body["available"] and not (isinstance(owner, str) and owner):
        raise ShortcutsApiError(f"check shortcut {shortcut!r}: taken, but the service names no owner: {_bounded(resp.text)!r}")
    return Availability(available=body["available"], owner=owner if isinstance(owner, str) else None)


async def create(*, base_url: str, token: str, owner: str, shortcut: str, url: str) -> None:
    """POST the shortcut. Its answer is not parsed for success: the read-back check is the proof."""
    async with _client(base_url, token) as client:
        resp = await _send(client, "POST", "/create", json={"shortcut": shortcut, "username": owner, "url": url})
    _classify(f"create shortcut {shortcut!r} → {url}", resp)


# --- plumbing ------------------------------------------------------------------------------


def _require_ours(found: Availability, owner: str, shortcut: str) -> None:
    if found.owner != owner:
        raise ShortcutTakenError(
            f"shortcut {shortcut!r} is taken by {found.owner!r}, not by SHORTCUT_OWNER {owner!r}: it may "
            "point anywhere, so it is not pointed at this cluster. Remove it in the shortcuts service (or "
            "keep it) — the merged cluster and its day2 folders stand"
        )


def _client(base_url: str, token: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=HTTP_TIMEOUT_SECONDS,
        verify=TLS_VERIFY,
    )


async def _send(client: httpx.AsyncClient, method: str, path: str, json: dict[str, Any] | None = None) -> httpx.Response:
    try:
        return await client.request(method, path, json=json)
    except httpx.HTTPError as err:
        raise ShortcutsApiError(
            f"{method} {path} on the shortcuts service failed: {type(err).__name__}: {_bounded(str(err))}"
        ) from err


def _classify(action: str, resp: httpx.Response) -> None:
    """A 2xx returns; anything else is the retryable ShortcutsApiError, the service's words bounded."""
    if 200 <= resp.status_code < 300:
        return
    hint = " — check SHORTCUTS_API_TOKEN" if resp.status_code in (401, 403) else ""
    raise ShortcutsApiError(f"{action}: the shortcuts service answered {resp.status_code}{hint}: {_bounded(resp.text)}")


def _json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        raise ShortcutsApiError(
            f"the shortcuts service answered {resp.status_code} with a body that is not JSON: {_bounded(resp.text)}"
        ) from None
