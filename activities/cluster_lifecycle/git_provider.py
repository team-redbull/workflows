"""The day1 repo's git provider: GitLab, its merge requests API (v4). The only one.

Plain parameters, no settings and no Temporal (CLAUDE.md §1): the activity
hands in the repo URL and the token. Each call opens its
own httpx.AsyncClient with a timeout below the activity's start_to_close
(60 s < 90 s), and classifies every answer:

  * 401/403 → GitProviderAuthError (deterministic: the token, or its scope)
  * 400/404/422 → GitProviderRequestRejectedError (deterministic: the request
    as sent — no such project for this token, no `main`, a merge request gone)
  * 409 (a merge request born between the find and the create), a 5xx, an
    unreachable provider, a body that is not what the API documents →
    GitProviderError (transient: the retry finds what was created)

The provider's own message goes into the error BOUNDED (CLAUDE.md §5, AC007).
The API base is derived from the clone URL: `<host>/api/v4/projects/<path>`,
the project's full path — every subgroup included — URL-encoded into ONE
segment (`redbull%2Fgitops-day1%2Fplatform-config`), as GitLab's API takes it.
The clone URL's host is taken as GitLab's root: an instance served under a
relative URL root (`https://host/gitlab/...`) is not supported.

TLS verification is OFF, as for the Segments Manager client: the air-gapped
GitLab serves an internal CA the image cannot carry, and git already runs
with http.sslVerify=false against the same host.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from shared.exceptions import GitProviderAuthError, GitProviderError, GitProviderRequestRejectedError
from shared.models.cluster_lifecycle import MergeRequestRef, MergeRequestState

HTTP_TIMEOUT_SECONDS = 60.0
TLS_VERIFY = False
MAIN = "main"

_MAX_DETAIL_CHARS = 500
_IDENTIFIER = re.compile(r"^[0-9]+$")


def _bounded(text: str) -> str:
    """The start of `text` an operator reads, saying how much was cut."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return f"{text[:_MAX_DETAIL_CHARS]}… ({len(text) - _MAX_DETAIL_CHARS} more characters cut)"


def _detail(resp: httpx.Response) -> str:
    """GitLab's own error text: `message` (a string, a list or a map of field errors) or `error`."""
    try:
        body = resp.json()
    except ValueError:
        return _bounded(resp.text)
    if isinstance(body, dict):
        parts = [str(body[key]) for key in ("message", "error", "error_description") if body.get(key) is not None]
        if parts:
            return _bounded(" ".join(parts))
    return _bounded(resp.text)


def classify(action: str, resp: httpx.Response) -> None:
    """Raise the error class a non-2xx answer means; a 2xx returns."""
    if 200 <= resp.status_code < 300:
        return
    detail = _detail(resp)
    if resp.status_code in (401, 403):
        raise GitProviderAuthError(f"{action}: the git provider rejected the token ({resp.status_code}): {detail}")
    if resp.status_code in (400, 404, 422):
        raise GitProviderRequestRejectedError(f"{action}: the git provider refused the request ({resp.status_code}): {detail}")
    raise GitProviderError(f"{action}: the git provider answered {resp.status_code}: {detail}")


def repo_path(repo_url: str) -> tuple[str, str]:
    """(scheme://host, `group/sub/project`) from a clone URL, `.git` stripped."""
    parts = urlsplit(repo_url)
    path = parts.path.strip("/").removesuffix(".git")
    if not parts.scheme or not parts.netloc or "/" not in path:
        raise GitProviderRequestRejectedError(f"{repo_url!r} is not a clone URL of the form https://host/owner/repo(.git)")
    return f"{parts.scheme}://{parts.netloc}", path


@dataclass(frozen=True)
class _Found:
    url: str
    identifier: str


class GitLabProvider:
    """One GitLab project's merge requests, authenticated with a group, project or personal token."""

    def __init__(self, repo_url: str, token: str) -> None:
        host, project = repo_path(repo_url)
        self._base = f"{host}/api/v4/projects/{quote(project, safe='')}"
        self._headers = {"PRIVATE-TOKEN": token} if token else {}

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base, headers=self._headers, timeout=HTTP_TIMEOUT_SECONDS, verify=TLS_VERIFY)

    async def find_open(self, branch: str) -> MergeRequestRef | None:
        async with self._client() as client:
            resp = await _get(client, "/merge_requests", params={"source_branch": branch, "target_branch": MAIN, "state": "opened"})
        classify(f"find the open merge request from {branch}", resp)
        rows = _rows(resp)
        if not rows:
            return None
        return MergeRequestRef(url=str(rows[0].get("web_url") or ""), identifier=str(rows[0].get("iid")), created=False)

    async def create(self, branch: str, title: str, description: str) -> MergeRequestRef:
        payload = {
            "source_branch": branch, "target_branch": MAIN, "title": title, "description": description,
            "remove_source_branch": True,
        }
        async with self._client() as client:
            resp = await _post(client, "/merge_requests", payload)
        classify(f"create the merge request from {branch}", resp)
        body = _object(resp)
        return MergeRequestRef(url=str(body.get("web_url") or ""), identifier=str(body.get("iid")), created=True)

    async def state(self, identifier: str) -> MergeRequestState:
        _require_identifier(identifier)
        async with self._client() as client:
            resp = await _get(client, f"/merge_requests/{identifier}")
        classify(f"read merge request !{identifier}", resp)
        body = _object(resp)
        state = str(body.get("state") or "")
        if state == "merged":
            # What landed on `main`, per the project's merge method: the merge commit,
            # else the squash commit, else — a FAST-FORWARD merge makes neither — the
            # merge request's own head, which is then main's commit (verified on
            # GitLab 19.5: `merge_commit_sha` and `squash_commit_sha` both null there).
            merged_sha = body.get("merge_commit_sha") or body.get("squash_commit_sha") or body.get("sha") or None
            return MergeRequestState(state="merged", merged_sha=merged_sha)
        if state == "closed":
            return MergeRequestState(state="closed")
        if state in ("opened", "locked"):
            return MergeRequestState(state="open")
        raise GitProviderError(f"merge request !{identifier} reports an unknown state {state!r}")


async def ensure_merge_request(provider: GitLabProvider, branch: str, title: str, description: str) -> MergeRequestRef:
    """The open merge request from `branch`: found, else created. Idempotent by find-before-create."""
    found = await provider.find_open(branch)
    if found is not None:
        return found
    return await provider.create(branch, title, description)


# --- plumbing ------------------------------------------------------------------------------


def _require_identifier(identifier: str) -> None:
    if not _IDENTIFIER.fullmatch(identifier):
        raise GitProviderRequestRejectedError(f"merge request identifier {identifier!r} is not a number")


async def _get(client: httpx.AsyncClient, path: str, params: dict[str, str] | None = None) -> httpx.Response:
    try:
        return await client.get(path, params=params)
    except httpx.HTTPError as err:
        raise GitProviderError(f"GET {path} on the git provider failed: {type(err).__name__}: {_bounded(str(err))}") from err


async def _post(client: httpx.AsyncClient, path: str, payload: dict[str, Any]) -> httpx.Response:
    try:
        return await client.post(path, json=payload)
    except httpx.HTTPError as err:
        raise GitProviderError(f"POST {path} on the git provider failed: {type(err).__name__}: {_bounded(str(err))}") from err


def _rows(resp: httpx.Response) -> list[dict[str, Any]]:
    try:
        body = resp.json()
    except ValueError:
        raise GitProviderError(f"the git provider answered {resp.status_code} with a body that is not JSON: {_bounded(resp.text)}") from None
    if not isinstance(body, list) or not all(isinstance(row, dict) for row in body):
        raise GitProviderError(f"the git provider answered a {type(body).__name__}, not a list of merge requests")
    return body


def _object(resp: httpx.Response) -> dict[str, Any]:
    try:
        body = resp.json()
    except ValueError:
        raise GitProviderError(f"the git provider answered {resp.status_code} with a body that is not JSON: {_bounded(resp.text)}") from None
    if not isinstance(body, dict):
        raise GitProviderError(f"the git provider answered a {type(body).__name__}, not a merge request")
    return body
