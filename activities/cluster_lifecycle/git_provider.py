"""The day1 repo's git provider — GitLab in production, GitHub in dev — behind one shape.

Plain parameters, no settings and no Temporal (CLAUDE.md §1): the activity
hands in the repo URL, the provider's name and the token. Each call opens its
own httpx.AsyncClient with a timeout below the activity's start_to_close
(60 s < 90 s), and classifies every answer:

  * 401/403 → GitProviderAuthError (deterministic: the token, or its scope)
  * 400/404/422 → GitProviderRequestRejectedError (deterministic: the request
    as sent — no such project for this token, no `main`, a merge request gone)
  * 409 (a merge request born between the find and the create), a 5xx, an
    unreachable provider, a body that is not what the API documents →
    GitProviderError (transient: the retry finds what was created)

The provider's own message goes into the error BOUNDED (CLAUDE.md §5, AC007).
The API base is derived from the clone URL: GitLab's `/api/v4/projects/<path>`
with the project path URL-encoded, GitHub's `api.github.com/repos/<o>/<r>`
(or `<host>/api/v3` for an enterprise host).

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
GITHUB_API_VERSION = "2022-11-28"

_MAX_DETAIL_CHARS = 500
_IDENTIFIER = re.compile(r"^[0-9]+$")


def _bounded(text: str) -> str:
    """The start of `text` an operator reads, saying how much was cut."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return f"{text[:_MAX_DETAIL_CHARS]}… ({len(text) - _MAX_DETAIL_CHARS} more characters cut)"


def _detail(resp: httpx.Response) -> str:
    """The provider's own error text: GitLab's `message`, GitHub's `message` (+ `errors`)."""
    try:
        body = resp.json()
    except ValueError:
        return _bounded(resp.text)
    if isinstance(body, dict):
        parts = [str(body[key]) for key in ("message", "error", "errors") if body.get(key) is not None]
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
    """GitLab's merge requests API (v4), authenticated with a project or personal token."""

    name = "gitlab"

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
            return MergeRequestState(state="merged", merged_sha=body.get("merge_commit_sha") or body.get("squash_commit_sha") or None)
        if state == "closed":
            return MergeRequestState(state="closed")
        if state in ("opened", "locked"):
            return MergeRequestState(state="open")
        raise GitProviderError(f"merge request !{identifier} reports an unknown state {state!r}")


class GitHubProvider:
    """GitHub's pull requests API, authenticated with a token that has `repo`."""

    name = "github"

    def __init__(self, repo_url: str, token: str) -> None:
        host, path = repo_path(repo_url)
        owner, _, repo = path.partition("/")
        if not owner or not repo or "/" in repo:
            raise GitProviderRequestRejectedError(f"{repo_url!r} is not a GitHub repository URL (https://host/owner/repo)")
        netloc = urlsplit(host).netloc
        api = "https://api.github.com" if netloc == "github.com" else f"{host}/api/v3"
        self._base = f"{api}/repos/{owner}/{repo}"
        self._owner = owner
        self._headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": GITHUB_API_VERSION}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base, headers=self._headers, timeout=HTTP_TIMEOUT_SECONDS, verify=TLS_VERIFY)

    async def find_open(self, branch: str) -> MergeRequestRef | None:
        async with self._client() as client:
            resp = await _get(client, "/pulls", params={"head": f"{self._owner}:{branch}", "base": MAIN, "state": "open"})
        classify(f"find the open pull request from {branch}", resp)
        rows = _rows(resp)
        if not rows:
            return None
        return MergeRequestRef(url=str(rows[0].get("html_url") or ""), identifier=str(rows[0].get("number")), created=False)

    async def create(self, branch: str, title: str, description: str) -> MergeRequestRef:
        payload = {"title": title, "head": branch, "base": MAIN, "body": description}
        async with self._client() as client:
            resp = await _post(client, "/pulls", payload)
        classify(f"create the pull request from {branch}", resp)
        body = _object(resp)
        return MergeRequestRef(url=str(body.get("html_url") or ""), identifier=str(body.get("number")), created=True)

    async def state(self, identifier: str) -> MergeRequestState:
        _require_identifier(identifier)
        async with self._client() as client:
            resp = await _get(client, f"/pulls/{identifier}")
        classify(f"read pull request #{identifier}", resp)
        body = _object(resp)
        if body.get("merged") is True:
            return MergeRequestState(state="merged", merged_sha=body.get("merge_commit_sha") or None)
        state = str(body.get("state") or "")
        if state == "open":
            return MergeRequestState(state="open")
        if state == "closed":
            return MergeRequestState(state="closed")
        raise GitProviderError(f"pull request #{identifier} reports an unknown state {state!r}")


Provider = GitLabProvider | GitHubProvider

_PROVIDERS: dict[str, type[GitLabProvider] | type[GitHubProvider]] = {
    GitLabProvider.name: GitLabProvider,
    GitHubProvider.name: GitHubProvider,
}


def provider_for(name: str, repo_url: str, token: str) -> Provider:
    """The provider class DAY1_GIT_PROVIDER names, bound to the repo and the token."""
    try:
        cls = _PROVIDERS[name]
    except KeyError:
        raise GitProviderRequestRejectedError(f"unknown git provider {name!r}; one of {sorted(_PROVIDERS)}") from None
    return cls(repo_url, token)


async def ensure_merge_request(provider: Provider, branch: str, title: str, description: str) -> MergeRequestRef:
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
