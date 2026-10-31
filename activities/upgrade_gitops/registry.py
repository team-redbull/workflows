"""The release registry, over the Docker Registry HTTP API v2 — tags, digests, payload completeness.

Plain parameters, no settings and no Temporal (CLAUDE.md §1). One
`RegistryClient` per activity invocation (`async with`), so tokens never
outlive the call that fetched them (CLAUDE.md §5).

  * Auth is the registry's own challenge: a 401 `WWW-Authenticate: Bearer
    realm=…,service=…,scope=…` is answered from the realm, with the auth file's
    credential for that registry when it has one and anonymously otherwise
    (quay.io's public ocp-release allows it; its ocp-v4.0-art-dev payload
    repository does not); a `Basic` challenge gets the credential directly.
    The auth file is the `.dockerconfigjson` shape: `auths.<host[/path]>.auth`
    = base64 `user:password`, the longest matching key winning.
  * Tags: `GET /v2/<name>/tags/list`, following `Link: <…>; rel="next"`
    (quay.io returns at most 100 a page).
  * Digests: `HEAD /v2/<name>/manifests/<tag>` with every manifest media type
    accepted, read from `Docker-Content-Digest` (a GET, hashed, when absent).
  * The payload: `oc adm release info` on the pinned digest (oc_release.py)
    lists every image the release references; each is HEADed by digest. For
    a mirrored layout (`release.mirrored`) the image is looked for where the
    mirror keeps payload images — the release repository and the CPO
    repository, the two mirrors day1's `defaultImageContentSources` gives
    `ocp-v4.0-art-dev` (configValues.yaml) — instead of where the payload
    names it, which an air gap cannot reach.

Absent is reported, never raised (PinnedReleases.missing / .problems). What
cannot be read at all — the registry unreachable, a 5xx, a 429, credentials
refused on the release repository itself, or `oc` failing for any reason but
an absent image (oc_release.py) — raises the retryable ReleaseRegistryError,
so a run waits out an outage or a credential fix instead of reading it as
"not mirrored". The first such failure cancels the plan's other work in
flight (asyncio.TaskGroup, never gather), so no oc process outlives it.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import httpx

from activities.upgrade_gitops import oc_release
from shared import upgrade_layout
from shared.exceptions import ReleaseRegistryError
from shared.models.cluster_upgrade import PinnedRelease, PinnedReleases, ReleaseLayout, ReleaseTags
from shared.upgrade_layout import expand

# Below the registry activities' timeouts (dispatch.REGISTRY_TIMEOUT is 10 min,
# for a whole plan): one request never holds the worker longer than this.
HTTP_TIMEOUT = httpx.Timeout(60.0)
# Every manifest type a release, a CPO image or a payload image may be stored as.
MANIFEST_ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])
_PAGE_SIZE = 100
# How many releases are pinned at once, and how many payload HEADs fly at once.
_RELEASES_AT_ONCE = 3
_HEADS_AT_ONCE = 16
# The whole plan's budget: the registry activities' start_to_close_timeout,
# dispatch.REGISTRY_TIMEOUT (a test keeps the two equal).
REGISTRY_BUDGET_SECONDS = 600.0
# oc may use at most this share of it, over the rounds of oc that run one
# after another (_RELEASES_AT_ONCE at a time); the HEADs keep the rest.
_OC_SHARE = 0.5
# How many digests a problem line names before "…".
_EXAMPLES = 3
_CHALLENGE_PARAM = re.compile(r'(\w+)="([^"]*)"')
_NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?')


@dataclass(frozen=True)
class Repository:
    """`host/name`, the way a reference names it."""

    host: str
    name: str

    @classmethod
    def parse(cls, reference: str) -> Repository:
        """`host/name` of a reference, with any `:tag` or `@digest` dropped."""
        without_digest = reference.split("@", 1)[0]
        host, _, rest = without_digest.partition("/")
        if not host or not rest:
            raise ValueError(f"not a registry reference: {reference!r}")
        last = rest.rsplit("/", 1)[-1]
        if ":" in last:
            rest = rest[: len(rest) - len(last)] + last.split(":", 1)[0]
        return cls(host=host, name=rest)

    def __str__(self) -> str:
        return f"{self.host}/{self.name}"


def load_auth_file(path: str) -> dict[str, tuple[str, str]]:
    """`host[/path]` -> (user, password) from a `.dockerconfigjson`-shaped file; {} for no file."""
    if not path:
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as err:
        # The path is configuration, the content a credential: name only the first.
        raise ReleaseRegistryError(f"the registry auth file {path} cannot be read as JSON ({type(err).__name__})") from None
    credentials: dict[str, tuple[str, str]] = {}
    for key, entry in ((data.get("auths") or {}) if isinstance(data, dict) else {}).items():
        if not isinstance(entry, dict):
            continue
        user, password = entry.get("username"), entry.get("password")
        if entry.get("auth"):
            try:
                user, _, password = base64.b64decode(entry["auth"]).decode().partition(":")
            except (binascii.Error, UnicodeDecodeError):
                continue
        if user is not None and password is not None:
            normalised = re.sub(r"^https?://", "", str(key)).rstrip("/")
            credentials[normalised] = (str(user), str(password))
    return credentials


class RegistryDeniedError(Exception):
    """The registry refused this repository with the credentials at hand (401/403)."""


class RegistryClient:
    """One invocation's view of the registries: credentials, cached tokens, one HTTP client."""

    def __init__(self, *, credentials: dict[str, tuple[str, str]], verify: bool) -> None:
        self._credentials = credentials
        self._client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, verify=verify, follow_redirects=True)
        self._authorization: dict[Repository, str] = {}
        # Refused once, refused for the whole invocation: the credentials do
        # not change under it. And the first request to a repository settles
        # how to authorize there before the others go, so 189 concurrent
        # payload HEADs ask the token endpoint once, not once each.
        self._denied: set[Repository] = set()
        self._settled: set[Repository] = set()
        self._locks: dict[Repository, asyncio.Lock] = {}

    async def __aenter__(self) -> RegistryClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    def _credential(self, repository: Repository) -> tuple[str, str] | None:
        full = str(repository)
        keys = [key for key in self._credentials if full == key or full.startswith(key + "/") or key == repository.host]
        return self._credentials[max(keys, key=len)] if keys else None

    async def _request(self, method: str, repository: Repository, url: str, *, accept: str | None = None) -> httpx.Response:
        if repository not in self._settled:
            async with self._locks.setdefault(repository, asyncio.Lock()):
                if repository not in self._settled:
                    try:
                        return await self._send(method, repository, url, accept)
                    finally:
                        self._settled.add(repository)
        return await self._send(method, repository, url, accept)

    async def _send(self, method: str, repository: Repository, url: str, accept: str | None) -> httpx.Response:
        headers = {"Accept": accept} if accept else {}
        if repository in self._denied:
            raise RegistryDeniedError(f"{repository}: refused earlier in this call")
        try:
            if repository in self._authorization:
                headers["Authorization"] = self._authorization[repository]
            response = await self._client.request(method, url, headers=headers)
            if response.status_code == 401:
                authorization = await self._authorize(repository, response.headers.get("www-authenticate", ""))
                if authorization is not None:
                    self._authorization[repository] = authorization
                    response = await self._client.request(method, url, headers={**headers, "Authorization": authorization})
        except httpx.HTTPError as err:
            raise ReleaseRegistryError(f"{method} {url} failed: {type(err).__name__}: {err}") from None
        if response.status_code in (401, 403):
            self._denied.add(repository)
            raise RegistryDeniedError(f"{repository}: {response.status_code}")
        if response.status_code == 429 or response.status_code >= 500:
            raise ReleaseRegistryError(f"{method} {url}: the registry answered {response.status_code}")
        return response

    async def _authorize(self, repository: Repository, challenge: str) -> str | None:
        scheme, _, params = challenge.partition(" ")
        credential = self._credential(repository)
        if scheme.lower() == "basic":
            if credential is None:
                return None
            return "Basic " + base64.b64encode(f"{credential[0]}:{credential[1]}".encode()).decode()
        if scheme.lower() != "bearer":
            return None
        fields = dict(_CHALLENGE_PARAM.findall(params))
        realm = fields.get("realm")
        if not realm:
            return None
        query = {"scope": fields.get("scope") or f"repository:{repository.name}:pull"}
        if fields.get("service"):
            query["service"] = fields["service"]
        try:
            response = await self._client.get(realm, params=query, auth=credential)
        except httpx.HTTPError as err:
            raise ReleaseRegistryError(f"the token endpoint {realm} failed: {type(err).__name__}") from None
        if response.status_code in (401, 403):
            self._denied.add(repository)
            raise RegistryDeniedError(f"{repository}: the token endpoint answered {response.status_code}")
        if response.status_code != 200:
            raise ReleaseRegistryError(f"the token endpoint {realm} answered {response.status_code}")
        body = response.json()
        token = body.get("token") or body.get("access_token")
        return f"Bearer {token}" if token else None

    async def tags(self, repository: Repository) -> list[str]:
        """Every tag of the repository; [] when it does not exist."""
        url = f"https://{repository.host}/v2/{repository.name}/tags/list?n={_PAGE_SIZE}"
        found: list[str] = []
        while url:
            response = await self._request("GET", repository, url)
            if response.status_code == 404:
                return found
            if response.status_code != 200:
                raise ReleaseRegistryError(f"listing tags of {repository}: {response.status_code}")
            found += response.json().get("tags") or []
            link = _NEXT_LINK.search(response.headers.get("link", ""))
            url = urljoin(url, link.group(1)) if link else ""
        return found

    async def digest(self, repository: Repository, reference: str) -> str | None:
        """The manifest digest of a tag (or the digest itself, confirmed); None when absent."""
        url = f"https://{repository.host}/v2/{repository.name}/manifests/{reference}"
        response = await self._request("HEAD", repository, url, accept=MANIFEST_ACCEPT)
        if response.status_code == 200 and response.headers.get("docker-content-digest"):
            return response.headers["docker-content-digest"]
        if response.status_code == 200:
            body = await self._request("GET", repository, url, accept=MANIFEST_ACCEPT)
            return "sha256:" + hashlib.sha256(body.content).hexdigest()
        if 400 <= response.status_code < 500:
            return None
        raise ReleaseRegistryError(f"HEAD {url}: {response.status_code}")


# --- the activities' work ----------------------------------------------------------


def release_repository(layout: ReleaseLayout) -> Repository:
    return Repository.parse(upgrade_layout.release_repository(layout))


def cpo_repository(layout: ReleaseLayout) -> Repository | None:
    name = upgrade_layout.cpo_repository(layout)
    return None if name is None else Repository.parse(name)


async def list_tags(*, layout: ReleaseLayout, auth_file: str, verify: bool) -> ReleaseTags:
    repository = release_repository(layout)
    async with RegistryClient(credentials=load_auth_file(auth_file), verify=verify) as client:
        try:
            return ReleaseTags(tags=await client.tags(repository))
        except RegistryDeniedError:
            raise _denied(repository) from None


def _denied(repository: Repository) -> ReleaseRegistryError:
    return ReleaseRegistryError(
        f"{repository} refused the configured credentials: check the repository name and that "
        "REGISTRY_AUTH_FILE holds a credential for it"
    )


def oc_timeout(versions: int) -> float:
    """How long each `oc adm release info` of a plan pinning `versions` releases may take.

    ceil(versions / _RELEASES_AT_ONCE) rounds of oc run one after another, so
    oc can never take more than _OC_SHARE of REGISTRY_BUDGET_SECONDS, however
    long the plan.
    """
    rounds = max(1, math.ceil(versions / _RELEASES_AT_ONCE))
    return min(oc_release.OC_TIMEOUT_SECONDS, REGISTRY_BUDGET_SECONDS * _OC_SHARE / rounds)


async def _each(coroutines) -> list:
    """Await every coroutine, results in order; the first failure CANCELS the rest.

    asyncio.gather would leave the others running — an oc process each — past
    the failure. The failure is raised as itself, not as the TaskGroup's
    ExceptionGroup, so Temporal records its type (ReleaseRegistryError).
    """
    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(coroutine) for coroutine in coroutines]
    except BaseExceptionGroup as failed:
        error: BaseException = failed
        while isinstance(error, BaseExceptionGroup):
            error = error.exceptions[0]
        raise error from None
    return [task.result() for task in tasks]


async def pin_releases(
    *,
    versions: list[str],
    layout: ReleaseLayout,
    auth_file: str,
    verify: bool,
    release_info=oc_release.release_references,
) -> PinnedReleases:
    """Each release (and its CPO image, when the layout names one) to a digest, its payload checked."""
    credentials = load_auth_file(auth_file)
    result = PinnedReleases()
    heads = asyncio.Semaphore(_HEADS_AT_ONCE)
    releases = asyncio.Semaphore(_RELEASES_AT_ONCE)
    timeout = oc_timeout(len(versions))
    async with RegistryClient(credentials=credentials, verify=verify) as client:

        async def pin(version: str) -> tuple[PinnedRelease | None, list[str], bool]:
            async with releases:
                return await _pin_one(
                    client, version, layout, heads=heads, auth_file=auth_file, verify=verify,
                    release_info=release_info, oc_seconds=timeout,
                )

        outcomes = await _each(pin(version) for version in versions)
    for version, (pinned, problems, missing) in zip(versions, outcomes):
        if pinned is not None:
            result.pinned[version] = pinned
        if missing:
            result.missing.append(version)
        result.problems += problems
    return result


async def _pin_one(
    client: RegistryClient,
    version: str,
    layout: ReleaseLayout,
    *,
    heads: asyncio.Semaphore,
    auth_file: str,
    verify: bool,
    release_info,
    oc_seconds: float,
) -> tuple[PinnedRelease | None, list[str], bool]:
    """(the pin or None, problem lines, whether a tag is missing) for one release."""
    repository = release_repository(layout)
    tag = expand(layout.tag, image_path=layout.image_path, version=version)
    try:
        release_digest = await client.digest(repository, tag)
    except RegistryDeniedError:
        raise _denied(repository) from None
    if release_digest is None:
        return None, [], True
    operator_digest: str | None = None
    operator = cpo_repository(layout)
    if operator is not None:
        operator_tag = expand(layout.cpo_tag or "", image_path=layout.image_path, version=version)
        try:
            operator_digest = await client.digest(operator, operator_tag)
        except RegistryDeniedError:
            raise _denied(operator) from None
        if operator_digest is None:
            return None, [f"{version}: the control-plane-operator image {operator}:{operator_tag} is missing"], True
    problems = await _payload_problems(
        client, version, f"{repository}@{release_digest}", layout,
        heads=heads, auth_file=auth_file, verify=verify, release_info=release_info, oc_seconds=oc_seconds,
    )
    return PinnedRelease(release=release_digest, control_plane_operator=operator_digest), problems, False


async def _payload_problems(
    client: RegistryClient,
    version: str,
    pullspec: str,
    layout: ReleaseLayout,
    *,
    heads: asyncio.Semaphore,
    auth_file: str,
    verify: bool,
    release_info,
    oc_seconds: float,
) -> list[str]:
    try:
        references = await release_info(pullspec, auth_file=auth_file, verify=verify, timeout=oc_seconds)
    except oc_release.ReleaseInfoError as err:
        # Only an image the registry says is absent; any other oc failure is
        # the ReleaseRegistryError that propagates, retried.
        return [f"{version}: oc adm release info {pullspec} failed: {err}"]
    mirrors = [repo for repo in (release_repository(layout), cpo_repository(layout)) if repo is not None]

    async def check(reference: str) -> str:
        """'ok', 'missing' or 'denied' for one payload image."""
        digest = reference.split("@", 1)[1] if "@" in reference else None
        candidates = mirrors if layout.mirrored and digest else [Repository.parse(reference)]
        denied = False
        for repository in candidates:
            async with heads:
                try:
                    if await client.digest(repository, digest or reference.rsplit(":", 1)[-1]) is not None:
                        return "ok"
                except RegistryDeniedError:
                    denied = True
        return "denied" if denied else "missing"

    unique = sorted(set(references))
    states = await _each(check(reference) for reference in unique)
    by_state: dict[str, list[str]] = {}
    for reference, state in zip(unique, states):
        by_state.setdefault(state, []).append(reference)
    problems = []
    where = ", ".join(str(repo) for repo in mirrors) if layout.mirrored else "where the payload names them"
    if by_state.get("missing"):
        problems.append(
            f"{version}: {len(by_state['missing'])} of {len(unique)} payload images are missing ({where}): "
            + _examples(by_state["missing"])
        )
    if by_state.get("denied"):
        problems.append(
            f"{version}: {len(by_state['denied'])} of {len(unique)} payload images cannot be read with the "
            f"configured credentials ({where}; REGISTRY_AUTH_FILE needs a pull secret for them): "
            + _examples(by_state["denied"])
        )
    return problems


def _examples(references: list[str]) -> str:
    shown = ", ".join(references[:_EXAMPLES])
    return shown + (", …" if len(references) > _EXAMPLES else "")
