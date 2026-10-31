"""The hub limb's release registry work (activities/upgrade_gitops/registry.py, oc_release.py).

A fake registry behind respx speaks the parts of the v2 API the limb uses,
the way quay.io does (checked 2026-10-31): anonymous tag listing paged by 100
with a relative `Link: rel="next"`, `Docker-Content-Digest` on manifest HEADs,
and a Bearer challenge whose realm hands an anonymous token that the private
payload repository (ocp-v4.0-art-dev) still refuses. `oc adm release info`
is replaced by a function, or by a fake `oc` on PATH to test the subprocess.
No network.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import sys
from pathlib import Path

import httpx
import pytest
import respx

from activities.upgrade_gitops import oc_release, registry
from shared.exceptions import ReleaseRegistryError
from shared.models.cluster_upgrade import ReleaseLayout
from workflow_domains.cluster_upgrade import gates

QUAY = "quay.io"
RELEASE_REPO = "openshift-release-dev/ocp-release"
PAYLOAD_REPO = "openshift-release-dev/ocp-v4.0-art-dev"
PUBLIC = ReleaseLayout(
    image_path="quay.io/openshift-release-dev", repo="{imagePath}/ocp-release", tag="{version}-x86_64", mirrored=False,
)
MIRROR = "mirror.test"
MIRRORED = ReleaseLayout(
    image_path=f"{MIRROR}/openshift-release",
    repo="{imagePath}/openshift/release-images",
    tag="{version}-x86_64",
    cpo_repo="{imagePath}/openshift/release",
    cpo_tag="{version}-x86_64-hypershift",
)
USER, PASSWORD = "puller", "pull-password"


def _digest(text: str) -> str:
    return "sha256:" + text.encode().hex().ljust(64, "0")[:64]


class FakeRegistry:
    """One registry host: repositories of tag -> digest and of digests; who may read what."""

    def __init__(self, host: str) -> None:
        self.host = host
        self.tags: dict[str, dict[str, str]] = {}
        self.digests: dict[str, set[str]] = {}
        self.private: set[str] = set()  # repositories that need USER/PASSWORD
        self.down: set[str] = set()  # repositories answering 503
        self.token_requests: list[httpx.Request] = []
        self.requests: list[httpx.Request] = []

    def add(self, repo: str, tag: str, digest: str) -> None:
        self.tags.setdefault(repo, {})[tag] = digest
        self.digests.setdefault(repo, set()).add(digest)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v2/auth":
            self.token_requests.append(request)
            scope = request.url.params.get("scope", "")
            auth = request.headers.get("authorization", "")
            granted = auth == "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
            return httpx.Response(200, json={"token": f"{'full' if granted else 'anon'}:{scope}"})
        self.requests.append(request)
        name, _, rest = path.removeprefix("/v2/").rpartition("/tags/list") if "/tags/list" in path else ("", "", "")
        if not name:
            name, _, reference = path.removeprefix("/v2/").partition("/manifests/")
        if name in self.down:
            return httpx.Response(503)
        if name in self.private and request.headers.get("authorization", "") != f"Bearer full:repository:{name}:pull":
            return httpx.Response(401, headers={
                "www-authenticate": f'Bearer realm="https://{self.host}/v2/auth",service="{self.host}",'
                                    f'scope="repository:{name}:pull"',
            })
        if "/tags/list" in path:
            if name not in self.tags:
                return httpx.Response(404)
            tags = sorted(self.tags[name])
            last = request.url.params.get("last")
            page = [t for t in tags if last is None or t > last][: int(request.url.params.get("n", "100"))]
            headers = {}
            if page and page[-1] != tags[-1]:
                headers["link"] = f'</v2/{name}/tags/list?n={len(page)}&last={page[-1]}>; rel="next"'
            return httpx.Response(200, json={"name": name, "tags": page}, headers=headers)
        digest = self.tags.get(name, {}).get(reference) or (
            reference if reference in self.digests.get(name, set()) else None
        )
        if digest is None:
            return httpx.Response(404)
        return httpx.Response(200, headers={"docker-content-digest": digest})


@pytest.fixture
def quay():
    fake = FakeRegistry(QUAY)
    with respx.mock(assert_all_called=False) as mock:
        mock.route(host=QUAY).mock(side_effect=fake.handle)
        yield fake


@pytest.fixture
def mirror():
    fake = FakeRegistry(MIRROR)
    with respx.mock(assert_all_called=False) as mock:
        mock.route(host=MIRROR).mock(side_effect=fake.handle)
        yield fake


def _auth_file(tmp_path: Path, host: str = QUAY) -> str:
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"auths": {host: {"auth": base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()}}}))
    return str(path)


def _payload(*digests: str):
    async def release_info(pullspec: str, *, auth_file: str, verify: bool) -> list[str]:
        release_info.calls.append(pullspec)
        return [f"{QUAY}/{PAYLOAD_REPO}@{digest}" for digest in digests]

    release_info.calls = []
    return release_info


class TestTags:
    async def test_every_page_is_followed(self, quay):
        for minor in range(150):
            quay.add(RELEASE_REPO, f"4.{minor // 10}.{minor}-x86_64", _digest(f"r{minor}"))
        tags = await registry.list_tags(layout=PUBLIC, credentials={}, verify=True)
        assert len(tags.tags) == 150 and len(quay.requests) == 2
        assert "last=" in str(quay.requests[1].url)

    async def test_a_repository_that_does_not_exist_has_no_tags(self, quay):
        assert (await registry.list_tags(layout=PUBLIC, credentials={}, verify=True)).tags == []

    async def test_an_outage_is_retryable_not_an_empty_mirror(self, quay):
        quay.down.add(RELEASE_REPO)
        with pytest.raises(ReleaseRegistryError, match="503"):
            await registry.list_tags(layout=PUBLIC, credentials={}, verify=True)

    async def test_refused_credentials_on_the_release_repository_are_retryable(self, quay):
        quay.private.add(RELEASE_REPO)
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        with pytest.raises(ReleaseRegistryError, match="REGISTRY_AUTH_FILE"):
            await registry.list_tags(layout=PUBLIC, credentials={}, verify=True)

    async def test_a_private_repository_is_read_with_the_auth_files_credential(self, quay, tmp_path):
        quay.private.add(RELEASE_REPO)
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        tags = await registry.list_tags(
            layout=PUBLIC, credentials=registry.load_auth_file(_auth_file(tmp_path)), verify=True
        )
        assert tags.tags == ["4.20.35-x86_64"]
        assert quay.token_requests[0].url.params["service"] == QUAY


class TestPinning:
    async def test_a_public_release_pins_with_no_operator_image(self, quay):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("release-4.20.35"))
        quay.add(PAYLOAD_REPO, "-", _digest("p1"))
        info = _payload(_digest("p1"))
        pinned = await registry.pin_releases(
            versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True, release_info=info
        )
        assert pinned.pinned["4.20.35"].release == _digest("release-4.20.35")
        assert pinned.pinned["4.20.35"].control_plane_operator is None
        assert pinned.missing == [] and pinned.problems == []
        # oc reads the release by the digest just pinned, never by its tag.
        assert info.calls == [f"{QUAY}/{RELEASE_REPO}@{_digest('release-4.20.35')}"]

    async def test_a_tag_the_registry_lacks_is_missing_not_an_error(self, quay):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        pinned = await registry.pin_releases(
            versions=["4.20.35", "4.21.40"], layout=PUBLIC, auth_file="", verify=True, release_info=_payload()
        )
        assert list(pinned.pinned) == ["4.20.35"] and pinned.missing == ["4.21.40"]

    async def test_the_same_tag_pins_the_same_digest_every_time(self, quay):
        """Each step re-pins and compares (D2): the answer must not drift between two calls."""
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        first = await registry.pin_releases(versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True,
                                            release_info=_payload())
        second = await registry.pin_releases(versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True,
                                             release_info=_payload())
        assert first.pinned == second.pinned

    async def test_a_payload_image_the_pull_secret_cannot_read_is_a_problem_not_a_raise(self, quay):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        quay.private.add(PAYLOAD_REPO)
        quay.add(PAYLOAD_REPO, "-", _digest("p1"))
        pinned = await registry.pin_releases(
            versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True,
            release_info=_payload(_digest("p1"), _digest("p2")),
        )
        assert "4.20.35" in pinned.pinned
        assert len(pinned.problems) == 1
        assert "2 of 2 payload images cannot be read with the configured credentials" in pinned.problems[0]
        # Refused once, refused for the call: one token request, not one per image.
        assert len(quay.token_requests) == 1

    async def test_with_the_pull_secret_the_payload_is_checked_image_by_image(self, quay, tmp_path):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        quay.private.add(PAYLOAD_REPO)
        quay.add(PAYLOAD_REPO, "-", _digest("p1"))
        pinned = await registry.pin_releases(
            versions=["4.20.35"], layout=PUBLIC, auth_file=_auth_file(tmp_path), verify=True,
            release_info=_payload(_digest("p1"), _digest("p2")),
        )
        assert pinned.problems == [
            f"4.20.35: 1 of 2 payload images are missing (where the payload names them): "
            f"{QUAY}/{PAYLOAD_REPO}@{_digest('p2')}"
        ]

    async def test_oc_failing_is_reported_as_the_releases_problem(self, quay):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))

        async def broken(pullspec: str, *, auth_file: str, verify: bool) -> list[str]:
            raise oc_release.ReleaseInfoError("error: unable to read image: manifest unknown")

        pinned = await registry.pin_releases(versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True,
                                             release_info=broken)
        assert "4.20.35" in pinned.pinned
        assert pinned.problems == [
            f"4.20.35: oc adm release info {QUAY}/{RELEASE_REPO}@{_digest('r')} failed: "
            "error: unable to read image: manifest unknown"
        ]

    async def test_a_mirrored_release_pins_its_operator_and_finds_the_payload_in_the_mirror(self, mirror):
        mirror.add("openshift-release/openshift/release-images", "4.20.44-x86_64", _digest("r"))
        mirror.add("openshift-release/openshift/release", "4.20.44-x86_64-hypershift", _digest("cpo"))
        mirror.add("openshift-release/openshift/release", "-", _digest("p1"))
        pinned = await registry.pin_releases(
            versions=["4.20.44"], layout=MIRRORED, auth_file="", verify=True, release_info=_payload(_digest("p1")),
        )
        assert pinned.pinned["4.20.44"].control_plane_operator == _digest("cpo")
        assert pinned.problems == []

    async def test_a_mirrored_release_without_its_operator_image_is_missing(self, mirror):
        mirror.add("openshift-release/openshift/release-images", "4.20.44-x86_64", _digest("r"))
        pinned = await registry.pin_releases(
            versions=["4.20.44"], layout=MIRRORED, auth_file="", verify=True, release_info=_payload(),
        )
        assert pinned.missing == ["4.20.44"] and "4.20.44" not in pinned.pinned
        assert "control-plane-operator image mirror.test/openshift-release/openshift/release:4.20.44-x86_64-hypershift" \
            in pinned.problems[0]

    async def test_an_outage_mid_payload_is_retryable(self, quay):
        quay.add(RELEASE_REPO, "4.20.35-x86_64", _digest("r"))
        quay.down.add(PAYLOAD_REPO)
        with pytest.raises(ReleaseRegistryError, match="503"):
            await registry.pin_releases(versions=["4.20.35"], layout=PUBLIC, auth_file="", verify=True,
                                        release_info=_payload(_digest("p1")))


class TestPlumbing:
    def test_references_are_split_into_host_and_name(self):
        assert registry.Repository.parse("quay.io/a/b@sha256:x") == registry.Repository("quay.io", "a/b")
        assert registry.Repository.parse("mirror.test:5000/a/b:4.20.35-x86_64") == registry.Repository(
            "mirror.test:5000", "a/b"
        )

    def test_the_template_rule_is_the_gates_rule(self):
        for template in (MIRRORED.repo, MIRRORED.cpo_tag or "", PUBLIC.tag):
            assert registry.expand(template, image_path="x/y", version="4.20.1") == gates.expand(
                template, image_path="x/y", version="4.20.1"
            )

    def test_the_auth_file_gives_the_longest_matching_credential(self, tmp_path):
        path = tmp_path / "auth.json"
        encode = lambda pair: base64.b64encode(pair.encode()).decode()  # noqa: E731
        path.write_text(json.dumps({"auths": {
            "quay.io": {"auth": encode("host:wide")},
            "https://quay.io/openshift-release-dev": {"auth": encode("org:narrow")},
            "registry.test": {"username": "u", "password": "p"},
        }}))
        credentials = registry.load_auth_file(str(path))
        client = registry.RegistryClient(credentials=credentials, verify=True)
        assert client._credential(registry.Repository("quay.io", PAYLOAD_REPO)) == ("org", "narrow")
        assert client._credential(registry.Repository("quay.io", "other/x")) == ("host", "wide")
        assert client._credential(registry.Repository("registry.test", "a")) == ("u", "p")
        assert client._credential(registry.Repository("elsewhere.test", "a")) is None

    def test_an_unreadable_auth_file_names_the_path_and_never_its_content(self, tmp_path):
        path = tmp_path / "auth.json"
        path.write_text('{"auths": {"quay.io": {"auth": "c2VjcmV0"')
        with pytest.raises(ReleaseRegistryError) as caught:
            registry.load_auth_file(str(path))
        assert str(path) in str(caught.value) and "c2VjcmV0" not in str(caught.value)


class TestOc:
    SAMPLE = {
        "image": "quay.io/openshift-release-dev/ocp-release@sha256:aa",
        "references": {"kind": "ImageStream", "spec": {"tags": [
            {"name": "hypershift", "from": {"kind": "DockerImage", "name": f"{QUAY}/{PAYLOAD_REPO}@sha256:01"}},
            {"name": "cli", "from": {"kind": "DockerImage", "name": f"{QUAY}/{PAYLOAD_REPO}@sha256:02"}},
            {"name": "broken"},
        ]}},
    }

    def test_the_references_are_read_from_the_imagestream(self):
        assert oc_release.references_of(self.SAMPLE) == [
            f"{QUAY}/{PAYLOAD_REPO}@sha256:01", f"{QUAY}/{PAYLOAD_REPO}@sha256:02",
        ]

    def _fake_oc(self, tmp_path: Path, monkeypatch, body: str) -> Path:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        log = tmp_path / "oc-calls.txt"
        script = bin_dir / "oc"
        script.write_text(f"#!{sys.executable}\nimport json, os, sys\n"
                          f"open({str(log)!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')\n{body}")
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("DAY1_GIT_TOKEN", "must-not-reach-oc")
        return log

    async def test_oc_runs_on_the_digest_with_the_auth_file_and_no_token(self, tmp_path, monkeypatch):
        log = self._fake_oc(tmp_path, monkeypatch, (
            "assert 'DAY1_GIT_TOKEN' not in os.environ\n"
            f"print(json.dumps({self.SAMPLE!r}))\n"
        ))
        references = await oc_release.release_references(
            "quay.io/openshift-release-dev/ocp-release@sha256:aa", auth_file="/etc/registry-auth/auth.json",
            verify=False,
        )
        assert len(references) == 2
        assert json.loads(log.read_text()) == [
            "adm", "release", "info", "-o", "json", "quay.io/openshift-release-dev/ocp-release@sha256:aa",
            "--registry-config", "/etc/registry-auth/auth.json", "--insecure=true",
        ]

    async def test_oc_is_retried_then_its_error_reported(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oc_release, "_BACKOFF_SECONDS", 0.0)
        log = self._fake_oc(tmp_path, monkeypatch, "sys.stderr.write('error: manifest unknown\\n'); sys.exit(1)\n")
        with pytest.raises(oc_release.ReleaseInfoError, match="manifest unknown"):
            await oc_release.release_references("quay.io/x/y@sha256:aa", auth_file="", verify=True)
        assert len(log.read_text().splitlines()) == oc_release.ATTEMPTS
