"""scripts/release_notes.py — what one run of build.yml shipped, image by image.

The notes are for whoever mirrors the images into the air-gap, so what is pinned here:

  * every image is in the notes, rebuilt or not, with the version it is at after the
    run; a rebuilt one with the version it replaces, its digest and its commits;
  * a release lists what changed since the PREVIOUS release, read from the comment
    each release's notes carry; the first one is a baseline: every image unchanged at
    its current version, the whole set to pull, published although nothing was built;
  * an image the run was asked to build whose version did not move is NOT RELEASED: a
    failed build is listed at the version it stays at;
  * which images were built is the `changes` job's answer, and a commit is attributed to
    an image by scripts/image_changes.py's own rule, never by a second one here;
  * build.yml's two jobs: the notes job reads, never writes, and never fails the run;
    the release job writes, on main only, and runs no code of the repo.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest
import yaml

from scripts import image_changes, release_notes as rn

REPO_ROOT = Path(__file__).resolve().parent.parent
IMAGES = image_changes.discover_images(REPO_ROOT)
NAMES = [image.name for image in IMAGES]
BRAIN = image_changes.BRAIN_IMAGE
JOBS = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "build.yml").read_text(encoding="utf-8"))["jobs"]
HEAD = "a" * 40
BASE = "b" * 40


# --- versions -----------------------------------------------------------------------------

_LS_REMOTE = "\n".join([
    "1111\trefs/tags/vm-lifecycle-worker/v1.0.9",
    "2222\trefs/tags/vm-lifecycle-worker/v1.0.10",
    "3333\trefs/tags/vm-lifecycle-worker/v1.0.8",
    "4444\trefs/tags/workflows-orchestrator/v0.1.102",
    "4444\trefs/tags/workflows-orchestrator/v0.1.102^{}",
    "5555\trefs/tags/v1.0.56",                                  # this repo's legacy tags: not a chart
    "6666\trefs/tags/segments-manager/v1.0.0-rc1",              # not vX.Y.Z
])


def test_release_tags_are_read_per_chart_and_sorted_by_number():
    """`sort -V` order, as the reusable workflow picks its latest: v1.0.10 after v1.0.9."""
    assert rn.release_tags(_LS_REMOTE) == {
        "vm-lifecycle-worker": ["v1.0.8", "v1.0.9", "v1.0.10"],
        "workflows-orchestrator": ["v0.1.102"],
    }


def test_the_newest_version_of_each_image_that_has_one():
    tags = rn.release_tags(_LS_REMOTE)
    assert rn.newest_versions(tags, ["vm-lifecycle-worker", BRAIN, "new-worker"]) == {
        "vm-lifecycle-worker": "v1.0.10", BRAIN: "v0.1.102",
    }


@pytest.mark.parametrize(("ref", "slug"), [
    ("feat/per-image-builds", "feat-per-image-builds"),
    ("ci/parallel-image-builds-and-release-notes", "ci-parallel-image-builds-and-release-notes"),
    ("Fix__Two--dashes/", "fix-two-dashes"),
    ("main", "main"),
])
def test_the_branch_slug_is_the_reusable_workflows(ref, slug):
    """ghcr-build-push.yml: tr -c 'a-zA-Z0-9' '-' | tr 'A-Z' 'a-z' | sed 's/-\\+/-/g; s/^-//; s/-$//'."""
    assert rn.branch_slug(ref) == slug


# --- the registry -------------------------------------------------------------------------


class _Answer:
    def __init__(self, body: bytes = b"", headers: dict | None = None) -> None:
        self._body, self.headers = body, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def read(self, *args) -> bytes:
        return self._body


class _FakeGhcr:
    """ghcr.io: an anonymous token per repository, and a digest per tag it has."""

    def __init__(self, digests: dict[str, str], token_fails: bool = False) -> None:
        self.digests, self.token_fails, self.calls = digests, token_fails, []

    def __call__(self, request, timeout):
        self.calls.append((request.get_method(), request.full_url))
        if "/token?" in request.full_url:
            if self.token_fails:
                raise urllib.error.URLError("no route")
            return _Answer(json.dumps({"token": "anonymous"}).encode())
        assert request.get_method() == "HEAD" and request.get_header("Authorization") == "Bearer anonymous"
        assert "application/vnd.oci.image.index.v1+json" in request.get_header("Accept")
        image_tag = request.full_url.split("/v2/team-redbull/", 1)[1].replace("/manifests/", ":")
        if image_tag not in self.digests:
            raise urllib.error.HTTPError(request.full_url, 404, "not found", {}, None)
        return _Answer(headers={"Docker-Content-Digest": self.digests[image_tag]})


def test_the_digest_is_the_registrys_answer_for_the_tag():
    ghcr = _FakeGhcr({"vm-lifecycle-worker:v1.0.10": "sha256:" + "1" * 64})
    registry = rn.Registry("team-redbull", opener=ghcr)
    assert registry.digest("vm-lifecycle-worker", "v1.0.10") == "sha256:" + "1" * 64
    assert registry.digest("vm-lifecycle-worker", "v1.0.11") is None          # no such tag
    assert registry.digest("vm-lifecycle-worker", None) is None               # no version, no question
    registry.digest("vm-lifecycle-worker", "v1.0.10")
    assert [method for method, _ in ghcr.calls] == ["GET", "HEAD", "HEAD"]    # one token, each answer once


def test_a_registry_that_cannot_answer_gives_unknown_not_an_error():
    registry = rn.Registry("team-redbull", opener=_FakeGhcr({}, token_fails=True))
    assert registry.digest("vm-lifecycle-worker", "v1.0.10") is None


def test_a_branch_build_is_found_at_its_short_sha_however_long():
    head = "abcdef0123456789" + "0" * 24
    ghcr = _FakeGhcr({"vm-lifecycle-worker:feat-x-abcdef012": "sha256:" + "2" * 64})
    registry = rn.Registry("team-redbull", opener=ghcr)
    assert registry.branch_tag("vm-lifecycle-worker", "feat/x", head) == "feat-x-abcdef012"
    assert registry.branch_tag("cluster-lifecycle-worker", "feat/x", head) is None


# --- commits ------------------------------------------------------------------------------


def _git(root: Path, *args: str) -> str:
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t", "GIT_COMMITTER_DATE": "2026-11-16T12:00:00Z",
           "GIT_AUTHOR_DATE": "2026-11-16T12:00:00Z", "HOME": str(root), "PATH": "/usr/bin:/bin:/usr/local/bin"}
    return subprocess.run(["git", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false", *args], cwd=root,
                          env=env, capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def history(tmp_path):
    """base, a feature branch of two commits merged as PR #7, then one direct commit."""
    _git(tmp_path, "init", "-q")
    (tmp_path / "README.md").write_text("x\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-qm", "base")
    base = _git(tmp_path, "rev-parse", "HEAD")
    _git(tmp_path, "checkout", "-qb", "feat/x")
    (tmp_path / "a.py").write_text("a = 1\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-qm", "feat: a")
    (tmp_path / "b.txt").write_text("b\n")
    (tmp_path / "a.py").write_text("a = 2\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-qm", "fix: a and b")
    _git(tmp_path, "checkout", "-q", "main")
    _git(tmp_path, "merge", "-q", "--no-ff", "feat/x", "-m", "Merge pull request #7 from team-redbull/feat/x")
    (tmp_path / "c.md").write_text("c\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-qm", "docs: c")
    return tmp_path, base, _git(tmp_path, "rev-parse", "HEAD")


def test_the_commits_of_the_range_oldest_first_with_their_files_and_no_merges(history):
    root, base, head = history
    commits = rn.read_commits(base, head, root)
    assert [(c.subject, sorted(c.files)) for c in commits] == [
        ("feat: a", ["a.py"]), ("fix: a and b", ["a.py", "b.txt"]), ("docs: c", ["c.md"]),
    ]
    assert all(len(c.sha) == 40 for c in commits)
    assert rn.read_pull_requests(base, head, root) == [7]
    assert rn.commit_date(head, root) == "2026-11-16"


def _commit(*files: str, subject: str = "x") -> rn.Commit:
    return rn.Commit(sha=hashlib.sha1(subject.encode()).hexdigest(), subject=subject, files=list(files))


def test_a_commit_is_attributed_by_the_decisions_own_rule():
    """affected_images() decides the builds; the same function attributes the commits."""
    one_limb = _commit("activities/vm_lifecycle/vm_spec.py", subject="vm")
    every = _commit(".dockerignore", subject="context")
    none = _commit(".github/workflows/build.yml", "README.md", subject="ci")        # CI plumbing: no image
    found = rn.commits_by_image([one_limb, every, none], IMAGES, REPO_ROOT)
    assert found["vm-lifecycle-worker"] == [one_limb, every]
    assert all(found[name] == [every] for name in NAMES if name != "vm-lifecycle-worker")
    for commit in (one_limb, every, none):
        expected = sorted(image_changes.affected_images(commit.files, IMAGES, REPO_ROOT))
        assert sorted(name for name, commits in found.items() if commit in commits) == expected


# --- the plan: what each image is after the run -------------------------------------------

_TAGS_AFTER = {BRAIN: ["v0.1.101", "v0.1.102"], "vm-lifecycle-worker": ["v1.0.9", "v1.0.10"],
               "cluster-lifecycle-worker": ["v1.0.6"], "workflows-docs": ["v1.0.80"]}
_BEFORE = {BRAIN: "v0.1.101", "vm-lifecycle-worker": "v1.0.9", "cluster-lifecycle-worker": "v1.0.6",
           "workflows-docs": "v1.0.80"}


def _plan(built, *, ref="main", before=_BEFORE, tags=_TAGS_AFTER, registry=None, commits=None, release=None):
    names = [BRAIN, "cluster-lifecycle-worker", "vm-lifecycle-worker", "workflows-docs"]
    return {e.image: e for e in rn.plan(names=names, built=built, ref=ref, head=HEAD, before=before, tags=tags,
                                        registry=registry or rn.NoRegistry("team-redbull"), commits=commits or {},
                                        release=release)}


def _ctx(ref="main", since=BASE, **kwargs) -> rn.Context:
    return rn.Context(ref=ref, head=HEAD, since=since, repo="team-redbull/workflows", **kwargs)


def test_on_main_a_built_image_whose_tag_moved_is_released():
    entries = _plan([BRAIN, "vm-lifecycle-worker"], commits={BRAIN: [_commit("x")]})
    assert (entries[BRAIN].status, entries[BRAIN].version, entries[BRAIN].previous) == (rn.RELEASED, "v0.1.102", "v0.1.101")
    assert entries[BRAIN].commits == [_commit("x")]
    assert entries["vm-lifecycle-worker"].status == rn.RELEASED


def test_on_main_a_built_image_whose_tag_did_not_move_is_not_released():
    """Its build failed, or a failed brain held it back: it stays at what it was."""
    entries = _plan([BRAIN, "cluster-lifecycle-worker"])
    assert (entries["cluster-lifecycle-worker"].status, entries["cluster-lifecycle-worker"].version) == (rn.NOT_RELEASED, "v1.0.6")


def test_an_image_the_run_did_not_build_is_unchanged_at_its_newest_release():
    entries = _plan([BRAIN])
    assert (entries["workflows-docs"].status, entries["workflows-docs"].version) == (rn.UNCHANGED, "v1.0.80")
    assert (entries["vm-lifecycle-worker"].status, entries["vm-lifecycle-worker"].version) == (rn.UNCHANGED, "v1.0.10")


def test_a_first_release_has_no_previous_version():
    entries = _plan(["cluster-lifecycle-worker"], before={})
    assert (entries["cluster-lifecycle-worker"].status, entries["cluster-lifecycle-worker"].previous) == (rn.RELEASED, None)


def test_without_the_record_from_before_a_built_image_counts_as_released_at_its_newest():
    entries = _plan([BRAIN], before=None)
    assert (entries[BRAIN].status, entries[BRAIN].version, entries[BRAIN].previous) == (rn.RELEASED, "v0.1.102", "v0.1.101")


def test_a_branch_build_is_its_branch_tag_and_releases_nothing():
    head_tag = f"feat-x-{HEAD[:7]}"
    ghcr = _FakeGhcr({f"{BRAIN}:{head_tag}": "sha256:" + "3" * 64, f"{BRAIN}:v0.1.102": "sha256:" + "4" * 64})
    entries = _plan([BRAIN, "vm-lifecycle-worker"], ref="feat/x", registry=rn.Registry("team-redbull", opener=ghcr))
    assert (entries[BRAIN].status, entries[BRAIN].version, entries[BRAIN].previous) == (rn.BRANCH, head_tag, "v0.1.102")
    assert entries[BRAIN].digest == "sha256:" + "3" * 64
    assert (entries["vm-lifecycle-worker"].status, entries["vm-lifecycle-worker"].version) == (rn.NOT_RELEASED, "v1.0.10")
    assert not any(e.status == rn.RELEASED for e in entries.values())


# --- the notes ----------------------------------------------------------------------------

_DIGESTS = {f"{BRAIN}:v0.1.102": "sha256:" + "a" * 64, "vm-lifecycle-worker:v1.0.10": "sha256:" + "b" * 64,
            "cluster-lifecycle-worker:v1.0.6": "sha256:" + "c" * 64, "workflows-docs:v1.0.80": "sha256:" + "d" * 64}


def _notes(built, **kwargs) -> str:
    registry = rn.Registry("team-redbull", opener=_FakeGhcr(_DIGESTS))
    commits = {BRAIN: [_commit("shared/exceptions.py", subject="fix: one error")],
               "vm-lifecycle-worker": [_commit("shared/exceptions.py", subject="fix: one error")]}
    entries = list(_plan(built, registry=registry, commits=commits).values())
    return rn.render(entries, _ctx(pull_requests=[23], **kwargs))


def test_the_notes_name_every_image_with_its_version_and_digest():
    notes = _notes([BRAIN, "vm-lifecycle-worker", "cluster-lifecycle-worker"])
    assert "## Rebuilt" in notes and "## Not released" in notes and "## Unchanged" in notes
    assert "| `workflows-orchestrator` | **`v0.1.102`** | `v0.1.101` | `sha256:aaaaaaaaaaaa…` |" in notes
    assert "| `cluster-lifecycle-worker` | `v1.0.6` |" in notes                 # not released: stays at
    assert "| `workflows-docs` | `v1.0.80` |" in notes                          # unchanged: stays at
    every = notes.split("Every image at this release", 1)[1]
    for image, version, digest in [(BRAIN, "v0.1.102", "a"), ("vm-lifecycle-worker", "v1.0.10", "b"),
                                   ("cluster-lifecycle-worker", "v1.0.6", "c"), ("workflows-docs", "v1.0.80", "d")]:
        assert f"ghcr.io/team-redbull/{image}:{version}@sha256:{digest * 64}" in every


def test_the_notes_list_what_to_pull_new_apart_from_the_whole_set():
    to_pull = _notes([BRAIN]).split("## To pull", 1)[1].split("Every image at this release", 1)[0]
    assert to_pull.count("ghcr.io/") == 1 and f"{BRAIN}:v0.1.102@sha256:" in to_pull


def test_images_changed_by_the_same_commits_share_one_list():
    notes = _notes([BRAIN, "vm-lifecycle-worker"])
    assert "**`workflows-orchestrator`, `vm-lifecycle-worker`**" in notes
    assert notes.count("fix: one error") == 1
    assert f"[`{BASE[:7]}`](https://github.com/team-redbull/workflows/commit/{BASE}), the diff base" in notes
    assert f"[compare](https://github.com/team-redbull/workflows/compare/{BASE}...{HEAD})" in notes
    assert "[#23](https://github.com/team-redbull/workflows/pull/23)" in notes


def test_a_run_without_a_diff_base_lists_no_commits():
    entries = list(_plan([BRAIN]).values())
    notes = rn.render(entries, _ctx(since=""))
    assert "No usable diff base" in notes and "What changed them" not in notes


def test_branch_notes_say_nothing_was_released():
    entries = list(_plan([BRAIN], ref="feat/x").values())
    notes = rn.render(entries, _ctx(ref="feat/x"))
    assert "nothing released or deployed" in notes and "## Built on this branch" in notes
    assert f"feat-x-{HEAD[:7]}" in notes and "Every image at this release" not in notes


# --- the previous release, and the first: a baseline --------------------------------------

_RELEASE = rn.Release(tag="release-2026-11-16-1234567", head="c" * 40,
                      versions={BRAIN: "v0.1.101", "cluster-lifecycle-worker": "v1.0.6", "vm-lifecycle-worker": "v1.0.9",
                                "workflows-docs": "v1.0.80"})


def test_against_the_previous_release_an_image_it_had_at_another_version_is_released():
    """Even one this run did not build: a release whose publishing failed is caught up."""
    entries = _plan([], release=_RELEASE, before={name: tags[-1] for name, tags in _TAGS_AFTER.items()})
    assert (entries[BRAIN].status, entries[BRAIN].version, entries[BRAIN].previous) == (rn.RELEASED, "v0.1.102", "v0.1.101")
    assert entries["vm-lifecycle-worker"].status == rn.RELEASED
    assert (entries["workflows-docs"].status, entries["workflows-docs"].version) == (rn.UNCHANGED, "v1.0.80")


def test_against_the_previous_release_a_built_image_that_did_not_move_is_not_released():
    entries = _plan(["cluster-lifecycle-worker"], release=_RELEASE)
    assert (entries["cluster-lifecycle-worker"].status, entries["cluster-lifecycle-worker"].version) == (rn.NOT_RELEASED, "v1.0.6")


def test_the_baseline_lists_every_image_unchanged_with_the_whole_set_to_pull():
    now = {name: tags[-1] for name, tags in _TAGS_AFTER.items()}
    registry = rn.Registry("team-redbull", opener=_FakeGhcr(_DIGESTS))
    entries = list(_plan([], before=now, registry=registry).values())
    assert {e.status for e in entries} == {rn.UNCHANGED}
    notes = rn.render(entries, _ctx(since="", release_state=rn.NONE))
    assert notes.startswith("**Baseline: every image at the version it runs now.**")
    assert "No usable diff base" not in notes and "## Rebuilt" not in notes
    pulled = notes.split("## To pull", 1)[1]
    for image, version in now.items():
        assert f"ghcr.io/team-redbull/{image}:{version}@sha256:" in pulled
    assert rn.title(entries, "2026-11-16", baseline=True) == "Images 2026-11-16: baseline, every image at its current version"


def test_main_notes_carry_the_versions_the_next_release_is_measured_from():
    entries = list(_plan([BRAIN]).values())
    notes = rn.render(entries, _ctx())
    assert rn.read_marker(notes) == (HEAD, {e.image: e.version for e in entries if e.version})
    assert rn.read_marker(rn.render(entries, _ctx(ref="feat/x"))) is None      # a branch publishes nothing


class _FakeApi:
    def __init__(self, releases=None, fails=False) -> None:
        self.releases, self.fails, self.requests = releases, fails, []

    def __call__(self, request, timeout):
        self.requests.append(request)
        if self.fails:
            raise urllib.error.URLError("down")
        return _Answer(json.dumps(self.releases).encode())


def _body(head: str, versions: dict) -> str:
    return "notes\n\n" + f"<!-- release-notes {json.dumps({'head': head, 'versions': versions})} -->\n"


def test_the_previous_release_is_the_newest_with_notes_that_is_not_this_commits_own():
    api = _FakeApi([
        {"tag_name": "release-x-own", "draft": False, "body": _body(HEAD, {BRAIN: "v0.1.103"})},   # a re-run's own
        {"tag_name": "draft", "draft": True, "body": _body("d" * 40, {BRAIN: "v0"})},
        {"tag_name": "by-hand", "draft": False, "body": "no marker here"},
        {"tag_name": "release-2026-11-16-ccccccc", "draft": False, "body": _body("c" * 40, {BRAIN: "v0.1.101"})},
        {"tag_name": "older", "draft": False, "body": _body("e" * 40, {BRAIN: "v0.1.100"})},
    ])
    state, release = rn.previous_release("https://api.example/releases", HEAD, opener=api, token="t")
    assert state == rn.FOUND and release == rn.Release("release-2026-11-16-ccccccc", "c" * 40, {BRAIN: "v0.1.101"})
    assert api.requests[0].get_header("Authorization") == "Bearer t"


def test_no_release_yet_is_none_and_an_unreadable_list_is_unknown():
    url = "https://api.example/releases"
    assert rn.previous_release(url, HEAD, opener=_FakeApi([])) == (rn.NONE, None)
    assert rn.previous_release(url, HEAD, opener=_FakeApi([{"tag_name": "v1", "body": "x"}])) == (rn.NONE, None)
    assert rn.previous_release(url, HEAD, opener=_FakeApi(fails=True)) == (rn.UNKNOWN, None)
    assert rn.previous_release(url, HEAD, opener=_FakeApi({"message": "rate limited"})) == (rn.UNKNOWN, None)
    assert rn.previous_release("not a url", HEAD) == (rn.UNKNOWN, None)


def test_the_title_and_the_tag_depend_on_the_commit_alone():
    entries = list(_plan([BRAIN, "vm-lifecycle-worker"]).values())
    assert rn.title(entries, "2026-11-16") == "Images 2026-11-16: workflows-orchestrator v0.1.102, vm-lifecycle-worker v1.0.10"
    assert rn.release_tag("2026-11-16", HEAD) == f"release-2026-11-16-{HEAD[:7]}"


# --- the command line the workflow runs ---------------------------------------------------


@pytest.fixture
def chart_repo(tmp_path):
    """A stand-in for redbull-platform: a repo with `<chart>/vX.Y.Z` tags."""
    repo = tmp_path / "platform"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "x").write_text("x\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "x")
    for tag in ("workflows-orchestrator/v0.1.101", "workflows-orchestrator/v0.1.102", "workflows-docs/v1.0.80"):
        _git(repo, "tag", tag)
    return repo


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "scripts/release_notes.py", *args], cwd=REPO_ROOT,
                          capture_output=True, text=True, timeout=120)


def test_the_cli_versions_are_the_newest_tag_of_every_image(chart_repo):
    result = _run("versions", "--chart-repo", str(chart_repo))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {BRAIN: "v0.1.102", "workflows-docs": "v1.0.80"}


def _outputs(text: str) -> dict[str, str]:
    """$GITHUB_OUTPUT's two forms: key=value, and key<<DELIMITER ... DELIMITER."""
    found, lines = {}, iter(text.splitlines())
    for line in lines:
        if "<<" in line and "=" not in line.split("<<", 1)[0]:
            key, delimiter = line.split("<<", 1)
            found[key] = "\n".join(iter(lambda: next(lines), delimiter))
        else:
            key, _, value = line.partition("=")
            found[key] = value
    return found


def _head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


def _cli_notes(tmp_path, chart_repo, releases, *, images, before) -> tuple[str, dict[str, str]]:
    api = tmp_path / "releases.json"
    api.write_text(json.dumps(releases))
    notes_file, output = tmp_path / "notes.md", tmp_path / "github_output"
    result = _run("notes", "--head", _head(), "--ref", "main", "--images", json.dumps(images),
                  "--before", json.dumps(before), "--chart-repo", str(chart_repo), "--no-registry",
                  "--releases-url", api.as_uri(), "--notes-file", str(notes_file), "--github-output", str(output))
    assert result.returncode == 0, result.stderr
    return notes_file.read_text(), _outputs(output.read_text())


_NOW = {BRAIN: "v0.1.102", "workflows-docs": "v1.0.80"}                    # the stand-in chart repo's newest tags


def test_the_cli_writes_the_notes_and_the_release_job_inputs(chart_repo, tmp_path):
    previous = [{"tag_name": "release-before", "body": _body("f" * 40, {BRAIN: "v0.1.101", "workflows-docs": "v1.0.80"})}]
    notes, values = _cli_notes(tmp_path, chart_repo, previous, images=[BRAIN],
                               before={BRAIN: "v0.1.101", "workflows-docs": "v1.0.80"})
    assert "**`v0.1.102`** | `v0.1.101`" in notes and "| `workflows-docs` | `v1.0.80` |" in notes
    assert "the previous release, [`release-before`]" not in notes          # not an ancestor: the base is used
    for name in NAMES:
        assert f"`{name}`" in notes                                           # every image, rebuilt or not
    assert (values["publish"], values["released"]) == ("true", "1")
    assert values["tag"].startswith("release-") and values["tag"].endswith(_head()[:7])
    assert values["title"].endswith(": workflows-orchestrator v0.1.102")
    assert values["notes"] == notes.rstrip()


def test_the_cli_publishes_a_baseline_when_there_is_no_release_yet(chart_repo, tmp_path):
    notes, values = _cli_notes(tmp_path, chart_repo, [], images=[], before=_NOW)
    assert (values["publish"], values["released"]) == ("true", "0")
    assert values["title"].endswith(": baseline, every image at its current version")
    assert notes.startswith("**Baseline") and rn.read_marker(notes)[1] == _NOW


def test_the_cli_publishes_nothing_when_nothing_changed_since_the_previous_release(chart_repo, tmp_path):
    previous = [{"tag_name": "release-before", "body": _body("f" * 40, _NOW)}]
    _, values = _cli_notes(tmp_path, chart_repo, previous, images=[], before=_NOW)
    assert (values["publish"], values["released"]) == ("false", "0")


# --- build.yml ----------------------------------------------------------------------------


def _script(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def test_the_changes_job_records_its_base_and_the_versions_before_any_build():
    changes = JOBS["changes"]
    assert changes["outputs"]["base"] == "${{ steps.filter.outputs.base }}"
    assert changes["outputs"]["versions"] == "${{ steps.versions.outputs.versions }}"
    versions = next(step for step in changes["steps"] if step.get("id") == "versions")
    # An unreadable record must not stop a build: the step swallows the failure itself.
    assert "if ! versions=\"$(python3 scripts/release_notes.py versions)\"" in versions["run"]
    assert 'echo "base=${base}"' in _script(changes)


def test_the_notes_take_the_changes_jobs_list_and_wait_for_every_build():
    notes = JOBS["release-notes"]
    assert set(notes["needs"]) == {"changes", "test", "build-workflow-brain", "build"}
    assert notes["if"].startswith("${{ !cancelled() &&")                       # also after a failed build
    # On main on every push, built or not: the baseline, and a release whose publishing failed.
    assert "(needs.changes.outputs.images != '[]' || github.ref_name == 'main')" in notes["if"]
    step = next(step for step in notes["steps"] if step.get("id") == "notes")
    assert step["env"]["IMAGES"] == "${{ needs.changes.outputs.images }}"
    assert step["env"]["BASE"] == "${{ needs.changes.outputs.base }}"
    assert step["env"]["BEFORE"] == "${{ needs.changes.outputs.versions }}"
    assert step["env"]["GH_TOKEN"] == "${{ github.token }}"                   # reads the previous release
    assert "python3 scripts/release_notes.py notes" in step["run"] and "GITHUB_STEP_SUMMARY" in step["run"]


def test_the_notes_never_fail_the_run_and_hold_no_write_token():
    """The diff base is the last run that SUCCEEDED: a red notes job would rebuild what shipped."""
    for name in ("release-notes", "release"):
        assert JOBS[name]["continue-on-error"] is True
    assert JOBS["release-notes"]["permissions"] == {"contents": "read"}
    checkout = JOBS["release-notes"]["steps"][0]["with"]
    assert checkout["persist-credentials"] is False and checkout["fetch-depth"] == 0


def test_the_release_is_published_on_main_only_by_a_job_that_runs_no_repo_code():
    release = JOBS["release"]
    assert release["needs"] == ["release-notes"]
    assert "github.ref_name == 'main'" in release["if"] and "needs.release-notes.outputs.publish == 'true'" in release["if"]
    assert release["permissions"] == {"contents": "write"}
    assert not any("uses" in step for step in release["steps"])                # no checkout
    assert "gh release create" in _script(release) and "gh release edit" in _script(release)


@pytest.mark.parametrize("name", ["release-notes", "release"])
def test_no_expression_is_pasted_into_a_script(name):
    """Values reach the shell through env: an expression in `run:` is shell code from a
    commit subject or a notes line (template injection)."""
    assert "${{" not in _script(JOBS[name])
