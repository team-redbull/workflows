#!/usr/bin/env python3
"""Release notes for one run of build.yml: what it shipped, image by image.

For whoever mirrors the images into the air-gap, which pulls nothing it is not told
to: every image the run REBUILT (its new version, the one it replaces, its digest and
the commits that changed it) and every image it did NOT (the version it stays at, with
its digest), so the list of what to pull is complete either way.

Which images a run builds is scripts/image_changes.py's answer, never this script's:
it is handed the `changes` job's list, and it attributes each commit of the run's range
to the images that commit affects with the same affected_images() the decision used.

Where the facts come from:

  * VERSIONS: the reusable build workflow (team-redbull/.github, ghcr-build-push.yml)
    tags each main release `<chart>/vX.Y.Z` in redbull-platform, and each chart
    directory is named after its image (tests/test_image_builds.py). `notes` reads the
    newest tags AFTER the builds.
  * WHAT CHANGED is measured against the PREVIOUS RELEASE of these notes: each one
    carries its versions and its commit in a comment nobody sees (MARKER), and the next
    one lists every image whose version differs from it, and the commits since it. So a
    release whose publishing failed is not lost: the next one lists its images too.
  * THE FIRST RELEASE IS A BASELINE (the operator, 2026-11-16): with no release before
    it, every image is listed at its current version and digest, unchanged, and the
    whole set is the pull list. Nothing is rebuilt for it.
  * A FAILED BUILD: the `changes` job records the newest tags BEFORE anything builds
    (`versions`). An image the run was asked to build whose tag did not move is listed
    as not released, at the version it stays at. Runs on one branch are serialized
    (build.yml's `concurrency`), so no other run moves the tags in between.
  * A BRANCH build bumps nothing: its image is tagged `<branch-slug>-<short sha>` (the
    reusable workflow's slug rule), looked up in the registry.
  * DIGESTS: ghcr.io, the manifest each tag points at, asked anonymously (the packages
    are public).
  * COMMITS: `git log <since>..<head>` without merges, `since` being the previous
    release's commit, else the `changes` job's diff base.

Nothing here decides what is built or deployed; a lookup that fails is written as
unknown, and the notes still name every image.

Usage (the repo root is the working directory):
  python3 scripts/release_notes.py versions
      -> {"<image>": "vX.Y.Z", ...}, the newest release of each image, on stdout
  python3 scripts/release_notes.py notes --head <sha> --ref <branch> --images '<json list>' \\
      [--base <sha>] [--before '<versions json>'] [--repo owner/name] [--run-url URL] \\
      [--releases-url URL] [--notes-file PATH] [--github-output PATH] [--no-registry]
      -> the Markdown notes (on stdout, or in --notes-file); --github-output gets
         `publish` (true: main, and the first release or a changed image), `released`
         (how many images changed), `tag`, `title` and `notes`. The previous release is
         read from --releases-url (GitHub's API; $GH_TOKEN when set).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path

if __package__ in (None, ""):                       # run as a script: scripts/ is sys.path[0]
    import image_changes
else:                                               # imported as scripts.release_notes (tests)
    from scripts import image_changes

REPO_ROOT = Path(__file__).resolve().parent.parent

MAIN = "main"                                       # the reusable workflow releases from main only
CHART_REPO = "https://github.com/team-redbull/redbull-platform.git"
REGISTRY = "ghcr.io"
DEFAULT_REPO = "team-redbull/workflows"
SHA_LENGTHS = range(7, 13)                          # `git rev-parse --short`: 7 chars, more if ambiguous
_MANIFESTS = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
_TAG = re.compile(r"^refs/tags/(?P<chart>[a-z0-9-]+)/v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
_PULL_REQUEST = re.compile(r"^Merge pull request #(?P<number>\d+)\b")


# --- versions -----------------------------------------------------------------------------


def release_tags(ls_remote: str) -> dict[str, list[str]]:
    """{chart: its `vX.Y.Z` tags, oldest first} from `git ls-remote --tags` output.

    Sorted by number, as the reusable workflow's `sort -V` does: v1.0.10 is after v1.0.9.
    """
    found: dict[str, set[tuple[int, int, int]]] = {}
    for line in ls_remote.splitlines():
        match = _TAG.match(line.split("\t")[-1].strip())
        if match is not None:                       # else another kind of tag, or a peeled `^{}` line
            found.setdefault(match["chart"], set()).add((int(match["major"]), int(match["minor"]), int(match["patch"])))
    return {chart: [f"v{a}.{b}.{c}" for a, b, c in sorted(numbers)] for chart, numbers in found.items()}


def newest_versions(tags: dict[str, list[str]], images: list[str]) -> dict[str, str]:
    """{image: its newest release}: what main runs, for each image that has one."""
    return {image: tags[image][-1] for image in images if tags.get(image)}


def read_release_tags(chart_repo: str = CHART_REPO) -> dict[str, list[str]]:
    done = subprocess.run(["git", "ls-remote", "--tags", "--refs", chart_repo],
                          capture_output=True, text=True, timeout=60, check=True)
    return release_tags(done.stdout)


def branch_slug(ref: str) -> str:
    """The reusable workflow's tag prefix for a branch build: every run of characters
    other than [A-Za-z0-9] one `-`, lower case, no leading or trailing `-`."""
    return re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9]", "-", ref).lower()).strip("-")


# --- the registry -------------------------------------------------------------------------


class Registry:
    """Digests from ghcr.io, asked anonymously; None for a tag it does not have or cannot say."""

    def __init__(self, owner: str, opener=urllib.request.urlopen) -> None:
        self.owner = owner
        self._open = opener
        self._bearers: dict[str, str | None] = {}
        self._digests: dict[tuple[str, str], str | None] = {}

    def _bearer(self, image: str) -> str | None:
        if image not in self._bearers:
            scope = urllib.parse.quote(f"repository:{self.owner}/{image}:pull")
            try:
                with self._open(urllib.request.Request(f"https://{REGISTRY}/token?scope={scope}&service={REGISTRY}"),
                                timeout=20) as answer:
                    self._bearers[image] = json.load(answer)["token"]
            except (urllib.error.URLError, OSError, ValueError, KeyError):
                self._bearers[image] = None
        return self._bearers[image]

    def digest(self, image: str, tag: str | None) -> str | None:
        if not tag:
            return None
        if (image, tag) not in self._digests:
            self._digests[image, tag] = self._head(image, tag)
        return self._digests[image, tag]

    def _head(self, image: str, tag: str) -> str | None:
        bearer = self._bearer(image)
        if not bearer:
            return None
        head = urllib.request.Request(f"https://{REGISTRY}/v2/{self.owner}/{image}/manifests/{tag}", method="HEAD")
        head.add_header("Authorization", f"Bearer {bearer}")
        head.add_header("Accept", _MANIFESTS)
        try:
            with self._open(head, timeout=20) as answer:
                return answer.headers.get("Docker-Content-Digest") or None
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def branch_tag(self, image: str, ref: str, head: str) -> str | None:
        """The tag a branch build of `head` pushed, `<slug>-<short sha>`, if the registry has it."""
        for length in SHA_LENGTHS:
            tag = f"{branch_slug(ref)}-{head[:length]}"
            if self.digest(image, tag):
                return tag
        return None


class NoRegistry(Registry):
    """--no-registry: no digests, and a branch build is assumed at its 7-character tag."""

    def digest(self, image: str, tag: str | None) -> str | None:
        return None

    def branch_tag(self, image: str, ref: str, head: str) -> str | None:
        return f"{branch_slug(ref)}-{head[:7]}"


# --- commits ------------------------------------------------------------------------------


@dataclass
class Commit:
    sha: str
    subject: str
    files: list[str]


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, timeout=60, check=True,
                          env={**os.environ, "TZ": "UTC"}).stdout


def read_commits(base: str, head: str, root: Path = REPO_ROOT) -> list[Commit]:
    """The non-merge commits of base..head, oldest first, each with the files it changed.

    A merge commit changes nothing of its own here: the commits it brings are in the range.
    """
    out = _git(root, "log", "--no-merges", "--reverse", "--format=%x1e%H%x1f%s", "--name-only", f"{base}..{head}")
    commits = []
    for record in out.split("\x1e")[1:]:
        header, _, rest = record.partition("\n")
        sha, _, subject = header.partition("\x1f")
        commits.append(Commit(sha=sha, subject=subject, files=[line for line in rest.splitlines() if line.strip()]))
    return commits


def read_pull_requests(base: str, head: str, root: Path = REPO_ROOT) -> list[int]:
    """The pull requests merged into the branch in base..head, oldest first (GitHub's merge subjects)."""
    out = _git(root, "log", "--first-parent", "--merges", "--reverse", "--format=%s", f"{base}..{head}")
    return [int(m["number"]) for line in out.splitlines() if (m := _PULL_REQUEST.match(line))]


def commit_date(head: str, root: Path = REPO_ROOT) -> str:
    """The head commit's date (committer, UTC), YYYY-MM-DD: the same for every run of that commit."""
    return _git(root, "log", "-1", "--date=format-local:%Y-%m-%d", "--format=%cd", head).strip()


def is_ancestor(older: str, newer: str, root: Path = REPO_ROOT) -> bool:
    done = subprocess.run(["git", "merge-base", "--is-ancestor", older, newer], cwd=root, capture_output=True, timeout=60)
    return done.returncode == 0


def commits_by_image(commits: list[Commit], images: list[image_changes.Image], root: Path = REPO_ROOT) -> dict[str, list[Commit]]:
    """{image: the commits that affect it}, by the decision's own rule, commit by commit: a
    commit that changes .github/image-builds.json is its changed rows' images only."""
    found: dict[str, list[Commit]] = {}
    for commit in commits:
        rows = (image_changes.changed_build_rows(f"{commit.sha}^", root, head=commit.sha)
                if image_changes.BUILDS_FILE in commit.files else None)
        for name in image_changes.affected_images(commit.files, images, root, changed_rows=rows):
            found.setdefault(name, []).append(commit)
    return found


# --- the previous release ----------------------------------------------------------------

# Each release's notes end with this comment, which GitHub does not show: the commit it
# is the release of and every image's version then. The next release is measured from it.
MARKER = "release-notes"
_MARKER = re.compile(r"<!-- release-notes (?P<data>\{.*?\}) -->", re.S)
FOUND, NONE, UNKNOWN = "found", "none", "unknown"


@dataclass
class Release:
    tag: str
    head: str
    versions: dict[str, str]


def marker(head: str, entries: list["Entry"]) -> str:
    versions = {e.image: e.version for e in entries if e.version}
    return f"<!-- {MARKER} {json.dumps({'head': head, 'versions': versions}, separators=(',', ':'))} -->"


def read_marker(body: str | None) -> tuple[str, dict[str, str]] | None:
    match = _MARKER.search(body or "")
    if match is None:
        return None
    try:
        data = json.loads(match["data"])
        return str(data["head"]), {str(k): str(v) for k, v in dict(data["versions"]).items()}
    except (ValueError, KeyError, TypeError):
        return None


def releases_url(repo: str) -> str:
    return f"https://api.github.com/repos/{repo}/releases?per_page=100"


def previous_release(url: str, head: str, opener=urllib.request.urlopen,
                     token: str | None = None) -> tuple[str, Release | None]:
    """(FOUND, the newest release of these notes that is not `head`'s own), (NONE, None)
    when the repo has none yet, or (UNKNOWN, None) when the list cannot be read. A
    re-run of a commit's jobs finds that commit's own release first: it is skipped."""
    try:
        request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                                       "X-GitHub-Api-Version": "2022-11-28"})
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        with opener(request, timeout=20) as answer:
            releases = json.load(answer)
    except (urllib.error.URLError, OSError, ValueError):
        return UNKNOWN, None
    if not isinstance(releases, list):
        return UNKNOWN, None
    for release in releases:                    # newest first
        if not isinstance(release, dict) or release.get("draft"):
            continue
        found = read_marker(release.get("body"))
        if found is None or found[0] == head:
            continue
        return FOUND, Release(tag=str(release.get("tag_name", "")), head=found[0], versions=found[1])
    return NONE, None


# --- the plan -----------------------------------------------------------------------------

RELEASED, BRANCH, NOT_RELEASED, UNCHANGED = "released", "branch", "not released", "unchanged"


@dataclass
class Entry:
    image: str
    status: str
    version: str | None = None           # the new version, the branch tag, or the one it stays at
    previous: str | None = None          # released: the one it replaces; branch: what main runs
    digest: str | None = None
    commits: list[Commit] = field(default_factory=list)


def _previous(name: str, *, release: Release | None, before: dict[str, str] | None,
              tags: dict[str, list[str]]) -> str | None:
    """The version an image had at the previous release, else before this run, else (no
    record at all) the tag before its newest."""
    if release is not None:
        return release.versions.get(name)       # absent: an image the previous release did not have
    if before is not None:
        return before.get(name)
    history = tags.get(name, [])
    return history[-2] if len(history) > 1 else None


def plan(*, names: list[str], built: list[str], ref: str, head: str, before: dict[str, str] | None,
         tags: dict[str, list[str]], registry: Registry, commits: dict[str, list[Commit]],
         release: Release | None = None) -> list[Entry]:
    """One Entry per image, in `names` order.

    On main an image is RELEASED when its version differs from the previous release's,
    even one this run did not build (a release whose publishing failed is caught up). With
    no release before (`release` None), only this run's images are compared, with the
    record taken before the run; `before` None (no record) counts a built image released
    at its newest tag. An image this run built whose version did not move is NOT
    RELEASED (the build failed); every other is UNCHANGED."""
    after = newest_versions(tags, names)
    entries = []
    for name in names:
        current = after.get(name)
        if ref != MAIN:
            if name not in built:
                entries.append(Entry(name, UNCHANGED, version=current, digest=registry.digest(name, current)))
                continue
            tag = registry.branch_tag(name, ref, head)
            if tag:
                entries.append(Entry(name, BRANCH, version=tag, previous=current, digest=registry.digest(name, tag),
                                     commits=commits.get(name, [])))
            else:                        # built, but not in the registry: the build failed
                entries.append(Entry(name, NOT_RELEASED, version=current, digest=registry.digest(name, current)))
            continue
        if release is None and name not in built:
            entries.append(Entry(name, UNCHANGED, version=current, digest=registry.digest(name, current)))
            continue
        previous = _previous(name, release=release, before=before, tags=tags)
        if current is not None and current != previous:
            entries.append(Entry(name, RELEASED, version=current, previous=previous,
                                 digest=registry.digest(name, current), commits=commits.get(name, [])))
        elif name in built:
            entries.append(Entry(name, NOT_RELEASED, version=current, digest=registry.digest(name, current)))
        else:
            entries.append(Entry(name, UNCHANGED, version=current, digest=registry.digest(name, current)))
    return entries


def release_tag(date: str, head: str) -> str:
    """This repo's tag for the release of `head`: the same for every run of that commit."""
    return f"release-{date}-{head[:7]}"


def title(entries: list[Entry], date: str, *, baseline: bool = False) -> str:
    released = [entry for entry in entries if entry.status == RELEASED]
    if not released:
        return f"Images {date}: baseline, every image at its current version" if baseline else (
            f"Images {date}: nothing released")
    names = ", ".join(f"{entry.image} {entry.version}" for entry in released[:3])
    more = f" and {len(released) - 3} more" if len(released) > 3 else ""
    return f"Images {date}: {names}{more}"


# --- the notes ----------------------------------------------------------------------------


def _code(text: str | None) -> str:
    return f"`{text}`" if text else "unknown"


def _short_digest(digest: str | None) -> str:
    return f"`{digest[:19]}…`" if digest else "unknown"


def _pull_ref(owner: str, entry: Entry) -> str | None:
    if not entry.version:
        return None
    ref = f"{REGISTRY}/{owner}/{entry.image}:{entry.version}"
    return f"{ref}@{entry.digest}" if entry.digest else ref


@dataclass
class Context:
    """Where the notes stand: the branch, the commits they cover, the release before them."""
    ref: str
    head: str
    since: str                            # the commits are since..head; '' = none listed
    repo: str
    run_url: str | None = None
    pull_requests: list[int] = field(default_factory=list)
    before_known: bool = True
    release_state: str = UNKNOWN          # FOUND, NONE or UNKNOWN (always UNKNOWN on a branch)
    release: Release | None = None

    @property
    def url(self) -> str:
        return f"https://github.com/{self.repo}"

    @property
    def baseline(self) -> bool:
        return self.ref == MAIN and self.release_state == NONE


def _header(entries: list[Entry], ctx: Context) -> list[str]:
    new = sum(entry.status in (RELEASED, BRANCH) for entry in entries)
    held = sum(entry.status == NOT_RELEASED for entry in entries)
    same = sum(entry.status == UNCHANGED for entry in entries)
    url, head = ctx.url, ctx.head
    head_link = f"[`{head[:7]}`]({url}/commit/{head})"
    lines = []
    if ctx.baseline:
        lines.append("**Baseline: every image at the version it runs now.** The first release of these notes; "
                     "each later one lists what changed since the release before it.")
    if ctx.ref == MAIN:
        counts = f"**{new} rebuilt, {same} unchanged**" + (f", **{held} not released**" if held else "")
        lines.append(f"{counts} — {head_link} on `{MAIN}`.")
    else:
        counts = f"{new} built as `{branch_slug(ctx.ref)}-<sha>`, {same} not rebuilt" + (
            f", {held} not found in the registry" if held else "")
        lines.append(f"**Branch `{ctx.ref}`: nothing released or deployed.** {counts} — {head_link}.")
    since = ctx.since
    if ctx.release is not None and since == ctx.release.head:
        lines.append(f"The change since the previous release, [`{ctx.release.tag}`]({url}/releases/tag/{ctx.release.tag}) "
                     f"([compare]({url}/compare/{since}...{head})).")
    elif since:
        # The `changes` job's base: the last run on the branch that succeeded, or one of its
        # fallbacks (the push range, the branch point), so the notes do not say which.
        lines.append(f"The change since [`{since[:7]}`]({url}/commit/{since}), the diff base the `changes` job "
                     f"measured from ([compare]({url}/compare/{since}...{head})).")
    elif not ctx.baseline:
        lines.append(f"No usable diff base on `{ctx.ref}`, so every image was rebuilt; no commits are listed.")
    if ctx.pull_requests:
        lines.append("Pull requests: " + ", ".join(f"[#{n}]({url}/pull/{n})" for n in ctx.pull_requests) + ".")
    if ctx.run_url:
        lines.append(f"[The run]({ctx.run_url}).")
    lines.append("")
    if ctx.ref == MAIN and ctx.release_state == UNKNOWN:
        lines += ["> The previous release could not be read: this compares with the versions before the run.", ""]
    if not ctx.before_known and ctx.ref == MAIN and ctx.release is None:
        lines += ["> The versions before the run were not recorded: an image whose build failed shows as "
                  "released at its last version. Check the run.", ""]
    return lines


def _rebuilt(new: list[Entry], ctx: Context) -> list[str]:
    main = ctx.ref == MAIN
    lines = ["## Rebuilt" if main else "## Built on this branch", "",
             f"| image | {'version' if main else 'tag'} | {'previous' if main else 'main runs'} | digest |",
             "| --- | --- | --- | --- |"]
    lines += [f"| `{e.image}` | **{_code(e.version)}** | {_code(e.previous) if e.previous else 'new image'} | "
              f"{_short_digest(e.digest)} |" for e in new]
    lines.append("")
    if not ctx.since:
        return lines
    # The commits behind each image; images changed by the same commits share one list
    # (a change to a file every image loads would otherwise repeat it image after image).
    groups: dict[tuple[str, ...], list[Entry]] = {}
    for e in new:
        groups.setdefault(tuple(c.sha for c in e.commits), []).append(e)
    lines += ["### What changed them", ""]
    for members in groups.values():
        lines.append("**" + ", ".join(f"`{e.image}`" for e in members) + "**")      # versions: the table
        if members[0].commits:
            lines += [f"- [`{c.sha[:7]}`]({ctx.url}/commit/{c.sha}) {c.subject}" for c in members[0].commits]
        else:
            lines.append("- built wide: no single commit of the range touches what it loads (the `changes` "
                         "job's log says why)")
        lines.append("")
    return lines


def _unchanged_text(ctx: Context) -> str:
    if ctx.ref != MAIN:
        return "Not rebuilt on this branch; main's current release."
    if ctx.baseline:
        return "Every image at the version it runs now: the whole set to mirror."
    if ctx.release is not None:
        return "Not rebuilt since the previous release."
    return "Not rebuilt: no file these images load changed."


def render(entries: list[Entry], ctx: Context) -> str:
    owner = ctx.repo.split("/", 1)[0]
    new = [e for e in entries if e.status in (RELEASED, BRANCH)]
    held = [e for e in entries if e.status == NOT_RELEASED]
    same = [e for e in entries if e.status == UNCHANGED]
    lines = _header(entries, ctx)
    if new:
        lines += _rebuilt(new, ctx)
    if held:
        lines += ["## Not released", "",
                  "In this run's list, but no new version: its build failed or did not run (a failed brain or "
                  "chart sync holds the other images back). The next push builds it again.", "",
                  "| image | stays at | digest |", "| --- | --- | --- |"]
        lines += [f"| `{e.image}` | {_code(e.version)} | {_short_digest(e.digest)} |" for e in held]
        lines.append("")
    if same:
        lines += ["## Unchanged", "", _unchanged_text(ctx), "",
                  "| image | stays at | digest |", "| --- | --- | --- |"]
        lines += [f"| `{e.image}` | {_code(e.version)} | {_short_digest(e.digest)} |" for e in same]
        lines.append("")
    pull = [ref_ for e in new if (ref_ := _pull_ref(owner, e))]
    every = [ref_ for e in entries if (ref_ := _pull_ref(owner, e))] if ctx.ref == MAIN else []
    if pull:
        intro = "This branch's images:" if ctx.ref != MAIN else (
            "New in this release, which rebuilt every image:" if pull == every else "New in this release:")
        lines += ["## To pull", "", intro, "", "```text", *pull, "```", ""]
    if every and every != pull:
        if not pull:
            lines += ["## To pull", ""]
        lines += ["Every image at this release, the unchanged ones included:", "", "```text", *every, "```", ""]
    if ctx.ref == MAIN:
        lines += [marker(ctx.head, entries), ""]
    return "\n".join(lines).rstrip() + "\n"


# --- the command line ---------------------------------------------------------------------


def _parse_json(text: str | None, what: str):
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--{what}: not JSON ({exc})") from exc


def _write_outputs(path: str, values: dict[str, str]) -> None:
    """Append to $GITHUB_OUTPUT; a value with a line break gets a delimiter nobody can type."""
    with open(path, "a", encoding="utf-8") as out:
        for key, value in values.items():
            if "\n" in value:
                delimiter = f"EOF_{uuid.uuid4().hex}"
                out.write(f"{key}<<{delimiter}\n{value.rstrip()}\n{delimiter}\n")
            else:
                out.write(f"{key}={value}\n")


def _attributed(since: str, head: str, images: list[image_changes.Image]) -> tuple[dict[str, list[Commit]], list[int]]:
    if not since:
        return {}, []
    try:
        return commits_by_image(read_commits(since, head), images), read_pull_requests(since, head)
    except (subprocess.SubprocessError, OSError, RuntimeError, ValueError) as exc:
        print(f"::warning::commits not attributed: {exc}", file=sys.stderr)
        return {}, []


def _release_before(args: argparse.Namespace) -> tuple[str, Release | None]:
    if args.ref != MAIN:
        return UNKNOWN, None
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or None
    state, release = previous_release(args.releases_url or releases_url(args.repo), args.head, token=token)
    if state == UNKNOWN:
        print("::warning::the previous release is unreadable: compared with the versions before the run, "
              "and no baseline is published", file=sys.stderr)
    return state, release


def _notes(args: argparse.Namespace) -> int:
    images = image_changes.discover_images()
    built = _parse_json(args.images, "images") or []
    names = [image.name for image in images]
    names += [name for name in built if name not in names]
    before = _parse_json(args.before, "before")
    try:
        tags = read_release_tags(args.chart_repo)
    except (subprocess.SubprocessError, OSError) as exc:
        print(f"::warning::release tags unreadable ({exc}): versions unknown", file=sys.stderr)
        tags = {}
    owner = args.repo.split("/", 1)[0]
    registry = NoRegistry(owner) if args.no_registry else Registry(owner)
    state, release = _release_before(args)
    # The commits since the previous release when it is this commit's ancestor (a force
    # push can leave it off the branch), else since the `changes` job's diff base.
    since = release.head if release is not None and is_ancestor(release.head, args.head) else args.base
    commits, pull_requests = _attributed(since, args.head, images)
    entries = plan(names=names, built=built, ref=args.ref, head=args.head, before=before, tags=tags,
                   registry=registry, commits=commits, release=release)
    ctx = Context(ref=args.ref, head=args.head, since=since, repo=args.repo, run_url=args.run_url,
                  pull_requests=pull_requests, before_known=before is not None, release_state=state, release=release)
    notes = render(entries, ctx)
    if args.notes_file:
        Path(args.notes_file).write_text(notes, encoding="utf-8")
    else:
        sys.stdout.write(notes)
    released = sum(entry.status == RELEASED for entry in entries)
    if args.github_output:
        date = commit_date(args.head)
        _write_outputs(args.github_output, {
            "publish": "true" if args.ref == MAIN and (ctx.baseline or released > 0) else "false",
            "released": str(released),
            "tag": release_tag(date, args.head),
            "title": title(entries, date, baseline=ctx.baseline),
            "notes": notes,
        })
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    commands = parser.add_subparsers(dest="command", required=True)
    versions = commands.add_parser("versions", help="the newest release of every image, as JSON")
    versions.add_argument("--chart-repo", default=CHART_REPO)
    notes = commands.add_parser("notes", help="the Markdown notes of one run")
    notes.add_argument("--head", required=True)
    notes.add_argument("--ref", required=True)
    notes.add_argument("--images", required=True, help="the `changes` job's JSON list")
    notes.add_argument("--base", default="")
    notes.add_argument("--before", default="", help="`versions` before the run; empty = not recorded")
    notes.add_argument("--repo", default=DEFAULT_REPO)
    notes.add_argument("--run-url", default=None)
    notes.add_argument("--chart-repo", default=CHART_REPO)
    notes.add_argument("--releases-url", default=None, help="the repo's releases; default GitHub's API")
    notes.add_argument("--notes-file", default=None)
    notes.add_argument("--github-output", default=None)
    notes.add_argument("--no-registry", action="store_true", help="no digests; a branch build assumed at its tag")
    args = parser.parse_args(argv)
    if args.command == "versions":
        names = [image.name for image in image_changes.discover_images()]
        print(json.dumps(newest_versions(read_release_tags(args.chart_repo), names), separators=(",", ":")))
        return 0
    return _notes(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
