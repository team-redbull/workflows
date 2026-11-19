#!/usr/bin/env python3
"""Which images does a set of changed files affect? CI's `changes` job asks this.

An image is rebuilt, re-tagged and redeployed only when a file IT LOADS has
changed — not when a file that merely sits in a folder it copies has. Until
2026-11-14 the answer was by folder (`shared/` -> every Python image,
`activities/` -> every limb), so one worker's new config key gave ten images a
new tag, and each of those is an image to mirror into the air-gap.

How an image's files are found — statically, from the source alone:

  * its ENTRYPOINTS: the module its Dockerfile's CMD runs (`python -m <module>`),
    plus the brain's second one, `workflow_domains.api` (the chart starts the
    same image with uvicorn);
  * every module reachable from them through `import` statements, wherever
    they stand — module level, inside a function (a lazy import), inside
    `with workflow.unsafe.imports_passed_through():` — with every package
    `__init__.py` on the way. Static on purpose: it needs nothing installed,
    and it sees a lazy import that a dry run of the entrypoint would not.
    tests/test_image_changes.py checks the other direction, that everything
    each entrypoint REALLY loads is in this set;
  * its own Dockerfile, and every single FILE that Dockerfile copies (its
    requirements.txt).

WHEN UNSURE, BUILD WIDE. A build that was needed and skipped ships a stale
image and nothing reports it; a redundant build costs minutes. So:

  * a changed file under a folder the image copies that is NOT a Python module
    this script could read at HEAD — a data file (the UI pages' index.html), a
    deleted or renamed path, a file that does not parse — affects EVERY image
    that copies that folder;
  * .dockerignore affects every image: it decides what every build's context holds;
  * the caller treats a crash here as "every image" (build.yml).

WHAT ONLY DECIDES HOW CI RUNS IS NO INPUT OF ANY IMAGE (the operator, 2026-11-16).
Until then build.yml, the build rows and this script rebuilt every image, so a
CI-only change gave all ten a new version with the same contents: ten images to
mirror into the air-gap for nothing. Now:

  * build.yml, this script, scripts/release_notes.py and their tests affect no
    image. build.yml holds no image's build input since every image is a row of
    .github/image-builds.json (checked by tests/test_image_builds.py);
  * .github/image-builds.json affects the images whose ROW changed, the rows at
    the diff base (`--base`) compared with the rows now, as data: reformatting
    or reordering the file changes no row. A base before that file existed is
    read from its build.yml, whose jobs then held the same inputs. Rows that
    cannot be read at the base build wide: every image;
  * a fix to how this script finds an image's files therefore rebuilds nothing
    by itself: an image it under-built is rebuilt by the next change to its
    files, or on purpose by a change to .dockerignore.

The docs image has no Python: it is affected by exactly what its Dockerfile
copies.

HOW AN IMAGE IS BUILT is not this script's question, but it answers it for the
build matrices: every image is one row of .github/image-builds.json, the reusable
workflow's inputs for it. `--first` turns the list this script answered into the
brain's one-row matrix (`build-workflow-brain`: the brain ships before every other
image) and `--matrix` into the `build` job's, every other row, so the workflow never
re-derives which images to build.

Usage (the repo root is the working directory):
  git diff --name-only <base> <head> | python3 scripts/image_changes.py --base <base>
      -> a JSON list of image names on stdout, the reasons on stderr
         (without --base, a change to .github/image-builds.json builds every image)
  python3 scripts/image_changes.py --all      every image
  python3 scripts/image_changes.py --table    which images load each file
  echo '<that JSON list>' | python3 scripts/image_changes.py --first
  echo '<that JSON list>' | python3 scripts/image_changes.py --matrix
      -> the brain's / the `build` job's matrix for those images, or nothing when
         none of them is built there (GitHub refuses an empty matrix, so the job is
         skipped)
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Every image's build inputs, one row each (the reusable workflow's inputs): the brain's
# row goes to `build-workflow-brain`, every other row to the `build` matrix. A change
# here affects the images whose row changed (changed_build_rows), never all of them.
BUILDS_FILE = ".github/image-builds.json"
BUILD_YML = ".github/workflows/build.yml"

# Files whose change alters EVERY image: .dockerignore decides what every build's
# context holds. What only decides how CI runs (build.yml, this script, the notes) is
# no input of any image (the module docstring says why).
GLOBAL_TRIGGERS = (".dockerignore",)

# Entrypoints a Dockerfile's CMD does not name: the chart runs the brain image a
# second time as the API (`uvicorn workflow_domains.api:app`).
EXTRA_ENTRYPOINTS = {"workflows-orchestrator": ("workflow_domains.api",)}

BRAIN_DOCKERFILE = "workflow_domains/Dockerfile"
BRAIN_IMAGE = "workflows-orchestrator"
DOCS_DOCKERFILE = "docs/Dockerfile"
DOCS_IMAGE = "workflows-docs"

_COPY = re.compile(r"^COPY\s+(?!--from)(?P<src>\S+)\s+\S+\s*$", re.M)
_CMD_MODULE = re.compile(r'^CMD\s*\[\s*"python"\s*,\s*"-m"\s*,\s*"(?P<module>[\w.]+)"\s*\]', re.M)


@dataclass
class Image:
    name: str
    dockerfile: str
    entrypoints: tuple[str, ...] = ()
    copied_dirs: tuple[str, ...] = ()          # `COPY shared/ ...` -> "shared/"
    copied_files: tuple[str, ...] = ()         # `COPY x/requirements.txt ...`
    loaded: set[str] = field(default_factory=set)   # repo-relative .py files

    def build_files(self) -> set[str]:
        return {self.dockerfile, *self.copied_files}


def _kebab(name: str) -> str:
    return name.replace("_", "-")


def _read_dockerfile(root: Path, dockerfile: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """(copied dirs, copied files, entrypoint modules) of one Dockerfile."""
    text = (root / dockerfile).read_text(encoding="utf-8")
    dirs, files = [], []
    for match in _COPY.finditer(text):
        src = match.group("src")
        if src.endswith("/") or (root / src).is_dir():
            dirs.append(src.rstrip("/") + "/")
        else:
            files.append(src)
    modules = tuple(m.group("module") for m in _CMD_MODULE.finditer(text))
    return tuple(dirs), tuple(files), modules


def discover_images(root: Path = REPO_ROOT) -> list[Image]:
    """Every image this repo builds: the brain, one per activities/<limb>/Dockerfile, the docs."""
    dockerfiles = [(BRAIN_IMAGE, BRAIN_DOCKERFILE)]
    dockerfiles += [
        (f"{_kebab(path.parent.name)}-worker", path.relative_to(root).as_posix())
        for path in sorted((root / "activities").glob("*/Dockerfile"))
    ]
    dockerfiles.append((DOCS_IMAGE, DOCS_DOCKERFILE))
    images = []
    for name, dockerfile in dockerfiles:
        if not (root / dockerfile).is_file():
            continue
        dirs, files, modules = _read_dockerfile(root, dockerfile)
        entrypoints = modules + EXTRA_ENTRYPOINTS.get(name, ())
        if name != DOCS_IMAGE and not modules:
            raise RuntimeError(f"{dockerfile}: no `CMD [\"python\", \"-m\", \"<module>\"]` — cannot tell what the image runs")
        image = Image(name=name, dockerfile=dockerfile, entrypoints=entrypoints, copied_dirs=dirs, copied_files=files)
        image.loaded = loaded_files(root, entrypoints)
        images.append(image)
    return images


# --- the import graph ---------------------------------------------------------------------


def _module_file(root: Path, module: str) -> str | None:
    """The repo file a dotted module name is, or None when it is not this repo's."""
    base = root.joinpath(*module.split("."))
    if base.with_suffix(".py").is_file():
        return base.with_suffix(".py").relative_to(root).as_posix()
    if (base / "__init__.py").is_file():
        return (base / "__init__.py").relative_to(root).as_posix()
    return None


def _is_repo_package(root: Path, module: str) -> bool:
    return root.joinpath(*module.split(".")).is_dir()


def _module_name(path: str) -> tuple[str, bool]:
    """(dotted name, is a package __init__) of a repo-relative .py path."""
    parts = path.removesuffix(".py").split("/")
    if parts[-1] == "__init__":
        return ".".join(parts[:-1]), True
    return ".".join(parts), False


def _imported_modules(path: str, tree: ast.AST) -> set[str]:
    """Every dotted module name `path` may load, from every import statement in it."""
    name, is_package = _module_name(path)
    package = name if is_package else name.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package.split(".") if package else []
                cut = len(parts) - (node.level - 1)
                if cut < 0:
                    continue
                base = ".".join(parts[:cut] + ([node.module] if node.module else []))
            else:
                base = node.module or ""
            if not base:
                continue
            found.add(base)
            # `from pkg import name`: `name` may be a submodule, not an attribute.
            found.update(f"{base}.{alias.name}" for alias in node.names if alias.name != "*")
    return found


def loaded_files(root: Path, entrypoints: tuple[str, ...]) -> set[str]:
    """Every repo .py file reachable from the entrypoint modules through imports."""
    loaded: set[str] = set()
    queue = list(entrypoints)
    seen: set[str] = set()
    while queue:
        module = queue.pop()
        if module in seen:
            continue
        seen.add(module)
        # Importing a.b.c runs a/__init__.py and a/b/__init__.py first.
        parents = module.split(".")
        queue.extend(".".join(parents[:i]) for i in range(1, len(parents)))
        path = _module_file(root, module)
        if path is None:
            continue                      # a dependency, the stdlib, or an attribute name
        if path in loaded:
            continue
        loaded.add(path)
        try:
            tree = ast.parse((root / path).read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue                      # counted as loaded; its change builds wide (affected_images)
        queue.extend(
            name for name in _imported_modules(path, tree)
            if _module_file(root, name) is not None or _is_repo_package(root, name)
        )
    return loaded


# --- the decision -------------------------------------------------------------------------


def _readable_module(root: Path, path: str) -> bool:
    if not path.endswith(".py") or not (root / path).is_file():
        return False
    try:
        ast.parse((root / path).read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return False
    return True


def affected_images(changed: list[str], images: list[Image], root: Path = REPO_ROOT, *,
                    changed_rows: set[str] | None = None) -> dict[str, list[str]]:
    """{image name: the reasons it must be built}, for the changed repo-relative paths.

    An image absent from the result is not affected. `changed_rows` names the images
    whose row of .github/image-builds.json changed (changed_build_rows); None, when that
    file changed, is "cannot tell", and builds every image.
    """
    reasons: dict[str, list[str]] = {}
    build_files = {path: image.name for image in images for path in image.build_files()}

    def add(image: Image, why: str) -> None:
        reasons.setdefault(image.name, []).append(why)

    for path in changed:
        path = path.strip()
        if not path:
            continue
        if path in GLOBAL_TRIGGERS:
            for image in images:
                add(image, f"{path} (decides or shapes every build)")
            continue
        if path == BUILDS_FILE:
            for image in images:
                if changed_rows is None:
                    add(image, f"{path} (its rows before the change are unknown — built wide)")
                elif image.name in changed_rows:
                    add(image, f"{path} (its build row changed)")
            continue
        for image in images:
            if path in image.build_files():
                add(image, f"{path} (its own build file)")
                continue
            if not any(path.startswith(folder) for folder in image.copied_dirs):
                continue
            if path in build_files:
                continue                  # another image's Dockerfile / requirements.txt
            if not image.entrypoints:
                add(image, f"{path} (copied into the image)")
            elif not _readable_module(root, path):
                add(image, f"{path} (not a Python module readable at HEAD: a data file, a deletion or a rename — built wide)")
            elif path in image.loaded:
                add(image, f"{path} (loaded)")
    return reasons


def fan_in_table(images: list[Image]) -> str:
    """Every loaded file with the images that load it — for a person, and for the tests."""
    python_images = [image for image in images if image.entrypoints]
    files = sorted({path for image in python_images for path in image.loaded})
    width = max(len(path) for path in files)
    lines = [f"{'file':<{width}}  loaded by"]
    for path in files:
        names = [image.name for image in python_images if path in image.loaded]
        label = "EVERY image" if len(names) == len(python_images) else ", ".join(names)
        lines.append(f"{path:<{width}}  {label}")
    return "\n".join(lines)


# --- the build matrix ---------------------------------------------------------------------

# A row's keys are the reusable workflow's inputs, passed through unchanged: every
# input an image's build gets, so build.yml holds none of them.
BUILD_KEYS = ("image-name", "dockerfile", "chart-repo", "helm-values-path", "helm-image-path")


def load_builds(root: Path = REPO_ROOT) -> list[dict[str, str]]:
    """The rows of .github/image-builds.json: one per image."""
    return parse_builds((root / BUILDS_FILE).read_text(encoding="utf-8"))


def parse_builds(text: str) -> list[dict[str, str]]:
    rows = json.loads(text)
    if not isinstance(rows, list):
        raise ValueError(f"{BUILDS_FILE}: a JSON list of rows")
    for row in rows:
        if not isinstance(row, dict) or sorted(row) != sorted(BUILD_KEYS) or not all(isinstance(v, str) and v for v in row.values()):
            raise ValueError(f"{BUILDS_FILE}: each row has exactly {', '.join(BUILD_KEYS)}, non-empty strings — {row!r}")
    names = [row["image-name"] for row in rows]
    if len(set(names)) != len(names):
        raise ValueError(f"{BUILDS_FILE}: an image has two rows")
    return rows


def _refuse_missing(names: list[str], builds: list[dict[str, str]]) -> None:
    """A name without a row is an image that would never be built — refused, not skipped."""
    missing = sorted(set(names) - {row["image-name"] for row in builds})
    if missing:
        raise ValueError(f"{', '.join(missing)}: no row in {BUILDS_FILE} — no job would ever build it")


def build_matrix(names: list[str], builds: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """The `build` job's matrix for the images this script answered: their rows, in the
    file's order, but the brain's (`first_matrix`: it ships before every other image)."""
    _refuse_missing(names, builds)
    wanted = set(names) - {BRAIN_IMAGE}
    return {"include": [row for row in builds if row["image-name"] in wanted]}


def first_matrix(names: list[str], builds: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """`build-workflow-brain`'s matrix: the brain's row, when the brain is in the list."""
    _refuse_missing(names, builds)
    return {"include": [row for row in builds if row["image-name"] == BRAIN_IMAGE and BRAIN_IMAGE in names]}


# --- which rows changed -------------------------------------------------------------------

_JOB = re.compile(r"^  [A-Za-z0-9_-]+:\s*$", re.M)
_JOB_INPUT = re.compile(rf"^      ({'|'.join(BUILD_KEYS)}):[ \t]*(\S+)[ \t]*$", re.M)


def legacy_build_rows(build_yml: str) -> dict[str, dict[str, str]]:
    """The rows of the layout before .github/image-builds.json: one job per image in
    build.yml, on the reusable ghcr-build-push.yml, its inputs in the job's `with:`. Read
    so a diff from a commit of that layout compares rows too. A job whose inputs are not
    all plain values is no row; its image then counts as changed."""
    starts = [match.start() for match in _JOB.finditer(build_yml)] + [len(build_yml)]
    rows: dict[str, dict[str, str]] = {}
    for begin, end in zip(starts, starts[1:]):
        job = build_yml[begin:end]
        if "/ghcr-build-push.yml@" not in job:
            continue
        row = dict(_JOB_INPUT.findall(job))
        if set(row) == set(BUILD_KEYS):
            rows[row["image-name"]] = row
    return rows


def _show(root: Path, ref: str, path: str) -> str | None:
    done = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=root, capture_output=True, text=True, timeout=60)
    return done.stdout if done.returncode == 0 else None


def _rows_by_name(text: str) -> dict[str, dict] | None:
    """{image: its row} from the file's text, as data only: a row of another shape (an older
    set of keys) is still that image's row, and differs from today's. None: not rows."""
    try:
        rows = json.loads(text)
    except ValueError:
        return None
    if not isinstance(rows, list) or not all(isinstance(row, dict) and isinstance(row.get("image-name"), str)
                                             for row in rows):
        return None
    return {row["image-name"]: row for row in rows}


def build_rows_at(ref: str, root: Path = REPO_ROOT) -> dict[str, dict] | None:
    """{image: its row} at a commit, or None when that commit's rows cannot be read."""
    text = _show(root, ref, BUILDS_FILE)
    if text is not None:
        return _rows_by_name(text)
    legacy = _show(root, ref, BUILD_YML)
    rows = legacy_build_rows(legacy) if legacy is not None else {}
    return rows or None


def changed_build_rows(base: str, root: Path = REPO_ROOT, head: str | None = None) -> set[str] | None:
    """The images whose row differs between `base` and `head` (the working tree when None):
    added, removed or changed, compared as data, so a reformatted file changes no row. None
    when either side cannot be read: the caller then builds every image."""
    old = build_rows_at(base, root)
    try:
        new = build_rows_at(head, root) if head else {row["image-name"]: row for row in load_builds(root)}
    except (OSError, ValueError):
        new = None
    if old is None or new is None:
        return None
    return {name for name in old.keys() | new.keys() if old.get(name) != new.get(name)}


def _option(argv: list[str], name: str) -> str | None:
    return argv[argv.index(name) + 1] if name in argv and argv.index(name) + 1 < len(argv) else None


def main(argv: list[str]) -> int:
    if "--matrix" in argv or "--first" in argv:
        # Only the file and the names: nothing here walks the import graph, so the
        # workflow's every-image fallback goes through this too.
        names = json.loads(sys.stdin.read() or "[]")
        matrix = (first_matrix if "--first" in argv else build_matrix)(names, load_builds())
        print(json.dumps(matrix, separators=(",", ":")) if matrix["include"] else "")
        return 0
    images = discover_images()
    if "--table" in argv:
        print(fan_in_table(images))
        return 0
    if "--all" in argv:
        print(json.dumps([image.name for image in images]))
        return 0
    changed = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
    base = _option(argv, "--base")
    changed_rows = changed_build_rows(base) if base and BUILDS_FILE in changed else None
    reasons = affected_images(changed, images, changed_rows=changed_rows)
    for image in images:
        if image.name in reasons:
            print(f"{image.name}: BUILD", file=sys.stderr)
            for why in reasons[image.name][:20]:
                print(f"    {why}", file=sys.stderr)
            if len(reasons[image.name]) > 20:
                print(f"    … and {len(reasons[image.name]) - 20} more", file=sys.stderr)
        else:
            print(f"{image.name}: unaffected", file=sys.stderr)
    print(json.dumps([image.name for image in images if image.name in reasons]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
