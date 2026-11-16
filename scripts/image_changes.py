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
  * this script, build.yml and .dockerignore affect every image;
  * the caller treats a crash here as "every image" (build.yml).

The docs image has no Python: it is affected by exactly what its Dockerfile
copies.

Usage (the repo root is the working directory):
  git diff --name-only <base> <head> | python3 scripts/image_changes.py
      -> a JSON list of image names on stdout, the reasons on stderr
  python3 scripts/image_changes.py --all      every image
  python3 scripts/image_changes.py --table    which images load each file
"""

from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Files whose change can alter ANY image or the decision itself.
GLOBAL_TRIGGERS = (".github/workflows/build.yml", "scripts/image_changes.py", ".dockerignore")

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


def affected_images(changed: list[str], images: list[Image], root: Path = REPO_ROOT) -> dict[str, list[str]]:
    """{image name: the reasons it must be built}, for the changed repo-relative paths.

    An image absent from the result is not affected.
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


def main(argv: list[str]) -> int:
    images = discover_images()
    if "--table" in argv:
        print(fan_in_table(images))
        return 0
    if "--all" in argv:
        print(json.dumps([image.name for image in images]))
        return 0
    changed = [line for line in sys.stdin.read().splitlines() if line.strip()]
    reasons = affected_images(changed, images)
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
