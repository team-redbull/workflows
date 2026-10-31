"""`oc adm release info` — the images a release payload references.

The `oc` client is in the limb image (its Dockerfile copies it from
quay.io/openshift/origin-cli). `-o json` prints the release's ImageStream of
references: `references.spec.tags[].from.name`, each
`<payload repository>@sha256:…` (189 of them for 4.20.35, all in
quay.io/openshift-release-dev/ocp-v4.0-art-dev; read 2026-10-31 with oc 4.22).

Plain parameters, no settings and no Temporal (CLAUDE.md §1).
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile

# One attempt gets this long; three attempts fit the 10-minute registry budget
# with the HEADs that follow (dispatch.REGISTRY_TIMEOUT).
OC_TIMEOUT_SECONDS = 120.0
# A transient registry error fails `oc` like a broken payload does; retried
# this many times before it is reported.
ATTEMPTS = 3
_BACKOFF_SECONDS = 5.0
_ERROR_TAIL = 600


class ReleaseInfoError(Exception):
    """`oc adm release info` failed every attempt; the message is its output's tail."""


def _environment(home: str) -> dict[str, str]:
    """This worker's environment for oc, minus every `*_TOKEN`, with a HOME it may write."""
    return {
        **{key: value for key, value in os.environ.items() if not key.upper().endswith("_TOKEN")},
        "HOME": home,
    }


def references_of(document: dict) -> list[str]:
    """Every image reference in `oc adm release info -o json` output, in its order."""
    tags = (((document.get("references") or {}).get("spec") or {}).get("tags")) or []
    return [
        str((tag.get("from") or {}).get("name"))
        for tag in tags
        if isinstance(tag, dict) and (tag.get("from") or {}).get("name")
    ]


async def release_references(pullspec: str, *, auth_file: str, verify: bool) -> list[str]:
    """The payload images of the release at `pullspec` (`<repo>@sha256:…`)."""
    argv = ["oc", "adm", "release", "info", "-o", "json", pullspec]
    if auth_file:
        argv += ["--registry-config", auth_file]
    if not verify:
        argv.append("--insecure=true")
    last = ""
    for attempt in range(1, ATTEMPTS + 1):
        # oc keeps caches under HOME; a pod's HOME may not be writable.
        with tempfile.TemporaryDirectory(prefix="oc-home-") as home:
            try:
                process = await asyncio.create_subprocess_exec(
                    *argv,
                    env=_environment(home),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except OSError as err:
                raise ReleaseInfoError(f"cannot run oc: {err}") from None
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=OC_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                last = f"timed out after {OC_TIMEOUT_SECONDS:.0f}s"
                continue
        if process.returncode == 0:
            try:
                return references_of(json.loads(stdout))
            except ValueError:
                last = "printed something that is not JSON"
        else:
            last = stderr.decode(errors="replace").strip()[-_ERROR_TAIL:] or f"exit {process.returncode}"
        if attempt < ATTEMPTS:
            await asyncio.sleep(_BACKOFF_SECONDS * attempt)
    raise ReleaseInfoError(last)
