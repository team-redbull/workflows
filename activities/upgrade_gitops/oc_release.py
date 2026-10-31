"""`oc adm release info` — the images a release payload references.

The `oc` client is in the limb image (its Dockerfile copies it from
quay.io/openshift/origin-cli). `-o json` prints the release's ImageStream of
references: `references.spec.tags[].from.name`, each
`<payload repository>@sha256:…` (189 of them for 4.20.35, all in
quay.io/openshift-release-dev/ocp-v4.0-art-dev; read 2026-10-31 with oc 4.22).

What a failure MEANS decides what happens to the plan, so it is classified:
  * the registry answered that an image oc needs is not there (manifest
    unknown, name unknown, not found) — ReleaseInfoError, which the plan
    reports as a payload problem ("mirror it first");
  * everything else — a timeout, oc missing, output that is not JSON, a
    refused connection, a 5xx, a refused or missing credential, or anything
    unrecognised — the retryable ReleaseRegistryError: the activity is
    retried until the registry answers, and is never read as "not mirrored".
One call per activity attempt, no retry loop inside it: Temporal retries the
activity, and the caller sizes `timeout` so every call of a plan fits its
budget (registry.oc_timeout). The process never outlives the call: a timeout
or a cancellation kills and reaps it.

Plain parameters, no settings and no Temporal (CLAUDE.md §1).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile

from shared.exceptions import ReleaseRegistryError

# The most one call ever gets; registry.oc_timeout gives less when a plan
# pins more releases than fit the registry budget at this much each.
OC_TIMEOUT_SECONDS = 120.0
_ERROR_TAIL = 600

# The registry ANSWERED: an image is not there. Only these are payload problems.
_ABSENT = re.compile(r"(?i)\b(?:manifest unknown|name unknown|image not found|not found)\b")
# No answer: oc could not ask, or was refused. These win over _ABSENT — an
# "unauthorized: ... not found" is a credential to fix, not a gap to mirror.
_UNANSWERED = re.compile(
    r"(?i)\b(?:unauthorized|authentication required|denied|forbidden|too ?many ?requests|"
    r"connection (?:refused|reset)|no such host|network is unreachable|i/o timeout|timeout|timed out|"
    r"deadline exceeded|tls|x509|eof|service unavailable|bad gateway|internal server error|"
    r"unexpected (?:http )?status(?: code)?:? 5\d\d)\b"
)


class ReleaseInfoError(Exception):
    """The registry answered that an image `oc adm release info` needs is absent; the message is oc's output's tail."""


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


async def _stop(process: asyncio.subprocess.Process) -> None:
    """Kill oc if it still runs, and reap it: an orphan per retry would pile up against the pod's memory limit."""
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    await process.wait()


async def release_references(
    pullspec: str, *, auth_file: str, verify: bool, timeout: float = OC_TIMEOUT_SECONDS
) -> list[str]:
    """The payload images of the release at `pullspec` (`<repo>@sha256:…`).

    Raises ReleaseInfoError when the registry says an image is absent, and the
    retryable ReleaseRegistryError for every other failure (module docstring).
    """
    argv = ["oc", "adm", "release", "info", "-o", "json", pullspec]
    if auth_file:
        argv += ["--registry-config", auth_file]
    if not verify:
        argv.append("--insecure=true")
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
            raise ReleaseRegistryError(f"cannot run oc: {err}") from None
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _stop(process)
            raise ReleaseRegistryError(f"oc adm release info {pullspec} timed out after {timeout:.0f}s") from None
        except asyncio.CancelledError:
            await _stop(process)
            raise
    if process.returncode == 0:
        try:
            document = json.loads(stdout)
        except ValueError:
            document = None
        if not isinstance(document, dict):
            raise ReleaseRegistryError(f"oc adm release info {pullspec} printed something that is not its JSON")
        return references_of(document)
    output = stderr.decode(errors="replace").strip()[-_ERROR_TAIL:] or f"exit {process.returncode}"
    if _ABSENT.search(output) and not _UNANSWERED.search(output):
        raise ReleaseInfoError(output)
    raise ReleaseRegistryError(f"oc adm release info {pullspec} failed (exit {process.returncode}): {output}")
