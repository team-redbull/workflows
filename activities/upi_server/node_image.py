"""`oc adm node-image create` and the ISO store the BMCs fetch from.

Plain parameters, no settings, no Temporal (CLAUDE.md §1); the caller passes a
`heartbeat` callable so the subprocess wait keeps the activity alive.

`oc` runs as the worker's own ServiceAccount (in-cluster config: no kubeconfig
here). node-image creates a temporary namespace and pod in the cluster to build
the ISO, so the chart grants the worker what that needs (its README says which).

THE STORE: ISO_DIR, a volume the chart shares with an nginx sidecar that serves it
at ISO_BASE_URL. `<hostname>.iso` appears only complete (built in a scratch folder,
then renamed), next to `<hostname>.sha256` holding the nodes-config hash it was
built from: the same hash = nothing to build (a retry), another = rebuilt.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

from shared.exceptions import NodeImageBuildError, UpiNodeApiError

BUILD_TIMEOUT_SECONDS = 30 * 60
HEARTBEAT_EVERY_SECONDS = 20
MAX_STDERR_CHARS = 1500


def iso_name(hostname: str) -> str:
    return f"{hostname}.iso"


def _hash_file(store: Path, hostname: str) -> Path:
    return store / f"{hostname}.sha256"


def existing(store: Path, hostname: str, config_sha256: str) -> Path | None:
    """The ISO already built for exactly this config, if any."""
    iso = store / iso_name(hostname)
    marker = _hash_file(store, hostname)
    if iso.is_file() and marker.is_file() and marker.read_text().strip() == config_sha256:
        return iso
    return None


async def build(
    *,
    oc_bin: str,
    store: Path,
    hostname: str,
    config_text: str,
    config_sha256: str,
    registry_config: str,
    insecure: bool,
    heartbeat: Callable[[], None],
) -> Path:
    """Build the ISO for `config_text` into the store; return its path."""
    store.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".build-{hostname}-", dir=store) as tmp:
        work = Path(tmp)
        (work / "nodes-config.yaml").write_text(config_text, encoding="utf-8")
        command = [oc_bin, "adm", "node-image", "create", f"--dir={work}"]
        if registry_config:
            command.append(f"--registry-config={registry_config}")
        if insecure:
            command.append("--insecure=true")
        env = {**os.environ, "HOME": str(work)}
        process = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE, env=env
        )
        stderr_task = asyncio.create_task(process.stderr.read() if process.stderr else asyncio.sleep(0, b""))
        waited = 0
        while True:
            try:
                await asyncio.wait_for(asyncio.shield(process.wait()), timeout=HEARTBEAT_EVERY_SECONDS)
                break
            except TimeoutError:
                waited += HEARTBEAT_EVERY_SECONDS
                heartbeat()
                if waited >= BUILD_TIMEOUT_SECONDS:
                    process.kill()
                    await process.wait()
                    stderr_task.cancel()
                    raise UpiNodeApiError(
                        f"oc adm node-image create for {hostname} ran over {BUILD_TIMEOUT_SECONDS // 60} min; "
                        "killed, retried from the start"
                    ) from None
        stderr = (await stderr_task or b"").decode("utf-8", "replace")
        if process.returncode != 0:
            raise NodeImageBuildError(
                f"oc adm node-image create for {hostname} exited {process.returncode}: "
                f"{stderr.strip()[-MAX_STDERR_CHARS:]}"
            )
        isos = sorted(work.glob("*.iso"), key=lambda p: p.stat().st_mtime)
        if not isos:
            raise NodeImageBuildError(f"oc adm node-image create for {hostname} wrote no .iso into its folder")
        target = store / iso_name(hostname)
        os.replace(isos[-1], target)
        _hash_file(store, hostname).write_text(config_sha256 + "\n")
        return target


def remove(store: Path, hostname: str) -> bool:
    """Delete the host's ISO and its marker; True when something was there."""
    found = False
    for path in (store / iso_name(hostname), _hash_file(store, hostname)):
        if path.exists():
            path.unlink()
            found = True
    return found


def oc_available(oc_bin: str) -> bool:
    return shutil.which(oc_bin) is not None
