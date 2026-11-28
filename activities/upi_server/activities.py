"""UPI-server activities — the worker INSIDE one UPI cluster (`upi-server-worker`). Thin.

Polls `upi_server_activity_queue(UPI_CLUSTER)`, acts on its own cluster's API as its
own ServiceAccount (node_api.py, and `oc` for node_image.py), drives BMCs over
Redfish (redfish_media.py) with this worker's per-vendor credentials, and asks
server-scan for the server through server-lifecycle's own client
(activities/server_lifecycle/server_scan.py, plain parameters — reused, not copied).

Budgets, under the workflow's timeouts (workflow_domains/upi_lifecycle/add_server_to_upi.py):
  * build_node_iso: `oc adm node-image create`, heartbeating every 20 s, killed at 30 min;
  * boot_from_virtual_media: a dozen Redfish calls, 30 s each at most;
  * await_node_joined: one heartbeating activity polling every 15 s, its clock
    (started_at) and the CSRs it approved in the heartbeat details.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from temporalio import activity

from activities.server_lifecycle import server_scan
from activities.upi_server import node_api, node_image, nodes_config, redfish_media
from shared.exceptions import BmcCredentialsMissingError, NodeJoinTimeoutError, UpiNodeApiError
from shared.models.server_lifecycle import (
    AcquiredServer,
    AllocateServerRequest,
    ReleaseServerRequest,
    ReserveServerRequest,
)
from shared.models.upi_lifecycle import (
    AllocateUpiServerRequest,
    FinishedUpiNode,
    FinishUpiNodeRequest,
    NodeIso,
    NodeIsoRequest,
    NodeJoined,
    NodeJoinRequest,
    UpiNode,
    UpiNodeInspectRequest,
    UpiNodeState,
    UpiServerLockRequest,
    VirtualMediaBoot,
    VirtualMediaBootRequest,
)
from shared.settings.upi_server import UpiServerActivitySettings

_settings = UpiServerActivitySettings()

LOCK_HOLDER = "add-server-to-upi"
OC_BIN = "oc"
_JOIN_POLL_SECONDS = 15


def _store() -> Path:
    return Path(_settings.iso_dir)


def _bmc_credentials(server: AcquiredServer) -> tuple[str, str]:
    vendor = (server.bmc_vendor or "").upper()
    username = getattr(_settings, f"{vendor.lower()}_bmc_username", "") if vendor else ""
    password = getattr(_settings, f"{vendor.lower()}_bmc_password", "") if vendor else ""
    if not username or not password:
        raise BmcCredentialsMissingError(
            f"no BMC credentials for vendor {vendor or 'unknown'} on this worker "
            f"({vendor or '<VENDOR>'}_BMC_USERNAME / _PASSWORD in Secret upi-server-credentials)"
        )
    return username, password


def _bmc_url(server: AcquiredServer) -> str:
    return redfish_media.base_url(server.bmc.scheme, server.bmc.host, server.bmc.port)


def _node_view(node: dict, hostname: str) -> UpiNode:
    return UpiNode(name=hostname, internal_ip=node_api.node_ip(node), ready=node_api.node_ready(node),
                   roles=node_api.node_roles(node))


@activity.defn
async def allocate_upi_server(request: AllocateUpiServerRequest) -> AcquiredServer:
    server = await server_scan.allocate_available_server(
        _settings.server_scan_url,
        _settings.server_scan_api_token,
        AllocateServerRequest(
            name=request.server_name,
            holder=LOCK_HOLDER,
            workflow_id=request.workflow_id,
            mce_cluster=request.cluster,
            ttl_seconds=request.ttl_seconds,
        ),
    )
    activity.logger.info("allocated %s (%s, BMC %s %s) for %s", server.name, server.id, server.bmc_vendor,
                         server.bmc.host, request.cluster)
    return server


@activity.defn
async def extend_upi_server_lock(request: UpiServerLockRequest) -> None:
    await server_scan.reserve_server(
        _settings.server_scan_url,
        _settings.server_scan_api_token,
        ReserveServerRequest(
            server_id=request.server.id, server_name=request.server.name, holder=LOCK_HOLDER,
            workflow_id=request.workflow_id, mce_cluster=request.cluster, ttl_seconds=request.ttl_seconds,
        ),
    )
    activity.logger.info("extended the lock on %s for %d s", request.server.name, request.ttl_seconds)


@activity.defn
async def release_upi_server(request: UpiServerLockRequest) -> None:
    await server_scan.release_server(
        _settings.server_scan_url,
        _settings.server_scan_api_token,
        ReleaseServerRequest(server_id=request.server.id, holder=LOCK_HOLDER, workflow_id=request.workflow_id),
    )
    activity.logger.info("released the lock on %s", request.server.name)


@activity.defn
async def inspect_upi_node(request: UpiNodeInspectRequest) -> UpiNodeState:
    node = await node_api.get_node(request.hostname)
    return UpiNodeState(exists=node is not None, ready=node_api.node_ready(node))


@activity.defn
async def build_node_iso(request: NodeIsoRequest) -> NodeIso:
    text = nodes_config.render(nodes_config.nodes_config(
        hostname=request.hostname,
        root_device=request.root_device or _settings.default_root_device,
        members=request.members,
        bond_mode=request.bond_mode,
        vlan_id=request.vlan_id,
    ))
    sha = nodes_config.config_hash(text)
    store = _store()
    iso = node_image.existing(store, request.hostname, sha)
    built = iso is None
    if iso is None:
        activity.logger.info("building the node-image ISO for %s (vlan %d, %s)", request.hostname, request.vlan_id,
                             request.bond_mode)
        iso = await node_image.build(
            oc_bin=OC_BIN, store=store, hostname=request.hostname, config_text=text, config_sha256=sha,
            registry_config=_settings.node_image_registry_config, insecure=_settings.node_image_insecure,
            heartbeat=lambda: activity.heartbeat(request.hostname),
        )
    name = node_image.iso_name(request.hostname)
    return NodeIso(file_name=name, url=f"{_settings.iso_base_url.rstrip('/')}/{name}", config_sha256=sha,
                   size_bytes=iso.stat().st_size, built=built)


@activity.defn
async def boot_from_virtual_media(request: VirtualMediaBootRequest) -> VirtualMediaBoot:
    username, password = _bmc_credentials(request.server)
    system_path, media_path, reset_type = await redfish_media.insert_and_boot(
        url=_bmc_url(request.server), username=username, password=password,
        system_hint=request.server.bmc.path, iso_url=request.iso_url,
    )
    activity.logger.info("%s: %s inserted at %s, one-time boot to CD, %s", request.server.name, request.iso_url,
                         media_path, reset_type)
    return VirtualMediaBoot(system_path=system_path, media_path=media_path, reset_type=reset_type)


@activity.defn
async def await_node_joined(request: NodeJoinRequest) -> NodeJoined:
    hostname = request.hostname
    details = activity.info().heartbeat_details
    started_at = datetime.fromisoformat(details[0]) if details else datetime.now(UTC)
    approved: list[str] = list(details[1]) if details and len(details) > 1 else []
    last = "no Node yet"
    while True:
        try:
            for csr in await node_api.list_csrs():
                if node_api.is_pending(csr) and node_api.csr_is_for_host(csr, hostname):
                    await node_api.approve_csr(csr, hostname)
                    approved.append(csr["metadata"]["name"])
                    activity.logger.info("approved CSR %s for %s", csr["metadata"]["name"], hostname)
            node = await node_api.get_node(hostname)
            if node_api.node_ready(node):
                waited = int((datetime.now(UTC) - started_at).total_seconds())
                return NodeJoined(node=_node_view(node, hostname), csrs_approved=approved, waited_seconds=waited)
            last = "a Node, not Ready yet" if node else "no Node yet"
        except UpiNodeApiError as err:
            last = str(err)
        if (datetime.now(UTC) - started_at).total_seconds() > request.timeout_seconds:
            raise NodeJoinTimeoutError(
                f"{hostname} did not become a Ready Node within {request.timeout_seconds // 60} min: {last[:300]}; "
                f"CSRs approved: {', '.join(approved) or 'none'}. Look at the server's console (it boots the "
                "node-image ISO, writes the disk and reboots into the cluster)"
            )
        activity.heartbeat(started_at.isoformat(), approved[-50:])
        await asyncio.sleep(_JOIN_POLL_SECONDS)


@activity.defn
async def finish_upi_node(request: FinishUpiNodeRequest) -> FinishedUpiNode:
    notes: list[str] = []
    labelled = True
    if request.node_labels:
        labels = await node_api.label_node(request.hostname, request.node_labels)
        wrong = {k: v for k, v in request.node_labels.items() if labels.get(k) != v}
        if wrong:
            raise UpiNodeApiError(f"Node {request.hostname}'s labels did not read back: {sorted(wrong)}")
    ejected = False
    try:
        username, password = _bmc_credentials(request.server)
        ejected = await redfish_media.eject(url=_bmc_url(request.server), username=username, password=password,
                                            system_hint=request.server.bmc.path)
    except Exception as err:  # best effort: the node is in; a person ejects by hand  # noqa: BLE001
        notes.append(f"virtual media not ejected: {type(err).__name__}: {str(err)[:200]}")
    try:
        removed = node_image.remove(_store(), request.hostname)
    except OSError as err:
        removed = False
        notes.append(f"ISO not removed: {err.strerror}")
    activity.logger.info("finished %s: labels %s, media ejected %s, ISO removed %s", request.hostname,
                         sorted(request.node_labels), ejected, removed)
    return FinishedUpiNode(labelled=labelled, media_ejected=ejected, iso_removed=removed, notes=notes)
