"""Server-lifecycle activity implementations — the execution limbs.

These run in the `server-lifecycle-worker` deployment, ONE PER MCE CLUSTER. This
module is deliberately thin: it is the @activity.defn surface and nothing else,
so what each activity does is readable in one screen. The work itself lives in
the modules beside it, each owning one technology and taking plain
parameters, which is what makes them testable without a Temporal environment:

  * server_scan.py    — the inventory read and the install lock (httpx against
                        SERVER_SCAN_URL).
  * pxe_map.py        — the site PXE VM's MAC -> iPXE map, for IPMI servers
                        (httpx against PXE_MAP_URLS[site]).
  * cluster_api.py    — every call to the target cluster's Kubernetes API,
                        including the idempotency rule and error classification.
  * bmh_resources.py  — the three resource BODIES, as pure functions.
  * hosted_resources.py — what uninstall-server-from-hc reads off CAPI Machines,
                        AgentMachines and HostedClusters, as pure functions.
  * hosted_cluster_api.py — the one read of a HOSTED cluster's own API server
                        (its Node), through its admin kubeconfig.
  * machine_selection.py — what scale-down-nodepool reads off a NodePool's
                        Machines, Nodes and Pods, and its delete-mark patch,
                        as pure functions.

This module owns what only an activity can: the settings instance (read once at
import, so a missing key crash-loops the worker instead of failing a run) and
`activity.logger`.

BMC credentials are the one thing server-scan never returns — it holds inventory
data only — so they come from this deployment's own Secret, per vendor, exactly
as the operator's did.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from temporalio import activity

from activities.server_lifecycle import (
    cluster_api,
    hosted_cluster_api,
    hosted_resources,
    machine_selection,
    pxe_map,
    server_scan,
)
from activities.server_lifecycle.bmh_resources import (
    AGENT_GROUP,
    AGENT_PLURAL,
    AGENT_VERSION,
    BMH_DEPROVISION_FINALIZERS,
    BMH_GROUP,
    BMH_PLURAL,
    BMH_VERSION,
    DETACHED_ANNOTATION,
    INFRAENV_GROUP,
    INFRAENV_PLURAL,
    INFRAENV_VERSION,
    NMSTATE_GROUP,
    NMSTATE_PLURAL,
    NMSTATE_VERSION,
    agent_belongs_to_host,
    agent_cluster_deployment,
    agent_matches_macs,
    baremetal_host_differences,
    build_baremetal_host,
    build_bmc_secret,
    build_nmstate_config,
    host_macs,
    infraenv_image_created_at,
    infraenv_image_not_ready_reason,
    infraenv_image_wait_reason,
    infraenv_ipxe_script_url,
    nmstate_config_differences,
)
from activities.server_lifecycle.server_scan import fetch_available_servers
# update-nodepool's day1 reader and replicas editor: the cluster-upgrade hub limb's
# technology module, called with this limb's own DAY1_* settings.
from activities.upgrade_gitops import day1_repo
from shared.bmc_address import k8s_resource_name, nmstate_config_names
from shared.exceptions import (
    AgentBindingChangedError,
    AgentBoundToClusterError,
    BmcCredentialsMissingError,
    BmhConflictError,
    BmhResourceError,
    BmhTeardownError,
    Day1ChangeRejectedError,
    Day1NotConfiguredError,
    HostedClusterAccessError,
    InfraEnvNotFoundError,
    InfraEnvNotReadyError,
    MachineMarkConflictError,
    NodeMetricsPendingError,
    PxeSiteNotConfiguredError,
)
from shared.models.cluster_upgrade import Day1ChangeKind, Day1ChangeRequest, Day1Cluster, Day1ClusterRequest, Day1Commit
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    AgentBinding,
    AgentBindingState,
    AgentDeleteRequest,
    AgentDeletion,
    AgentMachineCleanup,
    AgentMachineCleanupRequest,
    AgentMachineRef,
    AgentMachineState,
    AgentMarkRequest,
    AgentMarks,
    AgentNameRef,
    AgentRef,
    AgentRelease,
    AgentReleaseRequest,
    AgentState,
    AllocateServerRequest,
    BmcSecretRef,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    BoundHostState,
    CreatedResource,
    HostAgent,
    HostDeleteRequest,
    HostDeletion,
    HostedClusterRef,
    HostedNode,
    HostedNodeAction,
    HostedNodeDeletion,
    HostedNodeDrain,
    HostedNodeQuery,
    HostedNodeRef,
    HostedNodes,
    HostedNodeState,
    HostPowerRequest,
    InfraEnvBootRequest,
    InfraEnvBootState,
    InfraEnvRef,
    InfraEnvState,
    InstallCandidateRecord,
    InventoryHostState,
    MachineDeletion,
    MachineHookRelease,
    MachineMarkRequest,
    MachineMarks,
    MachineRef,
    MachineState,
    MarkedMachinesOutcome,
    MarkedMachinesWatch,
    MarkedMachinesWatchRequest,
    NmStateConfigReplacement,
    NmStateConfigReplaceRequest,
    PxeBootRegistration,
    PxeBootRequest,
    PxeHostEntriesRef,
    PxeHostEntriesRemoval,
    PxeMapRef,
    PxeUnregistration,
    ReleaseServerRequest,
    ReserveServerRequest,
    ScaleDownCandidates,
    ScaleDownCandidatesRequest,
    ScaleDownPolicy,
    ServerMaintenance,
    ServerMaintenanceRequest,
    ServerMembership,
    ServerMembershipRequest,
    ServerReservation,
    TeardownResult,
)
from shared.settings.server_lifecycle import ServerLifecycleActivitySettings

_settings = ServerLifecycleActivitySettings()

_BMH_KIND = "BareMetalHost"
_NMSTATE_KIND = "NMStateConfig"
_AGENT_KIND = "Agent"
_INFRAENV_KIND = "InfraEnv"

# How long teardown waits for the BareMetalHost to actually disappear after the
# delete, before dropping the finalizer itself. Short on purpose: the object is
# detached first, so a host metal3 can clean up goes almost at once, and one it
# cannot is not going to become cleanable by waiting — measured stuck past four
# minutes on an unreachable BMC.
_TEARDOWN_GONE_TIMEOUT = 30.0
_TEARDOWN_POLL_SECONDS = 3.0


def _bmc_credentials(bmc_vendor: str) -> tuple[str, str]:
    """This worker's configured BMC credentials for one vendor."""
    credentials = {
        "HP": (_settings.hp_bmc_username, _settings.hp_bmc_password),
        "DELL": (_settings.dell_bmc_username, _settings.dell_bmc_password),
        "CISCO": (_settings.cisco_bmc_username, _settings.cisco_bmc_password),
        "INTERSIGHT": (
            _settings.intersight_bmc_username,
            _settings.intersight_bmc_password,
        ),
    }
    username, password = credentials.get(bmc_vendor.upper(), ("", ""))
    if not username or not password:
        raise BmcCredentialsMissingError(
            f"No BMC credentials configured for vendor {bmc_vendor}: set "
            f"{bmc_vendor.upper()}_BMC_USERNAME and {bmc_vendor.upper()}_BMC_PASSWORD"
        )
    return username, password


def _log_waived(request: BmhResourceRequest) -> None:
    """Say, on every create of such an install, which health refusals the request waived."""
    if request.allowed_unhealthy:
        activity.logger.warning(
            "%s is installed with health checks WAIVED on request (allow_unhealthy): %s",
            request.server_name,
            "; ".join(f"{w.check}: {w.observed}" for w in request.allowed_unhealthy),
        )


def _log_outcome(resource: CreatedResource) -> CreatedResource:
    """Say whether this run wrote the resource or converged on an existing one."""
    if resource.changed:
        activity.logger.info("Created %s %s", resource.kind, resource.name)
    else:
        activity.logger.info(
            "%s %s already exists and matches — nothing to do",
            resource.kind,
            resource.name,
        )
    return resource


@activity.defn
async def acquire_servers(request: AcquireServerRequest) -> list[AcquiredServer]:
    """Ask server-scan for assignable candidates (GET /servers/available)."""
    servers = await fetch_available_servers(
        _settings.server_scan_url, _settings.server_scan_api_token, request
    )
    activity.logger.info(
        "server-scan returned %d candidate(s) of %d requested",
        len(servers),
        request.count,
    )
    return servers


# How often allocate_server tells Temporal it is alive while server-scan
# live-checks candidates: well inside the workflow's 2-minute heartbeat_timeout,
# so a dead worker is noticed in minutes rather than at the 30-minute timeout.
_ALLOCATE_HEARTBEAT_SECONDS = 20.0


@activity.defn
async def allocate_server(request: AllocateServerRequest) -> AcquiredServer:
    """ONE server for this run, locked and live-checked by server-scan (POST /servers/available)."""
    allocation = asyncio.ensure_future(
        server_scan.allocate_available_server(
            _settings.server_scan_url, _settings.server_scan_api_token, request
        )
    )
    try:
        while True:
            done, _ = await asyncio.wait({allocation}, timeout=_ALLOCATE_HEARTBEAT_SECONDS)
            if done:
                break
            activity.heartbeat()
    finally:
        # Cancelled or failed: closing the client ends the request, and
        # server-scan releases a candidate it was still checking.
        if not allocation.done():
            allocation.cancel()
    server = allocation.result()
    activity.logger.info(
        "server-scan allocated %s (%s) to %s, live-checked=%s",
        server.name,
        server.id,
        request.workflow_id,
        server.live_recheck_performed,
    )
    return server


@activity.defn
async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
    """Take server-scan's install lock on one chosen candidate (ADR-0035)."""
    reservation = await server_scan.reserve_server(
        _settings.server_scan_url, _settings.server_scan_api_token, request
    )
    activity.logger.info(
        "Reserved %s in server-scan for MCE %s until %s",
        request.server_name,
        request.mce_cluster,
        reservation.expires_at,
    )
    return reservation


@activity.defn
async def lookup_install_candidate(request: ServerMembershipRequest) -> InstallCandidateRecord:
    """server-scan's stored record of a named server /servers/available refused."""
    record = await server_scan.install_candidate_by_name(
        _settings.server_scan_url, _settings.server_scan_api_token, request.server_name
    )
    membership = record.membership
    activity.logger.info(
        "server-scan's stored record of %s (refused by /servers/available): found=%s id=%s "
        "state=%s cluster=%s mce=%s contested=%s maintenance=%s reachable=%s health=%s "
        "lock=%s",
        request.server_name,
        membership.found,
        membership.server_id,
        membership.lifecycle_state,
        membership.cluster_name,
        membership.mce_name,
        membership.contested_with,
        membership.maintenance,
        record.reachable,
        record.health,
        record.lock.model_dump() if record.lock.live else "none live",
    )
    return record


@activity.defn
async def release_server(request: ReleaseServerRequest) -> ServerReservation:
    """Give an install lock back: a rolled-back candidate's, or a server's leaving its MCE."""
    reservation = await server_scan.release_server(
        _settings.server_scan_url, _settings.server_scan_api_token, request
    )
    activity.logger.info(
        "Released %s in server-scan%s",
        request.server_name,
        f" — {reservation.detail}" if reservation.detail else "",
    )
    return reservation


@activity.defn
async def put_server_in_maintenance(request: ServerMaintenanceRequest) -> ServerMaintenance:
    """Put a server that produced no Agent in maintenance in server-scan."""
    maintenance = await server_scan.enable_maintenance(
        _settings.server_scan_url, _settings.server_scan_api_token, request
    )
    activity.logger.info(
        "%s %s in server-scan%s",
        request.server_name,
        "put in maintenance" if maintenance.enabled else "not put in maintenance",
        f" — {maintenance.detail}" if maintenance.detail else "",
    )
    return maintenance


async def _read(group: str, version: str, plural: str, kind: str, namespace: str, name: str):
    return await cluster_api.read_custom_object(
        group=group, version=version, plural=plural, kind=kind, namespace=namespace, name=name
    )


async def _read_nmstate_configs(namespace: str, server_name: str) -> list[dict]:
    """A server's NMStateConfigs: under its own name, and under the name used until 2026-11-25.

    Usually one. Both names are looked under wherever a server's NMStateConfig is
    read or deleted, because nothing renames the object of a host installed
    before (shared/bmc_address.nmstate_config_names).
    """
    found = []
    for name in nmstate_config_names(server_name):
        nmstate = await _read(
            NMSTATE_GROUP, NMSTATE_VERSION, NMSTATE_PLURAL, _NMSTATE_KIND, namespace, name
        )
        if nmstate is not None:
            found.append(nmstate)
    return found


async def _delete_nmstate_configs(namespace: str, server_name: str) -> list[str]:
    """Delete a server's NMStateConfig under both of its names; which were there."""
    removed = []
    for name in nmstate_config_names(server_name):
        if await cluster_api.delete_custom_object(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            plural=NMSTATE_PLURAL,
            kind=_NMSTATE_KIND,
            namespace=namespace,
            name=name,
        ):
            removed.append(f"{_NMSTATE_KIND}/{name}")
    return removed


def _created_at(resource: dict) -> str:
    """A resource's creationTimestamp, for ordering: RFC 3339 in UTC sorts as text."""
    return (resource.get("metadata") or {}).get("creationTimestamp") or ""


def _all_host_macs(bmh: dict | None, nmstates: list[dict]) -> set[str]:
    """Every MAC the host's BareMetalHost and its NMStateConfigs name."""
    macs = set(host_macs(bmh, None))
    for nmstate in nmstates:
        macs |= set(host_macs(None, nmstate))
    return macs


async def _list_agents(namespace: str) -> list[dict]:
    return await cluster_api.list_custom_objects(
        group=AGENT_GROUP,
        version=AGENT_VERSION,
        plural=AGENT_PLURAL,
        kind=_AGENT_KIND,
        namespace=namespace,
    )


def _refuse_if_bound(agent: dict, namespace: str) -> None:
    bound_to = agent_cluster_deployment(agent)
    if bound_to:
        name = (agent.get("metadata") or {}).get("name")
        raise AgentBoundToClusterError(
            f"Agent {namespace}/{name} is bound to cluster {bound_to}: it is not "
            "just inventory, so nothing was changed. Remove it from that cluster first"
        )


@activity.defn
async def inspect_inventory_host(ref: BmhRef) -> InventoryHostState:
    """What one server has on this MCE — BMH, NMStateConfig, Agents. Reads only."""
    bmh_name = k8s_resource_name(ref.server_name)
    bmh = await _read(BMH_GROUP, BMH_VERSION, BMH_PLURAL, _BMH_KIND, ref.namespace, bmh_name)
    nmstates = await _read_nmstate_configs(ref.namespace, ref.server_name)
    # server-scan's MACs find an Agent with no BMH / NMStateConfig left to
    # take a MAC from.
    scan_macs = await server_scan.server_macs_by_name(
        _settings.server_scan_url, _settings.server_scan_api_token, ref.server_name
    )
    macs = sorted(_all_host_macs(bmh, nmstates) | set(scan_macs))
    agents = [
        AgentBinding(
            name=(agent.get("metadata") or {}).get("name", ""),
            cluster_deployment=agent_cluster_deployment(agent),
        )
        for agent in await _list_agents(ref.namespace)
        if agent_belongs_to_host(agent, bmh_name, macs)
    ]
    state = InventoryHostState(
        bmh_found=bmh is not None,
        nmstate_found=bool(nmstates),
        powered_on=((bmh or {}).get("status") or {}).get("poweredOn"),
        agents=agents,
        server_scan_macs=scan_macs,
        # Always set explicitly — the workflow keys on its presence.
        macs=macs,
    )
    activity.logger.info(
        "%s in %s: bmh=%s nmstate=%s poweredOn=%s agents=%s server-scan macs=%d",
        bmh_name,
        ref.namespace,
        state.bmh_found,
        state.nmstate_found,
        state.powered_on,
        [(a.name, a.cluster_deployment) for a in agents],
        len(scan_macs),
    )
    return state


@activity.defn
async def remove_host_pxe_entries(ref: PxeHostEntriesRef) -> PxeHostEntriesRemoval:
    """Clear a removed host's MACs from its site's PXE VM, except another MCE's entries."""
    if ref.site is not None:
        # Its own site only — the VM add-server-to-mce mapped it on.
        base_url = _settings.pxe_map_urls.get(ref.site)
        if not base_url:
            detail = (
                f"no PXE map host is configured for site {ref.site!r} on this worker "
                f"(PXE_MAP_URLS: {sorted(_settings.pxe_map_urls)}); nothing to clear"
            )
            activity.logger.info("%s: %s", ref.server_name, detail)
            return PxeHostEntriesRemoval(detail=detail)
        targets = {ref.site: base_url}
    else:
        # A run recorded before `remove-server-pxe-own-site`: every VM.
        targets = dict(_settings.pxe_map_urls)
    if not targets:
        detail = "no PXE map host is configured on this worker (PXE_MAP_URLS); nothing to clear"
        activity.logger.info("%s: %s", ref.server_name, detail)
        return PxeHostEntriesRemoval(detail=detail)

    keys = sorted({pxe_map.map_key(mac) for mac in ref.macs})
    cleared: list[str] = []
    kept: list[str] = []
    for site, base_url in sorted(targets.items()):
        for key in keys:
            reason = await pxe_map.delete_mapping_for_mce(
                base_url, _settings.pxe_map_token, key, ref.mce_cluster
            )
            if reason is None:
                cleared.append(f"{site}/{key}")
            else:
                kept.append(f"{site}/{key}: {reason}")
    activity.logger.info(
        "PXE map for %s (MCE %s): cleared %s, kept %s",
        ref.server_name,
        ref.mce_cluster,
        cleared,
        kept,
    )
    return PxeHostEntriesRemoval(cleared=cleared, kept=kept)


@activity.defn
async def set_host_online(request: HostPowerRequest) -> bool:
    """Set a BareMetalHost's spec.online, after re-checking its Agents are unbound."""
    for name in request.agents_must_be_unbound:
        agent = await _read(
            AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, request.namespace, name
        )
        if agent is not None:
            _refuse_if_bound(agent, request.namespace)

    bmh_name = k8s_resource_name(request.server_name)
    patched = await cluster_api.patch_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=request.namespace,
        name=bmh_name,
        body={"spec": {"online": request.online}},
    )
    activity.logger.info(
        "BareMetalHost %s/%s online=%s%s",
        request.namespace,
        bmh_name,
        request.online,
        "" if patched else " (absent — nothing to do)",
    )
    return patched


@activity.defn
async def delete_agents(request: AgentDeleteRequest) -> AgentDeletion:
    """Delete Agents, each re-checked as still unbound right before its delete."""
    deleted: list[str] = []
    absent: list[str] = []
    for name in request.names:
        agent = await _read(
            AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, request.namespace, name
        )
        if agent is None:
            absent.append(name)
            continue
        _refuse_if_bound(agent, request.namespace)
        if await cluster_api.delete_custom_object(
            group=AGENT_GROUP,
            version=AGENT_VERSION,
            plural=AGENT_PLURAL,
            kind=_AGENT_KIND,
            namespace=request.namespace,
            name=name,
        ):
            deleted.append(name)
        else:
            absent.append(name)
    activity.logger.info(
        "Agents in %s: deleted %s, already absent %s", request.namespace, deleted, absent
    )
    return AgentDeletion(deleted=deleted, absent=absent)


@activity.defn
async def delete_host_resources(request: HostDeleteRequest) -> HostDeletion:
    """Delete a host's NMStateConfig and BareMetalHost; metal3's finalizer is metal3's."""
    for name in request.agents_must_be_unbound:
        agent = await _read(
            AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, request.namespace, name
        )
        if agent is not None:
            _refuse_if_bound(agent, request.namespace)

    namespace = request.namespace
    bmh_name = k8s_resource_name(request.server_name)
    # The Secret the host itself names, read while it still exists: it is
    # deleted only once the host is gone, because metal3 needs it to power the
    # host off on its way out.
    bmh = await _read(BMH_GROUP, BMH_VERSION, BMH_PLURAL, _BMH_KIND, namespace, bmh_name)
    bmc_secret = (((bmh or {}).get("spec") or {}).get("bmc") or {}).get("credentialsName")

    removed: list[str] = await _delete_nmstate_configs(namespace, request.server_name)
    # No `detached` annotation and no finalizer touched: metal3 deprovisions
    # the host — powers it off, deletes its ironic node — before it lets the
    # object go, and the workflow waits for exactly that.
    if await cluster_api.delete_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=namespace,
        name=bmh_name,
    ):
        removed.append(f"{_BMH_KIND}/{bmh_name}")
    activity.logger.info(
        "Deleted %s in %s, metal3 left to finish; BMC Secret %s kept until the host is gone",
        removed or ["nothing (already absent)"],
        namespace,
        bmc_secret,
    )
    return HostDeletion(removed=removed, bmc_secret=bmc_secret or None)


@activity.defn
async def delete_bmc_secret(ref: BmcSecretRef) -> bool:
    """Delete a BMC Secret once its BareMetalHost is gone; False when already absent."""
    deleted = await cluster_api.delete_secret_if_present(ref.namespace, ref.name)
    activity.logger.info(
        "BMC Secret %s/%s %s",
        ref.namespace,
        ref.name,
        "deleted: it outlived its BareMetalHost" if deleted else "already gone with its host",
    )
    return deleted


@activity.defn
async def check_infra_env(ref: InfraEnvRef) -> InfraEnvState:
    """Confirm the InfraEnv the run fills exists on this MCE."""
    infra_env = await cluster_api.read_custom_object(
        group=INFRAENV_GROUP,
        version=INFRAENV_VERSION,
        plural=INFRAENV_PLURAL,
        kind=_INFRAENV_KIND,
        namespace=ref.namespace,
        name=ref.name,
    )
    if infra_env is None:
        raise InfraEnvNotFoundError(
            f"InfraEnv {ref.name} does not exist in namespace {ref.namespace} on "
            f"MCE {_settings.mce_cluster}: create it, or fix infra_env / namespace "
            "in the request"
        )
    spec = infra_env.get("spec") or {}
    cluster_ref = (spec.get("clusterRef") or {}).get("name")
    labels = spec.get("agentLabels")
    agent_labels = (
        {str(key): str(value) for key, value in labels.items()} if isinstance(labels, dict) else {}
    )
    activity.logger.info(
        "InfraEnv %s/%s exists%s, agentLabels %s",
        ref.namespace,
        ref.name,
        f" (early-bound to cluster {cluster_ref})" if cluster_ref else "",
        agent_labels or "none",
    )
    return InfraEnvState(
        name=ref.name,
        namespace=ref.namespace,
        cluster_ref=cluster_ref,
        agent_labels=agent_labels,
    )


@activity.defn
async def unregister_pxe_boot(ref: PxeMapRef) -> PxeUnregistration:
    """Remove a rolled-back IPMI candidate's MACs from its site's PXE VM."""
    base_url = _settings.pxe_map_urls.get(ref.site)
    if not base_url:
        detail = (
            f"no PXE map host configured for site {ref.site!r} any more; the "
            f"entries of {ref.server_name} were left in place"
        )
        activity.logger.warning("%s", detail)
        return PxeUnregistration(detail=detail)

    removed: list[str] = []
    absent: list[str] = []
    for mac in ref.macs:
        if await pxe_map.delete_mapping(base_url, _settings.pxe_map_token, mac):
            removed.append(mac)
        else:
            absent.append(mac)
    activity.logger.info(
        "Unmapped %s on the %s PXE VM %s: removed %s, already absent %s",
        ref.server_name,
        ref.site,
        base_url,
        removed,
        absent,
    )
    return PxeUnregistration(removed=removed, absent=absent)


def _pxe_map_url(site: str, needed_by: str) -> str:
    """The site's PXE VM. Checked FIRST by every PXE activity: a missing entry
    is the candidate's skip, and costs nothing on the cluster to find out."""
    base_url = _settings.pxe_map_urls.get(site)
    if not base_url:
        raise PxeSiteNotConfiguredError(
            f"{needed_by}, but no PXE map host is configured for site "
            f"{site!r}: add it to PXE_MAP_URLS (configured: "
            f"{sorted(_settings.pxe_map_urls)})"
        )
    return base_url


def _boots_from_network(server_name: str) -> str:
    return f"{server_name} boots over IPMI from the network"


async def _read_infra_env(namespace: str, name: str) -> dict:
    infra_env = await cluster_api.read_custom_object(
        group=INFRAENV_GROUP,
        version=INFRAENV_VERSION,
        plural=INFRAENV_PLURAL,
        kind=_INFRAENV_KIND,
        namespace=namespace,
        name=name,
    )
    if infra_env is None:
        raise InfraEnvNotFoundError(
            f"InfraEnv {name} does not exist in {namespace}, "
            "so there is no iPXE script to boot from"
        )
    return infra_env


@activity.defn
async def register_pxe_boot(request: PxeBootRequest) -> PxeBootRegistration:
    """Map an IPMI server's MACs to the InfraEnv's boot entry (or iPXE script) on the site's PXE VM."""
    base_url = _pxe_map_url(request.site, _boots_from_network(request.server_name))

    if request.mce_cluster:
        # The boot entry this points at is published by publish_infra_env_boot
        # once the host's NMStateConfig is in the image; no InfraEnv read here.
        for mac in request.macs:
            await pxe_map.put_mapping_to_infra_env(
                base_url,
                _settings.pxe_map_token,
                mac,
                request.mce_cluster,
                request.infra_env,
            )
        activity.logger.info(
            "Mapped %s (%s) to boot entry %s/%s on the %s PXE VM %s",
            request.server_name,
            request.macs,
            request.mce_cluster,
            request.infra_env,
            request.site,
            base_url,
        )
        return PxeBootRegistration(
            site=request.site, pxe_map_url=base_url, macs=list(request.macs)
        )

    infra_env = await _read_infra_env(request.namespace, request.infra_env)
    ipxe_url = infraenv_ipxe_script_url(infra_env)
    if ipxe_url is None:
        raise InfraEnvNotReadyError(
            f"InfraEnv {request.infra_env} publishes no "
            "status.bootArtifacts.ipxeScript yet"
        )

    for mac in request.macs:
        await pxe_map.put_mapping(base_url, _settings.pxe_map_token, mac, ipxe_url)
    activity.logger.info(
        "Mapped %s (%s) to InfraEnv %s's iPXE script on the %s PXE VM %s",
        request.server_name,
        request.macs,
        request.infra_env,
        request.site,
        base_url,
    )
    return PxeBootRegistration(
        site=request.site, pxe_map_url=base_url, macs=list(request.macs)
    )


@activity.defn
async def publish_infra_env_boot(request: InfraEnvBootRequest) -> InfraEnvBootState:
    """Publish the InfraEnv's iPXE script as its boot entry, once its image includes this host.

    Without a `server_name` (publish-infraenv-to-pxe-server): its current image, as soon
    as there is a ready one.
    """
    if request.server_name:
        needed_by = _boots_from_network(request.server_name)
    else:
        needed_by = f"InfraEnv {request.mce_cluster}/{request.infra_env} is to be published"
    base_url = _pxe_map_url(request.site, needed_by)
    infra_env = await _read_infra_env(request.namespace, request.infra_env)

    if request.server_name:
        # The image must postdate the host's NMStateConfig: its newest, should
        # it have one under each name (a create by a limb from before the
        # rename, then one by this limb).
        nmstates = await _read_nmstate_configs(request.namespace, request.server_name)
        nmstate = max(nmstates, key=_created_at, default=None)
        awaited = (nmstate or {}).get("metadata", {}).get("name") or nmstate_config_names(
            request.server_name
        )[0]
        waiting = infraenv_image_wait_reason(infra_env, nmstate)
    else:
        awaited = "its current image"
        waiting = infraenv_image_not_ready_reason(infra_env)
    if waiting is not None:
        activity.logger.info(
            "Not publishing InfraEnv %s/%s for %s yet: %s",
            request.namespace,
            request.infra_env,
            awaited,
            waiting,
        )
        return InfraEnvBootState(published=False, detail=waiting)

    # Both non-None: infraenv_image_wait_reason only passes an InfraEnv that
    # publishes a script and a createdTime.
    ipxe_url = infraenv_ipxe_script_url(infra_env)
    image_created_at = infraenv_image_created_at(infra_env)
    written = await pxe_map.put_infra_env_boot(
        base_url,
        _settings.pxe_map_token,
        request.mce_cluster,
        request.infra_env,
        ipxe_url,
        image_created_at,
    )
    activity.logger.info(
        "Boot entry %s/%s on the %s PXE VM %s: %s (image of %s, after %s)",
        request.mce_cluster,
        request.infra_env,
        request.site,
        base_url,
        "published" if written else "already holds a newer image",
        image_created_at,
        awaited,
    )
    return InfraEnvBootState(
        published=True, image_created_at=image_created_at, superseded=not written
    )


@activity.defn
async def create_bmc_secret(request: BmhResourceRequest) -> CreatedResource:
    """Create the BMC credentials Secret (`{vendor}-cred-{server}`)."""
    _log_waived(request)
    username, password = _bmc_credentials(request.bmc_vendor)
    return _log_outcome(
        await cluster_api.create_secret_if_absent(
            request.namespace, build_bmc_secret(request, username, password)
        )
    )


@activity.defn
async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
    """Create the BareMetalHost (metal3.io/v1alpha1)."""
    _log_waived(request)
    bmh = build_baremetal_host(request)
    activity.logger.info(
        "BareMetalHost %s: bmc=%s boot_mac=%s",
        bmh["metadata"]["name"],
        bmh["spec"]["bmc"]["address"],
        bmh["spec"]["bootMACAddress"],
    )
    return _log_outcome(
        await cluster_api.create_custom_object_if_absent(
            group=BMH_GROUP,
            version=BMH_VERSION,
            plural=BMH_PLURAL,
            kind=_BMH_KIND,
            namespace=request.namespace,
            body=bmh,
            differences=baremetal_host_differences,
        )
    )


@activity.defn
async def create_nmstate_config(request: BmhResourceRequest) -> CreatedResource:
    """Create the NMStateConfig, named after the server.

    One still under the name used until 2026-11-25 (`nmstate-config-{server}`)
    that matches IS the server's NMStateConfig: success, nothing written, so a
    host never ends with two. One that differs is the conflict it always was.
    """
    _log_waived(request)
    desired = build_nmstate_config(request)
    _, legacy_name = nmstate_config_names(request.server_name)
    legacy = await _read(
        NMSTATE_GROUP, NMSTATE_VERSION, NMSTATE_PLURAL, _NMSTATE_KIND, request.namespace, legacy_name
    )
    if legacy is not None:
        mismatches = nmstate_config_differences(desired, legacy)
        if mismatches:
            raise BmhConflictError(
                f"{_NMSTATE_KIND} {legacy_name} already exists and does not match this "
                "request: " + "; ".join(mismatches) + ". Nothing was overwritten — "
                "reconcile the existing resource or install a different server"
            )
        return _log_outcome(CreatedResource(kind=_NMSTATE_KIND, name=legacy_name, changed=False))
    return _log_outcome(
        await cluster_api.create_custom_object_if_absent(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            plural=NMSTATE_PLURAL,
            kind=_NMSTATE_KIND,
            namespace=request.namespace,
            body=desired,
            differences=nmstate_config_differences,
        )
    )


@activity.defn
async def replace_conflicting_nmstate_config(
    request: NmStateConfigReplaceRequest,
) -> NmStateConfigReplacement:
    """Replace the server's NMStateConfig when it differs from this install.

    The contract, and why the order is what it is, is in
    shared/interfaces/server_lifecycle.py. The server may hold one under either
    of its two names (nmstate_config_names): one that matches is left where it
    is, whichever name it has; one that differs is deleted and the install's is
    created under the server's own name; and of two, the old-named one goes.
    """
    resource = request.resource
    namespace = resource.namespace
    desired = build_nmstate_config(resource)
    name, legacy_name = nmstate_config_names(resource.server_name)
    existing = {
        (found.get("metadata") or {}).get("name"): found
        for found in await _read_nmstate_configs(namespace, resource.server_name)
    }
    if not existing:
        return NmStateConfigReplacement(found=False)

    # What goes, and why: every one that differs, and the old-named one of two.
    reasons: dict[str, list[str]] = {}
    for found_name, found in existing.items():
        differences = nmstate_config_differences(desired, found)
        if differences:
            reasons[found_name] = differences
        elif found_name == legacy_name and name in existing:
            reasons[found_name] = [
                f"{legacy_name}: a second NMStateConfig of {resource.server_name}, beside {name}"
            ]
    if not reasons:
        return NmStateConfigReplacement(found=True)

    # Replace EXACTLY the objects compared, then create the install's and read
    # it back. Delete-and-create rather than an update: the limb holds no
    # update verb, and a NEW creationTimestamp is what an IPMI blade's
    # boot-entry publish waits for the discovery image to postdate.
    differences = [line for lines in reasons.values() for line in lines]
    for found_name in reasons:
        metadata = existing[found_name].get("metadata") or {}
        activity.logger.warning(
            "nmstateconfig-replacing %s/%s for %s: %s",
            namespace,
            found_name,
            resource.server_name,
            "; ".join(reasons[found_name]),
        )
        await cluster_api.delete_custom_object_exactly(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            plural=NMSTATE_PLURAL,
            kind=_NMSTATE_KIND,
            namespace=namespace,
            name=found_name,
            uid=metadata.get("uid"),
            resource_version=metadata.get("resourceVersion"),
        )
    if name not in existing or name in reasons:
        await cluster_api.create_custom_object_if_absent(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            plural=NMSTATE_PLURAL,
            kind=_NMSTATE_KIND,
            namespace=namespace,
            body=desired,
            differences=nmstate_config_differences,
        )
    written = await _read(NMSTATE_GROUP, NMSTATE_VERSION, NMSTATE_PLURAL, _NMSTATE_KIND, namespace, name)
    if written is None or nmstate_config_differences(desired, written):
        raise BmhResourceError(
            f"NMStateConfig {namespace}/{name} did not read back as written after its "
            "replacement; it is read and compared again"
        )
    activity.logger.warning(
        "nmstateconfig-replaced %s/%s for %s: %s",
        namespace,
        name,
        resource.server_name,
        "; ".join(differences),
    )
    return NmStateConfigReplacement(found=True, replaced=True, differences=differences)


@activity.defn
async def get_baremetal_host(ref: BmhRef) -> BmhState:
    """Read one BareMetalHost's status back, by name and namespace.

    An absent host is NOT an error — it reports found=False, which is the normal
    answer both while Ironic has not picked the host up and when probing whether
    a candidate is already installed. The workflow's bounded loop owns the
    waiting.
    """
    bmh = await cluster_api.read_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=ref.namespace,
        name=k8s_resource_name(ref.server_name),
    )
    if bmh is None:
        return BmhState(found=False)
    status = bmh.get("status") or {}
    return BmhState(
        found=True,
        provisioning_state=(status.get("provisioning") or {}).get("state"),
        operational_status=status.get("operationalStatus"),
        error_type=status.get("errorType") or None,
        error_message=status.get("errorMessage") or None,
        powered_on=status.get("poweredOn"),
    )


@activity.defn
async def find_agent_for_host(ref: AgentRef) -> AgentState:
    """Whether an Agent has registered for this host yet, matched by MAC."""
    agents = await cluster_api.list_custom_objects(
        group=AGENT_GROUP,
        version=AGENT_VERSION,
        plural=AGENT_PLURAL,
        kind=_AGENT_KIND,
        namespace=ref.namespace,
    )
    for agent in agents:
        if not agent_matches_macs(agent, ref.macs):
            continue
        name = (agent.get("metadata") or {}).get("name")
        activity.logger.info(
            "Agent %s registered for MACs %s in %s", name, ref.macs, ref.namespace
        )
        return AgentState(
            found=True,
            name=name,
            approved=(agent.get("spec") or {}).get("approved"),
        )
    activity.logger.info(
        "No Agent yet for MACs %s in %s (%d agent(s) in the namespace)",
        ref.macs,
        ref.namespace,
        len(agents),
    )
    return AgentState(found=False)


async def _bmh_is_gone(namespace: str, name: str) -> bool:
    """Whether the BareMetalHost object has actually left the API server.

    Not the same question as "was the delete accepted": a deleted host sits
    there with a `deletionTimestamp` for as long as any controller holds a
    finalizer on it.
    """
    return (
        await cluster_api.read_custom_object(
            group=BMH_GROUP,
            version=BMH_VERSION,
            plural=BMH_PLURAL,
            kind=_BMH_KIND,
            namespace=namespace,
            name=name,
        )
        is None
    )


@activity.defn
async def teardown_bmh_resources(ref: BmhRef) -> TeardownResult:
    """Remove one candidate's resources so the machine returns to the inventory.

    Runs recorded before 2026-11-24 only (delete_host_resources since). The
    ordering here was established against a live cluster and is load-bearing
    at every step — see the contract in shared/interfaces/server_lifecycle.py.
    """
    namespace = ref.namespace
    bmh_name = k8s_resource_name(ref.server_name)
    removed: list[str] = []

    # 0. Which Secret this host references, read from the host itself while it
    #    still exists. Not rebuilt from the vendor: a BmhRef carries no vendor,
    #    and the host's own `credentialsName` is authoritative anyway — it
    #    stays correct for a Secret an operator renamed by hand.
    existing = await cluster_api.read_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=namespace,
        name=bmh_name,
    )
    secret_name = (
        ((existing or {}).get("spec") or {}).get("bmc") or {}
    ).get("credentialsName")

    # 1. Detach FIRST. After `deletionTimestamp` is set this annotation does
    #    nothing, so ordering is the whole point of doing it here.
    if await cluster_api.annotate_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=namespace,
        name=bmh_name,
        annotations={DETACHED_ANNOTATION: ""},
    ):
        activity.logger.info("Detached BareMetalHost %s before deleting it", bmh_name)

    # 2. The NMStateConfig: no finalizer, no ownerReference, so nothing else
    #    removes it and it goes immediately.
    removed.extend(await _delete_nmstate_configs(namespace, ref.server_name))

    # 3. The host itself, then wait for it to really go.
    if await cluster_api.delete_custom_object(
        group=BMH_GROUP,
        version=BMH_VERSION,
        plural=BMH_PLURAL,
        kind=_BMH_KIND,
        namespace=namespace,
        name=bmh_name,
    ):
        removed.append(f"{_BMH_KIND}/{bmh_name}")

    waited = 0.0
    while waited < _TEARDOWN_GONE_TIMEOUT:
        if await _bmh_is_gone(namespace, bmh_name):
            break
        await asyncio.sleep(_TEARDOWN_POLL_SECONDS)
        waited += _TEARDOWN_POLL_SECONDS

    # 4. Still there means a finalizer is held. Expected here rather than
    #    exceptional: metal3 is trying to deprovision through a BMC that never
    #    answered, which is why this candidate is being rolled back at all.
    finalizers_cleared = False
    if not await _bmh_is_gone(namespace, bmh_name):
        activity.logger.warning(
            "BareMetalHost %s still present %.0fs after delete — dropping %s",
            bmh_name,
            waited,
            sorted(BMH_DEPROVISION_FINALIZERS),
        )
        finalizers_cleared = await cluster_api.clear_custom_object_finalizers(
            group=BMH_GROUP,
            version=BMH_VERSION,
            plural=BMH_PLURAL,
            kind=_BMH_KIND,
            namespace=namespace,
            name=bmh_name,
            finalizers=BMH_DEPROVISION_FINALIZERS,
        )

    # 5. The Secret normally cascades off the host's ownerReference, but only
    #    once the host is really gone. Deleted explicitly when it outlives it,
    #    because the alternative is a BMC credential left behind per rollback.
    if secret_name and await cluster_api.secret_exists(namespace, secret_name):
        if await cluster_api.delete_secret_if_present(namespace, secret_name):
            removed.append(f"Secret/{secret_name}")

    if not await _bmh_is_gone(namespace, bmh_name):
        raise BmhTeardownError(
            f"BareMetalHost {bmh_name} is still on the cluster in {namespace} "
            f"after detach, delete and finalizer removal. The server is NOT "
            f"being returned to the inventory while a BareMetalHost still points "
            f"at it"
        )

    activity.logger.info(
        "Rolled back %s: removed %s%s",
        ref.server_name,
        removed or ["nothing (already absent)"],
        " (finalizer dropped)" if finalizers_cleared else "",
    )
    return TeardownResult(removed=removed, finalizers_cleared=finalizers_cleared)


# --- uninstall-server-from-hc ---------------------------------------------------------

_INFRAENV_LABEL = "infraenvs.agent-install.openshift.io"


@activity.defn
async def lookup_server_membership(request: ServerMembershipRequest) -> ServerMembership:
    """server-scan's record of where this server is, and its install lock (search + exact-name detail)."""
    membership = await server_scan.server_membership_by_name(
        _settings.server_scan_url, _settings.server_scan_api_token, request.server_name
    )
    activity.logger.info(
        "server-scan on %s: found=%s id=%s state=%s cluster=%s mce=%s contested=%s maintenance=%s "
        "lock=%s",
        request.server_name,
        membership.found,
        membership.server_id,
        membership.lifecycle_state,
        membership.cluster_name,
        membership.mce_name,
        membership.contested_with,
        membership.maintenance,
        membership.lock.model_dump() if membership.lock.live else "none live",
    )
    return membership


async def _read_machine(ref: MachineRef) -> dict | None:
    group, version = hosted_resources.split_api_version(ref.api_version)
    return await cluster_api.read_custom_object(
        group=group,
        version=version,
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
        namespace=ref.namespace,
        name=ref.name,
    )


async def _read_agent_machine(namespace: str, name: str) -> dict | None:
    return await cluster_api.read_custom_object(
        group=hosted_resources.AGENT_MACHINE_GROUP,
        version=hosted_resources.AGENT_MACHINE_VERSION,
        plural=hosted_resources.AGENT_MACHINE_PLURAL,
        kind=hosted_resources.AGENT_MACHINE_KIND,
        namespace=namespace,
        name=name,
    )


async def _list_agent_machines() -> list[dict]:
    return await cluster_api.list_cluster_custom_objects(
        group=hosted_resources.AGENT_MACHINE_GROUP,
        version=hosted_resources.AGENT_MACHINE_VERSION,
        plural=hosted_resources.AGENT_MACHINE_PLURAL,
        kind=hosted_resources.AGENT_MACHINE_KIND,
    )


async def _list_machines() -> list[dict]:
    # v1beta1 is served by every CAPI the MCE ships, v1beta2 storage included.
    # A Machine is also always READ by the apiVersion its AgentMachine's owner
    # reference names, so a CAPI that stopped serving v1beta1 loses only the
    # providerID / infrastructureRef links, never the owner.
    return await cluster_api.list_cluster_custom_objects(
        group=hosted_resources.MACHINE_GROUP,
        version="v1beta1",
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
    )


async def _list_hosted_clusters() -> list[dict]:
    return await cluster_api.list_cluster_custom_objects(
        group=hosted_resources.HOSTED_CLUSTER_GROUP,
        version=hosted_resources.HOSTED_CLUSTER_VERSION,
        plural=hosted_resources.HOSTED_CLUSTER_PLURAL,
        kind=hosted_resources.HOSTED_CLUSTER_KIND,
    )


async def _with_agent_machine_found(state: MachineState) -> MachineState:
    """A Machine's state plus whether the AgentMachine its infrastructureRef names exists."""
    if state.found and state.agent_machine:
        namespace, name = state.agent_machine.split("/", 1)
        state.agent_machine_found = await _read_agent_machine(namespace, name) is not None
    return state


def _qualified(obj: dict) -> str:
    return hosted_resources.qualified_name(obj)


@activity.defn
async def inspect_bound_host(ref: BmhRef) -> BoundHostState:
    """The host on this MCE and, if it is one, the hosted-cluster node it is. Reads only."""
    bmh_name = k8s_resource_name(ref.server_name)
    bmh = await _read(BMH_GROUP, BMH_VERSION, BMH_PLURAL, _BMH_KIND, ref.namespace, bmh_name)
    nmstates = await _read_nmstate_configs(ref.namespace, ref.server_name)
    scan_macs = await server_scan.server_macs_by_name(
        _settings.server_scan_url, _settings.server_scan_api_token, ref.server_name
    )
    macs = sorted(_all_host_macs(bmh, nmstates) | set(scan_macs))
    matched = [
        agent
        for agent in await _list_agents(ref.namespace)
        if agent_belongs_to_host(agent, bmh_name, macs)
    ]
    # Only listed when there is an Agent to look for: an inventory-only MCE
    # need not have the CAPI provider or HyperShift at all (and an absent API
    # lists as empty).
    agent_machines: list[dict] = []
    machines: list[dict] = []
    hosted_clusters: list[dict] = []
    if matched:
        agent_machines = await _list_agent_machines()
        machines = await _list_machines()
        hosted_clusters = await _list_hosted_clusters()
    control_planes = {
        hosted_resources.hosted_control_plane_namespace(
            (hc.get("metadata") or {}).get("namespace", ""), (hc.get("metadata") or {}).get("name", "")
        )
        for hc in hosted_clusters
    }

    agents: list[HostAgent] = []
    linked: dict[str, tuple[dict | None, list[dict]]] = {}
    for agent in matched:
        name = (agent.get("metadata") or {}).get("name", "")
        agent_machine = hosted_resources.agent_machine_for(agent, ref.namespace, agent_machines)
        its_machines = hosted_resources.machines_for(name, agent_machine, machines)
        binding = hosted_resources.agent_binding_state(agent)
        namespaces = {(m.get("metadata") or {}).get("namespace") for m in its_machines}
        if agent_machine is not None:
            namespaces.add((agent_machine.get("metadata") or {}).get("namespace"))
        bound_namespace = ((agent.get("spec") or {}).get("clusterDeploymentName") or {}).get("namespace")
        if bound_namespace in control_planes:
            namespaces.add(bound_namespace)
        namespaces.discard(None)
        if agent_machine is not None or namespaces:
            linked[name] = (agent_machine, its_machines)
        agents.append(
            HostAgent(
                name=name,
                namespace=ref.namespace,
                cluster_deployment=binding.cluster_deployment,
                state=binding.state,
                approved=binding.approved,
                agent_machine=_qualified(agent_machine) if agent_machine else None,
                agent_machine_label=binding.agent_machine_label,
                labelled_agent_machine=binding.agent_machine,
                hostname=hosted_resources.agent_hostname(agent),
                hcp_namespaces=sorted(namespaces),
            )
        )

    machine: MachineRef | None = None
    machine_state: MachineState | None = None
    hosted_cluster = None
    machines_matched: list[str] = []
    if len(linked) == 1:
        agent_name, (agent_machine, its_machines) = next(iter(linked.items()))
        candidates = {_qualified(m): m for m in its_machines}
        owner_ref = None
        if agent_machine is not None:
            owner = hosted_resources.machine_owner(agent_machine)
            am_namespace = (agent_machine.get("metadata") or {}).get("namespace", "")
            if owner is not None:
                # Read by the version the owner reference names, whatever the list saw.
                owner_ref = MachineRef(namespace=am_namespace, name=owner[0], api_version=owner[1])
                owner_machine = await _read_machine(owner_ref)
                key = f"{am_namespace}/{owner[0]}"
                if owner_machine is not None:
                    candidates[key] = owner_machine
                else:
                    candidates.pop(key, None)
        machines_matched = sorted(candidates)
        if len(candidates) == 1:
            key, found_machine = next(iter(candidates.items()))
            namespace, name = key.split("/", 1)
            if owner_ref is not None and (owner_ref.namespace, owner_ref.name) == (namespace, name):
                machine = owner_ref
            else:
                machine = MachineRef(
                    namespace=namespace,
                    name=name,
                    api_version=found_machine.get("apiVersion") or "cluster.x-k8s.io/v1beta1",
                )
            machine_state = await _with_agent_machine_found(
                hosted_resources.machine_state(found_machine)
            )
        elif not candidates and owner_ref is not None:
            # The owner reference names a Machine that is gone.
            machine, machine_state = owner_ref, MachineState(found=False)
        [hosted] = [a for a in agents if a.name == agent_name]
        if len(hosted.hcp_namespaces) == 1:
            hosted_cluster = hosted_resources.hosted_cluster_for(
                hosted.hcp_namespaces[0], hosted_clusters
            )

    state = BoundHostState(
        bmh_found=bmh is not None,
        bmh_operational_status=((bmh or {}).get("status") or {}).get("operationalStatus"),
        infra_env=(((bmh or {}).get("metadata") or {}).get("labels") or {}).get(_INFRAENV_LABEL),
        agents=agents,
        machine=machine,
        machine_state=machine_state,
        hosted_cluster=hosted_cluster,
        machines_matched=machines_matched,
    )
    activity.logger.info(
        "%s in %s: bmh=%s agents=%s machines=%s deleting=%s node=%s hosted cluster=%s",
        bmh_name,
        ref.namespace,
        state.bmh_found,
        [(a.name, a.cluster_deployment, a.state, a.agent_machine, a.hcp_namespaces) for a in agents],
        machines_matched,
        machine_state.deleting if machine_state else None,
        machine_state.node_name if machine_state else None,
        f"{hosted_cluster.namespace}/{hosted_cluster.name}" if hosted_cluster else None,
    )
    return state


@activity.defn
async def ensure_machine_deleting(ref: MachineRef) -> MachineDeletion:
    """Make sure the Machine is being deleted — a no-op when it already is, or is gone."""
    existing = hosted_resources.machine_state(await _read_machine(ref))
    if not existing.found:
        activity.logger.info("Machine %s/%s is already gone", ref.namespace, ref.name)
        return MachineDeletion(found=False)
    if existing.deleting:
        activity.logger.info(
            "Machine %s/%s is already deleting (since %s) — nothing written",
            ref.namespace,
            ref.name,
            existing.deletion_timestamp,
        )
        return MachineDeletion(found=True, already_deleting=True)
    group, version = hosted_resources.split_api_version(ref.api_version)
    deleted = await cluster_api.delete_custom_object(
        group=group,
        version=version,
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
        namespace=ref.namespace,
        name=ref.name,
    )
    activity.logger.info(
        "Machine %s/%s: %s", ref.namespace, ref.name, "deleted" if deleted else "gone meanwhile"
    )
    return MachineDeletion(found=deleted, deleted=deleted)


@activity.defn
async def get_machine_state(ref: MachineRef) -> MachineState:
    """One Machine as it is now, and whether its AgentMachine still exists."""
    return await _with_agent_machine_found(hosted_resources.machine_state(await _read_machine(ref)))


@activity.defn
async def get_agent_machine_state(ref: AgentMachineRef) -> AgentMachineState:
    """Whether one AgentMachine is still on the MCE."""
    agent_machine = await _read_agent_machine(ref.namespace, ref.name)
    if agent_machine is None:
        return AgentMachineState(found=False)
    return AgentMachineState(
        found=True,
        deleting=bool((agent_machine.get("metadata") or {}).get("deletionTimestamp")),
    )


@activity.defn
async def get_agent_binding(ref: AgentNameRef) -> AgentBindingState:
    """One Agent's binding and state as they are now."""
    agent = await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, ref.namespace, ref.name)
    return hosted_resources.agent_binding_state(agent)


async def _hosted_kubeconfig(cluster: HostedClusterRef) -> str | None:
    """The hosted cluster's admin kubeconfig text; None when the HostedCluster is gone."""
    label = f"{cluster.namespace}/{cluster.name}"
    hosted = await cluster_api.read_custom_object(
        group=hosted_resources.HOSTED_CLUSTER_GROUP,
        version=hosted_resources.HOSTED_CLUSTER_VERSION,
        plural=hosted_resources.HOSTED_CLUSTER_PLURAL,
        kind=hosted_resources.HOSTED_CLUSTER_KIND,
        namespace=cluster.namespace,
        name=cluster.name,
    )
    if hosted is None:
        return None
    secret_name = hosted_resources.admin_kubeconfig_secret(hosted)
    if not secret_name:
        raise HostedClusterAccessError(
            f"HostedCluster {label} publishes no status.kubeconfig, so its Nodes cannot be read"
        )
    data = await cluster_api.read_secret_data(cluster.namespace, secret_name)
    # The one decode of an admin kubeconfig, shared with the cluster-upgrade limb.
    return hosted_cluster_api.admin_kubeconfig(data, f"{cluster.namespace}/{secret_name}", label)


@activity.defn
async def check_hosted_node(ref: HostedNodeRef) -> HostedNodeState:
    """Whether the hosted cluster still has this Node, through its admin kubeconfig."""
    cluster = ref.hosted_cluster
    label = f"{cluster.namespace}/{cluster.name}"
    kubeconfig = await _hosted_kubeconfig(cluster)
    if kubeconfig is None:
        # No HostedCluster, no control plane, no Nodes: nothing can be left.
        activity.logger.info("HostedCluster %s is gone, so Node %s is too", label, ref.node_name)
        return HostedNodeState(found=False, node_name=ref.node_name)
    found = await hosted_cluster_api.node_exists(
        kubeconfig,
        hosted_cluster_api.kas_service_url(cluster.hcp_namespace),
        ref.node_name,
        label,
    )
    activity.logger.info(
        "Hosted cluster %s: Node %s %s", label, ref.node_name, "present" if found else "absent"
    )
    return HostedNodeState(found=found, node_name=ref.node_name)


# --- uninstall-server-from-hc: leftovers of a broken HyperShift deletion ----------------------


@activity.defn
async def find_hosted_nodes(query: HostedNodeQuery) -> HostedNodes:
    """Every Node of the hosted cluster that is this server. Reads only."""
    cluster = query.hosted_cluster
    if cluster is None and query.cluster_name:
        named = [
            hc for hc in await _list_hosted_clusters()
            if ((hc.get("metadata") or {}).get("name") or "").lower() == query.cluster_name.lower()
        ]
        if len(named) != 1:
            reason = (
                f"no HostedCluster on this MCE is named {query.cluster_name}"
                if not named
                else f"{len(named)} HostedClusters on this MCE are named {query.cluster_name}: "
                + ", ".join(_qualified(hc) for hc in named)
            )
            activity.logger.info("No hosted cluster to look in for %s: %s", query.server_name, reason)
            return HostedNodes(reason=reason)
        metadata = named[0]["metadata"]
        cluster = HostedClusterRef(
            namespace=metadata["namespace"],
            name=metadata["name"],
            hcp_namespace=hosted_resources.hosted_control_plane_namespace(
                metadata["namespace"], metadata["name"]
            ),
        )
    if cluster is None:
        return HostedNodes(reason="nothing names a hosted cluster for this server")
    label = f"{cluster.namespace}/{cluster.name}"
    kubeconfig = await _hosted_kubeconfig(cluster)
    if kubeconfig is None:
        return HostedNodes(
            hosted_cluster=cluster, reason=f"HostedCluster {label} is gone, and its Nodes with it"
        )
    nodes = await hosted_cluster_api.list_nodes(
        kubeconfig, hosted_cluster_api.kas_service_url(cluster.hcp_namespace), label
    )
    wanted = hosted_resources.provider_id(query.agent_name) if query.agent_name else None
    names = {
        n.lower()
        for n in (query.node_name, query.hostname, k8s_resource_name(query.server_name))
        if n
    }
    matched = []
    for node in nodes:
        why = hosted_resources.node_matches(node, wanted, names)
        if why:
            matched.append(
                HostedNode(
                    name=(node.get("metadata") or {}).get("name", ""),
                    provider_id=hosted_resources.node_provider_id(node),
                    ready=hosted_resources.node_ready(node),
                    unschedulable=bool((node.get("spec") or {}).get("unschedulable")),
                    matched_by=why,
                )
            )
    activity.logger.info(
        "Hosted cluster %s: %s's Nodes: %s",
        label,
        query.server_name,
        [(n.name, n.matched_by, "Ready" if n.ready else "NotReady") for n in matched] or "none",
    )
    return HostedNodes(hosted_cluster=cluster, nodes=matched)


@activity.defn
async def drain_hosted_node(action: HostedNodeAction) -> HostedNodeDrain:
    """One drain pass over a Node with no Machine behind it: cordon, evict."""
    cluster = action.hosted_cluster
    label = f"{cluster.namespace}/{cluster.name}"
    kubeconfig = await _hosted_kubeconfig(cluster)
    if kubeconfig is None:
        return HostedNodeDrain(found=False)
    outcome = await hosted_cluster_api.drain_node(
        kubeconfig,
        hosted_cluster_api.kas_service_url(cluster.hcp_namespace),
        action.node_name,
        action.provider_id,
        label,
    )
    drain = HostedNodeDrain(**outcome)
    activity.logger.info(
        "Hosted cluster %s: drain of Node %s — cordoned now=%s evicted=%s remaining=%s blocked=%s",
        label,
        action.node_name,
        drain.cordoned,
        drain.evicted,
        len(drain.remaining),
        drain.blocked,
    )
    return drain


@activity.defn
async def delete_hosted_node(action: HostedNodeAction) -> HostedNodeDeletion:
    """Delete a Node nothing runs as any more (NotReady), when nothing else will."""
    cluster = action.hosted_cluster
    label = f"{cluster.namespace}/{cluster.name}"
    kubeconfig = await _hosted_kubeconfig(cluster)
    if kubeconfig is None:
        return HostedNodeDeletion(found=False)
    outcome = await hosted_cluster_api.delete_node(
        kubeconfig,
        hosted_cluster_api.kas_service_url(cluster.hcp_namespace),
        action.node_name,
        action.provider_id,
        label,
    )
    deletion = HostedNodeDeletion(**outcome)
    activity.logger.info(
        "Hosted cluster %s: Node %s found=%s ready=%s deleted=%s",
        label,
        action.node_name,
        deletion.found,
        deletion.ready,
        deletion.deleted,
    )
    return deletion


@activity.defn
async def release_agent(request: AgentReleaseRequest) -> AgentRelease:
    """The Agent provider's unbind, written by the run because the provider no longer can."""
    ref = request.agent
    agent = await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, ref.namespace, ref.name)
    if agent is None:
        return AgentRelease(found=False)
    binding = hosted_resources.agent_binding_state(agent)
    if binding.cluster_deployment and binding.cluster_deployment != request.cluster_deployment:
        raise AgentBindingChangedError(
            f"Agent {ref.namespace}/{ref.name} is bound to {binding.cluster_deployment}, "
            f"not {request.cluster_deployment} — the cluster this run is taking it out of. "
            "Nothing was changed"
        )
    if not binding.cluster_deployment and not binding.agent_machine_label:
        activity.logger.info("Agent %s/%s is already released", ref.namespace, ref.name)
        return AgentRelease(found=True)
    holder = await _agent_machine_holder(binding.agent_machine)
    if holder is not None:
        agent_machine, machine = holder
        raise AgentBindingChangedError(
            f"Agent {ref.namespace}/{ref.name} is held by AgentMachine {agent_machine} of "
            f"Machine {machine} — a live Machine, so the Agent provider can release it itself "
            "and the run does not. Typically the NodePool replaced the Machine this run "
            "deleted and took this same server again. Nothing was changed"
        )
    resource_version = (agent.get("metadata") or {}).get("resourceVersion")
    patched = await cluster_api.patch_custom_object(
        group=AGENT_GROUP,
        version=AGENT_VERSION,
        plural=AGENT_PLURAL,
        kind=_AGENT_KIND,
        namespace=ref.namespace,
        name=ref.name,
        body=hosted_resources.agent_release_patch(resource_version),
    )
    activity.logger.info(
        "Agent %s/%s: %s", ref.namespace, ref.name,
        f"released from {request.cluster_deployment}" if patched else "gone meanwhile",
    )
    return AgentRelease(found=patched, released=patched)


@activity.defn
async def mark_agents_uninstalling(request: AgentMarkRequest) -> AgentMarks:
    """`redbull.io/uninstalling: "true"` on each Agent, read back."""
    result = AgentMarks()
    for ref in request.agents:
        where = f"{ref.namespace}/{ref.name}"
        agent = await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, ref.namespace, ref.name)
        if agent is None:
            result.missing.append(where)
            continue
        if hosted_resources.agent_marked_uninstalling(agent):
            result.unchanged.append(where)
            continue
        patched = await cluster_api.patch_custom_object(
            group=AGENT_GROUP,
            version=AGENT_VERSION,
            plural=AGENT_PLURAL,
            kind=_AGENT_KIND,
            namespace=ref.namespace,
            name=ref.name,
            body=hosted_resources.agent_uninstalling_patch(),
        )
        back = (
            await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, ref.namespace, ref.name)
            if patched
            else None
        )
        if back is None:
            result.missing.append(where)
            continue
        if not hosted_resources.agent_marked_uninstalling(back):
            # Accepted but not there on the read just after: rewritten in between.
            raise BmhResourceError(f"the uninstalling label on Agent {where} did not read back")
        result.changed.append(where)
    activity.logger.info(
        "Uninstalling label: put on %s, already on %s, gone %s",
        result.changed, result.unchanged, result.missing,
    )
    return result


@activity.defn
async def release_machine_hook(ref: MachineRef) -> MachineHookRelease:
    """Remove the provider's pre-terminate hook from a deleting Machine whose AgentMachine is gone."""
    machine = await _read_machine(ref)
    if machine is None:
        return MachineHookRelease(found=False)
    state = await _with_agent_machine_found(hosted_resources.machine_state(machine))
    reason = None
    if not state.provider_hook:
        reason = "it carries no provider hook"
    elif not state.deleting:
        reason = "it is not deleting"
    elif state.agent_machine_found is not False:
        reason = (
            f"its AgentMachine {state.agent_machine} exists — the provider releases its own hook"
            if state.agent_machine_found
            else "its infrastructureRef names no AgentMachine"
        )
    if reason is not None:
        activity.logger.info("Machine %s/%s: hook left alone — %s", ref.namespace, ref.name, reason)
        return MachineHookRelease(found=True, reason=reason)
    group, version = hosted_resources.split_api_version(ref.api_version)
    patched = await cluster_api.patch_custom_object(
        group=group,
        version=version,
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
        namespace=ref.namespace,
        name=ref.name,
        body={"metadata": {"annotations": {hosted_resources.PROVIDER_MACHINE_HOOK: None}}},
    )
    activity.logger.info(
        "Machine %s/%s: provider hook %s", ref.namespace, ref.name,
        "removed" if patched else "— the Machine went meanwhile",
    )
    return MachineHookRelease(found=patched, released=patched)


async def _agent_machine_holder(labelled: str | None) -> tuple[str, str] | None:
    """(AgentMachine, Machine) when the AgentMachine an Agent's label names still has a Machine.

    Then the Agent provider can still act through it, so the run must not.
    `labelled` is `<namespace>/<name>`, or `-/<name>` without the namespace
    annotation — matched by name in every namespace then.
    """
    if not labelled:
        return None
    namespace, name = labelled.split("/", 1)
    if namespace == "-":
        candidates = [
            am for am in await _list_agent_machines() if (am.get("metadata") or {}).get("name") == name
        ]
    else:
        found = await _read_agent_machine(namespace, name)
        candidates = [found] if found is not None else []
    if not candidates:
        return None
    machines = await _list_machines()
    for agent_machine in candidates:
        machine = await _machine_holding(agent_machine, machines)
        if machine is not None:
            return _qualified(agent_machine), machine
    return None


async def _machine_holding(agent_machine: dict, machines: list[dict]) -> str | None:
    """The Machine that owns or names this AgentMachine, if one still exists."""
    key = _qualified(agent_machine)
    for machine in machines:
        if hosted_resources.machine_agent_machine(machine) == key:
            return _qualified(machine)
    owner = hosted_resources.machine_owner(agent_machine)
    if owner is not None:
        namespace = (agent_machine.get("metadata") or {}).get("namespace", "")
        ref = MachineRef(namespace=namespace, name=owner[0], api_version=owner[1])
        if await _read_machine(ref) is not None:
            return f"{namespace}/{owner[0]}"
    return None


@activity.defn
async def clean_up_agent_machines(request: AgentMachineCleanupRequest) -> AgentMachineCleanup:
    """Remove every Machine-less AgentMachine that still names the Agent; report what is left."""
    ref = request.agent
    agent = await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, ref.namespace, ref.name)
    agent_machines = await _list_agent_machines()
    machines = await _list_machines()
    naming = {
        _qualified(am): am
        for am in hosted_resources.agent_machines_naming(agent, ref.name, ref.namespace, agent_machines)
    }
    for known in request.known:
        key = f"{known.namespace}/{known.name}"
        if key not in naming:
            existing = await _read_agent_machine(known.namespace, known.name)
            if existing is not None:
                naming[key] = existing
    linked_machines = {
        _qualified(m)
        for am in [None, *naming.values()]
        for m in hosted_resources.machines_for(ref.name, am, machines)
    }
    binding = hosted_resources.agent_binding_state(agent)
    released = not binding.cluster_deployment and not binding.agent_machine_label

    result = AgentMachineCleanup(machines=sorted(linked_machines))
    for key, agent_machine in sorted(naming.items()):
        holder = await _machine_holding(agent_machine, machines)
        if holder is not None:
            result.held_by_machine.append(f"{key} (Machine {holder})")
            continue
        namespace, name = key.split("/", 1)
        metadata = agent_machine.get("metadata") or {}
        if not metadata.get("deletionTimestamp"):
            await cluster_api.delete_custom_object(
                group=hosted_resources.AGENT_MACHINE_GROUP,
                version=hosted_resources.AGENT_MACHINE_VERSION,
                plural=hosted_resources.AGENT_MACHINE_PLURAL,
                kind=hosted_resources.AGENT_MACHINE_KIND,
                namespace=namespace,
                name=name,
            )
            result.deleted.append(key)
        elif (
            request.clear_finalizers
            and released
            and hosted_resources.machine_owner(agent_machine) is None
            and hosted_resources.AGENT_MACHINE_FINALIZER in (metadata.get("finalizers") or [])
        ):
            # The finalizer's one job is the unbind, which is done; with NO
            # owner Machine at all the provider keeps it for ever. One that
            # still names its owner — gone — the provider drops itself, so it
            # is left to the provider (the operator, 2026-11-24): a hold is
            # removed here only where no controller ever will.
            if await cluster_api.clear_custom_object_finalizers(
                group=hosted_resources.AGENT_MACHINE_GROUP,
                version=hosted_resources.AGENT_MACHINE_VERSION,
                plural=hosted_resources.AGENT_MACHINE_PLURAL,
                kind=hosted_resources.AGENT_MACHINE_KIND,
                namespace=namespace,
                name=name,
                finalizers=frozenset({hosted_resources.AGENT_MACHINE_FINALIZER}),
            ):
                result.finalizers_cleared.append(key)
        left = await _read_agent_machine(namespace, name)
        if left is not None:
            finalizers = ", ".join((left.get("metadata") or {}).get("finalizers") or []) or "none"
            owner = hosted_resources.machine_owner(left)
            waiting = (
                f"; left to the Agent provider, which drops its finalizer once it sees "
                f"owner Machine {owner[0]} gone"
                if owner is not None and (left.get("metadata") or {}).get("deletionTimestamp")
                else ""
            )
            result.remaining.append(f"{key} (deleting; finalizers: {finalizers}{waiting})")
    activity.logger.info(
        "AgentMachines of Agent %s/%s: deleted=%s finalizers cleared=%s remaining=%s "
        "held by a Machine=%s, Machines=%s",
        ref.namespace,
        ref.name,
        result.deleted,
        result.finalizers_cleared,
        result.remaining,
        result.held_by_machine,
        result.machines,
    )
    return result


# --- scale-down-nodepool: the servers that leave ----------------------------------------

# A watch looks this often, and heartbeats each time (the workflow sets the
# heartbeat_timeout well above it).
_MACHINE_WATCH_POLL_SECONDS = 15
# The CAPI version every Machine here is listed and patched by (as _list_machines).
_MACHINE_VERSION = "v1beta1"


async def _machines_in(namespace: str) -> list[dict]:
    return await cluster_api.list_custom_objects(
        group=hosted_resources.MACHINE_GROUP,
        version=_MACHINE_VERSION,
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
        namespace=namespace,
    )


async def _machine(namespace: str, name: str) -> dict | None:
    return await _read_machine(MachineRef(namespace=namespace, name=name))


async def _patch_mark(namespace: str, machine: dict, mark: str | None) -> bool:
    """Set (`mark`) or remove (None) the delete mark at the resourceVersion read; False when gone."""
    metadata = machine.get("metadata") or {}
    return await cluster_api.patch_custom_object(
        group=hosted_resources.MACHINE_GROUP,
        version=_MACHINE_VERSION,
        plural=hosted_resources.MACHINE_PLURAL,
        kind=hosted_resources.MACHINE_KIND,
        namespace=namespace,
        name=metadata.get("name") or "",
        body=machine_selection.mark_patch(metadata.get("resourceVersion"), mark),
    )


def _day1() -> tuple[str, str]:
    """(repo URL, token) for update-nodepool's day1 calls; Day1NotConfiguredError while either is unset."""
    if not _settings.day1_repo_url or not _settings.day1_git_token:
        raise Day1NotConfiguredError(
            f"the server-lifecycle-worker of {_settings.mce_cluster} has no "
            f"{'DAY1_REPO_URL' if not _settings.day1_repo_url else 'DAY1_GIT_TOKEN'}: update-nodepool "
            "reads and commits day1 through it. Set it in the chart; nothing was read or written"
        )
    return _settings.day1_repo_url, _settings.day1_git_token


@activity.defn
async def read_day1_nodepools(request: Day1ClusterRequest) -> Day1Cluster:
    """The cluster's day1 file at main: path, blob, HostedCluster, NodePools, problems."""
    repo_url, token = _day1()
    cluster = await day1_repo.read_cluster(repo_url=repo_url, token=token, cluster=request.cluster)
    activity.logger.info(
        "day1 %s at %s: HostedCluster %s/%s, %d NodePools, %d problems",
        cluster.path, cluster.revision, cluster.hosted_cluster_namespace, cluster.hosted_cluster,
        len(cluster.nodepools), len(cluster.problems),
    )
    return cluster


@activity.defn
async def commit_day1_replicas(request: Day1ChangeRequest) -> Day1Commit:
    """`nodepool[].replicas` in ONE commit to day1 main — a REPLICAS change and nothing else."""
    if request.change is not Day1ChangeKind.REPLICAS:
        raise Day1ChangeRejectedError(
            f"the server-lifecycle limb commits NodePool replicas only, not a {request.change.value} change "
            f"to {request.path}; nothing was written"
        )
    repo_url, token = _day1()
    if not _settings.day1_validate_command:
        activity.logger.warning("DAY1_VALIDATE_COMMAND is not set: %s is pushed without day1's validator", request.path)
    commit = await day1_repo.commit_change(
        repo_url=repo_url, token=token, request=request, validate_command=_settings.day1_validate_command,
    )
    activity.logger.info(
        "day1 replicas %s for %s: %s at %s",
        "committed" if commit.changed else "already so", request.cluster, request.path, commit.revision,
    )
    return commit


async def _machine_agent(machine: dict) -> dict | None:
    """The Agent behind a Machine: its infrastructureRef's AgentMachine, then that one's agentRef."""
    ref = hosted_resources.machine_agent_machine(machine)
    if ref is None:
        return None
    namespace, name = ref.split("/", 1)
    agent = machine_selection.agent_ref(await _read_agent_machine(namespace, name))
    if agent is None:
        return None
    return await _read(AGENT_GROUP, AGENT_VERSION, AGENT_PLURAL, _AGENT_KIND, agent[0], agent[1])


@activity.defn
async def read_scale_down_candidates(request: ScaleDownCandidatesRequest) -> ScaleDownCandidates:
    """Every Machine of these NodePools, with what their policies rank by — and the server each is. Reads only."""
    cluster = HostedClusterRef(
        namespace=request.hosted_cluster_namespace,
        name=request.hosted_cluster,
        hcp_namespace=hosted_resources.hosted_control_plane_namespace(
            request.hosted_cluster_namespace, request.hosted_cluster
        ),
    )
    label = f"{cluster.namespace}/{cluster.name}"
    machines = await _machines_in(cluster.hcp_namespace)
    # The hosted cluster's Nodes and Pods are what a POLICY ranks by; a
    # scale-down that names its servers ranks nothing and does not read them.
    nodes: dict = {}
    pods: list[dict] = []
    metrics = None
    if any(np.policy is not None for np in request.nodepools):
        kubeconfig = await _hosted_kubeconfig(cluster)
        if kubeconfig is None:
            raise HostedClusterAccessError(
                f"HostedCluster {label} is not on this MCE, so its Nodes cannot be ranked"
            )
        with_metrics = any(np.policy is ScaleDownPolicy.LEAST_UTILIZED for np in request.nodepools)
        objects = await hosted_cluster_api.read_scale_down_objects(
            kubeconfig, hosted_cluster_api.kas_service_url(cluster.hcp_namespace), label, with_metrics
        )
        nodes = {(n.get("metadata") or {}).get("name"): n for n in objects["nodes"]}
        pods = objects["pods"]
        metrics = (
            None if objects["metrics"] is None
            else {(m.get("metadata") or {}).get("name"): m for m in objects["metrics"]}
        )
    found = []
    for nodepool in request.nodepools:
        uses_metrics = nodepool.policy is ScaleDownPolicy.LEAST_UTILIZED
        for machine in machine_selection.machines_of(machines, f"{cluster.namespace}/{nodepool.name}"):
            state = machine_selection.candidate(
                machine, nodepool.name, nodes, pods, metrics if uses_metrics else None
            )
            if request.with_servers:
                state.server = machine_selection.machine_server(machine, await _machine_agent(machine))
            # A Node with no Machine-side problem must be ranked by its use; one
            # with no metrics yet (it just joined) is waited for, not guessed.
            if uses_metrics and state.node_found and state.ready and not state.unschedulable and (
                state.used_permille is None
            ):
                raise NodeMetricsPendingError(
                    f"Node {state.node} of NodePool {nodepool.name} in hosted cluster {label} has no "
                    "metrics.k8s.io NodeMetrics yet; read again on the next attempt"
                )
            found.append(state)
    activity.logger.info(
        "Scale-down candidates in %s: %s",
        label,
        [
            f"{m.nodepool}/{m.name} server={m.server} node={m.node} ready={m.ready} pods={m.workload_pods} "
            f"requested={m.requested_permille} used={m.used_permille} mark={m.delete_mark!r} "
            f"deleting={m.deleting}"
            for m in found
        ],
    )
    return ScaleDownCandidates(hcp_namespace=cluster.hcp_namespace, machines=found)


@activity.defn
async def mark_machines_for_deletion(request: MachineMarkRequest) -> MachineMarks:
    """`cluster.x-k8s.io/delete-machine: <mark>` on each Machine, read back."""
    result = MachineMarks()
    for name in request.machines:
        where = f"{request.namespace}/{name}"
        machine = await _machine(request.namespace, name)
        if machine is None:
            raise MachineMarkConflictError(
                f"Machine {where} is gone: it is no longer the server the scale-down chose"
            )
        mark = machine_selection.delete_mark(machine)
        if mark == request.mark:
            result.unchanged.append(name)
            continue
        if machine_selection.machine_deleting(machine):
            raise MachineMarkConflictError(
                f"Machine {where} is already deleting: something else is removing it, so it is "
                "not the server the scale-down chose"
            )
        if mark is not None:
            raise MachineMarkConflictError(
                f"Machine {where} carries another delete mark ({mark!r}): CAPI would delete it "
                "first, so the scale-down will not mark it as well"
            )
        if not await _patch_mark(request.namespace, machine, request.mark):
            raise MachineMarkConflictError(f"Machine {where} went while it was being marked")
        if machine_selection.delete_mark(await _machine(request.namespace, name)) != request.mark:
            # Accepted but not there on the read just after: rewritten in between.
            raise BmhResourceError(f"the delete mark on Machine {where} did not read back")
        result.changed.append(name)
    activity.logger.info(
        "Delete mark %s in %s: marked %s, already marked %s",
        request.mark, request.namespace, result.changed, result.unchanged,
    )
    return result


@activity.defn
async def unmark_machines(request: MachineMarkRequest) -> MachineMarks:
    """THIS run's delete mark off each Machine — any other mark is left alone."""
    result = MachineMarks()
    for name in request.machines:
        where = f"{request.namespace}/{name}"
        machine = await _machine(request.namespace, name)
        if machine is None or machine_selection.delete_mark(machine) != request.mark:
            result.unchanged.append(name)
            continue
        if not await _patch_mark(request.namespace, machine, None):
            result.unchanged.append(name)
            continue
        back = await _machine(request.namespace, name)
        if back is not None and machine_selection.delete_mark(back) == request.mark:
            raise BmhResourceError(f"the delete mark on Machine {where} is still there after removing it")
        result.changed.append(name)
    activity.logger.info(
        "Delete mark %s in %s: removed from %s, not on %s",
        request.mark, request.namespace, result.changed, result.unchanged,
    )
    return result


def _watch_started() -> datetime:
    """When this watch began: the last heartbeat's, when this attempt retries one."""
    details = activity.info().heartbeat_details
    if details:
        try:
            return datetime.fromisoformat(str(details[0]))
        except ValueError:
            pass
    return datetime.now(UTC)


@activity.defn
async def watch_marked_machines(request: MarkedMachinesWatchRequest) -> MarkedMachinesWatch:
    """Until every marked Machine is deleting or gone, an unmarked one deletes, or the deadline."""
    started = _watch_started()
    deadline = started + timedelta(minutes=request.deadline_minutes)
    marked = set(request.marked)
    keys = {f"{request.hosted_cluster_namespace}/{name}" for name in request.nodepools}
    while True:
        machines = {
            (m.get("metadata") or {}).get("name"): m
            for m in await _machines_in(request.hcp_namespace)
            if machine_selection.machine_nodepool(m) in keys
        }
        deleting = sorted(
            name for name in marked
            if name not in machines or machine_selection.machine_deleting(machines[name])
        )
        watch = MarkedMachinesWatch(
            result=MarkedMachinesOutcome.DONE,
            deleting=deleting,
            pending=sorted(marked - set(deleting)),
            unexpected=sorted(
                name for name, machine in machines.items()
                if name not in marked and machine_selection.machine_deleting(machine)
            ),
        )
        activity.heartbeat(started.isoformat())
        if watch.unexpected:
            watch.result = MarkedMachinesOutcome.WRONG_MACHINE
        elif watch.pending and datetime.now(UTC) >= deadline:
            watch.result = MarkedMachinesOutcome.DEADLINE
        elif watch.pending:
            await asyncio.sleep(_MACHINE_WATCH_POLL_SECONDS)
            continue
        activity.logger.info(
            "Marked Machines in %s: %s (deleting %s, pending %s, unexpected %s)",
            request.hcp_namespace, watch.result.value, watch.deleting, watch.pending, watch.unexpected,
        )
        return watch
