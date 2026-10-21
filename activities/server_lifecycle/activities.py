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

This module owns what only an activity can: the settings instance (read once at
import, so a missing key crash-loops the worker instead of failing a run) and
`activity.logger`.

BMC credentials are the one thing server-scan never returns — it holds inventory
data only — so they come from this deployment's own Secret, per vendor, exactly
as the operator's did.
"""

from __future__ import annotations

import asyncio

from temporalio import activity

from activities.server_lifecycle import cluster_api, pxe_map, server_scan
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
from shared.bmc_address import k8s_resource_name, nmstate_config_name
from shared.exceptions import (
    AgentBoundToClusterError,
    BmcCredentialsMissingError,
    BmhTeardownError,
    InfraEnvNotFoundError,
    InfraEnvNotReadyError,
    PxeSiteNotConfiguredError,
)
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    AgentBinding,
    AgentDeleteRequest,
    AgentDeletion,
    AgentRef,
    AgentState,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    HostPowerRequest,
    InfraEnvBootRequest,
    InfraEnvBootState,
    InfraEnvRef,
    InfraEnvState,
    InventoryHostState,
    PxeBootRegistration,
    PxeBootRequest,
    PxeHostEntriesRef,
    PxeHostEntriesRemoval,
    PxeMapRef,
    PxeUnregistration,
    ReleaseServerRequest,
    ReserveServerRequest,
    ServerReservation,
    TeardownResult,
)
from shared.settings import ServerLifecycleActivitySettings

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
async def release_server(request: ReleaseServerRequest) -> ServerReservation:
    """Give the install lock back after a candidate was rolled back."""
    reservation = await server_scan.release_server(
        _settings.server_scan_url, _settings.server_scan_api_token, request
    )
    activity.logger.info(
        "Released %s in server-scan%s",
        request.server_name,
        f" — {reservation.detail}" if reservation.detail else "",
    )
    return reservation


async def _read(group: str, version: str, plural: str, kind: str, namespace: str, name: str):
    return await cluster_api.read_custom_object(
        group=group, version=version, plural=plural, kind=kind, namespace=namespace, name=name
    )


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
    nmstate = await _read(
        NMSTATE_GROUP,
        NMSTATE_VERSION,
        NMSTATE_PLURAL,
        _NMSTATE_KIND,
        ref.namespace,
        nmstate_config_name(ref.server_name),
    )
    # server-scan's MACs find an Agent with no BMH / NMStateConfig left to
    # take a MAC from.
    scan_macs = await server_scan.server_macs_by_name(
        _settings.server_scan_url, _settings.server_scan_api_token, ref.server_name
    )
    macs = sorted(set(host_macs(bmh, nmstate)) | set(scan_macs))
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
        nmstate_found=nmstate is not None,
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
        # Its own site only — the VM install-server mapped it on.
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
    cluster_ref = ((infra_env.get("spec") or {}).get("clusterRef") or {}).get("name")
    activity.logger.info(
        "InfraEnv %s/%s exists%s",
        ref.namespace,
        ref.name,
        f" (early-bound to cluster {cluster_ref})" if cluster_ref else "",
    )
    return InfraEnvState(name=ref.name, namespace=ref.namespace, cluster_ref=cluster_ref)


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

    Without a `server_name` (publish-infraenv-boot): its current image, as soon
    as there is a ready one.
    """
    if request.server_name:
        needed_by = _boots_from_network(request.server_name)
    else:
        needed_by = f"InfraEnv {request.mce_cluster}/{request.infra_env} is to be published"
    base_url = _pxe_map_url(request.site, needed_by)
    infra_env = await _read_infra_env(request.namespace, request.infra_env)

    if request.server_name:
        awaited = nmstate_config_name(request.server_name)
        nmstate = await _read(
            NMSTATE_GROUP,
            NMSTATE_VERSION,
            NMSTATE_PLURAL,
            _NMSTATE_KIND,
            request.namespace,
            awaited,
        )
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
    username, password = _bmc_credentials(request.bmc_vendor)
    return _log_outcome(
        await cluster_api.create_secret_if_absent(
            request.namespace, build_bmc_secret(request, username, password)
        )
    )


@activity.defn
async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
    """Create the BareMetalHost (metal3.io/v1alpha1)."""
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
    """Create the NMStateConfig (`nmstate-config-{server}`)."""
    return _log_outcome(
        await cluster_api.create_custom_object_if_absent(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            plural=NMSTATE_PLURAL,
            kind=_NMSTATE_KIND,
            namespace=request.namespace,
            body=build_nmstate_config(request),
            differences=nmstate_config_differences,
        )
    )


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

    The ordering here was established against a live cluster and is load-bearing
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
    nmstate_name = nmstate_config_name(ref.server_name)
    if await cluster_api.delete_custom_object(
        group=NMSTATE_GROUP,
        version=NMSTATE_VERSION,
        plural=NMSTATE_PLURAL,
        kind=_NMSTATE_KIND,
        namespace=namespace,
        name=nmstate_name,
    ):
        removed.append(f"{_NMSTATE_KIND}/{nmstate_name}")

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
