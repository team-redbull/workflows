"""install-server — takes one healthy, unclaimed server from the server-scan
inventory platform and makes it installable in an MCE InfraEnv, by creating the
BareMetalHost + BMC Secret + NMStateConfig that the Assisted Installer needs.

The FIRST workflow of the `server-lifecycle` domain. It replaces the
`bmh-generator-operator` (BareMetalHostUCS), a Kopf operator that queried HP
OneView / UCS Central / Dell OME / Intersight live on every reconcile to learn a
server's MACs and BMC address. server-scan already collects that from all five
collectors on a 6-hour cron and knows which servers are unclaimed, so the whole
vendor-strategy layer is gone: one HTTP call replaces four vendor SDKs.

Shape of the run:

  1. resolving-vlan      — the VLAN belongs to the target MCE, not to the
                           server or the caller: each MCE owns one inventory
                           segment, looked up in the Segments Manager. Runs on
                           the SEGMENT-LIFECYCLE queue, where that credential
                           already lives.
  2. acquiring-server    — GET /servers/available. The InfraEnv name states
                           which hardware it is for
                           (cisco-m6-bat-yam-64c-512gb), and server names carry
                           the same tokens behind an `ocp-` prefix, so the
                           InfraEnv IS the query. Several candidates are drawn
                           so one unusable server is a retry, not a failure.
  3. selecting-bond      — pure workflow logic, no activity. Two link-up NICs
                           on two DISTINCT physical ports (see below).
  4. creating-secret
     creating-baremetalhost
     creating-nmstateconfig  — all idempotent; an existing resource is success.
  5. verifying-registration — a BOUNDED poll. Storing the BareMetalHost only
                           means the API server accepted it; Ironic still has
                           to reach the BMC, and a wrong address or credential
                           surfaces nowhere else.

Why the bond members come from `interfaces` and never from a MAC list: server-
scan reduces Dell NPAR partitions to one entry per physical port in
`network.interfaces`, but leaves `identity.nic_macs` deliberately whole. A
4-port partitioned card therefore reports 4 interfaces and 16 MACs. bmhgen
indexed that MAC list positionally, which can bond two partitions of ONE
physical port — a bond with no redundancy over a single wire, which looks
correct until that wire fails.

On cancellation there is deliberately no compensating cleanup: the three
resources are idempotent, so a re-run converges on them, and a half-created set
is what an operator needs in order to see how far the run got. Removing a
server is a separate uninstall-server workflow, not a rollback of this one.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import (
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
        SERVER_LIFECYCLE_ACTIVITY_QUEUE,
    )
    from shared.bmc_address import build_bmc_address
    from shared.interfaces.segment_lifecycle import get_inventory_segment
    from shared.interfaces.server_lifecycle import (
        acquire_servers,
        create_baremetal_host,
        create_bmc_secret,
        create_nmstate_config,
        get_baremetal_host,
    )
    from shared.models.server_lifecycle import (
        AcquiredServer,
        AcquireServerRequest,
        BmhResourceRequest,
        BondMember,
        InstallServerProgress,
        InstallServerResult,
        InstallServerRunArgs,
        LinkState,
    )

# Same budget rules as the segment-lifecycle workflows: a bounded per-attempt
# timeout (with the HTTP client timing out below it), UNBOUNDED retries so a
# transient outage is out-waited, and every known-permanent error classified.
# An UNCLASSIFIED permanent error retries every minute forever and leaves the
# run RUNNING rather than FAILED — which is why this list matters more than it
# looks.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[
        "ServerScanAuthError",
        "ServerNotAvailableError",
        "AmbiguousServerNameError",
        "UnknownBmcVendorError",
        "BmcCredentialsMissingError",
        "BmhConflictError",
        "InventorySegmentNotFoundError",
        "SegmentsManagerAuthError",
    ],
)

# Registration is MACHINE convergence — Ironic reaching the BMC — so the wait
# gets a real deadline and fails loudly, exactly as allocate-segment's DHCP
# scope poll does. A host that has not registered in 10 minutes has a bad BMC
# address or credential, not a slow one. Changing either constant is a
# non-deterministic change for in-flight runs.
_BMH_POLL_INTERVAL = timedelta(seconds=15)
_BMH_REGISTRATION_DEADLINE = timedelta(minutes=10)

# A BareMetalHost is registered once Ironic has moved it out of the empty
# initial state. "inspecting" is included because inspection is disabled on
# these hosts, so it is a state they pass through rather than settle in.
_REGISTERED_STATES = frozenset(
    {"registering", "inspecting", "preparing", "available", "ready", "provisioning",
     "provisioned", "externally provisioned"}
)

# The server-scan health verdict this workflow accepts. Deliberately only the
# top tier: the endpoint would otherwise fill HEALTHY, then WARNING, then MAJOR.
_REQUIRED_HEALTH = "HEALTHY"

# A bond needs two members, so a candidate with fewer usable ports is no use.
_BOND_MEMBER_COUNT = 2

# The BMC driver vocabularies shared/bmc_address.py can map. server-scan reports
# `bmc_vendor: null` for a STANDALONE machine on purpose — the driver is the
# caller's decision — and this workflow declines rather than guessing.
_KNOWN_BMC_VENDORS = frozenset({"HP", "DELL", "CISCO", "INTERSIGHT"})


def select_bond_members(server: AcquiredServer) -> list[BondMember] | None:
    """Pick two link-up NICs on two DISTINCT physical ports, or None.

    Pure and deterministic, so it runs in workflow code and replays safely.

    Link state is required to be UP strictly, never merely "not DOWN". That is
    a deliberate, accepted trade: HPE OneView reports no link state at all (its
    portMap carries none, so server-scan stores UNKNOWN unconditionally) and
    Intersight vNICs usually report none either, so servers from those
    collectors cannot satisfy this and fail in the caller with an explicit
    message — rather than being silently passed over.

    Distinctness is by physical port, not by interface: two NPAR partitions of
    one port are two interfaces with two MACs on ONE wire, and bonding them
    yields no redundancy at all.
    """
    members: list[BondMember] = []
    seen_ports: set[str] = set()
    for interface in server.interfaces:
        if interface.mac is None or interface.link_state != LinkState.UP:
            continue
        port_key = interface.physical_port_key()
        if port_key in seen_ports:
            continue
        seen_ports.add(port_key)
        members.append(
            BondMember(
                logical_name=f"nic{len(members) + 1}",
                mac=interface.mac,
                port_key=port_key,
            )
        )
        if len(members) == _BOND_MEMBER_COUNT:
            return members
    return None


def _describe_candidate(server: AcquiredServer) -> str:
    """One candidate's interfaces, for the no-bondable-interfaces failure.

    Names the provider and every link state, because the usual cause is that
    the collector cannot report link state at all rather than that the hardware
    is down — and those two look identical from the workflow's side.
    """
    if not server.interfaces:
        interfaces = "no interfaces reported"
    else:
        interfaces = ", ".join(
            f"{i.name}[{i.physical_port_key()}]={i.link_state.value}"
            for i in server.interfaces
        )
    return f"{server.name} (provider={server.source_provider}): {interfaces}"


@workflow.defn
class InstallServerWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._server_name: str | None = None

    @workflow.query
    def progress(self) -> InstallServerProgress:
        """Cheap progress surface for the async caller (GET status endpoint)."""
        return InstallServerProgress(phase=self._phase, server_name=self._server_name)

    @workflow.run
    async def run(self, run_args: InstallServerRunArgs) -> InstallServerResult:
        install_input = run_args.input
        infra_env = install_input.infra_env
        workflow.logger.info(
            "Installing a server into InfraEnv=%s for MCE=%s",
            infra_env,
            install_input.mce_cluster,
        )

        # Step 1 — the VLAN is the MCE's, so it is read from the Segments
        # Manager rather than taken from the caller: a supplied VLAN could
        # contradict the segment the cluster actually owns. Runs on the
        # segment-lifecycle queue, which already holds that credential.
        self._phase = "resolving-vlan"
        segment = await workflow.execute_activity(
            get_inventory_segment,
            install_input.mce_cluster,
            task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if segment.cluster_name != install_input.mce_cluster:
            raise ApplicationError(
                f"Segments Manager returned segment {segment.segment} allocated "
                f"to {segment.cluster_name!r}, not to the requested MCE "
                f"{install_input.mce_cluster!r}",
                type="InventorySegmentMismatch",
            )

        # Step 2 — the InfraEnv name IS the hardware query. It encodes vendor,
        # model, site and spec, and server names carry the same tokens behind
        # an `ocp-` prefix, so no part of either is parsed apart.
        self._phase = "acquiring-server"
        request = (
            AcquireServerRequest(
                name=install_input.server_name,
                count=1,
                health=_REQUIRED_HEALTH,
                min_nic_macs=_BOND_MEMBER_COUNT,
            )
            if install_input.server_name
            else AcquireServerRequest(
                pattern=f"^ocp-{infra_env}",
                count=install_input.candidate_count,
                health=_REQUIRED_HEALTH,
                min_nic_macs=_BOND_MEMBER_COUNT,
            )
        )
        candidates = await workflow.execute_activity(
            acquire_servers,
            request,
            task_queue=SERVER_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Step 3 — the first candidate that can actually carry a bond. Pure
        # logic, so no activity and no extra round trip.
        self._phase = "selecting-bond"
        server: AcquiredServer | None = None
        bond_members: list[BondMember] | None = None
        for candidate in candidates:
            members = select_bond_members(candidate)
            if members is not None:
                server, bond_members = candidate, members
                break
        if server is None or bond_members is None:
            raise ApplicationError(
                f"No candidate for InfraEnv {infra_env} offers two link-up NICs "
                f"on two distinct physical ports. Checked "
                f"{len(candidates)} candidate(s): "
                + "; ".join(_describe_candidate(c) for c in candidates)
                + ". Note that HPE OneView reports no link state at all and "
                "Intersight vNICs usually report none, so servers from those "
                "collectors cannot satisfy a strict link-up requirement",
                type="NoBondableInterfacesError",
            )
        self._server_name = server.name

        if server.bmc_vendor is None or server.bmc_vendor.upper() not in _KNOWN_BMC_VENDORS:
            raise ApplicationError(
                f"server-scan reports no usable BMC driver vocabulary for "
                f"{server.name} (bmc_vendor={server.bmc_vendor!r}, "
                f"vendor={server.vendor}, provider={server.source_provider}). "
                "A STANDALONE machine's driver is the caller's decision, and "
                "guessing IPMI would be silently wrong for a Redfish-only BMC",
                type="UnknownBmcVendorError",
            )

        resource_request = BmhResourceRequest(
            server_name=server.name,
            namespace=install_input.namespace,
            infra_env=infra_env,
            bmc_vendor=server.bmc_vendor,
            bmc=server.bmc,
            bond_members=bond_members,
            vlan_id=segment.vlan_id,
            labels=install_input.labels,
        )

        # Step 4 — the three resources, in dependency order: the BareMetalHost
        # references the Secret by name, so the Secret goes first. Each create
        # treats "already exists" as success, which is what makes the whole run
        # re-runnable.
        self._phase = "creating-secret"
        secret = await self._create(create_bmc_secret, resource_request)

        self._phase = "creating-baremetalhost"
        bmh = await self._create(create_baremetal_host, resource_request)

        self._phase = "creating-nmstateconfig"
        nmstate = await self._create(create_nmstate_config, resource_request)

        # Step 5 — a stored object is not a registered host. Ironic still has
        # to reach the BMC, and a wrong address or credential shows up only
        # here, so the run does not claim success until it has.
        self._phase = "verifying-registration"
        await self._await_registration(resource_request)

        self._phase = "completed"
        boot_mac = bond_members[0].mac
        bmc_address = build_bmc_address(server.bmc_vendor, server.bmc)
        workflow.logger.info(
            "Server %s installed into InfraEnv %s (vlan=%d, bond=%s)",
            server.name,
            infra_env,
            segment.vlan_id,
            [member.mac for member in bond_members],
        )
        return InstallServerResult(
            server_id=server.id,
            server_name=server.name,
            infra_env=infra_env,
            namespace=resource_request.namespace,
            mce_cluster=install_input.mce_cluster,
            vlan_id=segment.vlan_id,
            bmc_vendor=server.bmc_vendor,
            bmc_address=bmc_address,
            bond_macs=[member.mac for member in bond_members],
            boot_mac=boot_mac,
            resources_changed=any(
                resource.changed for resource in (secret, bmh, nmstate)
            ),
            bmh_registered=True,
        )

    async def _create(self, activity_fn, request: BmhResourceRequest):
        """Run one idempotent resource create on the server-lifecycle queue."""
        return await workflow.execute_activity(
            activity_fn,
            request,
            task_queue=SERVER_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _await_registration(self, request: BmhResourceRequest) -> None:
        """Poll until Ironic has registered the host, or fail at the deadline."""
        started_at = workflow.now()
        while True:
            state = await workflow.execute_activity(
                get_baremetal_host,
                request,
                task_queue=SERVER_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if state.found and (state.provisioning_state or "").lower() in _REGISTERED_STATES:
                return
            if workflow.now() - started_at >= _BMH_REGISTRATION_DEADLINE:
                observed = (
                    f"provisioning state {state.provisioning_state!r}, "
                    f"operationalStatus {state.operational_status!r}"
                    f"{f', error: {state.error_message}' if state.error_message else ''}"
                    if state.found
                    else "the BareMetalHost was never observed"
                )
                raise ApplicationError(
                    f"BareMetalHost {request.server_name} did not register within "
                    f"{int(_BMH_REGISTRATION_DEADLINE.total_seconds() // 60)} minutes: "
                    f"{observed}. The Secret, BareMetalHost and NMStateConfig "
                    "stand — check the BMC address and credentials, and Ironic's "
                    "reachability of that BMC",
                    type="BmhNotRegisteredError",
                )
            await workflow.sleep(_BMH_POLL_INTERVAL)  # durable, replay-safe timer
