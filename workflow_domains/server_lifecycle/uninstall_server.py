"""uninstall-server — takes ONE server all the way out of an MCE, whatever state its Agent is in.

A server on an MCE is in one of two shapes, and the run picks its path by the
BINDING it observes at inspection — never by the Agent's state string:

  * A HOSTED CLUSTER'S NODE: the Agent is bound (`spec.clusterDeploymentName`)
    AND an AgentMachine is behind it — or an AgentMachine still names it in
    `status.agentRef`, which is a Machine deletion already under way (the
    Agent provider clears the binding before the Machine finishes, so a
    re-run must recognise that shape and resume, not skip). The
    HOSTED-CLUSTER HALF runs first: the CAPI Machine is deleted, CAPI drains
    the Node, the provider releases the Agent, and the host reboots back into
    the discovery image.
  * AN INVENTORY HOST: nothing binds it. The hosted-cluster half is skipped.

Every path then ends by running remove-server as an AWAITED CHILD
(`remove-server-<server>`, its own queue), so the inventory half — power off,
BareMetalHost, NMStateConfig, the Agent last, the PXE map of the MCE's own site —
is never reimplemented here, and remove-server's own route keeps working alone.
A server with nothing on the MCE completes as a no-op through the child's
`found: false`.

Shape of the run (the `progress` query exposes the phase and the MCE's queue):

  1. checking-inventory  — server-scan's record of WHERE the server is
                           (search + exact-name detail; never /servers/available).
                           Refused only on a CONTRADICTION: unknown to
                           server-scan (no lock could be taken), contested
                           between two claims, or reported on another MCE.
  2. inspecting          — BMH, the host's Agents (bmh label or any MAC), each
                           one's binding, state and AgentMachine, the owning
                           Machine (deletionTimestamp, nodeRef, hooks) and the
                           HostedCluster. The Node name is captured NOW: the
                           Machine is about to go. Refused: two bound Agents;
                           an Agent bound with no AgentMachine behind it (an
                           early-bound ACM/ZTP ClusterDeployment or a binding
                           left behind — v2); server-scan naming another
                           cluster than the Agent, or INSTALLED_TO_INVENTORY
                           while the Agent is bound.
  3. reserving-server    — server-scan's install lock, holder `uninstall-server`,
                           ALWAYS, before any write (below for why).
  4. the hosted-cluster half, ONLY for a hosted node —
       deleting-machine          ensure the Machine is deleting: a no-op when
                                 it already is (a scale-down, an earlier run)
                                 or is gone. The ONE write on the MCE.
       awaiting-machine-deletion every 30 s, until the Machine is gone and the
                                 Agent is `known-unbound`: `draining` while
                                 the Agent is still bound (20 min from when
                                 the Machine is first seen deleting, else
                                 MachineDrainTimeoutError), `unbinding` once
                                 the provider released it (1 h — a bare-metal
                                 reboot — else AgentUnbindTimeoutError).
       verifying-agentmachine    the AgentMachine is gone (bounded 5 min).
       verifying-node            the hosted cluster has no Node of that name,
                                 read through its admin kubeconfig; SKIPPED
                                 when the Machine never had a nodeRef. Never
                                 deleted: HostedNodeStillPresentError.
       verifying-agent           unbound, no agentMachineRef label,
                                 known-unbound, approved, BMH present and not
                                 in error (bounded 5 min) — what remove-server
                                 needs to take it.
  5. holding-server      — the lock extended to 24 h BEFORE the child starts,
                           so the hold covers the inventory half too.
  6. removing-from-inventory — remove-server, awaited; its result is embedded.

Every skipped step is named in the result's `skipped`.

STANDALONE IS A REPLACE; A SCALE-DOWN IS A SHRINK. NodePool replicas are NOT
this workflow's business, and that has a consequence an operator must know:
run on its own, the Machine is deleted but the NodePool still wants the same
number of nodes, so its MachineSet creates a replacement Machine and the Agent
provider binds ANOTHER Agent from the InfraEnv — the hosted cluster ends with
the same size, on a different server. Only a caller that lowered the replicas
first — the future `scale-down-nodepool` workflow, which owns the NodePool's
annotation and replicas and then triggers this run per server — shrinks the
cluster; it reaches this run with the Machine already deleting, and step 4's
delete is a no-op.

WHY THE LOCK, EVEN FOR AN UNINSTALL. Two server-scan jobs see this machine
and neither sees the whole move: the hosted cluster's nodes job frees it the
moment its Node disappears, and the MCE's agents job re-claims it as
INSTALLED_TO_INVENTORY on its own 15-minute cadence. Between the two the
document can read AVAILABLE while the host is still in this MCE's InfraEnv —
and another MCE's install-server would draw it. The lock closes that window
from before the first write; on success it is held for a day (install-server's
`_INSTALLED_HOLD`), and on failure it is left to its TTL, because the host may
still be half-way out. A lock another run holds — install-server's day-long
hold after it installed the machine, typically — fails the run before anything
is written.

WHAT IS NEVER DONE: no NodePool change, no Node delete, no Node-side drain (CAPI
owns it), no PXE map write (remove-server's), no undo — a Machine delete
cannot be undone, so a failure after it leaves everything as it is and a re-run
of the same id RESUMES.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    WorkflowAlreadyStartedError,
)
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import canonical_mce_cluster
    from shared.consts import (
        REMOVE_SERVER_WORKFLOW_QUEUE,
        server_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        AgentStillBoundError,
        AgentUnbindTimeoutError,
        AmbiguousServerNameError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        ClusterMembershipMismatchError,
        HostedClusterAccessError,
        HostedNodeStillPresentError,
        InvalidServerNameError,
        MachineDeletionTimeoutError,
        MachineDrainTimeoutError,
        MultipleBoundAgentsError,
        NotHostedClusterNodeError,
        RemoveServerAlreadyRunningError,
        RemoveServerFailedError,
        ServerNotAvailableError,
        ServerNotInServerScanError,
        ServerReservedError,
        ServerScanAuthError,
        ServerScanRequestInvalidError,
        ServerStillPresentError,
    )
    from shared.interfaces.server_lifecycle import (
        check_hosted_node,
        ensure_machine_deleting,
        get_agent_binding,
        get_agent_machine_state,
        get_baremetal_host,
        get_machine_state,
        inspect_bound_host,
        lookup_server_membership,
        reserve_server,
    )
    from shared.models.server_lifecycle import (
        AgentBindingState,
        AgentMachineRef,
        AgentNameRef,
        BmhRef,
        BmhState,
        BoundHostState,
        HostAgent,
        HostedClusterRef,
        HostedNodeRef,
        MachineRef,
        MachineState,
        RemoveServerInput,
        RemoveServerResult,
        RemoveServerRunArgs,
        ReserveServerRequest,
        ServerMembership,
        ServerMembershipRequest,
        UninstallServerInput,
        UninstallServerProgress,
        UninstallServerResult,
        UninstallServerRunArgs,
    )
    from shared.workflow_ids import remove_server_workflow_id

from workflow_domains.server_lifecycle.remove_server import RemoveServerWorkflow

_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# The same signal remove-server and install-server use for "no worker polls
# this MCE's queue" (see install_server.py).
_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)

# Activity-raised failures no retry can fix, as CLASSES (a wrong name is an
# ImportError at worker start, not a silent "retry forever").
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    AmbiguousServerNameError,
    ServerScanAuthError,
    ServerScanRequestInvalidError,
    ServerReservedError,
    ServerNotAvailableError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    InvalidServerNameError,
    HostedClusterAccessError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# The Agent state an inventory host settles in, and the only one remove-server
# is handed a host in.
_KNOWN_UNBOUND = "known-unbound"

# server-scan's lifecycle readings (kept as strings on ServerMembership).
_INSTALLED = "INSTALLED"
_INSTALLED_TO_INVENTORY = "INSTALLED_TO_INVENTORY"

# MACHINE convergence, so real deadlines (CLAUDE.md §5). Changing any of these
# is a non-deterministic change for in-flight runs.
#
# Draining: pre-drain hooks, the Node drain, volume detach — what CAPI does
# while the Agent is still bound. Twenty minutes, decided with the operator: a
# drain that has not finished by then is held by something (a
# PodDisruptionBudget, a hook) that waiting will not clear.
_DRAIN_DEADLINE = timedelta(minutes=20)
# Unbinding: the provider has released the Agent and the host reboots into the
# discovery image — on bare metal the same POST-and-boot install-server allows
# an hour for.
_UNBIND_DEADLINE = timedelta(hours=1)
_MACHINE_POLL_INTERVAL = timedelta(seconds=30)
# The checks after the Machine is gone: a finalizer or a status update takes
# seconds, so five minutes is ample and short enough to fail rather than hide.
_VERIFY_DEADLINE = timedelta(minutes=5)
_VERIFY_POLL_INTERVAL = timedelta(seconds=10)

# server-scan's install lock (ADR-0035). The TTL covers the whole
# hosted-cluster half — the drain and the reboot budgets, both verifications —
# plus a margin, so the bounded waits always reach the day-long hold (or fail)
# before it lapses. Held a day once the hosted-cluster half is done: the
# ceiling server-scan accepts, and the precedent of install-server's
# `_INSTALLED_HOLD`.
_RESERVATION_HOLDER = "uninstall-server"
_RESERVATION_TTL = _DRAIN_DEADLINE + _UNBIND_DEADLINE + 2 * _VERIFY_DEADLINE + timedelta(minutes=10)
_RELEASED_HOLD = timedelta(hours=24)


def _qualified(namespace: str | None, name: str | None) -> str:
    return f"{namespace}/{name}"


def _machine_detail(machine: MachineState) -> str:
    conditions = "; ".join(machine.conditions) or "none reported"
    hooks = ", ".join(machine.pending_hooks) or "none"
    return f"deletion conditions: {conditions}. Pending delete hooks: {hooks}"


def _agent_problems(agent: AgentBindingState, bmh: BmhState) -> list[str]:
    """Why the Agent is not yet the clean inventory host remove-server takes."""
    if not agent.found:
        return ["the Agent no longer exists"]
    problems = []
    if agent.cluster_deployment:
        problems.append(f"still bound to {agent.cluster_deployment}")
    if agent.agent_machine_label:
        problems.append("still carries the agentMachineRef label")
    if agent.state != _KNOWN_UNBOUND:
        problems.append(f"its state is {agent.state or 'unknown'}, not {_KNOWN_UNBOUND}")
    if agent.approved is not True:
        problems.append("it is not approved")
    if not bmh.found:
        problems.append("its BareMetalHost is gone, so remove-server could not power the host off")
    elif bmh.is_errored():
        problems.append(
            f"its BareMetalHost is in error ({bmh.error_type or 'no type'}: "
            f"{bmh.error_message or 'no message'})"
        )
    return problems


@workflow.defn
class UninstallServerWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._sub_phase: str | None = None
        self._activity_queue: str | None = None
        self._agent_state: str | None = None

    @workflow.query
    def progress(self) -> UninstallServerProgress:
        return UninstallServerProgress(
            phase=self._phase,
            sub_phase=self._sub_phase,
            activity_queue=self._activity_queue,
            agent_state=self._agent_state,
        )

    @workflow.run
    async def run(self, run_args: UninstallServerRunArgs) -> UninstallServerResult:
        request = run_args.input
        activity_queue = server_lifecycle_activity_queue(request.mce_cluster)
        self._activity_queue = activity_queue
        skipped: list[str] = []

        # Step 1 — where server-scan says the server is.
        self._phase = "checking-inventory"
        membership: ServerMembership = await self._call(
            lookup_server_membership,
            ServerMembershipRequest(server_name=request.server_name),
            activity_queue,
        )
        self._refuse_membership(request, membership)

        # Step 2 — what is on the MCE, and whose node it is.
        self._phase = "inspecting"
        host: BoundHostState = await self._call(
            inspect_bound_host,
            BmhRef(server_name=request.server_name, namespace=request.namespace),
            activity_queue,
        )
        hosted_agent = self._hosted_agent(request, host)
        self._refuse_contradiction(request, membership, host)
        followed = hosted_agent or (host.agents[0] if host.agents else None)
        self._agent_state = followed.state if followed else None
        result = UninstallServerResult(
            server_name=request.server_name,
            namespace=request.namespace,
            mce_cluster=request.mce_cluster,
            server_id=membership.server_id,
            lifecycle_state_at_start=membership.lifecycle_state,
            hosted=hosted_agent is not None,
            bound_at_start=bool(host.bound_agents),
            agent_state_at_start=self._agent_state,
            agent_name=followed.name if followed else None,
        )

        # Step 3 — the lock, before anything is written.
        self._phase = "reserving-server"
        await self._reserve(request, membership, host, activity_queue)

        # Step 4 — the hosted-cluster half, for a hosted cluster's node only.
        if hosted_agent is not None and host.hosted_cluster is not None:
            # _hosted_agent refuses a hosted Agent with no HostedCluster, so
            # the second condition only narrows the type.
            await self._uninstall_from_hosted_cluster(
                request, host, host.hosted_cluster, hosted_agent, result, skipped, activity_queue
            )
        else:
            skipped.append(
                "hosted-cluster half: no Agent of the host is bound to a hosted "
                "cluster or still has an AgentMachine"
            )

        # Step 5 — hold the machine through the inventory half and beyond.
        self._phase = "holding-server"
        result.reservation_expires_at = await self._hold(request, membership, host, activity_queue)

        # Step 6 — the inventory half: remove-server, awaited.
        self._phase = "removing-from-inventory"
        result.remove_server_workflow_id = remove_server_workflow_id(request.server_name)
        result.remove_server = await self._remove_from_inventory(
            request, result.remove_server_workflow_id
        )
        result.skipped = skipped

        self._phase = "completed"
        workflow.logger.info(
            "Uninstalled %s from MCE %s: hosted=%s machine=%s node=%s, removal found=%s",
            request.server_name,
            request.mce_cluster,
            result.hosted,
            result.machine,
            result.node_name,
            result.remove_server.found,
        )
        return result

    # --- refusals --------------------------------------------------------------

    def _refuse_membership(self, request: UninstallServerInput, membership: ServerMembership) -> None:
        """Step 1's refusals: what server-scan alone can contradict. Nothing is written yet."""
        server = request.server_name
        if not membership.found:
            raise ApplicationError(
                f"server-scan has no server named {server}. The install lock that "
                "keeps it out of other MCEs' draws cannot be taken on a machine it "
                "does not know. Nothing was changed. A host that is only on the MCE "
                "is remove-server's",
                type=ServerNotInServerScanError.__name__,
            )
        if membership.maintenance:
            workflow.logger.info(
                "%s is in maintenance in server-scan (%s) — the uninstall goes ahead",
                server,
                membership.maintenance_reason or "no reason given",
            )
        if membership.contested_with:
            raise ApplicationError(
                f"server-scan reports {server} contested: its claim "
                f"({membership.lifecycle_state}, cluster {membership.cluster_name}, MCE "
                f"{membership.mce_name}) keeps flipping with {membership.contested_with}. "
                "Two clusters claim the machine. Nothing was changed — settle which "
                "one owns it first",
                type=ClusterMembershipMismatchError.__name__,
            )
        if (
            membership.mce_name
            and membership.lifecycle_state in (_INSTALLED, _INSTALLED_TO_INVENTORY)
            and canonical_mce_cluster(membership.mce_name) != request.mce_cluster
        ):
            raise ApplicationError(
                f"server-scan reports {server} {membership.lifecycle_state} on MCE "
                f"{membership.mce_name}, not {request.mce_cluster}. Nothing was "
                "changed — uninstall it from the MCE that holds it",
                type=ClusterMembershipMismatchError.__name__,
            )

    def _hosted_agent(self, request: UninstallServerInput, host: BoundHostState) -> HostAgent | None:
        """The Agent the hosted-cluster half follows, or None for an inventory host."""
        server = request.server_name
        if len(host.bound_agents) > 1 or len(host.hosted_agents) > 1:
            several = {a.name: a for a in host.bound_agents + host.hosted_agents}
            raise ApplicationError(
                f"{server} has {len(several)} Agents bound to a cluster on MCE "
                f"{request.mce_cluster}: "
                + "; ".join(
                    f"Agent {a.name} -> {a.cluster_deployment or a.agent_machine}"
                    for a in several.values()
                )
                + ". One machine is one node, so nothing was changed",
                type=MultipleBoundAgentsError.__name__,
            )
        bound = host.bound_agents[0] if host.bound_agents else None
        if bound is not None and bound.agent_machine is None:
            raise ApplicationError(
                f"Agent {bound.name} of {server} is bound to {bound.cluster_deployment}, "
                "but no AgentMachine is behind it — an early-bound ACM/ZTP "
                "ClusterDeployment, or a binding left behind by a cluster that is "
                "gone. There is no hosted-cluster Machine to delete, so nothing was "
                "changed",
                type=NotHostedClusterNodeError.__name__,
            )
        hosted = host.hosted_agents[0] if host.hosted_agents else None
        if hosted is None:
            return None
        cluster = host.hosted_cluster
        if cluster is None:
            raise ApplicationError(
                f"AgentMachine {hosted.agent_machine} behind Agent {hosted.name} of "
                f"{server} is in a namespace no HostedCluster's control plane uses, so "
                "there is no hosted cluster to verify the Node against. Nothing was "
                "changed",
                type=NotHostedClusterNodeError.__name__,
            )
        expected = _qualified(cluster.hcp_namespace, cluster.name)
        if hosted.cluster_deployment and hosted.cluster_deployment != expected:
            raise ApplicationError(
                f"Agent {hosted.name} of {server} is bound to "
                f"{hosted.cluster_deployment}, but its AgentMachine "
                f"{hosted.agent_machine} belongs to hosted cluster "
                f"{_qualified(cluster.namespace, cluster.name)} (ClusterDeployment "
                f"{expected}). Nothing was changed",
                type=NotHostedClusterNodeError.__name__,
            )
        return hosted

    def _refuse_contradiction(
        self, request: UninstallServerInput, membership: ServerMembership, host: BoundHostState
    ) -> None:
        """Step 2's refusals: server-scan against what the MCE shows. Nothing is written yet.

        Only a claim can contradict: an Agent that is not bound names no
        cluster, so a server-scan record of INSTALLED that has not caught up
        with an unbinding is not refused — that is a re-run's normal view.
        """
        server = request.server_name
        cluster = host.hosted_cluster if host.hosted_agents else None
        if (
            membership.lifecycle_state == _INSTALLED
            and membership.cluster_name
            and cluster is not None
            and membership.cluster_name.lower() != cluster.name.lower()
        ):
            raise ApplicationError(
                f"server-scan reports {server} INSTALLED in cluster "
                f"{membership.cluster_name}, but on MCE {request.mce_cluster} it is a "
                f"node of hosted cluster {_qualified(cluster.namespace, cluster.name)}. "
                "Nothing was changed",
                type=ClusterMembershipMismatchError.__name__,
            )
        if membership.lifecycle_state == _INSTALLED_TO_INVENTORY and host.bound_agents:
            agent = host.bound_agents[0]
            raise ApplicationError(
                f"server-scan reports {server} INSTALLED_TO_INVENTORY (bound to no "
                f"cluster), but its Agent {agent.name} on MCE {request.mce_cluster} is "
                f"bound to {agent.cluster_deployment}. Nothing was changed — server-scan's "
                "agents job re-reads the MCE every 15 minutes; re-run once it agrees",
                type=ClusterMembershipMismatchError.__name__,
            )

    # --- the lock ----------------------------------------------------------------

    def _reservation(
        self,
        request: UninstallServerInput,
        membership: ServerMembership,
        host: BoundHostState,
        ttl: timedelta,
    ) -> ReserveServerRequest:
        return ReserveServerRequest(
            server_id=membership.server_id or "",
            server_name=membership.server_name or request.server_name,
            holder=_RESERVATION_HOLDER,
            workflow_id=workflow.info().workflow_id,
            mce_cluster=request.mce_cluster,
            infra_env=host.infra_env,
            namespace=request.namespace,
            ttl_seconds=int(ttl.total_seconds()),
        )

    async def _reserve(
        self,
        request: UninstallServerInput,
        membership: ServerMembership,
        host: BoundHostState,
        task_queue: str,
    ) -> None:
        """Take the lock before the first write; a lock held elsewhere fails the run."""
        try:
            await self._call(
                reserve_server,
                self._reservation(request, membership, host, _RESERVATION_TTL),
                task_queue,
            )
        except ActivityError as err:
            cause = err.cause
            if isinstance(cause, ApplicationError) and cause.type == ServerReservedError.__name__:
                raise ApplicationError(
                    f"{cause.message}. Nothing was changed. If that is install-server's "
                    "day-long hold after installing the machine, release it in "
                    "server-scan, then re-run",
                    type=ServerReservedError.__name__,
                ) from err
            raise

    async def _hold(
        self,
        request: UninstallServerInput,
        membership: ServerMembership,
        host: BoundHostState,
        task_queue: str,
    ) -> str | None:
        """Extend the lock to a day. Refused now, the machine must still leave this MCE."""
        try:
            reservation = await self._call(
                reserve_server,
                self._reservation(request, membership, host, _RELEASED_HOLD),
                task_queue,
            )
        except ActivityError as err:
            cause = err.cause
            if not isinstance(cause, ApplicationError) or cause.type not in (
                ServerReservedError.__name__,
                ServerNotAvailableError.__name__,
            ):
                raise
            workflow.logger.warning(
                "server-scan refused to extend the hold on %s (%s); removing it from "
                "MCE %s anyway — it must not stay in this InfraEnv",
                request.server_name,
                cause.message,
                request.mce_cluster,
            )
            return None
        return reservation.expires_at

    # --- the hosted-cluster half ---------------------------------------------------

    async def _uninstall_from_hosted_cluster(
        self,
        request: UninstallServerInput,
        host: BoundHostState,
        cluster: HostedClusterRef,
        agent: HostAgent,
        result: UninstallServerResult,
        skipped: list[str],
        task_queue: str,
    ) -> None:
        agent_machine_ns, agent_machine_name = (agent.agent_machine or "/").split("/", 1)
        machine_ref = host.machine
        inspected = host.machine_state or MachineState(found=False)
        node_name = inspected.node_name
        result.hosted_cluster = _qualified(cluster.namespace, cluster.name)
        result.hcp_namespace = cluster.hcp_namespace
        result.agent_machine = agent.agent_machine
        result.machine = _qualified(machine_ref.namespace, machine_ref.name) if machine_ref else None
        result.node_name = node_name

        # Delete the Machine — the one write on the MCE — unless it is going already.
        self._phase = "deleting-machine"
        if machine_ref is None:
            skipped.append(
                f"deleting-machine: AgentMachine {agent.agent_machine} names no Machine any more"
            )
        else:
            deletion = await self._call(ensure_machine_deleting, machine_ref, task_queue)
            if deletion.deleted or (deletion.already_deleting and not inspected.deleting):
                result.machine_deleted_by_run = True
            elif deletion.already_deleting:
                skipped.append(
                    f"deleting-machine: Machine {result.machine} was already deleting "
                    f"(since {inspected.deletion_timestamp}) — a NodePool scale-down, an "
                    "operator or an earlier run of this id"
                )
            else:
                skipped.append(f"deleting-machine: Machine {result.machine} was already gone")

        # Wait for CAPI: the drain, the provider's unbind, the Machine gone.
        self._phase = "awaiting-machine-deletion"
        agent_ref = AgentNameRef(namespace=agent.namespace, name=agent.name)
        captured = await self._await_machine_gone(
            request, machine_ref, agent_ref, result.hosted_cluster, task_queue
        )
        node_name = node_name or captured
        result.node_name = node_name
        self._sub_phase = None

        self._phase = "verifying-agentmachine"
        agent_machine_ref = AgentMachineRef(namespace=agent_machine_ns, name=agent_machine_name)
        started_at = workflow.now()
        while True:
            agent_machine = await self._call(get_agent_machine_state, agent_machine_ref, task_queue)
            if not agent_machine.found:
                break
            if workflow.now() - started_at >= _VERIFY_DEADLINE:
                held = (
                    " (deleting: the Agent provider's finalizer still holds it)"
                    if agent_machine.deleting
                    else ""
                )
                raise ApplicationError(
                    f"AgentMachine {agent.agent_machine} of hosted cluster "
                    f"{result.hosted_cluster} is still on MCE {request.mce_cluster} "
                    f"{_VERIFY_DEADLINE} after its Machine went{held}. Nothing further "
                    "was changed",
                    type=ServerStillPresentError.__name__,
                )
            await workflow.sleep(_VERIFY_POLL_INTERVAL)  # durable, replay-safe

        self._phase = "verifying-node"
        if node_name:
            node = await self._call(
                check_hosted_node,
                HostedNodeRef(hosted_cluster=cluster, node_name=node_name),
                task_queue,
            )
            if node.found:
                raise ApplicationError(
                    f"Node {node_name} is still in hosted cluster {result.hosted_cluster} "
                    "after its Machine went. It is an orphan, and this run never deletes "
                    f"a Node: run `oc delete node {node_name}` against the hosted cluster, "
                    "then re-run. remove-server was not started",
                    type=HostedNodeStillPresentError.__name__,
                )
        else:
            skipped.append(
                "verifying-node: the Machine never had a nodeRef (CAPI skips its drain "
                "for the same reason), so there is no Node to check"
            )

        self._phase = "verifying-agent"
        bmh_ref = BmhRef(server_name=request.server_name, namespace=request.namespace)
        started_at = workflow.now()
        while True:
            binding = await self._call(get_agent_binding, agent_ref, task_queue)
            bmh = await self._call(get_baremetal_host, bmh_ref, task_queue)
            self._agent_state = binding.state
            problems = _agent_problems(binding, bmh)
            if not problems:
                break
            if workflow.now() - started_at >= _VERIFY_DEADLINE:
                raise ApplicationError(
                    f"Agent {agent.name} of {request.server_name} is not a clean "
                    f"inventory host {_VERIFY_DEADLINE} after its Machine went: "
                    + "; ".join(problems)
                    + ". remove-server was not started",
                    type=AgentStillBoundError.__name__,
                )
            await workflow.sleep(_VERIFY_POLL_INTERVAL)  # durable, replay-safe

    async def _await_machine_gone(
        self,
        request: UninstallServerInput,
        machine_ref: MachineRef | None,
        agent_ref: AgentNameRef,
        hosted_cluster: str,
        task_queue: str,
    ) -> str | None:
        """Poll until the Machine is gone and the Agent is known-unbound.

        Returns a Node name a Machine reported only after the inspection. The
        two clocks start when their sub-phase is first observed, in THIS run,
        so a re-run after a fix gets each one afresh.
        """
        drain_since = None
        unbind_since = None
        node_name: str | None = None
        while True:
            machine = (
                await self._call(get_machine_state, machine_ref, task_queue)
                if machine_ref is not None
                else MachineState(found=False)
            )
            agent = await self._call(get_agent_binding, agent_ref, task_queue)
            self._agent_state = agent.state
            node_name = machine.node_name or node_name
            released = not agent.cluster_deployment and not agent.agent_machine_label
            if not machine.found and agent.found and released and agent.state == _KNOWN_UNBOUND:
                return node_name

            now = workflow.now()
            machine_name = (
                _qualified(machine_ref.namespace, machine_ref.name) if machine_ref else "(none)"
            )
            if machine.found and not released:
                self._sub_phase = "draining"
                drain_since = drain_since or now
                if now - drain_since >= _DRAIN_DEADLINE:
                    raise ApplicationError(
                        f"Machine {machine_name} of hosted cluster {hosted_cluster} has "
                        f"not got past its drain {_DRAIN_DEADLINE} after it was first "
                        f"seen deleting: Agent {agent_ref.name} is still bound. "
                        f"{_machine_detail(machine)}. Node: {machine.node_name or 'none'}. "
                        "Nothing further was deleted and nothing was undone (a Machine "
                        "delete cannot be). Clear what holds it — a PodDisruptionBudget, "
                        "a hook — and re-run: the run resumes",
                        type=MachineDrainTimeoutError.__name__,
                    )
            else:
                self._sub_phase = "unbinding"
                unbind_since = unbind_since or now
                if now - unbind_since >= _UNBIND_DEADLINE:
                    if agent.found and released and agent.state == _KNOWN_UNBOUND:
                        raise ApplicationError(
                            f"Machine {machine_name} of hosted cluster {hosted_cluster} "
                            f"still exists {_UNBIND_DEADLINE} after the Agent provider "
                            f"released Agent {agent_ref.name} (it is {_KNOWN_UNBOUND}): "
                            f"{_machine_detail(machine)}. Nothing further was changed",
                            type=MachineDeletionTimeoutError.__name__,
                        )
                    if not agent.found:
                        why = "the Agent no longer exists"
                    elif not released:
                        why = (
                            f"it is still bound to {agent.cluster_deployment or 'its AgentMachine'} "
                            "although its Machine is gone (a paused AgentMachine skips the unbind)"
                        )
                    else:
                        why = f"its state is {agent.state or 'unknown'}"
                    if machine.found:
                        why += f"; Machine {machine_name} is still present"
                    raise ApplicationError(
                        f"Agent {agent_ref.name} of {request.server_name} did not reach "
                        f"{_KNOWN_UNBOUND} within {_UNBIND_DEADLINE}: {why}. Nothing "
                        "further was changed — re-run once the host is back in "
                        "discovery: the run resumes",
                        type=AgentUnbindTimeoutError.__name__,
                    )
            await workflow.sleep(_MACHINE_POLL_INTERVAL)  # durable, replay-safe

    # --- the inventory half ----------------------------------------------------------

    async def _remove_from_inventory(
        self, request: UninstallServerInput, child_id: str
    ) -> RemoveServerResult:
        """remove-server as an awaited child, on its own id and queue."""
        try:
            return await workflow.execute_child_workflow(
                RemoveServerWorkflow.run,
                RemoveServerRunArgs(
                    input=RemoveServerInput(
                        server_name=request.server_name,
                        mce_cluster=request.mce_cluster,
                        namespace=request.namespace,
                    )
                ),
                id=child_id,
                task_queue=REMOVE_SERVER_WORKFLOW_QUEUE,
                # The child powers the host off and deletes things in a
                # load-bearing order; closing this run must never cut it off
                # between two of those steps.
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            raise ApplicationError(
                f"remove-server is already running for {request.server_name} as "
                f"{child_id}: a second removal of the same host. Let it finish, then "
                "re-run this uninstall",
                type=RemoveServerAlreadyRunningError.__name__,
            ) from None
        except ChildWorkflowError as err:
            cause = err.cause
            detail = (
                f"{cause.type}: {cause.message}"
                if isinstance(cause, ApplicationError)
                else f"{type(cause).__name__ if cause else 'ChildWorkflowError'}: {cause or err}"
            )
            raise ApplicationError(
                f"remove-server {child_id} failed taking {request.server_name} out of "
                f"MCE {request.mce_cluster}'s inventory — {detail}. Its history has "
                "the detail; re-running this uninstall runs the removal again",
                type=RemoveServerFailedError.__name__,
            ) from err

    async def _call(self, activity_fn, arg, task_queue: str):
        """One call on the target MCE's queue, under the domain's budget."""
        return await workflow.execute_activity(
            activity_fn,
            arg,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
