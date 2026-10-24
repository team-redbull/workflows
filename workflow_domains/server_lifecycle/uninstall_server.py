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
    the discovery image. What a broken HyperShift deletion left of one — a
    Machine with no AgentMachine, an AgentMachine with no Machine, a binding
    with both gone, a Node with neither — is this shape too, and cleared.
  * AN INVENTORY HOST: nothing links it to a hosted cluster. The
    hosted-cluster half is skipped (but for a stale Node server-scan's cluster
    still has of it).

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
                           one's binding, state, hostname and AgentMachine, its
                           Machine by ANY link (the AgentMachine's owner, an
                           infrastructureRef naming it, or the providerID
                           `agent://<agent>`), and the HostedCluster whatever
                           still points into its control plane. The Node name
                           is captured NOW: the Machine is about to go.
                           Refused: two bound Agents; a binding into no
                           control plane (ACM/ZTP); two Machines, or links into
                           two control planes; server-scan naming another
                           cluster than the Agent, or INSTALLED_TO_INVENTORY
                           while the Agent is bound.
  3. checking-hosted-node — the hosted cluster's own Nodes, through its admin
                           kubeconfig: which one IS this server, by providerID,
                           nodeRef, hostname or the server's name — looked up
                           in the cluster server-scan names when nothing on the
                           MCE links the host any more. Refused: two such
                           Nodes; a Ready one nothing on the MCE links (nothing
                           to take the host out of it through).
  4. reserving-server    — server-scan's install lock, holder `uninstall-server`,
                           ALWAYS, before any write (below for why).
  5. the hosted-cluster half, for a hosted node — or what is left of one —
       draining-node             a Node with NO Machine behind it is drained by
                                 nobody else (CAPI drains only its Machine's
                                 nodeRef): cordon, then evict every Pod CAPI's
                                 drain would (20 min, NodeDrainTimeoutError).
                                 A NotReady one is not drained, as CAPI skips
                                 an unreachable Node.
       deleting-machine          the run's own DELETE of the Machine — nothing
                                 has to happen first; a no-op when it already
                                 is deleting (an operator, an earlier run) or
                                 is gone.
       awaiting-machine-deletion every 30 s, until the Machine is gone and the
                                 Agent is `known-unbound`: `draining` while
                                 the Agent is still bound (20 min from when
                                 the Machine is first seen deleting, else
                                 MachineDrainTimeoutError), `unbinding` once
                                 it is released (1 h — a bare-metal reboot —
                                 else AgentUnbindTimeoutError), and
                                 `releasing` when the provider CANNOT release
                                 it: with no Machine left, or a Machine at its
                                 pre-terminate hook whose AgentMachine is gone,
                                 the run unbinds the Agent with the provider's
                                 exact write, and then removes the provider's
                                 hook once the Agent is in a state the provider
                                 itself would release it in.
       verifying-agentmachine    re-derived fresh: no AgentMachine names the
                                 Agent. One with no Machine is deleted; one
                                 stuck deleting on the provider's finalizer
                                 (which it drops only through a Machine) has
                                 that finalizer dropped after a minute. A
                                 Machine still linked fails it
                                 (ServerStillPresentError). Bounded 5 min.
       verifying-node            no Node is the server any more. A stale one —
                                 NotReady, the host back in discovery, no
                                 Machine to delete it — is deleted; one still
                                 Ready fails (HostedNodeStillPresentError).
                                 Bounded 5 min.
       verifying-agent           unbound, no agentMachineRef label,
                                 known-unbound, approved, BMH present and not
                                 in error (bounded 5 min) — what remove-server
                                 needs to take it.
       Every awaiting pass also stops (AgentRetakenError) when ANOTHER
       AgentMachine holds the Agent: the NodePool replacing the deleted Machine
       with this same server.
     For an INVENTORY HOST, only verifying-agentmachine: inspected again,
       fresh, by step 2's links — its Agent is free, so any NodePool may take
       it. Bound or labelled = AgentRetakenError; any other AgentMachine,
       Machine or control-plane link = ServerStillPresentError. Nothing written.
  6. holding-server     — the lock extended to 24 h BEFORE the child starts,
                           so the hold covers the inventory half too.
  7. removing-from-inventory — remove-server, awaited; its result is embedded.

Every skipped step is named in the result's `skipped`; what HyperShift had
left is in `leftovers_at_start`, and every write the run made on a
controller's behalf — with why that controller could not — in `cleaned_up`.

WHY THE RUN DOES THE CONTROLLERS' WRITES. Every object here is normally
cleaned up by a finalizer or a hook: CAPI drains and deletes the Node through
the Machine, the Agent provider unbinds the Agent through its hook on the
Machine and its finalizer on the AgentMachine. A HyperShift deletion that broke
half-way leaves exactly the shapes where that chain cannot run any more — an
AgentMachine with no Machine, a Machine with no AgentMachine, a binding with
both gone, a Node with neither — and then nothing ever finishes the job. So the
run never assumes the chain worked: it checks each outcome itself, makes the
missing write only where its owner cannot, and verifies everything gone before
the inventory half. Decided with the operator 2026-10-24. Runs recorded before
that (`uninstall-server-hosted-leftovers` unpatched) keep the old flow.

IT REPLACES A NODE; IT NEVER SHRINKS THE CLUSTER. The run deletes the Machine
itself — nothing has to happen first. NodePool replicas are NOT this
workflow's business (who owns them is not decided, 2026-10-24), so the NodePool
still wants the same number of nodes: its MachineSet creates a replacement
Machine and the Agent provider binds ANOTHER free Agent the NodePool matches —
the hosted cluster keeps its size, on a different server. The provider takes
ANY free Agent (`findAgent`), this server's own once released; when it is the
only one, the replacement takes it back, and the run stops with
AgentRetakenError rather than release it again from a Machine the provider is
installing. A Machine someone else already started deleting is waited on, and
step 4's delete is a no-op.

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

WHAT IS NEVER DONE: no NodePool change; no drain or Node delete where CAPI can
still do it (only for a Node no Machine stands behind, and a Node is deleted
only once NotReady); no finalizer or hook of anyone but the Agent provider's;
no PXE map write (remove-server's); no undo — a Machine delete cannot be
undone, so a failure after it leaves everything as it is and a re-run of the
same id RESUMES.
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
        AgentBindingChangedError,
        AgentRetakenError,
        AgentStillBoundError,
        AgentUnbindTimeoutError,
        AmbiguousServerNameError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        ClusterMembershipMismatchError,
        ConflictingHostedObjectsError,
        HostedClusterAccessError,
        HostedNodeStillPresentError,
        InvalidServerNameError,
        MachineDeletionTimeoutError,
        MachineDrainTimeoutError,
        MultipleBoundAgentsError,
        NodeDrainTimeoutError,
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
        clean_up_agent_machines,
        delete_hosted_node,
        drain_hosted_node,
        ensure_machine_deleting,
        find_hosted_nodes,
        get_agent_binding,
        get_agent_machine_state,
        get_baremetal_host,
        get_machine_state,
        inspect_bound_host,
        lookup_server_membership,
        release_agent,
        release_machine_hook,
        reserve_server,
    )
    from shared.models.server_lifecycle import (
        AgentBindingState,
        AgentMachineCleanupRequest,
        AgentMachineRef,
        AgentNameRef,
        AgentReleaseRequest,
        BmhRef,
        BmhState,
        BoundHostState,
        HostAgent,
        HostedClusterRef,
        HostedNode,
        HostedNodeAction,
        HostedNodeQuery,
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
    AgentBindingChangedError,
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

# Runs recorded before this patch keep the flow they were recorded with; every
# new run clears what a broken HyperShift deletion left (see the module
# docstring). Keep it until no run started before it can remain in history.
_LEFTOVERS_PATCH = "uninstall-server-hosted-leftovers"
# An inventory host is inspected again right before remove-server. Runs
# recorded before it go straight to the hold.
_INVENTORY_VERIFY_PATCH = "uninstall-server-inventory-verify"
# The leftover flow can drain a lost Node AND wait out a Machine drain, and
# verifies three things after the unbind instead of two.
_LEFTOVERS_RESERVATION_TTL = (
    2 * _DRAIN_DEADLINE + _UNBIND_DEADLINE + 3 * _VERIFY_DEADLINE + timedelta(minutes=10)
)
# How long an AgentMachine with no Machine may sit deleting before the run drops
# the provider's finalizer: the provider gets the first chance (it removes it
# itself when the owner reference names a Machine that is gone).
_FINALIZER_GRACE = timedelta(minutes=1)
# Where the Agent provider releases its own hook (handleDeletionHook's
# removeHookStates) — the run waits for the same before releasing it.
_PROVIDER_RELEASE_STATES = frozenset({
    "discovering-unbound",
    "known-unbound",
    "disconnected-unbound",
    "insufficient-unbound",
    "disabled-unbound",
    "unbinding-pending-user-action",
    "error",
})


def _qualified(namespace: str | None, name: str | None) -> str:
    return f"{namespace}/{name}"


def _machine_detail(machine: MachineState) -> str:
    conditions = "; ".join(machine.conditions) or "none reported"
    hooks = ", ".join(machine.pending_hooks) or "none"
    return f"deletion conditions: {conditions}. Pending delete hooks: {hooks}"


def _started_from(agent: HostAgent) -> frozenset[str] | None:
    """The AgentMachines the run found the Agent with — None when the limb could not say.

    Both the one found behind it (label or agentRef) and the one its label
    named, which may be long gone in a broken deletion: neither is a retake.
    """
    if "labelled_agent_machine" not in agent.model_fields_set:
        return None
    return frozenset(am for am in (agent.agent_machine, agent.labelled_agent_machine) if am)


def _retaken_by(binding: AgentBindingState, started_from: frozenset[str] | None) -> str | None:
    """The AgentMachine that took the Agent since the run started, if any.

    Gated on the key's presence (CLAUDE.md §5): a payload from a limb
    predating the field never reads as a retake, on replay or live.
    """
    if started_from is None or "agent_machine" not in binding.model_fields_set:
        return None
    if binding.agent_machine and binding.agent_machine not in started_from:
        return binding.agent_machine
    return None


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
        leftovers = workflow.patched(_LEFTOVERS_PATCH)
        hosted_agent = (
            self._hosted_agent_or_leftover(request, host)
            if leftovers
            else self._hosted_agent(request, host)
        )
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

        if leftovers:
            # Step 2b — the hosted cluster's own view: is the server a Node there?
            cluster, nodes = await self._find_nodes(
                request, membership, host, hosted_agent, followed, skipped, activity_queue
            )
            result.leftovers_at_start = self._leftovers(host, hosted_agent, nodes)

            if request.keep_inventory and hosted_agent is None and not nodes:
                # Already only inventory, and asked to stay there: nothing to
                # write, so no lock either.
                skipped.append(
                    "hosted-cluster half: nothing links the host to a hosted cluster — no "
                    "AgentMachine, no Machine, no binding into a control plane, and no Node"
                )
                skipped.append(self._kept_note(request, host))
                result.kept_in_inventory = True
                result.skipped = skipped
                self._phase = "completed"
                workflow.logger.info(
                    "%s is already only inventory on MCE %s and keep_inventory was asked — "
                    "nothing to do",
                    request.server_name,
                    request.mce_cluster,
                )
                return result

            self._phase = "reserving-server"
            await self._reserve(
                request, membership, host, activity_queue, ttl=_LEFTOVERS_RESERVATION_TTL
            )
            if hosted_agent is not None or nodes:
                await self._clear_hosted_cluster(
                    request, host, cluster, hosted_agent, followed, nodes, result, skipped,
                    activity_queue,
                )
            else:
                skipped.append(
                    "hosted-cluster half: nothing links the host to a hosted cluster — no "
                    "AgentMachine, no Machine, no binding into a control plane, and no Node"
                )
                if workflow.patched(_INVENTORY_VERIFY_PATCH):
                    await self._verify_still_inventory(request, activity_queue)
            return await self._remove_and_finish(request, membership, host, result, skipped, activity_queue)

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

        return await self._remove_and_finish(request, membership, host, result, skipped, activity_queue)

    async def _verify_still_inventory(self, request: UninstallServerInput, task_queue: str) -> None:
        """An inventory host, inspected again right before remove-server: nothing hosted links it.

        The first inspection found no AgentMachine, Machine or binding into a
        control plane. But a free Agent is exactly what any NodePool's new
        AgentMachine takes, at any moment (cluster-api-provider-agent
        `findAgent`), so the same links are derived again, fresh, before the
        child powers the host off and deletes it. One found means the host is
        a node again, or on its way to being one, and the run leaves it alone.
        """
        self._phase = "verifying-agentmachine"
        fresh: BoundHostState = await self._call(
            inspect_bound_host,
            BmhRef(server_name=request.server_name, namespace=request.namespace),
            task_queue,
        )
        taken = [a for a in fresh.agents if a.cluster_deployment or a.agent_machine_label]
        if taken:
            held = "; ".join(
                f"Agent {a.name} is bound to {a.cluster_deployment or '(nothing yet)'}"
                + (f", held by AgentMachine {a.agent_machine}" if a.agent_machine else "")
                for a in taken
            )
            raise ApplicationError(
                f"{request.server_name} was taken while the run was taking it out of the "
                f"inventory of MCE {request.mce_cluster}: {held}. A free Agent is what a "
                "NodePool's new Machine takes. Nothing on the MCE was changed — re-run to take "
                "it out of that hosted cluster as well, or leave it to install",
                type=AgentRetakenError.__name__,
            )
        links = [
            f"AgentMachine {a.agent_machine} names Agent {a.name}"
            for a in fresh.agents
            if a.agent_machine
        ]
        links += [f"Machine {m} is linked to it" for m in fresh.machines_matched]
        links += [
            f"Agent {a.name} is linked into control plane {', '.join(a.hcp_namespaces)}"
            for a in fresh.agents
            if a.hcp_namespaces and not a.agent_machine
        ]
        if links:
            raise ApplicationError(
                f"{request.server_name} is no longer only inventory on MCE "
                f"{request.mce_cluster}: " + "; ".join(links) + ". Nothing on the MCE was "
                "changed — re-run: it takes the server out of the hosted cluster first",
                type=ServerStillPresentError.__name__,
            )

    async def _remove_and_finish(
        self,
        request: UninstallServerInput,
        membership: ServerMembership,
        host: BoundHostState,
        result: UninstallServerResult,
        skipped: list[str],
        activity_queue: str,
    ) -> UninstallServerResult:
        # Step 5 — hold the machine through the inventory half and beyond. Kept
        # in the inventory too: it stays in this InfraEnv, and server-scan may
        # read it AVAILABLE until the MCE's agents job reports it.
        self._phase = "holding-server"
        result.reservation_expires_at = await self._hold(request, membership, host, activity_queue)

        if request.keep_inventory:
            skipped.append(self._kept_note(request, host))
            result.kept_in_inventory = True
            result.skipped = skipped
            self._phase = "completed"
            workflow.logger.info(
                "Took %s out of hosted cluster %s and kept it in the inventory of MCE %s: "
                "machine=%s node=%s",
                request.server_name,
                result.hosted_cluster,
                request.mce_cluster,
                result.machine,
                result.node_name,
            )
            return result

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

    @staticmethod
    def _kept_note(request: UninstallServerInput, host: BoundHostState) -> str:
        where = f"InfraEnv {host.infra_env}" if host.infra_env else f"namespace {request.namespace}"
        return (
            f"removing-from-inventory: keep_inventory was asked — the server stays in {where} "
            f"of MCE {request.mce_cluster}: its BareMetalHost, NMStateConfig, Agent and PXE "
            "mapping are left as they are"
        )

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

    def _hosted_agent_or_leftover(
        self, request: UninstallServerInput, host: BoundHostState
    ) -> HostAgent | None:
        """_hosted_agent for the leftover flow: a hosted node, or what HyperShift left of one.

        An Agent is hosted when ANYTHING still links it to a hosted cluster —
        an AgentMachine, a Machine (by any link) or a binding into a
        HostedCluster's control plane — so the half-deleted shapes are
        recognised and cleared instead of refused. Refused, as before: two
        bound Agents, a binding into no control plane (ACM/ZTP), a binding to
        another cluster than its links; and now also links that contradict
        each other.
        """
        server = request.server_name
        if len(host.bound_agents) > 1 or len(host.hosted_agents) > 1:
            several = {a.name: a for a in host.bound_agents + host.hosted_agents}
            raise ApplicationError(
                f"{server} has {len(several)} Agents bound to a cluster on MCE "
                f"{request.mce_cluster}: "
                + "; ".join(
                    f"Agent {a.name} -> "
                    f"{a.cluster_deployment or a.agent_machine or ', '.join(a.hcp_namespaces)}"
                    for a in several.values()
                )
                + ". One machine is one node, so nothing was changed",
                type=MultipleBoundAgentsError.__name__,
            )
        bound = host.bound_agents[0] if host.bound_agents else None
        hosted = host.hosted_agents[0] if host.hosted_agents else None
        if bound is not None and (hosted is None or hosted.name != bound.name):
            raise ApplicationError(
                f"Agent {bound.name} of {server} is bound to {bound.cluster_deployment}, "
                "which is no HostedCluster's control plane on MCE "
                f"{request.mce_cluster}, and no AgentMachine or Machine is behind it — "
                "an early-bound ACM/ZTP ClusterDeployment. Nothing was changed",
                type=NotHostedClusterNodeError.__name__,
            )
        if hosted is None:
            return None
        if len(hosted.hcp_namespaces) > 1:
            raise ApplicationError(
                f"Agent {hosted.name} of {server} is linked into several hosted control "
                f"planes at once ({', '.join(hosted.hcp_namespaces)}): its AgentMachine, "
                "Machine and binding disagree. Nothing was changed",
                type=ConflictingHostedObjectsError.__name__,
            )
        if len(host.machines_matched) > 1:
            raise ApplicationError(
                f"{len(host.machines_matched)} Machines are linked to Agent {hosted.name} "
                f"of {server}: {', '.join(host.machines_matched)}. One server is one "
                "Machine; the run cannot tell which is real, so nothing was changed",
                type=ConflictingHostedObjectsError.__name__,
            )
        cluster = host.hosted_cluster
        if cluster is None:
            where = hosted.agent_machine or ", ".join(hosted.hcp_namespaces)
            raise ApplicationError(
                f"Agent {hosted.name} of {server} is linked to {where}, a namespace no "
                "HostedCluster's control plane uses, so there is no hosted cluster to "
                "verify the Node against. Nothing was changed",
                type=NotHostedClusterNodeError.__name__,
            )
        expected = _qualified(cluster.hcp_namespace, cluster.name)
        if hosted.cluster_deployment and hosted.cluster_deployment != expected:
            raise ApplicationError(
                f"Agent {hosted.name} of {server} is bound to "
                f"{hosted.cluster_deployment}, but its AgentMachine or Machine belongs "
                f"to hosted cluster {_qualified(cluster.namespace, cluster.name)} "
                f"(ClusterDeployment {expected}). Nothing was changed",
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
        ttl: timedelta = _RESERVATION_TTL,
    ) -> None:
        """Take the lock before the first write; a lock held elsewhere fails the run."""
        try:
            await self._call(
                reserve_server,
                self._reservation(request, membership, host, ttl),
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
        await self._verify_agent(request, agent_ref, task_queue)

    async def _verify_agent(
        self, request: UninstallServerInput, agent_ref: AgentNameRef, task_queue: str
    ) -> None:
        """Until the Agent is the clean inventory host remove-server takes (bounded)."""
        bmh_ref = BmhRef(server_name=request.server_name, namespace=request.namespace)
        started_at = workflow.now()
        while True:
            binding = await self._call(get_agent_binding, agent_ref, task_queue)
            bmh = await self._call(get_baremetal_host, bmh_ref, task_queue)
            self._agent_state = binding.state
            problems = _agent_problems(binding, bmh)
            if not problems:
                return
            if workflow.now() - started_at >= _VERIFY_DEADLINE:
                raise ApplicationError(
                    f"Agent {agent_ref.name} of {request.server_name} is not a clean "
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

    # --- the leftover flow: whatever a broken HyperShift deletion left ---------------

    async def _find_nodes(
        self,
        request: UninstallServerInput,
        membership: ServerMembership,
        host: BoundHostState,
        hosted_agent: HostAgent | None,
        followed: HostAgent | None,
        skipped: list[str],
        task_queue: str,
    ) -> tuple[HostedClusterRef | None, list[HostedNode]]:
        """Is the server a Node of the hosted cluster? Read before anything is written.

        The cluster is the one the MCE links the host to, else the one
        server-scan says it is INSTALLED in — the only trace left of a Node
        whose Machine, AgentMachine and binding are all gone.
        """
        server = request.server_name
        cluster = host.hosted_cluster if hosted_agent is not None else None
        query = HostedNodeQuery(
            hosted_cluster=cluster,
            server_name=server,
            agent_name=followed.name if followed else None,
            hostname=followed.hostname if followed else None,
            node_name=(host.machine_state.node_name if hosted_agent and host.machine_state else None),
        )
        if cluster is None:
            if membership.lifecycle_state != _INSTALLED or not membership.cluster_name:
                skipped.append(
                    "checking-hosted-node: neither the MCE nor server-scan places the "
                    "server in a hosted cluster"
                )
                return None, []
            query.cluster_name = membership.cluster_name
        self._phase = "checking-hosted-node"
        found = await self._call(find_hosted_nodes, query, task_queue)
        if found.hosted_cluster is None:
            skipped.append(f"checking-hosted-node: {found.reason}")
            return cluster, []
        cluster = found.hosted_cluster
        label = _qualified(cluster.namespace, cluster.name)
        if len(found.nodes) > 1:
            raise ApplicationError(
                f"{len(found.nodes)} Nodes of hosted cluster {label} are {server}: "
                + "; ".join(f"{n.name} (by {', '.join(n.matched_by)})" for n in found.nodes)
                + ". One server is one Node; the run cannot tell which is real, so "
                "nothing was changed",
                type=ConflictingHostedObjectsError.__name__,
            )
        if hosted_agent is None and found.nodes and found.nodes[0].ready:
            node = found.nodes[0]
            raise ApplicationError(
                f"Node {node.name} of hosted cluster {label} is Ready and is {server} "
                f"(by {', '.join(node.matched_by)}), but nothing on MCE "
                f"{request.mce_cluster} links it to that cluster any more — no Machine, "
                "no AgentMachine, no binding — so the run has nothing to take the host "
                "out of the cluster through. Nothing was changed. Drain it and power the "
                "host off (or boot it into discovery), then re-run: the stale Node is "
                "deleted then",
                type=HostedNodeStillPresentError.__name__,
            )
        if not found.nodes:
            skipped.append(f"checking-hosted-node: hosted cluster {label} has no Node that is {server}")
        return cluster, found.nodes

    def _leftovers(
        self, host: BoundHostState, agent: HostAgent | None, nodes: list[HostedNode]
    ) -> list[str]:
        """What a broken deletion left, in words, as inspected."""
        found: list[str] = []
        machine = host.machine if agent else None
        state = (host.machine_state if agent else None) or MachineState()
        live = machine is not None and state.found
        machine_name = _qualified(machine.namespace, machine.name) if machine else None
        if agent is not None:
            if agent.agent_machine and not live:
                found.append(f"AgentMachine {agent.agent_machine} has no Machine")
            if live and state.agent_machine and state.agent_machine_found is False:
                found.append(f"Machine {machine_name} has no AgentMachine ({state.agent_machine} is gone)")
            if not agent.agent_machine and not live and agent.cluster_deployment:
                found.append(
                    f"Agent {agent.name} is still bound to {agent.cluster_deployment} with no "
                    "AgentMachine or Machine behind it"
                )
        for node in nodes:
            if not (live and node.name == state.node_name):
                found.append(
                    f"Node {node.name} has no Machine behind it "
                    f"({'Ready' if node.ready else 'NotReady'})"
                )
        if live and state.node_name and not any(n.name == state.node_name for n in nodes):
            found.append(f"Machine {machine_name} names Node {state.node_name}, which is gone")
        return found

    async def _clear_hosted_cluster(
        self,
        request: UninstallServerInput,
        host: BoundHostState,
        cluster: HostedClusterRef | None,
        agent: HostAgent | None,
        followed: HostAgent | None,
        nodes: list[HostedNode],
        result: UninstallServerResult,
        skipped: list[str],
        task_queue: str,
    ) -> None:
        """The hosted-cluster half, whatever shape it is in, ending with everything verified gone.

        `agent` is the hosted Agent (None when only a stale Node is left);
        `followed` is whichever Agent the Nodes were matched by, so the last
        check looks for exactly what the first one did.

        Each object goes the way its own controller would take it; where that
        controller can no longer act — the provider without a Machine or an
        AgentMachine, CAPI without a Machine — the run makes the same write
        itself, and says so in `result.cleaned_up`.
        """
        result.hosted = True
        machine_ref = host.machine if agent else None
        inspected = (host.machine_state if agent else None) or MachineState()
        live = machine_ref is not None and inspected.found
        node_name = inspected.node_name
        if cluster is not None:
            result.hosted_cluster = _qualified(cluster.namespace, cluster.name)
            result.hcp_namespace = cluster.hcp_namespace
        result.agent_machine = agent.agent_machine if agent else None
        result.machine = _qualified(machine_ref.namespace, machine_ref.name) if machine_ref else None
        result.node_name = node_name or (nodes[0].name if nodes else None)

        # 1. A Node no Machine stands behind is drained by nobody else: CAPI
        #    drains only its Machine's nodeRef.
        lost = [n for n in nodes if not (live and n.name == node_name)]
        self._phase = "draining-node"
        if lost and lost[0].ready and cluster is not None:
            await self._drain_lost_node(cluster, lost[0], result, task_queue)
        elif lost:
            skipped.append(
                f"draining-node: Node {lost[0].name} is NotReady — nothing runs as it, so "
                "there is nothing to drain (CAPI skips an unreachable Node's drain the same way)"
            )

        if agent is not None and cluster is not None:
            # 2. The Machine — the normal way out, when there is one.
            self._phase = "deleting-machine"
            if not live:
                skipped.append(
                    f"deleting-machine: no Machine stands behind Agent {agent.name} any more"
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

            # 3. The Machine gone and the Agent released and back in discovery.
            self._phase = "awaiting-machine-deletion"
            agent_ref = AgentNameRef(namespace=agent.namespace, name=agent.name)
            captured = await self._await_release(
                request, machine_ref if live else None, agent_ref, cluster, result, task_queue,
                started_from=_started_from(agent),
            )
            self._sub_phase = None
            node_name = node_name or captured
            result.node_name = result.node_name or node_name

            # 4. No AgentMachine — and no Machine — left that names the Agent.
            self._phase = "verifying-agentmachine"
            await self._clear_agent_machines(request, agent, agent_ref, result, task_queue)

        # 5. No Node left that is the server; a stale one is deleted.
        self._phase = "verifying-node"
        if cluster is not None:
            await self._clear_nodes(request, cluster, followed, node_name, result, task_queue)

        # 6. The clean inventory host remove-server takes.
        if agent is not None and cluster is not None:
            self._phase = "verifying-agent"
            await self._verify_agent(
                request, AgentNameRef(namespace=agent.namespace, name=agent.name), task_queue
            )
        else:
            skipped.append(
                "verifying-agent: no Agent of the host was ever linked to the hosted cluster"
            )

    async def _drain_lost_node(
        self,
        cluster: HostedClusterRef,
        node: HostedNode,
        result: UninstallServerResult,
        task_queue: str,
    ) -> None:
        """Cordon and evict, pass after pass, until no Pod a drain moves is left (20 min)."""
        label = _qualified(cluster.namespace, cluster.name)
        action = HostedNodeAction(hosted_cluster=cluster, node_name=node.name, provider_id=node.provider_id)
        started_at = workflow.now()
        evicted: set[str] = set()
        while True:
            drain = await self._call(drain_hosted_node, action, task_queue)
            if not drain.found:
                return
            if not drain.matches:
                raise ApplicationError(
                    f"Node {node.name} of hosted cluster {label} no longer carries the "
                    f"providerID it was matched by ({node.provider_id or 'none'}): it is "
                    "another machine's now. Nothing further was changed",
                    type=ConflictingHostedObjectsError.__name__,
                )
            evicted |= set(drain.evicted)
            if not drain.remaining:
                result.cleaned_up.append(
                    f"drained Node {node.name} of hosted cluster {label} — no Machine stands "
                    f"behind it, so CAPI would not: cordoned, {len(evicted)} Pod(s) evicted"
                )
                return
            if workflow.now() - started_at >= _DRAIN_DEADLINE:
                raise ApplicationError(
                    f"Node {node.name} of hosted cluster {label} did not drain within "
                    f"{_DRAIN_DEADLINE}: {len(drain.remaining)} Pod(s) left "
                    f"({', '.join(drain.remaining[:10])}"
                    f"{', …' if len(drain.remaining) > 10 else ''}); refused: "
                    f"{'; '.join(drain.blocked) or 'none this pass'}. The Node stays "
                    "cordoned (`oc adm uncordon` undoes it) and nothing else was changed. "
                    "Clear what holds it — a PodDisruptionBudget, typically — and re-run",
                    type=NodeDrainTimeoutError.__name__,
                )
            await workflow.sleep(_MACHINE_POLL_INTERVAL)  # durable, replay-safe

    async def _await_release(
        self,
        request: UninstallServerInput,
        machine_ref: MachineRef | None,
        agent_ref: AgentNameRef,
        cluster: HostedClusterRef,
        result: UninstallServerResult,
        task_queue: str,
        started_from: frozenset[str] | None = None,
    ) -> str | None:
        """_await_machine_gone, doing the Agent provider's writes when it can no longer make them.

        The provider unbinds an Agent ONLY from its Machine's pre-terminate
        hook, through its AgentMachine. So with the Machine gone, the Agent
        still bound is released here; and with the Machine deleting at that
        hook but its AgentMachine gone, the run releases the Agent, waits for
        the state the provider waits for, then removes the provider's hook.
        Same deadlines as _await_machine_gone.

        It stops the moment another AgentMachine holds the Agent: the NodePool
        replacing the deleted Machine with this same server. Releasing it then
        would unbind a node the provider is installing, and the release checks
        alone cannot tell — the replacement binds into the same control plane.
        """
        expected = _qualified(cluster.hcp_namespace, cluster.name)
        hosted_cluster = _qualified(cluster.namespace, cluster.name)
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
            taken_by = _retaken_by(agent, started_from)
            if taken_by is not None:
                raise ApplicationError(
                    f"AgentMachine {taken_by} took Agent {agent_ref.name} of "
                    f"{request.server_name} again while the run was taking it out of hosted "
                    f"cluster {hosted_cluster}: the NodePool replaced the deleted Machine, and "
                    "this server was the free Agent its selector found — the InfraEnv has no "
                    "other. Nothing further was changed, and the run does not fight the "
                    "NodePool for the host. Free another Agent the NodePool matches (or lower "
                    "its replicas), then re-run: it deletes the replacement Machine and the "
                    "NodePool takes the other Agent",
                    type=AgentRetakenError.__name__,
                )
            released = not agent.cluster_deployment and not agent.agent_machine_label
            if not machine.found and agent.found and released and agent.state == _KNOWN_UNBOUND:
                return node_name

            now = workflow.now()
            machine_name = (
                _qualified(machine_ref.namespace, machine_ref.name) if machine_ref else "(none)"
            )
            provider_gone = (
                machine.found
                and machine.deleting
                and machine.provider_hook
                and machine.agent_machine_found is False
            )
            if agent.found and not released and (
                not machine.found or (provider_gone and machine.at_pre_terminate_hook)
            ):
                self._sub_phase = "releasing"
                release = await self._call(
                    release_agent,
                    AgentReleaseRequest(agent=agent_ref, cluster_deployment=expected),
                    task_queue,
                )
                if release.released:
                    why = (
                        "no Machine is left to release it through"
                        if not machine.found
                        else f"Machine {machine_name}'s AgentMachine {machine.agent_machine} is gone"
                    )
                    result.cleaned_up.append(
                        f"released Agent {agent_ref.name} from {expected} — {why}, so the "
                        "Agent provider could not"
                    )
            elif (
                provider_gone
                and machine.at_pre_terminate_hook
                and released
                and agent.state in _PROVIDER_RELEASE_STATES
            ):
                self._sub_phase = "releasing"
                hook = await self._call(release_machine_hook, machine_ref, task_queue)
                if hook.released:
                    result.cleaned_up.append(
                        f"removed the Agent provider's pre-terminate hook from Machine "
                        f"{machine_name} — its AgentMachine {machine.agent_machine} is gone, "
                        "so the provider could not"
                    )
            elif machine.found and not released:
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
                            f"still exists {_UNBIND_DEADLINE} after Agent {agent_ref.name} "
                            f"was released (it is {_KNOWN_UNBOUND}): "
                            f"{_machine_detail(machine)}. Nothing further was changed",
                            type=MachineDeletionTimeoutError.__name__,
                        )
                    if not agent.found:
                        why = "the Agent no longer exists"
                    elif not released:
                        why = f"it is still bound to {agent.cluster_deployment or 'its AgentMachine'}"
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

    async def _clear_agent_machines(
        self,
        request: UninstallServerInput,
        agent: HostAgent,
        agent_ref: AgentNameRef,
        result: UninstallServerResult,
        task_queue: str,
    ) -> None:
        """Until no AgentMachine names the Agent, re-derived fresh each pass (bounded 5 min).

        One left with no Machine is deleted; one stuck deleting on the
        provider's finalizer — which it removes only through a Machine — has
        that finalizer dropped after _FINALIZER_GRACE. A Machine still linked
        to the Agent fails the run: the Machine's deletion is CAPI's, and one
        appearing now is a NodePool's replacement taking this host again.
        """
        known = []
        if agent.agent_machine:
            namespace, name = agent.agent_machine.split("/", 1)
            known.append(AgentMachineRef(namespace=namespace, name=name))
        started_at = workflow.now()
        deleting_since = None
        while True:
            now = workflow.now()
            cleanup = await self._call(
                clean_up_agent_machines,
                AgentMachineCleanupRequest(
                    agent=agent_ref,
                    known=known,
                    clear_finalizers=deleting_since is not None
                    and now - deleting_since >= _FINALIZER_GRACE,
                ),
                task_queue,
            )
            for name in cleanup.deleted:
                result.cleaned_up.append(
                    f"deleted AgentMachine {name} — no Machine owns or names it, so nothing "
                    "else would"
                )
            for name in cleanup.finalizers_cleared:
                result.cleaned_up.append(
                    f"dropped the Agent provider's finalizer from AgentMachine {name} — it "
                    "removes it only through a Machine, there is none, and the Agent was "
                    "already released"
                )
            if cleanup.machines or cleanup.held_by_machine:
                held = cleanup.held_by_machine or cleanup.machines
                raise ApplicationError(
                    f"Agent {agent.name} of {request.server_name} is still linked to a "
                    f"Machine after its own went: {', '.join(held)}. A NodePool's "
                    "replacement Machine may have taken this host again. Nothing further "
                    "was changed",
                    type=ServerStillPresentError.__name__,
                )
            if not cleanup.remaining:
                return
            deleting_since = deleting_since or now
            if now - started_at >= _VERIFY_DEADLINE:
                raise ApplicationError(
                    f"AgentMachine(s) of Agent {agent.name} still on MCE "
                    f"{request.mce_cluster} {_VERIFY_DEADLINE} after the Agent was released: "
                    f"{'; '.join(cleanup.remaining)}. Nothing further was changed",
                    type=ServerStillPresentError.__name__,
                )
            await workflow.sleep(_VERIFY_POLL_INTERVAL)  # durable, replay-safe

    async def _clear_nodes(
        self,
        request: UninstallServerInput,
        cluster: HostedClusterRef,
        agent: HostAgent | None,
        node_name: str | None,
        result: UninstallServerResult,
        task_queue: str,
    ) -> None:
        """Until the hosted cluster has no Node that is the server (bounded 5 min).

        By this point nothing runs as the server any more — it is back in
        discovery, or was NotReady already — so a Node still there is stale:
        CAPI deletes a Node only through its Machine, and there is none. It is
        deleted once NotReady; one still Ready means something still runs as
        it, and fails the run.
        """
        label = _qualified(cluster.namespace, cluster.name)
        query = HostedNodeQuery(
            hosted_cluster=cluster,
            server_name=request.server_name,
            agent_name=agent.name if agent else None,
            hostname=agent.hostname if agent else None,
            node_name=node_name,
        )
        started_at = workflow.now()
        while True:
            found = await self._call(find_hosted_nodes, query, task_queue)
            if not found.nodes:
                return
            still_ready = []
            for node in found.nodes:
                if node.ready:
                    still_ready.append(node.name)
                    continue
                deletion = await self._call(
                    delete_hosted_node,
                    HostedNodeAction(
                        hosted_cluster=cluster, node_name=node.name, provider_id=node.provider_id
                    ),
                    task_queue,
                )
                if deletion.deleted:
                    result.cleaned_up.append(
                        f"deleted Node {node.name} of hosted cluster {label} — NotReady, "
                        "with no Machine left to delete it"
                    )
            if workflow.now() - started_at >= _VERIFY_DEADLINE:
                ready = ", ".join(still_ready) or "none"
                raise ApplicationError(
                    f"Hosted cluster {label} still has a Node that is {request.server_name} "
                    f"{_VERIFY_DEADLINE} after the host left it: "
                    f"{', '.join(n.name for n in found.nodes)} (still Ready: {ready} — "
                    "something still runs as it). Nothing further was changed; "
                    "remove-server was not started",
                    type=HostedNodeStillPresentError.__name__,
                )
            await workflow.sleep(_VERIFY_POLL_INTERVAL)  # durable, replay-safe

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
