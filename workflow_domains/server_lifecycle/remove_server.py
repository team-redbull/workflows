"""remove-server — takes one server out of an MCE's inventory.

The reverse of install-server's end state: the BareMetalHost `<server>`, its
NMStateConfig `nmstate-config-<server>` and the server's Agent are deleted, so
the machine no longer sits in that MCE's InfraEnv. ONLY for a server that is
just inventory: if its Agent is bound to a cluster, the run fails having
changed nothing.

Shape of the run:

  1. inspecting       — read the BMH, the NMStateConfig and the Agents matched
                        to the host: BMAC's bmh label, or ANY MAC of the machine
                        — from either resource and every one server-scan holds
                        for the name, which is what still finds an Agent whose
                        BMH is already gone. Any of the three may be missing;
                        nothing found at all is a successful no-op.
  2. (refuse)         — an Agent bound to a cluster (`spec.clusterDeploymentName`)
                        fails the run with AgentBoundToClusterError. Nothing has
                        been touched yet.
  3. powering-off     — `spec.online: false` on the BMH, then wait for metal3 to
                        report `status.poweredOn: false`. Bounded: a BMC that
                        never answers fails the run with HostPowerOffTimeoutError
                        and nothing is deleted. The activity re-checks the Agents
                        are still unbound right before it acts.
  4. removing-host    — the NMStateConfig and the BMH (and its BMC Secret), by
                        the same teardown install-server's rollback uses: detach
                        first, so metal3 does not try to deprovision through the
                        BMC, then delete, then drop the finalizer if it sticks.
  5. removing-agent   — the Agent(s), each re-checked as unbound right before
                        its delete.
     removing-pxe-boot — every MAC the host is known by is cleared from every
                        site PXE VM this MCE's worker has configured, so the
                        machine can no longer network-boot this MCE's discovery
                        image and re-register. CONDITIONAL on the PXE VM: a MAC
                        chaining to ANOTHER MCE's boot entry is kept (reported
                        in `pxe_kept`), since that MCE has mapped the machine
                        since. Runs for every server — a Redfish one simply has
                        no entries.
  6. verifying        — inspect again until no Agent, BMH or NMStateConfig is
                        left (an Agent's finalizer takes seconds). Anything
                        still there after a bounded wait fails the run with
                        ServerStillPresentError naming it — typically an Agent
                        re-registered by a host that had no BMH to power it
                        off through.

WHY POWER OFF FIRST, AND DELETE THE AGENT LAST. The host is still running the
discovery image, and its agent re-registers with assisted-service when its
Agent disappears — a fresh Agent would be back within a minute. Powering it off
through its BMH stops that, and deleting the Agent only once the host is off
means nothing is left to bring it back. (The detach in step 4 is then safe: the
host is already off, so there is nothing for a deprovision to do.)

The BMC Secret goes with the BMH (ownerReference, or explicitly). Not touched:
server-scan's install lock (it expires on its own) and the InfraEnv's boot
entry on the PXE VM (`<mce>/<infraenv>.ipxe`), which every other host of the
InfraEnv chains through. A server found NOWHERE on the MCE is a no-op that
touches the PXE map neither: nothing then ties its MACs to this MCE.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.consts import server_lifecycle_activity_queue
    from shared.exceptions import (
        AgentBoundToClusterError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        HostPowerOffTimeoutError,
        InvalidServerNameError,
        PxeMapAuthError,
        PxeMapRequestRejectedError,
        ServerScanAuthError,
        ServerStillPresentError,
    )
    from shared.interfaces.server_lifecycle import (
        delete_agents,
        get_baremetal_host,
        inspect_inventory_host,
        remove_host_pxe_entries,
        set_host_online,
        teardown_bmh_resources,
    )
    from shared.models.server_lifecycle import (
        AgentDeleteRequest,
        BmhRef,
        HostPowerRequest,
        InventoryHostState,
        PxeHostEntriesRef,
        RemoveServerProgress,
        RemoveServerResult,
        RemoveServerRunArgs,
    )

_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# How long a cluster call may wait for the MCE's worker to pick it up before
# Temporal reports it — the same signal install-server uses for "no worker
# polls this MCE's queue".
_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)

# Activity-raised failures no retry can fix, as CLASSES (a wrong name is an
# ImportError at worker start, not a silent "retry forever").
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    AgentBoundToClusterError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    InvalidServerNameError,
    ServerScanAuthError,
    PxeMapAuthError,
    PxeMapRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# Machine convergence, so a real deadline (CLAUDE.md §5). A BMC normally
# answers a power-off in seconds; ten minutes covers a slow one without
# leaving a run waiting on a BMC that is gone. Changing either constant is a
# non-deterministic change for in-flight runs.
_POWER_OFF_DEADLINE = timedelta(minutes=10)
_POWER_POLL_INTERVAL = timedelta(seconds=15)
# The final check: an Agent's finalizer holds it for seconds after the delete.
# Five minutes, polled every 10s, is ample for that and short enough that a
# host re-registering its Agent fails the run rather than hiding.
_GONE_DEADLINE = timedelta(minutes=5)
_GONE_POLL_INTERVAL = timedelta(seconds=10)

# Gates the PXE map cleanup: a run recorded before it would otherwise meet a
# new activity between its Agent delete and its final check.
_PXE_CLEANUP_PATCH = "remove-server-pxe-cleanup"


def _describe_leftover(state: InventoryHostState) -> str:
    parts = []
    if state.bmh_found:
        parts.append("BareMetalHost")
    if state.nmstate_found:
        parts.append("NMStateConfig")
    parts += [f"Agent {agent.name}" for agent in state.agents]
    return ", ".join(parts)


@workflow.defn
class RemoveServerWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._activity_queue: str | None = None
        self._pxe_cleanup = False

    @workflow.query
    def progress(self) -> RemoveServerProgress:
        return RemoveServerProgress(phase=self._phase, activity_queue=self._activity_queue)

    @workflow.run
    async def run(self, run_args: RemoveServerRunArgs) -> RemoveServerResult:
        request = run_args.input
        activity_queue = server_lifecycle_activity_queue(request.mce_cluster)
        self._activity_queue = activity_queue
        self._pxe_cleanup = workflow.patched(_PXE_CLEANUP_PATCH)
        ref = BmhRef(server_name=request.server_name, namespace=request.namespace)

        def result(**fields) -> RemoveServerResult:
            return RemoveServerResult(
                server_name=request.server_name,
                namespace=request.namespace,
                mce_cluster=request.mce_cluster,
                **fields,
            )

        # Step 1 — what is there.
        self._phase = "inspecting"
        state: InventoryHostState = await self._call(
            inspect_inventory_host, ref, activity_queue
        )
        if not (state.bmh_found or state.nmstate_found or state.agents):
            self._phase = "completed"
            workflow.logger.info(
                "%s has nothing on MCE %s in %s — nothing to remove",
                request.server_name,
                request.mce_cluster,
                request.namespace,
            )
            return result(found=False)

        # Step 2 — only inventory may be removed.
        bound = state.bound_agents
        if bound:
            raise ApplicationError(
                f"{request.server_name} is not just inventory on MCE "
                f"{request.mce_cluster}: "
                + "; ".join(f"Agent {a.name} is bound to {a.cluster_deployment}" for a in bound)
                + ". Nothing was changed — remove it from that cluster first",
                type=AgentBoundToClusterError.__name__,
            )
        agent_names = [agent.name for agent in state.agents]

        # Step 3 — power off, so the host cannot re-register its Agent.
        powered_off = False
        if state.bmh_found:
            self._phase = "powering-off"
            await self._call(
                set_host_online,
                HostPowerRequest(
                    server_name=request.server_name,
                    namespace=request.namespace,
                    online=False,
                    agents_must_be_unbound=agent_names,
                ),
                activity_queue,
            )
            if not await self._await_power_off(ref, activity_queue):
                raise ApplicationError(
                    f"BareMetalHost {request.server_name} in {request.namespace} on "
                    f"MCE {request.mce_cluster} did not report powered off within "
                    f"{_POWER_OFF_DEADLINE}. It is left with online: false and "
                    "NOTHING was deleted — check its BMC, then re-run",
                    type=HostPowerOffTimeoutError.__name__,
                )
            powered_off = True

        # Step 4 — the BMH (+ its Secret) and the NMStateConfig.
        resources_removed: list[str] = []
        if state.bmh_found or state.nmstate_found:
            self._phase = "removing-host"
            teardown = await self._call(teardown_bmh_resources, ref, activity_queue)
            resources_removed = list(teardown.removed)

        # Step 5 — the Agent, last, once nothing can bring it back.
        agents_removed: list[str] = []
        if agent_names:
            self._phase = "removing-agent"
            deletion = await self._call(
                delete_agents,
                AgentDeleteRequest(namespace=request.namespace, names=agent_names),
                activity_queue,
            )
            agents_removed = list(deletion.deleted)

        # Step 5b — the host's MACs off the site PXE VMs, once nothing on the
        # MCE drives it any more: a host left mapped would PXE-boot this MCE's
        # discovery image on its next power-on and re-register its Agent.
        pxe_cleared: list[str] = []
        pxe_kept: list[str] = []
        if self._pxe_cleanup:
            # A limb that predates `macs` sent only server-scan's (CLAUDE.md §5).
            macs = (
                state.macs if "macs" in state.model_fields_set else state.server_scan_macs
            )
            if macs:
                self._phase = "removing-pxe-boot"
                removal = await self._call(
                    remove_host_pxe_entries,
                    PxeHostEntriesRef(
                        server_name=request.server_name,
                        mce_cluster=request.mce_cluster,
                        macs=macs,
                    ),
                    activity_queue,
                )
                pxe_cleared = list(removal.cleared)
                pxe_kept = list(removal.kept)
                if removal.kept:
                    workflow.logger.warning(
                        "Left %s mapped on the PXE VM: another MCE's boot entry — %s",
                        request.server_name,
                        removal.kept,
                    )
                if removal.detail:
                    workflow.logger.info("PXE map cleanup: %s", removal.detail)

        # Step 6 — nothing of the server may be left on the MCE.
        self._phase = "verifying"
        leftover = await self._await_gone(ref, activity_queue)
        if leftover is not None:
            remaining = _describe_leftover(leftover)
            hint = (
                " The host could not be powered off (there was no BareMetalHost "
                "to do it through) and is likely still running the discovery "
                "image, re-registering its Agent: power it off through its BMC, "
                "then re-run."
                if leftover.agents and not powered_off
                else ""
            )
            raise ApplicationError(
                f"{request.server_name} is still on MCE {request.mce_cluster} in "
                f"{request.namespace} {_GONE_DEADLINE} after the deletes: {remaining}.{hint}",
                type=ServerStillPresentError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "Removed %s from MCE %s inventory: agents %s, resources %s",
            request.server_name,
            request.mce_cluster,
            agents_removed,
            resources_removed,
        )
        return result(
            found=True,
            powered_off=powered_off,
            agents_removed=agents_removed,
            resources_removed=resources_removed,
            pxe_cleared=pxe_cleared,
            pxe_kept=pxe_kept,
        )

    async def _call(self, activity_fn, arg, task_queue: str):
        """One cluster call on the target MCE's queue, under the domain's budget."""
        return await workflow.execute_activity(
            activity_fn,
            arg,
            task_queue=task_queue,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _await_gone(self, ref: BmhRef, task_queue: str) -> InventoryHostState | None:
        """Re-inspect until no Agent, BMH or NMStateConfig is left; the leftover on timeout."""
        started_at = workflow.now()
        while True:
            state: InventoryHostState = await self._call(inspect_inventory_host, ref, task_queue)
            if not (state.bmh_found or state.nmstate_found or state.agents):
                return None
            if workflow.now() - started_at >= _GONE_DEADLINE:
                return state
            await workflow.sleep(_GONE_POLL_INTERVAL)  # durable, replay-safe

    async def _await_power_off(self, ref: BmhRef, task_queue: str) -> bool:
        """Poll the BMH until metal3 reports the host off (or the BMH is gone)."""
        started_at = workflow.now()
        while True:
            bmh = await self._call(get_baremetal_host, ref, task_queue)
            if not bmh.found or bmh.powered_on is False:
                return True
            if workflow.now() - started_at >= _POWER_OFF_DEADLINE:
                return False
            await workflow.sleep(_POWER_POLL_INTERVAL)  # durable, replay-safe
