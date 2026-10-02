"""Typed state for the server-lifecycle workflows — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model, never
an untyped dict.

None of these models carries `extra="forbid"`: they are decoded from Temporal
history on every replay, so a field removed later would wedge in-flight runs.
Strictness about unknown fields belongs on an API-edge subclass in the router
(InstallServerRequest), exactly as InitializeSegmentRequest does it.
"""

from __future__ import annotations

import re
from enum import Enum

from pydantic import BaseModel, Field

_MAC_RE = re.compile(r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}")


def mac_is_valid(mac: str) -> bool:
    """Whether a string is a colon-separated MAC.

    Lives in `shared/` because BOTH sides check it: the workflow rejects a bad
    MAC before the first resource is written, and the resource builders keep
    the check as a last guard on their own inputs.
    """
    return bool(_MAC_RE.fullmatch(mac))


class LinkState(str, Enum):
    """A NIC's link state as server-scan normalizes it across every collector.

    The enum is uniform; the DATA behind it is not. Dell (OpenManage) and
    standalone Redfish report a real Redfish `LinkStatus`. Cisco UCS gained a
    usable vNIC signal in 2026-09 (`operability`). HPE OneView reports
    `UNKNOWN` unconditionally — its `portMap` carries no link state at all, so
    there is nothing to normalize — and Intersight vNICs mostly report it
    empty. install-server requires UP strictly (see `select_bond_members`), so
    those two vendors fail the bond selection rather than being silently
    skipped; that is a deliberate, accepted gap.
    """

    UP = "UP"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"
    DISABLED = "DISABLED"


class BmcEndpoint(BaseModel):
    """A server's BMC as server-scan parsed it, not as a vendor table guesses it.

    server-scan already splits the address it collected into parts, so the BMH's
    `bmc.address` is rebuilt from these rather than from a hardcoded per-vendor
    path. That is what keeps a BMC behind an OpenShift Route working: it has a
    DNS `host`, a non-default `port` and its own `path`, all of which a fixed
    `redfish-virtualmedia://{ip}/redfish/v1/Systems/1` template would discard.

    `host_is_ip` is why nothing here is validated as an IPv4 address.
    """

    scheme: str | None = None
    host: str
    host_is_ip: bool = False
    port: int | None = None
    path: str | None = None


class ServerInterface(BaseModel):
    """One NIC of a candidate server, as `GET /servers/available` reports it.

    This is the ONLY source install-server selects bond members from. The
    server's `nic_macs` list is deliberately not modelled: server-scan reduces
    NPAR partitions to one entry per physical port in `network.interfaces` but
    leaves `identity.nic_macs` whole on purpose, so a partitioned Dell card
    exposes four ports here and sixteen MACs there. Selecting from the MAC list
    can bond two partitions of a single physical port — one wire, no redundancy.

    `location` is `controller/port/partition` for Dell via OpenManage and the
    BMC's own raw identifier (often absent) elsewhere; `physical_port_key`
    turns whichever form into a port identity.
    """

    name: str
    mac: str | None = None
    location: str | None = None
    link_state: LinkState = LinkState.UNKNOWN

    def physical_port_key(self) -> str:
        """The identity of the physical port this interface rides.

        A Dell `controller/port/partition` location collapses to
        `controller/port`, so two NPAR partitions of one port share a key.
        Anything else falls back to the interface name, which is the finest
        distinction the data supports for that vendor.
        """
        if self.location is not None:
            parts = self.location.split("/")
            if len(parts) == 3 and all(part.isdigit() for part in parts):
                return f"{parts[0]}/{parts[1]}"
        return self.name


class AcquiredServer(BaseModel):
    """One candidate returned by server-scan's `GET /servers/available`.

    A projection of `AvailableServerItem`, carrying only what building a
    BareMetalHost + Secret + NMStateConfig needs. `id` is the identity the run
    is keyed on: server NAMES are not unique in server-scan (correlation is on
    vendor + serial), while `srv_…` ids are.
    """

    id: str
    name: str
    vendor: str
    source_provider: str | None = None
    bmc_vendor: str | None = None
    bmc: BmcEndpoint
    interfaces: list[ServerInterface] = Field(default_factory=list)
    site_id: str | None = None
    health_overall: str
    live_recheck_performed: bool = False


class AcquireServerRequest(BaseModel):
    """What to ask server-scan for.

    Exactly one of `pattern`/`name`, mirroring the endpoint's own contract.
    `pattern` is how an InfraEnv resolves to hardware: the InfraEnv name encodes
    vendor, model, site and spec (`cisco-m6-bat-yam-64c-512gb`) and server names
    carry the same tokens after an `ocp-` prefix, so the pattern is built by
    prefixing rather than by parsing either apart.
    """

    pattern: str | None = None
    name: str | None = None
    count: int = 1
    health: str = "HEALTHY"


class ReserveServerRequest(BaseModel):
    """Take — or extend — server-scan's install lock on one machine (ADR-0035).

    `/servers/available` hands out candidates WITHOUT locking them, because it
    cannot know which one a caller will choose; the claim belongs to the caller,
    after it has chosen. server-scan treats a repeat claim with the same
    `holder` + `workflow_id` as an EXTENSION, never a lost race, which is what
    makes this safe to retry and lets the same call renew the lock later.

    `server_name` is not sent — the lock is keyed on the `srv_…` id — it only
    makes a refusal readable.
    """

    server_id: str = Field(min_length=1)
    server_name: str
    holder: str = Field(min_length=1, max_length=64)
    workflow_id: str = Field(min_length=1)
    mce_cluster: str = Field(min_length=1)
    infra_env: str | None = None
    namespace: str | None = None
    ttl_seconds: int = Field(ge=300, le=86_400)


class ReleaseServerRequest(BaseModel):
    """Give server-scan's install lock back, so the machine is drawable at once.

    `holder` + `workflow_id` are always sent: without them server-scan treats a
    release as an operator override and clears ANYONE's lock, and a run whose
    own lock expired would then free the machine another run had since taken.
    """

    server_id: str = Field(min_length=1)
    server_name: str
    holder: str = Field(min_length=1, max_length=64)
    workflow_id: str = Field(min_length=1)


class ServerReservation(BaseModel):
    """What a reserve or release left behind on the server.

    `held` is True after a reserve, and False after a release — including the
    releases server-scan answers without clearing anything (the server has left
    the inventory, or the lock is no longer ours), which `detail` then names.
    """

    server_id: str
    held: bool
    expires_at: str | None = None
    detail: str | None = None


class BondMember(BaseModel):
    """One resolved bond member: a MAC bound to a logical interface name.

    The name is a placeholder (`nic1`, `nic2`), not the kernel's own device
    name. NMStateConfig binds MAC -> name in `spec.interfaces` and the agent
    renames the NIC to match before applying `spec.config`, so nothing here has
    to agree with what the OS would have called it — which is what removed
    bmhgen's per-server-type NIC-name profile ConfigMap entirely.
    """

    logical_name: str
    mac: str
    port_key: str


class BmhResourceRequest(BaseModel):
    """Everything the three Kubernetes resources are built from.

    One request model for all three creates: they share every field, and
    splitting it would let a BareMetalHost and its NMStateConfig disagree about
    the server they describe.
    """

    server_name: str
    namespace: str
    infra_env: str
    bmc_vendor: str
    bmc: BmcEndpoint
    bond_members: list[BondMember]
    vlan_id: int
    bond_name: str = "bond0"
    bond_mode: str = "802.3ad"
    labels: dict[str, str] = Field(default_factory=dict)


class CreatedResource(BaseModel):
    """The outcome of one idempotent create.

    `changed` is False when the resource already existed (HTTP 409), which is a
    success: it is what makes a re-run visibly a no-op instead of a failure.
    """

    kind: str
    name: str
    changed: bool


class BmhRef(BaseModel):
    """Just enough to name one BareMetalHost: its name and namespace.

    Separate from BmhResourceRequest because reading a host back needs neither
    a BMC vendor nor a bond nor a VLAN — and the candidate probe that runs
    BEFORE a server is chosen has none of them to give.
    """

    server_name: str = Field(min_length=1)
    namespace: str = Field(min_length=1)


class BmhState(BaseModel):
    """A BareMetalHost's observed status, for the registration poll.

    `provisioning_state` empty means the API server has stored the object but
    Ironic has not acted on it yet — which is why creating the resource is not
    the same as the host being registered, and why the workflow waits.

    `operational_status` and `error_type` are what distinguish "still working
    on it" from "tried and failed": Metal3 leaves a host that cannot be
    registered in the `registering` state indefinitely, marking the failure
    only in these two fields.
    """

    found: bool = False
    provisioning_state: str | None = None
    operational_status: str | None = None
    error_type: str | None = None
    error_message: str | None = None

    def is_errored(self) -> bool:
        """Metal3 has recorded a failure rather than work still in progress."""
        return (self.operational_status or "").lower() == "error"

    def is_registration_error(self) -> bool:
        """The failure is specifically Ironic failing to reach or log in to the BMC.

        Metal3 sets errorType `registration error` (and `provisioned
        registration error` once provisioned) for a wrong BMC address, a wrong
        credential, or an unreachable BMC — the exact faults this workflow can
        introduce and nothing downstream can correct.
        """
        return self.is_errored() and "registration error" in (self.error_type or "").lower()


class AgentRef(BaseModel):
    """Which host to look for an Agent for: a namespace and the host's bond MACs.

    MATCHED ON MAC, never on name. BMAC names an Agent after the host's own
    inventory UUID, and `agent.spec` carries no reference back to the
    BareMetalHost at all — verified against the live CRD, whose spec holds
    `approved`, `clusterDeploymentName`, role and installation disk and nothing
    else. `status.inventory.interfaces[].macAddress` is the only link between
    the two that holds across BMAC versions, so it is the one used.

    The MACs are the bond members, both of them: the host registers with
    whichever NIC brought the discovery ISO up, and which of the two that was
    is not knowable in advance.
    """

    namespace: str = Field(min_length=1)
    macs: list[str] = Field(min_length=1)


class AgentState(BaseModel):
    """Whether an Agent has registered for one host, and which one it is.

    `found=False` is the normal answer for most of the wait — bare metal takes
    far longer to POST, boot the ISO and phone home than a VM does — so it is
    an observation, not an error, and the workflow's bounded timer owns the
    waiting.
    """

    found: bool = False
    name: str | None = None
    approved: bool | None = None


class TeardownResult(BaseModel):
    """What rolling one candidate back actually removed.

    `finalizers_cleared` records the blunt path being taken: Metal3 holds
    `baremetalhost.metal3.io` while it tries to deprovision through the BMC,
    and a rollback happens precisely when that BMC never answered — so the
    teardown detaches the host first and, if the object still stands at the
    deadline, drops the finalizer itself. True here means that happened, which
    is worth seeing in the history of a run rather than inferring.

    The Secret is absent from `removed` when it cascaded: it carries an
    ownerReference to the BareMetalHost, so the API server's garbage collector
    takes it once the host is really gone — which is only after the finalizer
    clears, never at the moment delete is called.
    """

    removed: list[str] = Field(default_factory=list)
    finalizers_cleared: bool = False


class InstallServerInput(BaseModel):
    """Input to InstallServerWorkflow: which InfraEnv to fill, and from where.

    `infra_env` does double duty by design. It labels the BareMetalHost and the
    NMStateConfig, and its name states which hardware the InfraEnv is for
    (`cisco-m6-bat-yam-64c-512gb` — vendor, model, site, cores, memory), so it
    is also what selects the server. A caller wanting one specific machine
    passes `server_name` instead and the InfraEnv is then only a label.

    `mce_cluster` resolves the VLAN: each MCE holds its own inventory segment,
    so the VLAN is a property of the target cluster and is looked up in the
    Segments Manager rather than supplied here. There is deliberately no
    `vlan_id` field — a caller-supplied VLAN could contradict the segment the
    MCE actually owns.
    """

    infra_env: str = Field(min_length=1)
    mce_cluster: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    server_name: str | None = None
    candidate_count: int = Field(default=3, ge=1, le=20)
    labels: dict[str, str] = Field(default_factory=dict)


class InstallServerRunArgs(BaseModel):
    """The workflow's single argument.

    A single-model argument is the Temporal-recommended shape, and it avoids an
    SDK gotcha: typed conversion is silently SKIPPED when the payload count
    differs from the declared run() parameter count, so a multi-parameter run()
    started with one payload receives a raw dict. The wrapper stays even with
    one field — it is the shape run() is pinned to.
    """

    input: InstallServerInput


class InstallServerProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API).

    `activity_queue` names the MCE-scoped queue this run's cluster writes go
    to. It is what turns "the run is stuck" into "no server-lifecycle-worker is
    polling this queue, so that MCE has none deployed or it cannot reach
    Temporal".
    """

    phase: str
    server_name: str | None = None
    activity_queue: str | None = None


class InstallServerResult(BaseModel):
    """The run's outcome: the server that is now installable in the InfraEnv.

    `resources_changed` is False for a re-run that found all three resources
    already present — the run still succeeds, and the flag says nothing was
    written this time.
    """

    server_id: str
    server_name: str
    infra_env: str
    namespace: str
    mce_cluster: str
    vlan_id: int
    bmc_vendor: str
    bmc_address: str
    bond_macs: list[str]
    boot_mac: str
    resources_changed: bool
    bmh_registered: bool
    # The Agent that proved the host actually booted and reached
    # assisted-service. Defaulted because a run COMPLETED before this field
    # existed has a result payload in history without it, and the status
    # endpoint decodes those.
    agent_name: str | None = None
    # How many candidates were installed and watched before one produced an
    # Agent. 1 is the happy path; more means earlier candidates were created,
    # waited on for the full deadline, and rolled back.
    attempts: int = 1
    # Until when server-scan's install lock keeps the machine out of every
    # other MCE's draw. None for a run that predates the lock, and for one whose
    # final extension was refused — which its log then explains.
    reservation_expires_at: str | None = None
