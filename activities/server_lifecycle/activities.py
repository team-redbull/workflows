"""Server-lifecycle activity implementations — the execution limbs.

These run in the `server-lifecycle-worker` deployment. They talk to:
  - server-scan (SERVER_SCAN_URL) — the inventory platform. One read:
    GET /servers/available, which returns servers that are unclaimed, healthy,
    reachable and not in maintenance, live-rechecking each one it hands back.
    A VIEWER token is enough; only server-scan's four mutation endpoints need
    admin.
  - the target cluster's Kubernetes API — creates the BMC Secret,
    BareMetalHost and NMStateConfig, and reads the BareMetalHost back.

Conventions enforced here:
  * activity.logger only (not the root logger).
  * Idempotency: every create treats ALREADY EXISTS (409) as success with
    changed=False, so a re-run converges instead of failing — and never
    overwrites, so an operator's hand-correction survives.
  * The httpx client is created INSIDE each activity via `async with`, with an
    explicit timeout below the 90s start_to_close_timeout, so a network hang
    frees the worker before Temporal reaps the activity and the token never
    leaks across concurrent runs.
  * Blocking kubernetes-client calls run in asyncio.to_thread — that SDK is
    synchronous, and calling it directly would block the worker's event loop
    and stall every other activity on this queue.
  * TLS verification is disabled (_TLS_VERIFY), as in the segment-lifecycle
    limb: the airgapped environment's internal CA cannot be injected into this
    image, and with unbounded retries a handshake failure would retry forever.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from temporalio import activity

from activities.server_lifecycle.bmh_resources import (
    BMH_GROUP,
    BMH_PLURAL,
    BMH_VERSION,
    NMSTATE_GROUP,
    NMSTATE_PLURAL,
    NMSTATE_VERSION,
    build_baremetal_host,
    build_bmc_secret,
    build_nmstate_config,
)
from shared.exceptions import (
    AmbiguousServerNameError,
    BmcCredentialsMissingError,
    BmhConflictError,
    BmhResourceError,
    ServerNotAvailableError,
    ServerScanAuthError,
    ServerScanError,
)
from shared.models.server_lifecycle import (
    AcquiredServer,
    AcquireServerRequest,
    BmcEndpoint,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    ServerInterface,
)
from shared.settings import ServerLifecycleActivitySettings

_settings = ServerLifecycleActivitySettings()

# Must stay strictly below the activity start_to_close_timeout (90s). A live
# recheck inside server-scan reaches a vendor manager, so this is not a fast
# endpoint — 60s is the same budget the segment-lifecycle limb uses.
_HTTP_TIMEOUT = httpx.Timeout(60.0)

# See the segment-lifecycle limb's _TLS_VERIFY for the full reasoning: the
# airgapped internal CA is not in this image's trust store. Flip to True (and
# mount a CA bundle via SSL_CERT_FILE) once the certificates can be trusted.
_TLS_VERIFY = False


def _server_scan_client() -> httpx.AsyncClient:
    """A fresh, per-invocation client for server-scan."""
    return httpx.AsyncClient(
        base_url=_settings.server_scan_url, timeout=_HTTP_TIMEOUT, verify=_TLS_VERIFY
    )


def _server_scan_auth() -> dict[str, str]:
    """Bearer header. Empty when server-scan runs with auth disabled."""
    if not _settings.server_scan_api_token:
        return {}
    return {"Authorization": f"Bearer {_settings.server_scan_api_token}"}


def _server_scan_detail(resp: httpx.Response) -> str:
    """server-scan's own RFC 9457 `detail`, which is what an operator acts on.

    It distinguishes "nothing matched the pattern" from "everything matching is
    CRITICAL, claimed, unreachable or in maintenance" — a difference the caller
    cannot otherwise see.
    """
    try:
        body = resp.json()
    except ValueError:
        return resp.text
    if isinstance(body, dict):
        return str(body.get("detail") or body)
    return str(body)


def _to_acquired_server(item: dict[str, Any]) -> AcquiredServer:
    """Project one `AvailableServerItem` onto this domain's model.

    `nic_macs` is deliberately dropped rather than carried: for a Dell server
    it holds every NPAR partition MAC while `interfaces` is reduced to one
    entry per physical port, so keeping both would invite selecting from the
    wrong one.
    """
    bmc = item.get("bmc") or {}
    return AcquiredServer(
        id=item["id"],
        name=item["name"],
        vendor=item.get("vendor", ""),
        source_provider=item.get("source_provider"),
        bmc_vendor=item.get("bmc_vendor"),
        bmc=BmcEndpoint(
            scheme=bmc.get("scheme"),
            host=bmc.get("host") or "",
            host_is_ip=bool(bmc.get("host_is_ip", False)),
            port=bmc.get("port"),
            path=bmc.get("path"),
        ),
        interfaces=[
            ServerInterface(
                name=interface.get("name", ""),
                mac=interface.get("mac"),
                location=interface.get("location"),
                link_state=interface.get("link_state") or "UNKNOWN",
            )
            for interface in item.get("interfaces") or []
        ],
        site_id=item.get("site_id"),
        health_overall=item.get("health_overall", "UNKNOWN"),
        live_recheck_performed=bool(item.get("live_recheck_performed", False)),
    )


@activity.defn
async def acquire_servers(request: AcquireServerRequest) -> list[AcquiredServer]:
    """Ask server-scan for assignable candidates (GET /servers/available)."""
    params: dict[str, Any] = {
        "health": request.health,
        "min_nic_macs": request.min_nic_macs,
    }
    if request.name is not None:
        params["name"] = request.name
    else:
        params["pattern"] = request.pattern
        params["count"] = request.count

    async with _server_scan_client() as client:
        resp = await client.get(
            "/servers/available", params=params, headers=_server_scan_auth()
        )

    if resp.status_code in (401, 403):
        raise ServerScanAuthError(
            f"server-scan rejected our credentials ({resp.status_code}): check "
            "SERVER_SCAN_API_TOKEN (a viewer token is sufficient)"
        )
    if resp.status_code == 404:
        raise ServerNotAvailableError(
            f"server-scan has no assignable server for this request: "
            f"{_server_scan_detail(resp)}"
        )
    if resp.status_code == 409:
        raise AmbiguousServerNameError(
            f"server name {request.name!r} matches more than one server-scan "
            f"document: {_server_scan_detail(resp)}"
        )
    if resp.status_code != 200:
        raise ServerScanError(
            f"server-scan lookup failed: {resp.status_code} {resp.text}"
        )

    payload = resp.json()
    items = payload.get("items", [])
    activity.logger.info(
        "server-scan returned %d candidate(s) of %s requested",
        len(items),
        payload.get("requested", request.count),
    )
    if not items:
        # A 200 with nothing in it is the honest partial-fulfilment answer, but
        # for one install it means the same as a 404 and is classified as such.
        raise ServerNotAvailableError(
            "server-scan returned no candidates for this request "
            f"(mode={payload.get('mode')}, requested={payload.get('requested')})"
        )
    return [_to_acquired_server(item) for item in items]


def _load_kube() -> None:
    """Load in-cluster config, falling back to a kubeconfig for local runs."""
    try:
        k8s_config.load_incluster_config()
    except Exception:  # noqa: BLE001 — any failure here means "not in a pod"
        k8s_config.load_kube_config()


def _bmc_credentials(bmc_vendor: str) -> tuple[str, str]:
    """This worker's configured BMC credentials for one vendor.

    server-scan never holds these — it returns inventory data only — so they
    come from this deployment's own Secret, exactly as the operator's did.
    """
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


def _classify_api_error(exc: k8s_client.ApiException, kind: str, name: str) -> Exception:
    """Turn a Kubernetes ApiException into a classified domain error.

    404 on a CREATE means the resource TYPE is missing — Metal3 or the Assisted
    Installer is not installed in this cluster — which no retry can fix. Every
    other status is treated as transient.
    """
    if exc.status == 404:
        return BmhConflictError(
            f"Cannot create {kind} {name}: its CRD is not installed in the "
            "target cluster (Metal3 / Assisted Installer missing?)"
        )
    return BmhResourceError(f"Failed to create {kind} {name}: {exc.status} {exc.reason}")


def _create_namespaced(create_fn, kind: str, name: str) -> CreatedResource:
    """Run one create, treating ALREADY EXISTS as success.

    Not an upsert: an existing resource is left exactly as it is. A re-run must
    converge without overwriting a BMC credential or a bond an operator has
    since corrected by hand.
    """
    try:
        create_fn()
    except k8s_client.ApiException as exc:
        if exc.status == 409:
            activity.logger.info("%s %s already exists — nothing to do", kind, name)
            return CreatedResource(kind=kind, name=name, changed=False)
        raise _classify_api_error(exc, kind, name) from exc
    activity.logger.info("Created %s %s", kind, name)
    return CreatedResource(kind=kind, name=name, changed=True)


@activity.defn
async def create_bmc_secret(request: BmhResourceRequest) -> CreatedResource:
    """Create the BMC credentials Secret (`{vendor}-cred-{server}`)."""
    username, password = _bmc_credentials(request.bmc_vendor)
    secret = build_bmc_secret(request, username, password)
    name = secret["metadata"]["name"]

    def _create() -> None:
        _load_kube()
        k8s_client.CoreV1Api().create_namespaced_secret(
            namespace=request.namespace, body=secret
        )

    return await asyncio.to_thread(_create_namespaced, _create, "Secret", name)


@activity.defn
async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
    """Create the BareMetalHost (metal3.io/v1alpha1)."""
    bmh = build_baremetal_host(request)
    name = bmh["metadata"]["name"]
    activity.logger.info(
        "BareMetalHost %s: bmc=%s boot_mac=%s",
        name,
        bmh["spec"]["bmc"]["address"],
        bmh["spec"]["bootMACAddress"],
    )

    def _create() -> None:
        _load_kube()
        k8s_client.CustomObjectsApi().create_namespaced_custom_object(
            group=BMH_GROUP,
            version=BMH_VERSION,
            namespace=request.namespace,
            plural=BMH_PLURAL,
            body=bmh,
        )

    return await asyncio.to_thread(_create_namespaced, _create, "BareMetalHost", name)


@activity.defn
async def create_nmstate_config(request: BmhResourceRequest) -> CreatedResource:
    """Create the NMStateConfig (`nmstate-config-{server}`)."""
    nmstate = build_nmstate_config(request)
    name = nmstate["metadata"]["name"]

    def _create() -> None:
        _load_kube()
        k8s_client.CustomObjectsApi().create_namespaced_custom_object(
            group=NMSTATE_GROUP,
            version=NMSTATE_VERSION,
            namespace=request.namespace,
            plural=NMSTATE_PLURAL,
            body=nmstate,
        )

    return await asyncio.to_thread(_create_namespaced, _create, "NMStateConfig", name)


@activity.defn
async def get_baremetal_host(request: BmhResourceRequest) -> BmhState:
    """Read the BareMetalHost's status back — the registration poll's observation."""

    def _get() -> BmhState:
        _load_kube()
        try:
            bmh = k8s_client.CustomObjectsApi().get_namespaced_custom_object(
                group=BMH_GROUP,
                version=BMH_VERSION,
                namespace=request.namespace,
                plural=BMH_PLURAL,
                name=request.server_name,
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                # Not an error: the normal answer while Ironic has not picked
                # the host up yet. The workflow's bounded loop owns the waiting.
                return BmhState(found=False)
            raise BmhResourceError(
                f"Failed to read BareMetalHost {request.server_name}: "
                f"{exc.status} {exc.reason}"
            ) from exc
        status = bmh.get("status") or {}
        return BmhState(
            found=True,
            provisioning_state=(status.get("provisioning") or {}).get("state"),
            operational_status=status.get("operationalStatus"),
            error_message=status.get("errorMessage") or None,
        )

    return await asyncio.to_thread(_get)
