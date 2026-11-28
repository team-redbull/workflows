"""UPI-provisioning activities — the hub limb of the upi-lifecycle domain (`upi-provisioning-worker`). Thin.

Settings, `activity.logger`, and calls into installer_job.py (the Jobs, pure),
hub_api.py (the hub's API), spoke_api.py (the installed cluster's API, as its
admin) and cluster_ready.py (the rules, pure).

Budgets, under the workflow's timeouts (workflow_domains/upi_lifecycle/create_upi_cluster.py):
  * run_installer_step: one heartbeating activity per Job, polling every 15 s; the
    Job's own activeDeadlineSeconds (INSTALLER_ACTIVE_DEADLINE_SECONDS) ends a hung
    installer, below the workflow's start_to_close.
  * await_upi_cluster_ready: one heartbeating activity polling every 20 s, its
    clock (started_at) in the heartbeat details so a retry resumes the wait.
  * register_upi_cluster_in_argocd: a handful of calls on each side.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from temporalio import activity

from activities.upi_provisioning import cluster_ready, hub_api, installer_job, spoke_api
from shared.exceptions import (
    ArgoClusterSecretConflictError,
    InstallerConfigRejectedError,
    InstallerJobFailedError,
    UpiClusterAccessError,
    UpiClusterApiError,
    UpiClusterNotReadyError,
)
from shared.models.upi_lifecycle import (
    ArgoRegistration,
    ArgoRegistrationRequest,
    ClusterReadyRequest,
    InstallerStepRequest,
    InstallerStepResult,
    UpiClusterStatus,
)
from shared.settings.upi_provisioning import UpiProvisioningActivitySettings

_settings = UpiProvisioningActivitySettings()

_JOB_POLL_SECONDS = 15
_READY_POLL_SECONDS = 20


def _job_times(job: dict) -> tuple[datetime | None, datetime | None]:
    status = job.get("status") or {}

    def parse(value: str | None) -> datetime | None:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None

    return parse(status.get("startTime")), parse(status.get("completionTime"))


def _job_outcome(job: dict) -> str:
    """`succeeded`, `failed` or `running`, from the Job's conditions and counts."""
    status = job.get("status") or {}
    for condition in status.get("conditions") or []:
        if condition.get("status") == "True" and condition.get("type") in ("Complete", "SuccessCriteriaMet"):
            return "succeeded"
        if condition.get("status") == "True" and condition.get("type") == "Failed":
            return "failed"
    if status.get("succeeded"):
        return "succeeded"
    if status.get("failed"):
        return "failed"
    return "running"


@activity.defn
async def run_installer_step(request: InstallerStepRequest) -> InstallerStepResult:
    namespace = _settings.namespace
    sites = await hub_api.read_config_map_data(namespace, _settings.installer_sites_configmap) or {}
    if not installer_job.site_profile_present(sites, request.site):
        raise InstallerConfigRejectedError(
            f"site {request.site!r} has no bootstrap-installer profile: ConfigMap "
            f"{_settings.installer_sites_configmap} holds {', '.join(sorted(sites)) or 'no keys'}, "
            f"not {request.site}.yaml"
        )
    vm_network = installer_job.vm_network_for(_settings.vm_network_template, request=request)
    text = installer_job.cluster_yaml(request, vm_network=vm_network)
    if await hub_api.ensure_pvc(namespace, installer_job.pvc_manifest(
        request.cluster, size=_settings.work_volume_size, storage_class=_settings.work_storage_class
    )):
        activity.logger.info("created work volume %s", installer_job.pvc_name(request.cluster))
    if await hub_api.ensure_config_map(namespace, installer_job.config_map_manifest(request.cluster, text)):
        activity.logger.info("wrote %s (vm_network %s)", installer_job.config_map_name(request.cluster), vm_network)

    name = installer_job.job_name(request.cluster, request.step, request.run_token)
    job = await hub_api.get_job(namespace, name)
    if job is None:
        job = await hub_api.create_job(namespace, installer_job.job_manifest(
            request.cluster, request.step, request.run_token,
            image=_settings.installer_image,
            service_account=_settings.installer_service_account,
            secret_name=_settings.installer_secret_name,
            sites_config_map=_settings.installer_sites_configmap,
            active_deadline_seconds=_settings.installer_active_deadline_seconds,
            csr_timeout_minutes=_settings.installer_csr_timeout_minutes,
        ))
        activity.logger.info("started Job %s (%s of %s)", name, request.step.value, request.cluster)
    else:
        activity.logger.info("attached to Job %s", name)

    while True:
        outcome = _job_outcome(job)
        if outcome != "running":
            break
        activity.heartbeat(name)
        await asyncio.sleep(_JOB_POLL_SECONDS)
        job = await hub_api.get_job(namespace, name) or job

    tail = installer_job.bounded_log_tail(
        await hub_api.job_log_tail(namespace, name, installer_job.LOG_TAIL_LINES)
    )
    started, completed = _job_times(job)
    if outcome == "failed":
        raise InstallerJobFailedError(
            f"bootstrap-installer {request.step.value} Job {name} failed for {request.cluster}. "
            f"Last lines of its log:\n{tail or '(no log could be read)'}"
        )
    return InstallerStepResult(
        step=request.step, job_name=name, started_at=started, completed_at=completed, log_tail=tail
    )


async def _cluster_credentials(cluster: str) -> tuple[spoke_api.Credentials, list[str]]:
    secret_name = installer_job.kubeconfig_secret_name(cluster)
    data = await hub_api.read_secret(_settings.namespace, secret_name)
    if not data or not data.get(installer_job.KUBECONFIG_KEY):
        raise UpiClusterAccessError(
            f"no admin kubeconfig for {cluster}: Secret {secret_name} is missing or empty in "
            f"{_settings.namespace} (the prepare Job writes it)"
        )
    creds = spoke_api.Credentials(data[installer_job.KUBECONFIG_KEY], cluster)
    return creds, installer_job.expected_ips(data.get(installer_job.NODES_KEY))


@activity.defn
async def await_upi_cluster_ready(request: ClusterReadyRequest) -> UpiClusterStatus:
    cluster = request.cluster
    creds, expected = await _cluster_credentials(cluster)
    details = activity.info().heartbeat_details
    started_at = datetime.fromisoformat(details[0]) if details else datetime.now(UTC)
    approved: list[str] = list(details[1]) if details and len(details) > 1 else []
    last = "the cluster's API has not answered yet"

    while True:
        try:
            csrs = await spoke_api.call(creds, cluster, "a list of CSRs", spoke_api.list_csrs) or []
            for csr in cluster_ready.to_approve(csrs):
                await spoke_api.call(creds, cluster, "a CSR approval", lambda api, c=csr: spoke_api.approve_csr(
                    api, c, "UpiProvisioningApprove", "approved by create-upi-cluster for a new node"))
                approved.append(csr["metadata"]["name"])
                activity.logger.info("approved CSR %s (%s)", csr["metadata"]["name"], csr["spec"].get("username"))
            nodes = [cluster_ready.node_view(n) for n in
                     await spoke_api.call(creds, cluster, "a list of Nodes", spoke_api.list_nodes) or []]
            available, version = cluster_ready.cluster_version(
                await spoke_api.call(creds, cluster, "a read of ClusterVersion", spoke_api.read_cluster_version))
            problems = cluster_ready.missing(nodes, expected, available, version)
            if not problems:
                return UpiClusterStatus(api_url=creds.server, version=version, nodes=nodes,
                                        expected_ips=expected, csrs_approved=approved)
            last = "; ".join(problems)
        except UpiClusterApiError as err:
            last = str(err)
        waited = (datetime.now(UTC) - started_at).total_seconds()
        if waited > request.timeout_seconds:
            raise UpiClusterNotReadyError(
                f"{cluster} did not serve within {request.timeout_seconds // 60} min: {last[:600]}. "
                "Look at it with `oc get nodes,co,csr` (its admin kubeconfig is in Secret "
                f"{installer_job.kubeconfig_secret_name(cluster)})"
            )
        activity.heartbeat(started_at.isoformat(), approved[-200:])
        await asyncio.sleep(_READY_POLL_SECONDS)


@activity.defn
async def register_upi_cluster_in_argocd(request: ArgoRegistrationRequest) -> ArgoRegistration:
    cluster = request.cluster
    creds, _ = await _cluster_credentials(cluster)
    token = None
    for _ in range(10):
        token = await spoke_api.call(creds, cluster, "the argocd-manager objects", spoke_api.ensure_argocd_manager)
        if token:
            break
        await asyncio.sleep(3)
    if not token:
        raise UpiClusterApiError(f"{cluster}'s token controller has not populated argocd-manager-token yet")

    namespace = _settings.argocd_namespace
    wanted = cluster_ready.argocd_cluster_secret(
        cluster=cluster, kind=request.kind.value, server=creds.server, token=token, ca_b64=creds.ca_b64
    )
    name = wanted["metadata"]["name"]
    found = await hub_api.read_secret_object(namespace, name)
    changed = False
    if found is None:
        await hub_api.create_secret(namespace, wanted)
        changed = True
    else:
        present_server = cluster_ready.secret_string(found, "server")
        if present_server and present_server != creds.server:
            raise ArgoClusterSecretConflictError(
                f"Argo CD Secret {namespace}/{name} already points at {present_server}, not {creds.server}: "
                "another cluster of that name is registered — this workflow never overwrites it"
            )
        if not cluster_ready.same_registration(found, wanted):
            await hub_api.replace_secret(namespace, wanted, found["metadata"]["resourceVersion"])
            changed = True

    read_back = await hub_api.read_secret_object(namespace, name)
    if read_back is None or not cluster_ready.same_registration(read_back, wanted):
        raise UpiClusterApiError(f"Argo CD Secret {namespace}/{name} did not read back as written; retrying")
    activity.logger.info("%s Argo CD cluster %s (%s)", "registered" if changed else "found", cluster, creds.server)
    return ArgoRegistration(secret_name=name, namespace=namespace, name=cluster, server=creds.server, changed=changed)
