"""UPI-provisioning activity signatures — the HUB limb of the upi-lifecycle domain.

Implemented in activities/upi_provisioning/activities.py, registered on
UPI_PROVISIONING_ACTIVITY_QUEUE by the `upi-provisioning-worker` on the hub. The
worker acts as its own ServiceAccount on the hub: it runs bootstrap-installer as
Jobs in its namespace, reads the installed cluster's admin kubeconfig from the
Secret the prepare Job stored, and writes Argo CD's cluster Secret.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.upi_lifecycle import (
    ArgoRegistration,
    ArgoRegistrationRequest,
    ClusterReadyRequest,
    InstallerStepRequest,
    InstallerStepResult,
    UpiClusterStatus,
)


@activity.defn
async def run_installer_step(request: InstallerStepRequest) -> InstallerStepResult:
    """Run ONE bootstrap-installer step as a Kubernetes Job and wait for it, heartbeating.

    Ensures, in the worker's namespace: the cluster's work volume (PVC
    `upi-<cluster>-work`, kept for ever: it holds terraform's state), the cluster
    config the step reads (ConfigMap `upi-<cluster>-config`: the run's keys on top
    of `install_config`), and the Job `upi-<cluster>-<step>-<run_token>` running
    the INSTALLER_IMAGE. Each object is found before it is created, so a retry —
    on this worker or another — attaches to the same Job. Then polls the Job every
    15 s, heartbeating, until it completes.

    Raises InstallerConfigRejectedError (a key the run owns in `install_config`, a
    site without a profile), InstallerJobFailedError (the Job Failed — its log
    tail in the message), HubAccessError (403, or a 400/422 for a manifest) — all
    deterministic; a hub API outage is the retryable HubApiError.
    """
    ...


@activity.defn
async def await_upi_cluster_ready(request: ClusterReadyRequest) -> UpiClusterStatus:
    """Wait until the installed cluster serves, approving its nodes' CSRs, heartbeating.

    Reads the cluster's admin kubeconfig and cluster-context.yaml from the hub
    Secret `upi-<cluster>-kubeconfig`. Every 20 s: approves pending CSRs of a
    node bootstrapper or a `system:node:*` user (a brand-new cluster's own
    nodes, as bootstrap-installer's loop does), reads the Nodes and the
    ClusterVersion. Done when every expected IP (the context's control plane,
    infra and compute IPs) is a Ready Node and ClusterVersion is Available.
    Its clock is in the heartbeat details, so a retry resumes the wait.

    Raises UpiClusterAccessError (no or unusable kubeconfig Secret, 403) and
    UpiClusterNotReadyError (the deadline passed: what was missing is named) —
    deterministic; an API that does not answer yet is the retryable
    UpiClusterApiError inside the wait, which just polls again.
    """
    ...


@activity.defn
async def register_upi_cluster_in_argocd(request: ArgoRegistrationRequest) -> ArgoRegistration:
    """Register the cluster in the hub's Argo CD, read back.

    On the cluster (its admin kubeconfig): ServiceAccount
    `kube-system/argocd-manager`, its long-lived token Secret
    `argocd-manager-token`, and a ClusterRoleBinding to cluster-admin — the
    objects bootstrap-installer's argocd.py creates, found before created. On the
    hub: Secret `cluster-<cluster>` in ARGOCD_NAMESPACE, label
    `argocd.argoproj.io/secret-type: cluster`, `name: <cluster>` (the destination
    name the hub's ApplicationSets target), `server`, and a bearer-token config
    with the cluster's CA — created, or updated when it points at the same server.

    Raises ArgoClusterSecretConflictError (a Secret of that name for ANOTHER
    server), UpiClusterAccessError, HubAccessError — deterministic; an API
    outage on either side is retryable.
    """
    ...
