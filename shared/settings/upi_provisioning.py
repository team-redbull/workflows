"""UpiProvisioningActivitySettings — the upi-provisioning activity worker only (ONE deployment, on the hub).

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class UpiProvisioningActivitySettings(BaseSettings):
    """Config for the hub limb of the upi-lifecycle domain.

    The worker talks to the hub's API as its own ServiceAccount (in-cluster
    config) and holds no credential of its own: the vCenter password, the pull
    secret, the trust bundle and the SSH key are in INSTALLER_SECRET_NAME, which
    only the Jobs mount; each installed cluster's admin kubeconfig is in a Secret
    the prepare Job writes.

    Keep aligned with deploy/charts/upi-provisioning-worker (ConfigMap
    `upi-provisioning-config`).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- where the Jobs run ---------------------------------------------------
    # The worker's own namespace (the chart sets it from the downward API): the
    # Jobs, their ConfigMaps, the work volumes and the kubeconfig Secrets live here.
    namespace: str
    # bootstrap-installer's image (team-redbull/bootstrap-installer `main`, the
    # vSphere variant, built with openshift-install, oc and terraform inside).
    installer_image: str
    # The ServiceAccount the Jobs run as: create/patch Secrets in this namespace
    # (the prepare Job stores the kubeconfig), nothing else.
    installer_service_account: str = "upi-provisioning-installer"
    # Secret keys: pull-secret.json (with the registry nodes are added from — the
    # runbook's Artifactory), additional-trust-bundle.pem, ssh.pub, vsphere-password.
    installer_secret_name: str = "upi-provisioning-install"
    # ConfigMap of bootstrap-installer site profiles, one key per site
    # (`<site>.yaml`), mounted over the image's config/sites/; optional key
    # `defaults.yaml` replaces the image's config/defaults.yaml.
    installer_sites_configmap: str = "upi-provisioning-sites"
    # The per-cluster work volume (terraform state, ignition, auth). Never deleted.
    work_volume_size: str = "5Gi"
    work_storage_class: str = ""
    # A Job is killed past this (prepare takes minutes, install up to ~1.5 h).
    installer_active_deadline_seconds: int = 4 * 3600
    # The CSR loop bootstrap-installer runs inside the install Job, in minutes.
    installer_csr_timeout_minutes: int = 90

    # --- what the run fills in ------------------------------------------------
    # The vSphere port group a cluster's VMs attach to, from its allocated
    # segment: `{epg_name}`, `{vlan_id}`, `{site}`, `{cluster}`. A request's own
    # `vm_network` wins.
    vm_network_template: str = "{epg_name}"

    # --- Argo CD on the hub ----------------------------------------------------
    argocd_namespace: str = "openshift-gitops"
