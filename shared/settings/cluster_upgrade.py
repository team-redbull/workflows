"""ClusterUpgradeActivitySettings — cluster-upgrade's per-MCE cluster limb only: where Argo CD is.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

import re

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.cluster_names import canonical_mce_cluster


class ClusterUpgradeActivitySettings(BaseSettings):
    """Config for the per-MCE cluster-upgrade limb (`cluster-upgrade-worker`) only.

    Polls `cluster-upgrade-activity-<mce_cluster>`; reads that MCE's
    HostedClusters and NodePools and syncs their Argo CD Applications as its
    own ServiceAccount. It never writes a HostedCluster or NodePool (DESIGN
    D1): Argo CD applies the day1 commit. Keep aligned with
    deploy/charts/cluster-upgrade-worker/templates/configmap.yaml.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Which MCE this worker serves: names its queue. Canonical, `ocp4-` prefixed.
    mce_cluster: str

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)

    # --- Argo CD on this MCE --------------------------------------------------
    # Where the hosted clusters' Applications live: the namespace of the
    # hcAppset that renders them, the shared openshift-gitops instance in
    # production and on the sandbox. The limb reads and syncs the Application
    # named after the cluster file there, and its chart's Role is in it.
    argo_namespace: str = "openshift-gitops"
    # Which source of a MULTI-SOURCE Application holds the day1 values: the one
    # with this `ref`. The hcAppset renders two sources, the hostedclusters-setup
    # chart and the day1 repo as `ref: values`, read by the chart's valueFiles
    # as `$values/...`; a day1 commit is synced in that source while the chart's
    # keeps its revision. A single-source Application never reads this.
    argo_values_ref: str = "values"

    @field_validator("argo_namespace")
    @classmethod
    def _namespace_name(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", value):
            raise ValueError(f"argo_namespace must be a Kubernetes namespace name (got {value!r})")
        return value

    @field_validator("argo_values_ref")
    @classmethod
    def _ref_key(cls, value: str) -> str:
        # Argo CD's own rule for a source's `ref` (GetRefSources, util/argo/argo.go).
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", value):
            raise ValueError(f"argo_values_ref must be an Argo CD source ref: letters, digits, _ and - (got {value!r})")
        return value
