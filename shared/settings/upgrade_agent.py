"""UpgradeAgentActivitySettings — cluster-upgrade's per-MCE agent limb only: the upgrade agent endpoint.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.cluster_names import canonical_mce_cluster


class UpgradeAgentActivitySettings(BaseSettings):
    """Config for the per-MCE upgrade-agent limb (`upgrade-agent-worker`) only.

    Holds no Kubernetes credential: it calls the upgrade agent's endpoint.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Which MCE this worker serves: names its queue. Canonical, `ocp4-` prefixed.
    mce_cluster: str

    @field_validator("mce_cluster")
    @classmethod
    def _canonical_mce_cluster(cls, value: str) -> str:
        return canonical_mce_cluster(value)

    # The upgrade agent service in this MCE: an agent-sandbox `Sandbox` behind
    # a Service (the k8s-agent chart), e.g. http://upgrade-agent.<ns>.svc:8080.
    # The token, when the service asks for one, comes from a Secret.
    upgrade_agent_url: str
    upgrade_agent_token: str = ""

    @field_validator("upgrade_agent_url")
    @classmethod
    def _http_upgrade_agent_url(cls, url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"upgrade_agent_url must be an http:// or https:// URL (got {url!r})")
        return url.rstrip("/")
