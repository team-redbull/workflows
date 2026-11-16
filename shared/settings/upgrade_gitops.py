"""UpgradeGitopsActivitySettings — cluster-upgrade's HUB limb only: the day1, day2 and Brain repos and the release registry.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# cluster-upgrade has THREE limbs (CLAUDE.md §2), one settings class — and one
# module here — per limb: this one, cluster_upgrade.py and upgrade_agent.py.


class UpgradeGitopsActivitySettings(BaseSettings):
    """Config for the cluster-upgrade HUB limb (`upgrade-gitops-worker`) only.

    The day1 and day2 repos, the release registry, Brain: everything the
    upgrade reads or writes in git or a registry. The brain never reads these.

    Keep aligned with redbull-platform's gitops/charts/upgrade-gitops-worker/
    (upgrade-gitops-config and the Secrets it names).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- day1: the rules, the cluster files, the upgrade's commits ------------
    # The same repo and the same keys as segment-lifecycle's (DAY1_REPO_URL,
    # DAY1_GIT_TOKEN); the chart fills DAY1_GIT_TOKEN from the shared
    # `redbull-git-token` Secret. This limb reads and
    # pushes `main` only: Argo CD renders main, and nothing else (CONTEXT §6.1).
    day1_repo_url: str
    day1_git_token: str
    # Run in the clone before every push, e.g. `python3 scripts/validate_all_clusters.py`
    # (D3: day1 CI does not run on main). Split like a shell line, run without
    # a shell. Empty = no validator, which every commit logs.
    day1_validate_command: str = ""

    # --- day2: version layers (check 4) ----------------------------------------
    # Empty = not checked: check_day2_layers reports nothing missing and says why.
    day2_repo_url: str = ""
    day2_git_token: str = ""

    # --- Brain: where an agent verdict is recorded (D10) -----------------------
    # Empty = not configured: write_incident records a note in the run's
    # history and pushes nothing, so no run ever waits on Brain.
    brain_repo_url: str = ""
    brain_git_token: str = ""

    # --- the release registry ---------------------------------------------------
    # A container auth file (`.dockerconfigjson` shape) for the release, CPO and
    # payload repositories, passed to `oc adm release info` as well. Empty =
    # anonymous, which quay.io's public ocp-release allows; its payload
    # repository (ocp-v4.0-art-dev) needs the pull secret.
    registry_auth_file: str = ""
    # Off only for a registry whose CA cannot be mounted (the internal mirror).
    registry_tls_verify: bool = True

    # Who the upgrade's commits are by is NOT config: activities/git_repo.py's
    # UPGRADE_AUTHOR, hard-coded like segment-lifecycle's — it identifies the
    # writer, so it must not vary per environment.

    @field_validator("day1_repo_url", "day2_repo_url", "brain_repo_url")
    @classmethod
    def _require_https_repo_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url).

        Another scheme would silently drop it, and every clone would then fail
        as the retryable ValuesRepoGitError, forever. Fail at worker start
        instead. An empty optional repo stays empty (not configured).
        """
        if url and not url.startswith("https://"):
            raise ValueError(
                "repo URLs must be https:// — the token is only injected into "
                f"https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url
