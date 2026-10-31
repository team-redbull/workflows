"""Cluster-upgrade layouts and names that BOTH sides of the boundary read — pure.

Lives in `shared/` because both sides apply the same rule (shared/ocp_version.py
is the precedent): a limb reads day1, the mirror or a cluster, and the brain's
checks (workflow_domains/cluster_upgrade/gates.py) decide on what it read. Two
copies of one rule drift apart without anyone noticing, so there is one here.
No I/O, no clock, no third-party import: gates.py runs it inside the workflow
sandbox on every replay.

  * DAY1_CLUSTER_FILE — where day1 keeps a hosted cluster's file; its folders
    name the site and the MCE (CONTEXT §6). `.yaml` only: the hub limb looks
    for `<cluster>.yaml` and nothing else, so a `.yml` path is not a cluster
    file anywhere.
  * expand, release_repository, cpo_repository, release_repositories — how a
    ladder.yaml `release` template becomes the repositories a release is
    pulled from. An empty `cpoRepo` names no CPO repository, the same as an
    absent one.
  * IN_PLACE, ZONE_LABEL, ADMIN_ACK_REQUIRED — values HyperShift and the CVO
    write, which a limb reads and the brain decides on.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.models.cluster_upgrade import ReleaseLayout

DAY1_CLUSTER_FILE = re.compile(
    r"sites/(?P<site>[^/]+)/mces/(?P<mce>[^/]+)/hostedClusters/(?P<name>[^/]+)\.yaml"
)

# NodePool `spec.management.upgradeType`. The chart's default, so the hub limb
# reads an unset one as it; and the only type the run rolls: a Replace roll
# would reprovision bare-metal hosts, which the workflow is not built to watch.
IN_PLACE = "InPlace"

# The node label a NodePool's zone is read from (`nodeLabels` in day1, the
# NodePool's spec.nodeLabels on the MCE): what makes a cluster zoned (D7).
ZONE_LABEL = "topology.kubernetes.io/zone"

# The CVO's Upgradeable=False reason, and its UpgradeableAdminAckRequired
# condition's, while an admin gate of the current minor has no ack
# (pkg/cvo/upgradeable.go, cluster-version-operator release-4.19).
ADMIN_ACK_REQUIRED = "AdminAckRequired"


def expand(template: str, *, image_path: str, version: str | None = None) -> str:
    """A ladder.yaml `release` template with `{imagePath}` (and `{version}`) filled in."""
    text = template.replace("{imagePath}", image_path)
    return text if version is None else text.replace("{version}", version)


def release_repository(release: ReleaseLayout) -> str:
    """The repository the release payload is pulled from."""
    return expand(release.repo, image_path=release.image_path)


def cpo_repository(release: ReleaseLayout) -> str | None:
    """The control-plane-operator image's repository; None when the layout names none (or "")."""
    return expand(release.cpo_repo, image_path=release.image_path) if release.cpo_repo else None


def release_repositories(release: ReleaseLayout) -> list[str]:
    """The repositories a release is pulled from: payload, then operator when the layout names one."""
    operator = cpo_repository(release)
    return [release_repository(release), *([operator] if operator is not None else [])]
