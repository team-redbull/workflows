"""MCE cluster names — one canonical spelling, whatever a caller typed.

Every MCE is named `ocp4-<rest>` (`ocp4-prod-mce-bat-yam`), and callers often
drop the prefix (`prod-mce-bat-yam`). The name is load-bearing: it is the
Segments Manager's `cluster_name` the VLAN is found by, and it builds the
activity queue only that MCE's worker polls. So both spellings are accepted
and turned into the canonical one, here, before anything uses it.

Applied at the API EDGE and to the worker's own MCE_CLUSTER setting — never to
a model decoded from Temporal history, where changing the name of a recorded
run would change the queue it replays against. Pure, so workflow code may
import it too.
"""

from __future__ import annotations

import re

MCE_CLUSTER_PREFIX = "ocp4-"


def canonical_mce_cluster(name: str) -> str:
    """`prod-mce-bat-yam` -> `ocp4-prod-mce-bat-yam`; a prefixed name is unchanged."""
    name = name.strip()
    if name.startswith(MCE_CLUSTER_PREFIX):
        return name
    return f"{MCE_CLUSTER_PREFIX}{name}"


# --- the naming convention: MCE -> site, (site, zone) -> seed and KV clusters ----------
#
# Under the convention an MCE is `ocp4-<env>-mce-<site>-<letter>`
# (`ocp4-prod-mce-bat-yam-a`): the site is what the MCE's segment, PXE VM and
# seed cluster are found by. Per zone of a site one SEED cluster,
# `ocp4-seed-<site>-<zone>`, hosts that zone's KubeVirt clusters, whose names
# start with `ocp4-kubevirt-<site>-<zone>`. A seed is a "seed", never an
# "mce": it hosts the platform's own KubeVirt clusters, not clients' clusters.
# Pure, so workflow code may import these too; the tokens are validated so a
# day1 path built from them can hold no `/`, `..` or glob character.

_MCE = re.compile(r"^ocp4-(?P<env>[a-z0-9]+)-mce-(?P<site>[a-z0-9]+(?:-[a-z0-9]+)*)-(?P<letter>[a-z])$")
_TOKEN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
SEED_CLUSTER_PREFIX = "ocp4-seed-"
KV_CLUSTER_PREFIX = "ocp4-kubevirt-"


class MceNameError(ValueError):
    """The MCE name is not `ocp4-<env>-mce-<site>-<letter>`. A ValueError too, so a
    Pydantic validator turns it into a 422 at the API edge."""


def site_of_mce_cluster(name: str) -> str:
    """`ocp4-prod-mce-bat-yam-a` -> `bat-yam`. Takes the CANONICAL name (canonical_mce_cluster first)."""
    match = _MCE.fullmatch(name)
    if match is None:
        raise MceNameError(f"MCE {name!r} is not named `ocp4-<env>-mce-<site>-<letter>`, so its site is unknown")
    return match.group("site")


def name_token(value: str, what: str) -> str:
    """A site or zone token as the convention spells it; refused otherwise (MceNameError)."""
    if not _TOKEN.fullmatch(value):
        raise MceNameError(f"{what} {value!r} must be lowercase letters, digits and single dashes")
    return value


def seed_cluster(site: str, zone: str) -> str:
    """`ocp4-seed-<site>-<zone>`: the zone's seed cluster, whose day1 folder lists its KV clusters."""
    return f"{SEED_CLUSTER_PREFIX}{name_token(site, "site")}-{name_token(zone, "zone")}"


def kv_cluster_prefix(site: str, zone: str) -> str:
    """`ocp4-kubevirt-<site>-<zone>`: what every KubeVirt cluster of the zone is named after."""
    return f"{KV_CLUSTER_PREFIX}{name_token(site, "site")}-{name_token(zone, "zone")}"


# --- hosted clusters: the day1 file stem and its parts ---------------------------------
#
# A hosted cluster's day1 file is `sites/<site>/mces/<mce>/hostedClusters/ocp4-<clusterName>.yaml`
# (argocd-platform's `clusterFilePrefix`). The STEM, `ocp4-<clusterName>`, is the
# Argo CD Application's name and the Segments Manager's `cluster_name`; the
# chart gets `<clusterName>` (hcAppset.yaml strips the prefix). Callers name a
# cluster either way, so the edge canonicalises to the stem — once, carried in
# the run's input, never re-derived on replay (the MCE-name precedent above).

HOSTED_CLUSTER_FILE_PREFIX = "ocp4-"
# `hcp-<clusterName>` and HyperShift's `hcp-<clusterName>-openshift-<clusterName>`
# must fit a 63-character DNS label: 24 keeps the longest at 59.
MAX_HOSTED_CLUSTER_NAME_LENGTH = 24
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_MCE_FOLDER_ENV = re.compile(r"^ocp4-(?P<env>[a-z0-9]+)-mce(?:-|$)")


class HostedClusterNameError(ValueError):
    """The name is not a usable hosted-cluster name. A ValueError too, so a Pydantic
    validator (or a route) turns it into a 422 at the API edge."""


def hosted_cluster_stem(name: str) -> str:
    """`prep-e2e-site2-a` or `ocp4-prep-e2e-site2-a` -> `ocp4-prep-e2e-site2-a`.

    The clusterName part must be a lowercase DNS-1123 label of at most
    MAX_HOSTED_CLUSTER_NAME_LENGTH characters: it becomes the `hcp-<name>`
    namespace and the `openshift-<name>` HostedCluster.
    """
    cluster_name = name.strip().removeprefix(HOSTED_CLUSTER_FILE_PREFIX)
    if not _DNS_LABEL.fullmatch(cluster_name):
        raise HostedClusterNameError(
            f"cluster {name!r} must be lowercase letters, digits and dashes, starting and ending "
            f"with a letter or digit (with or without the {HOSTED_CLUSTER_FILE_PREFIX!r} prefix)"
        )
    if len(cluster_name) > MAX_HOSTED_CLUSTER_NAME_LENGTH:
        raise HostedClusterNameError(
            f"cluster name {cluster_name!r} is {len(cluster_name)} characters; at most "
            f"{MAX_HOSTED_CLUSTER_NAME_LENGTH}, so hcp-<name>-openshift-<name> fits a DNS label"
        )
    return f"{HOSTED_CLUSTER_FILE_PREFIX}{cluster_name}"


def hosted_cluster_name(stem: str) -> str:
    """`ocp4-prep-e2e-site2-a` -> `prep-e2e-site2-a`: what hcAppset.yaml hands the chart."""
    return stem.removeprefix(HOSTED_CLUSTER_FILE_PREFIX)


def env_of_hosted_cluster(stem: str) -> str | None:
    """`ocp4-prod-iac-site2-a` -> `prod`: the environment token day2 files a cluster under,
    the first field of the clusterName. None when the name has no second field."""
    fields = hosted_cluster_name(stem).split("-")
    return fields[0] if len(fields) >= 2 and fields[0] else None


PROVISION_CLUSTER_BRANCH_PREFIX = "provision-cluster/"


def provision_cluster_branch(stem: str) -> str:
    """`provision-cluster/<stem>`: the day1 branch a cluster's file is proposed on.

    Named after the WORKFLOW and the cluster, so a reviewer reads what the
    merge request is from its branch, and so a re-run converges onto the same
    branch instead of opening a second one. Both sides build it: the workflow
    (progress, the result) and the limb (the clone, the push).
    """
    return f"{PROVISION_CLUSTER_BRANCH_PREFIX}{stem}"


def env_of_mce_folder(mce: str) -> str | None:
    """`ocp4-prod-mce-site2-a` -> `prod`; None for a folder not named `ocp4-<env>-mce…`
    (`ocp4-mce-site2`, `ocp4-mce-site4-prep` carry no environment)."""
    match = _MCE_FOLDER_ENV.match(mce)
    return match.group("env") if match else None


def is_kv_cluster_of(name: str, site: str, zone: str) -> bool:
    """True when `name` is one of (site, zone)'s KubeVirt clusters.

    A prefix match on a VALIDATED name: after `ocp4-kubevirt-<site>-<zone>` the
    name ends, or goes on with `-` or a digit (`…-a1`, `…-a-1`), so zone `a`
    never claims zone `ab`'s clusters.
    """
    if not _TOKEN.fullmatch(name):
        return False
    prefix = kv_cluster_prefix(site, zone)
    if not name.startswith(prefix):
        return False
    rest = name[len(prefix):]
    return rest == "" or rest[0] == "-" or rest[0].isdigit()
