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
from dataclasses import dataclass

MCE_CLUSTER_PREFIX = "ocp4-"


def canonical_mce_cluster(name: str) -> str:
    """`prod-mce-bat-yam` -> `ocp4-prod-mce-bat-yam`; a prefixed name is unchanged."""
    name = name.strip()
    if name.startswith(MCE_CLUSTER_PREFIX):
        return name
    return f"{MCE_CLUSTER_PREFIX}{name}"


# --- the platform naming convention: clusters, seeds, zones ------------------------------
#
# Every cluster is `ocp4-<lifecycle>-<flavor>-<site>-<id>` (`ocp4-prod-mce-five-a`,
# `ocp4-prod-kv-five-a`): `<id>` is a sequence letter given in creation order, so
# sorting a flavor's clusters by it is oldest first. Two flavors are reserved:
# `mce` (an MCE) and `kv` (a KubeVirt cluster, a "KV cluster": the hosted
# cluster whose nodes run the warm-pool VMs).
#
# A zone is `az<N>`. Per zone of a site ONE seed cluster, `ocp4-seed-<site>-<N>`,
# hosts that zone's KV clusters (decided 2026-11-20: the seed is one per zone
# and immutable, so its name carries the zone, where a KV cluster's does not).
# A seed is a "seed", never an "mce": it hosts the platform's own KV clusters,
# not clients' clusters.
#
# Pure, so workflow code may import these too; the tokens are validated so a
# day1 path built from them can hold no `/`, `..` or glob character.

_SITE = r"[a-z0-9]+(?:-[a-z0-9]+)*"
_MCE = re.compile(rf"^ocp4-(?P<env>[a-z0-9]+)-mce-(?P<site>{_SITE})-(?P<letter>[a-z])$")
_CLUSTER = re.compile(rf"^ocp4-(?P<lifecycle>[a-z0-9]+)-(?P<flavor>[a-z0-9]+)-(?P<site>{_SITE})-(?P<id>[a-z])$")
_SEED = re.compile(rf"^ocp4-seed-(?P<site>{_SITE})-(?P<number>[1-9][0-9]*)$")
_ZONE = re.compile(r"^az(?P<number>[1-9][0-9]*)$")
_TOKEN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
SEED_CLUSTER_PREFIX = "ocp4-seed-"
MCE_FLAVOR = "mce"
KV_FLAVOR = "kv"


class MceNameError(ValueError):
    """The MCE name is not `ocp4-<env>-mce-<site>-<letter>`. A ValueError too, so a
    Pydantic validator turns it into a 422 at the API edge."""


class ClusterNameError(ValueError):
    """The name is not `ocp4-<lifecycle>-<flavor>-<site>-<id>` (or not of the flavor
    asked for). A ValueError too, so a Pydantic validator turns it into a 422."""


@dataclass(frozen=True)
class ParsedClusterName:
    """What a cluster name says under the convention."""

    name: str
    lifecycle: str
    flavor: str
    site: str
    id: str


def site_of_mce_cluster(name: str) -> str:
    """`ocp4-prod-mce-bat-yam-a` -> `bat-yam`. Takes the CANONICAL name (canonical_mce_cluster first)."""
    match = _MCE.fullmatch(name)
    if match is None:
        raise MceNameError(f"MCE {name!r} is not named `ocp4-<env>-mce-<site>-<letter>`, so its site is unknown")
    return match.group("site")


def name_token(value: str, what: str) -> str:
    """A site token as the convention spells it; refused otherwise (MceNameError)."""
    if not _TOKEN.fullmatch(value):
        raise MceNameError(f"{what} {value!r} must be lowercase letters, digits and single dashes")
    return value


def parse_cluster_name(name: str) -> ParsedClusterName:
    """`ocp4-prod-kv-five-a` -> lifecycle prod, flavor kv, site five, id a.

    The lifecycle and the flavor are one field each and the id one letter, so
    a site of several fields (`bat-yam`) is everything between them.
    """
    match = _CLUSTER.fullmatch(name)
    if match is None:
        raise ClusterNameError(f"cluster {name!r} is not named `ocp4-<lifecycle>-<flavor>-<site>-<id>`")
    return ParsedClusterName(
        name=name, lifecycle=match.group("lifecycle"), flavor=match.group("flavor"),
        site=match.group("site"), id=match.group("id"),
    )


def parse_kv_cluster(name: str) -> ParsedClusterName:
    """A KV cluster's name, `ocp4-<lifecycle>-kv-<site>-<id>`; ClusterNameError for anything else."""
    parsed = parse_cluster_name(name)
    if parsed.flavor != KV_FLAVOR:
        raise ClusterNameError(
            f"cluster {name!r} is of flavor {parsed.flavor!r}: a KV cluster is `ocp4-<lifecycle>-{KV_FLAVOR}-<site>-<id>`"
        )
    return parsed


def zone_number(zone: str) -> int:
    """`az1` -> 1. A zone is `az<N>`, N from 1; anything else is refused (MceNameError)."""
    match = _ZONE.fullmatch(zone)
    if match is None:
        raise MceNameError(f"zone {zone!r} is not `az<N>` (az1, az2, …)")
    return int(match.group("number"))


def seed_cluster(site: str, zone: str) -> str:
    """`ocp4-seed-<site>-<N>` for zone `az<N>`: the zone's seed, whose day1 folder lists its KV clusters."""
    return f"{SEED_CLUSTER_PREFIX}{name_token(site, 'site')}-{zone_number(zone)}"


def seed_zone(folder: str) -> str | None:
    """`ocp4-seed-five-1` -> `az1`: the zone of a seed's day1 folder; None for any other folder."""
    match = _SEED.fullmatch(folder)
    return f"az{match.group('number')}" if match else None


def seed_site(folder: str) -> str | None:
    """`ocp4-seed-bat-yam-2` -> `bat-yam`: the site a seed's name carries; None for any other folder."""
    match = _SEED.fullmatch(folder)
    return match.group("site") if match else None


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
