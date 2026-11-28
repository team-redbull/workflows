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
# The convention is kubevirt-workflows' docs/naming-convention.md (the
# `naming-convention` skill fetches it). A tenant cluster or an MCE is
# `ocp4-<lifecycle>-<flavor>-<site>-<id>` (`ocp4-prod-mce-five-a`); a SEED or a
# KV cluster also carries its zone, `ocp4-<lifecycle>-<seed|kv>-<site>-<zone>-<id>`
# (`ocp4-prod-seed-five-az1-a`, `ocp4-prod-kv-five-az1-a`). `<id>` is a sequence
# letter given in creation order, so sorting by it is oldest first; a zone is
# `az<N>`. Three flavors are reserved: `mce`, `seed` and `kv`.
#
# A seed hosts its zone's KV clusters (the hosted clusters whose nodes run the
# warm-pool VMs); its day1 folder sits beside the MCEs' in `sites/<site>/mces/`.
# A seed is a "seed", never an "mce". Until 2026-11-22 a seed was
# `ocp4-seed-<site>-<zone number>` and a KV cluster carried no zone (the
# operator's 2026-11-20 decision against the convention's draft, reversed when
# the convention left draft).
#
# Pure, so workflow code may import these too; the tokens are validated so a
# day1 path built from them can hold no `/`, `..` or glob character.

_SITE = r"[a-z0-9]+(?:-[a-z0-9]+)*"
_MCE = re.compile(rf"^ocp4-(?P<env>[a-z0-9]+)-mce-(?P<site>{_SITE})-(?P<letter>[a-z])$")
_CLUSTER = re.compile(rf"^ocp4-(?P<lifecycle>[a-z0-9]+)-(?P<flavor>[a-z0-9]+)-(?P<site>{_SITE})-(?P<id>[a-z])$")
# A seed or a KV cluster: the zone, `az<N>`, sits between the site and the id, so a
# site of several fields (`bat-yam`) is everything between the flavor and the zone.
_ZONED = re.compile(
    rf"^ocp4-(?P<lifecycle>[a-z0-9]+)-(?P<flavor>seed|kv)-(?P<site>{_SITE})-(?P<zone>az[1-9][0-9]*)-(?P<id>[a-z])$"
)
_ZONE = re.compile(r"^az(?P<number>[1-9][0-9]*)$")
_TOKEN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
MCE_FLAVOR = "mce"
SEED_FLAVOR = "seed"
KV_FLAVOR = "kv"


class MceNameError(ValueError):
    """The MCE name is not `ocp4-<lifecycle>-mce-<site>-<id>`. A ValueError too, so a
    Pydantic validator turns it into a 422 at the API edge."""


class ClusterNameError(ValueError):
    """The name is not the convention's (or not of the flavor asked for). A
    ValueError too, so a Pydantic validator turns it into a 422."""


@dataclass(frozen=True)
class ParsedClusterName:
    """What a cluster name says under the convention. `zone` only for a seed or a KV cluster."""

    name: str
    lifecycle: str
    flavor: str
    site: str
    id: str
    zone: str | None = None


def site_of_mce_cluster(name: str) -> str:
    """`ocp4-prod-mce-bat-yam-a` -> `bat-yam`. Takes the CANONICAL name (canonical_mce_cluster first)."""
    return parse_mce_cluster(name).site


def parse_mce_cluster(name: str) -> ParsedClusterName:
    """`ocp4-prod-mce-bat-yam-a` -> lifecycle prod, site bat-yam, id a; MceNameError otherwise."""
    match = _MCE.fullmatch(name)
    if match is None:
        raise MceNameError(f"MCE {name!r} is not named `ocp4-<lifecycle>-mce-<site>-<id>`, so its site is unknown")
    return ParsedClusterName(
        name=name, lifecycle=match.group("env"), flavor=MCE_FLAVOR, site=match.group("site"), id=match.group("letter")
    )


def name_token(value: str, what: str) -> str:
    """A site (or lifecycle) token as the convention spells it; refused otherwise (MceNameError)."""
    if not _TOKEN.fullmatch(value):
        raise MceNameError(f"{what} {value!r} must be lowercase letters, digits and single dashes")
    return value


def parse_cluster_name(name: str) -> ParsedClusterName:
    """`ocp4-prod-eck-five-a` -> lifecycle prod, flavor eck, site five, id a; a seed or a
    KV cluster (`ocp4-prod-kv-five-az1-a`) with its zone too.

    The lifecycle and the flavor are one field each, the zone `az<N>` and the id
    one letter, so a site of several fields (`bat-yam`) is everything between them.
    """
    zoned = _ZONED.fullmatch(name)
    if zoned is not None:
        return ParsedClusterName(
            name=name, lifecycle=zoned.group("lifecycle"), flavor=zoned.group("flavor"),
            site=zoned.group("site"), id=zoned.group("id"), zone=zoned.group("zone"),
        )
    match = _CLUSTER.fullmatch(name)
    if match is None or match.group("flavor") in (SEED_FLAVOR, KV_FLAVOR):
        raise ClusterNameError(
            f"cluster {name!r} is not named `ocp4-<lifecycle>-<flavor>-<site>-<id>`, nor, for a seed or a KV "
            "cluster, `ocp4-<lifecycle>-<seed|kv>-<site>-<zone>-<id>`"
        )
    return ParsedClusterName(
        name=name, lifecycle=match.group("lifecycle"), flavor=match.group("flavor"),
        site=match.group("site"), id=match.group("id"),
    )


def _parse_zoned(name: str, flavor: str) -> ParsedClusterName:
    match = _ZONED.fullmatch(name)
    if match is None or match.group("flavor") != flavor:
        raise ClusterNameError(
            f"{name!r} is not a {flavor} cluster's name, `ocp4-<lifecycle>-{flavor}-<site>-<zone>-<id>` "
            f"(`ocp4-prod-{flavor}-five-az1-a`)"
        )
    return parse_cluster_name(name)


def parse_kv_cluster(name: str) -> ParsedClusterName:
    """A KV cluster's name, `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`; ClusterNameError for anything else."""
    return _parse_zoned(name, KV_FLAVOR)


def parse_seed(name: str) -> ParsedClusterName:
    """A seed's name (and its day1 folder's), `ocp4-<lifecycle>-seed-<site>-<zone>-<id>`; ClusterNameError otherwise."""
    return _parse_zoned(name, SEED_FLAVOR)


def zone_number(zone: str) -> int:
    """`az1` -> 1. A zone is `az<N>`, N from 1; anything else is refused (MceNameError)."""
    match = _ZONE.fullmatch(zone)
    if match is None:
        raise MceNameError(f"zone {zone!r} is not `az<N>` (az1, az2, …)")
    return int(match.group("number"))


def seed_of(folder: str) -> ParsedClusterName | None:
    """What a day1 folder under `mces/` says when it is a SEED's (`ocp4-prod-seed-five-az1-a`): its
    lifecycle, site, zone and id. None for any other folder — an MCE's — whatever else it is called."""
    try:
        return parse_seed(folder)
    except ClusterNameError:
        return None


def seed_zone(folder: str) -> str | None:
    """`ocp4-prod-seed-five-az1-a` -> `az1`: the zone of a seed's day1 folder; None for any other folder."""
    seed = seed_of(folder)
    return seed.zone if seed else None


def seed_site(folder: str) -> str | None:
    """`ocp4-prod-seed-bat-yam-az2-a` -> `bat-yam`: the site a seed's name carries; None for any other folder."""
    seed = seed_of(folder)
    return seed.site if seed else None


def seed_lifecycle(folder: str) -> str | None:
    """`ocp4-prep-seed-five-az1-a` -> `prep`: the lifecycle a seed's name carries; None for any other folder."""
    seed = seed_of(folder)
    return seed.lifecycle if seed else None


def seed_folder_pattern(lifecycle: str, site: str, zone: str) -> str:
    """`ocp4-prod-seed-five-az1-*`: how a message names the seed folders of one (lifecycle, site,
    zone) — a zone may have several seeds (`-a`, `-b`), so no one folder is derived."""
    return f"ocp4-{name_token(lifecycle, 'lifecycle')}-{SEED_FLAVOR}-{name_token(site, 'site')}-{zone}-*"


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


CREATE_UPI_CLUSTER_BRANCH_PREFIX = "create-upi-cluster/"


def create_upi_cluster_branch(cluster: str) -> str:
    """`create-upi-cluster/<cluster>`: the day1 branch an MCE's folder is proposed on.

    provision_cluster_branch's rule for create-upi-cluster: the workflow and the
    cluster in the name, so a re-run converges onto the same branch.
    """
    return f"{CREATE_UPI_CLUSTER_BRANCH_PREFIX}{cluster}"


# A UPI cluster's name: what bootstrap-installer accepts (alphanumeric and
# hyphens, no hyphen at either end, at most 27 characters) held to lowercase,
# because it is also a DNS label, a Kubernetes name and a queue suffix.
UPI_CLUSTER_NAME_MAX = 27
_UPI_CLUSTER_NAME = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")


class UpiClusterNameError(ValueError):
    """A UPI cluster name bootstrap-installer, DNS or Kubernetes would refuse."""


def upi_cluster_name(name: str) -> str:
    """The name, checked: lowercase letters, digits and inner hyphens, at most 27."""
    if len(name) > UPI_CLUSTER_NAME_MAX or not _UPI_CLUSTER_NAME.fullmatch(name):
        raise UpiClusterNameError(
            f"UPI cluster name {name!r} must be 1-{UPI_CLUSTER_NAME_MAX} lowercase letters, digits "
            "and inner hyphens (bootstrap-installer's limit, a DNS label, a Kubernetes name)"
        )
    return name


def env_of_mce_folder(mce: str) -> str | None:
    """`ocp4-prod-mce-site2-a` -> `prod`; None for a folder not named `ocp4-<env>-mce…`
    (`ocp4-mce-site2`, `ocp4-mce-site4-prep` carry no environment)."""
    match = _MCE_FOLDER_ENV.match(mce)
    return match.group("env") if match else None
