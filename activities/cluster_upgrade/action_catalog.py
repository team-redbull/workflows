"""The tier-2 catalog: what execute_proposed_action may run, and on what (DESIGN §8.3).

Pure: it checks a proposal and says where it acts. While everything starts in
tier 2 (AGENT.md #25) the catalog holds exactly the tier-1 kinds, with the same
allowlists. Permissions live here and in the admission policies, never in a
proposal or in its approval (AGENT.md #23).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from shared.exceptions import ProposedActionRefusedError
from shared.models.cluster_upgrade import ProposedAction

DELETE_POD = "delete_pod"  # a pod in openshift-* on the hosted cluster
DELETE_HCP_POD = "delete_hcp_pod"  # a pod in this cluster's HCP namespace on the MCE
RERUN_CRONJOB = "rerun_cronjob"  # a Job from a CronJob in openshift-* on the hosted cluster
KINDS = (DELETE_POD, DELETE_HCP_POD, RERUN_CRONJOB)

_HOSTED_NAMESPACE_PREFIX = "openshift-"
_HOSTED_DENIED_NAMESPACES = ("openshift-ovn-kubernetes", "openshift-multus")
_HCP_DENIED_PODS = ("etcd-", "kube-apiserver-")
_NAME = re.compile(r"[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?")


@dataclass(frozen=True)
class CatalogAction:
    kind: str
    namespace: str | None  # None for delete_hcp_pod: the HCP namespace, which the activity finds
    name: str
    uid: str | None  # a delete's precondition, when the proposal carries one


def check(action: ProposedAction) -> CatalogAction:
    """The proposal as a catalog action, or ProposedActionRefusedError saying why not."""

    def refuse(why: str) -> ProposedActionRefusedError:
        return ProposedActionRefusedError(f"action {action.action_id} ({action.kind} {action.target}): {why}")

    uid = action.args.get("uid") or None
    if action.kind in (DELETE_POD, RERUN_CRONJOB):
        namespace, _, name = action.target.partition("/")
        if not (_NAME.fullmatch(namespace) and _NAME.fullmatch(name)):
            raise refuse("the target must be <namespace>/<name>")
        if not namespace.startswith(_HOSTED_NAMESPACE_PREFIX):
            raise refuse(f"only namespaces {_HOSTED_NAMESPACE_PREFIX}* are in the catalog")
        if action.kind == DELETE_POD and namespace in _HOSTED_DENIED_NAMESPACES:
            raise refuse(f"pods in {namespace} are never deleted by the upgrade")
        return CatalogAction(action.kind, namespace, name, uid if action.kind == DELETE_POD else None)
    if action.kind == DELETE_HCP_POD:
        if not _NAME.fullmatch(action.target):
            raise refuse("the target must be the name of a pod in the cluster's HCP namespace")
        if action.target.startswith(_HCP_DENIED_PODS):
            raise refuse(f"{', '.join(p + '*' for p in _HCP_DENIED_PODS)} pods are never deleted by the upgrade")
        return CatalogAction(action.kind, None, action.target, uid)
    raise refuse(f"kind {action.kind!r} is not in the catalog ({', '.join(KINDS)})")


def rerun_job_name(cronjob: str, action_id: str) -> str:
    """`<cronjob>-rerun-<8 hex of the action id>`: one name per action, so a retry finds its Job.

    At most 52 characters, which leaves room for the suffix a Job adds to its pods.
    """
    return f"{cronjob[:37].rstrip('-.')}-rerun-{hashlib.sha256(action_id.encode()).hexdigest()[:8]}"
