"""The tier-2 catalog: what execute_proposed_action may run, and on what (DESIGN §8.3).

Pure: it checks a proposal and says where it acts, filters an agent's verdict
down to what it can run, names and annotates the Job a re-run creates, and
says what each write's outcome means. While everything starts in tier 2
(AGENT.md #25) the catalog holds exactly the tier-1 kinds, with the same
allowlists. Permissions live here and in the admission policies, never in a
proposal or in its approval (AGENT.md #23).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from shared.exceptions import ProposedActionRefusedError
from shared.models.cluster_upgrade import AgentVerdict, ProposedAction

DELETE_POD = "delete_pod"  # a pod in openshift-* on the hosted cluster
DELETE_HCP_POD = "delete_hcp_pod"  # a pod in this cluster's HCP namespace on the MCE
RERUN_CRONJOB = "rerun_cronjob"  # a Job from a CronJob in openshift-* on the hosted cluster
# A FAILED Job in openshift-* on the hosted cluster (owner's decision D21): Red Hat's fix
# for ImagePrunerDegraded is deleting the failed image-pruner Jobs (KCS 6988893). A
# failed Job is already finished, so deleting it stops nothing.
DELETE_FAILED_JOB = "delete_failed_job"
KINDS = (DELETE_POD, DELETE_HCP_POD, RERUN_CRONJOB, DELETE_FAILED_JOB)

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
    if action.kind in (DELETE_POD, RERUN_CRONJOB, DELETE_FAILED_JOB):
        namespace, _, name = action.target.partition("/")
        if not (_NAME.fullmatch(namespace) and _NAME.fullmatch(name)):
            raise refuse("the target must be <namespace>/<name>")
        if not namespace.startswith(_HOSTED_NAMESPACE_PREFIX):
            raise refuse(f"only namespaces {_HOSTED_NAMESPACE_PREFIX}* are in the catalog")
        if action.kind == DELETE_POD and namespace in _HOSTED_DENIED_NAMESPACES:
            raise refuse(f"pods in {namespace} are never deleted by the upgrade")
        return CatalogAction(action.kind, namespace, name, None if action.kind == RERUN_CRONJOB else uid)
    if action.kind == DELETE_HCP_POD:
        if not _NAME.fullmatch(action.target):
            raise refuse("the target must be the name of a pod in the cluster's HCP namespace")
        if action.target.startswith(_HCP_DENIED_PODS):
            raise refuse(f"{', '.join(p + '*' for p in _HCP_DENIED_PODS)} pods are never deleted by the upgrade")
        return CatalogAction(action.kind, None, action.target, uid)
    raise refuse(f"kind {action.kind!r} is not in the catalog ({', '.join(KINDS)})")


def checked(verdict: AgentVerdict) -> AgentVerdict:
    """The verdict with only proposals this catalog can run, each with its own id.

    The agent service checks them too; this is the workflow's side of the
    boundary (the upgrade-agent limb applies it to every reply). A proposal
    outside the catalog is dropped and named in the diagnosis, and a verdict
    that had one is never `retry_gate`: a person sees it.
    """
    kept: list[ProposedAction] = []
    notes: list[str] = []
    ids: set[str] = set()
    for action in verdict.actions_proposed:
        try:
            check(action)
        except ProposedActionRefusedError as err:
            notes.append(f"dropped {err}")
            continue
        if not action.action_id or action.action_id in ids or not action.action_id.replace("-", "").isalnum():
            action = action.model_copy(update={"action_id": f"{len(kept) + 1}"})
            while action.action_id in ids:
                action = action.model_copy(update={"action_id": f"{action.action_id}-x"})
        ids.add(action.action_id)
        kept.append(action)
    if not notes and kept == verdict.actions_proposed:
        return verdict
    return verdict.model_copy(update={
        "actions_proposed": kept,
        "diagnosis": verdict.diagnosis + ("\n\nChecked by the upgrade-agent worker: " + "; ".join(notes) if notes else ""),
        "verdict": "needs_human" if notes and verdict.verdict == "retry_gate" else verdict.verdict,
    })


def rerun_job_name(cronjob: str, action_id: str, invocation_id: str | None = None) -> str:
    """`<cronjob>-rerun-<8 hex>`: one name per action, so a retry finds its Job.

    The hex is of the action id, and of the agent invocation that proposed it
    when given: action ids repeat across invocations and runs ("1",
    "act-pruner"), so without it a later approval of the "same" action meets
    the earlier one's Job. None keeps the name a request without an
    invocation id always had. At most 52 characters, which leaves room for
    the suffix a Job adds to its pods.
    """
    key = action_id if invocation_id is None else f"{invocation_id}/{action_id}"
    return f"{cronjob[:37].rstrip('-.')}-rerun-{hashlib.sha256(key.encode()).hexdigest()[:8]}"


# On every Job execute_proposed_action creates: which request made it, so a 409
# on the name tells this request's Job (an earlier attempt's) from anyone
# else's — an older run's, another invocation's — instead of claiming it.
ACTION_ANNOTATION = "workflows.redbull/action-id"
CLUSTER_ANNOTATION = "workflows.redbull/cluster"
WORKFLOW_ANNOTATION = "workflows.redbull/workflow-id"
REQUEST_ANNOTATION = "workflows.redbull/request"


def request_fingerprint(request_json: str, workflow_id: str, run_id: str, activity_id: str) -> str:
    """16 hex identifying one execution of one approved request.

    The request as sent, and the activity execution carrying it: every retry
    of that activity has the same, any later execution — a new approval, a
    new run — another, even with an identical request.
    """
    return hashlib.sha256("\n".join((request_json, workflow_id, run_id, activity_id)).encode()).hexdigest()[:16]


def job_annotations(*, action_id: str, cluster: str, workflow_id: str, fingerprint: str) -> dict[str, str]:
    """The annotations a created Job carries (above): who made it, and for which request."""
    return {
        ACTION_ANNOTATION: action_id,
        CLUSTER_ANNOTATION: cluster,
        WORKFLOW_ANNOTATION: workflow_id,
        REQUEST_ANNOTATION: fingerprint,
    }


# What each outcome of a write means for the run: (executed, detail).
_OUTCOMES = {
    "deleted": (True, "deleted"),
    "gone": (False, "not executed: already gone"),
    "replaced": (False, "not executed: replaced since the proposal (its UID differs)"),
    "created": (True, "created Job {job}"),
    "exists": (True, "created Job {job} (found on retry)"),
    "conflict": (False, "not executed: a Job {job} already exists that this approval did not make "
                        "(an older run's or another invocation's); nothing was created"),
    "vanished": (False, "not executed: the name {job} was taken, and the Job was gone when read back; "
                        "this attempt created nothing"),
    "missing":(False, "not executed: no such CronJob"),
    "not-failed": (False, "not executed: the Job has not failed (it is running or Complete)"),
}


def outcome(word: str, job: str | None = None) -> tuple[bool, str]:
    """(executed, detail) of one write's outcome word, the detail naming the Job made."""
    executed, detail = _OUTCOMES[word]
    return executed, detail.format(job=job)
