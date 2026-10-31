"""Brain `incidents/`: one page per agent verdict, pushed straight to `main` (D10, DESIGN §8.4).

Brain's own rules shape the page (redbull-brain `CLAUDE.md`, `docs/FRONTMATTER.md`,
`templates/incident.md`): `incidents/<yyyy>/<yyyy-mm-dd>-<slug>.md`, the
frontmatter every page needs plus `severity`, `status`, `clusters`, `started`,
the template's sections. Its CI refuses real cluster and MCE names, private
IPs and `.internal` hosts (`scripts/ci_checks.py`), and treats a warning as an
error — so every text that came from a cluster or a model is scrubbed here,
before the page exists, and the page links nowhere and leaves no placeholder.

One page per invocation: its file name ends in a hash of the invocation id,
so a retry after a push that landed finds the page and writes nothing.
`scripts/brain.py validate` judges the page and `brain.py index` regenerates
INDEX.md (never edited by hand). FAIL-CLOSED: the page is pushed only when
the validator answered (exit 0 or 1, with its summary line) and no line of
that answer names the page, and the index was regenerated. Anything else —
refused, a timeout, a crash, a failed index, no brain.py — is not pushed, and
the run gets a note instead, because an upgrade never waits on Brain.

ONE clone per call: a rejected push raises the retryable ValuesRepoGitError
(git_repo.commit_and_push); the activity decides how often Brain is worth a
fresh clone before the verdict stays in the run's history only.

Plain parameters, no settings and no Temporal (CLAUDE.md §1).
"""

from __future__ import annotations

import hashlib
import re
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from activities import git_repo
from shared.models.cluster_upgrade import IncidentRecord, IncidentRequest

VALIDATE_TIMEOUT_SECONDS = 60.0
_BRAIN = "scripts/brain.py"
# brain.py validate's last line, printed whenever it ran to the end:
# "<n> pages checked — <e> errors, <w> warnings" (exit 1 if errors, else 0).
_VALIDATE_SUMMARY = re.compile(r"\d+ pages checked\b")
_AREA = "hypershift"

# Brain CI's leak rules (scripts/ci_checks.py), applied before Brain sees the text.
_REAL_CLUSTER = re.compile(r"\bocp4-(?:prod|prep|test)-(?!<)[a-z0-9][a-z0-9-]*")
_RFC1918 = re.compile(
    r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"
)
_INTERNAL_HOST = re.compile(r"\b[a-z0-9][a-z0-9.-]*\.internal\b")
# And its secret scan (scripts/brain.py SECRET_PATTERNS), the ones a verdict could carry.
_SECRETS = [
    re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"),
    re.compile(r'"auth"\s*:\s*"[A-Za-z0-9+/=]{24,}"'),
    re.compile(r"sha256~[A-Za-z0-9_-]{20,}"),
    re.compile(r"eyJhbGciOi[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    re.compile(r"glpat-[A-Za-z0-9_-]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"rhn-support-[A-Za-z0-9]+_[A-Za-z0-9_]{10,}"),
    re.compile(r"(?:client-key-data|client-certificate-data|certificate-authority-data)\s*:\s*[A-Za-z0-9+/=]{40,}"),
    re.compile(r"[Bb]earer\s+[A-Za-z0-9._~+/-]{30,}"),
    re.compile(r"(?i)\b(?:password|passwd|pwd|secret)\b\s*[:=]\s*\S{6,}"),
]
# A link or a template placeholder is a CI warning, and a warning is an error there.
_LINKISH = re.compile(r"\[\[|\]\(")
_PLACEHOLDERS = ("<Title>", "YYYY-MM-DD")
_SEVERITY = {"retry_gate": "sev4", "needs_human": "sev3", "abort_recommended": "sev2"}
_STATUS = {"retry_gate": "mitigated", "needs_human": "open", "abort_recommended": "open"}


def names_of(request: IncidentRequest) -> list[str]:
    """Every spelling of this cluster's and MCE's real names a text may carry, longest first."""
    names = {request.cluster, request.mce_cluster}
    names |= {name.removeprefix("ocp4-") for name in names}
    return sorted((name for name in names if len(name) > 2), key=len, reverse=True)


def scrub(text: str, request: IncidentRequest) -> str:
    """Real names, private addresses, internal hosts and secrets out; Brain's placeholders in."""
    for name in names_of(request):
        placeholder = "<mce>" if name in (request.mce_cluster, request.mce_cluster.removeprefix("ocp4-")) else "<cluster>"
        text = text.replace(name, placeholder)
    text = _REAL_CLUSTER.sub("<cluster>", text)
    text = _RFC1918.sub("<ip>", text)
    text = _INTERNAL_HOST.sub("<host>", text)
    for pattern in _SECRETS:
        text = pattern.sub("<redacted>", text)
    text = _LINKISH.sub("(", text)
    for placeholder in _PLACEHOLDERS:
        text = text.replace(placeholder, "<placeholder>")
    return text.replace("\r", "")


def _one_line(text: str, limit: int = 90) -> str:
    line = " ".join(text.split())
    return line if len(line) <= limit else line[: limit - 1] + "…"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40] or "gate"


def incident_id(request: IncidentRequest) -> str:
    return hashlib.sha256(request.invocation_id.encode()).hexdigest()[:10]


def incident_path(request: IncidentRequest, now: datetime) -> str:
    day = now.strftime("%Y-%m-%d")
    return f"incidents/{now:%Y}/{day}-{_AREA}-upgrade-{_slug(request.phase)}-{incident_id(request)}.md"


def render(request: IncidentRequest, now: datetime) -> str:
    """The incident page, scrubbed: Brain's template, filled from the verdict."""
    verdict = request.verdict
    step = request.step
    minor = ".".join(step.version.split(".")[:2])
    where = f"{step.kind.value} step to {step.version}" + (f", NodePool {request.nodepool}" if request.nodepool else "")
    title = _one_line(f"{now:%Y-%m-%d} upgrade agent at {request.phase}: {verdict.verdict}", 120).replace("#", "")
    taken = "\n".join(
        f"- tier {call.tier} `{call.tool}` ({call.judged}): {_one_line(call.result or 'no result', 200)}"
        for call in verdict.actions_taken
    ) or "- none"
    proposed = "\n".join(
        f"| {action.kind} `{action.target}` ({action.action_id}) | a person approves in the run | — | "
        f"{_one_line(action.rationale or '', 200)} |"
        for action in verdict.actions_proposed
    ) or "| none proposed |  |  |  |"
    page = f"""---
title: {title}
type: incident
area: {_AREA}
tags: [upgrade, upgrade-agent, {_slug(request.phase)}]
applies_to: [hosted-cp, ocp-{minor}]
confidence: {verdict.confidence}
last_verified: {now:%Y-%m-%d}
owners: [upgrade-agent]
source: {request.workflow_id} invocation {incident_id(request)}
severity: {_SEVERITY[verdict.verdict]}
status: {_STATUS[verdict.verdict]}
clusters: [<cluster>]
started: {now:%Y-%m-%dT%H:%M}+00:00
---

# {title}

## Summary

The upgrade agent was asked about a failed gate during the {where} of <cluster> on <mce>, and answered
`{verdict.verdict}` ({verdict.confidence}). Control went back to the same gate; the workflow, not the
agent, decided what happened next.

## Impact

- Clusters / workloads: <cluster>, phase {request.phase}
- Duration: see the run's log
- Data loss: none

## Timeline (UTC)

| Time | Event |
|---|---|
| {now:%H:%M} | The agent returned `{verdict.verdict}` for {where} |

## Root cause

{verdict.diagnosis.strip() or "The agent gave no diagnosis."} ({verdict.confidence})

## Contributing factors

Actions the agent took itself:

{taken}

## What went well / what didn't

- The verdict was recorded; a person reviews it before the fix graduates to tier 1 (AGENT.md #25).

## Action items

| Action | Owner | Due | Tracking |
|---|---|---|---|
{proposed}

## Related knowledge

- None linked by the agent.
"""
    return scrub(page, request)


def _existing(clone: Path, request: IncidentRequest) -> str | None:
    incidents = clone / "incidents"
    matches = sorted(incidents.glob(f"*/*-{incident_id(request)}.md")) if incidents.is_dir() else []
    return str(matches[0].relative_to(clone)) if matches else None


async def _why_not_pushed(clone: Path, relative: str, token: str) -> str | None:
    """Why the page must not be pushed, or None when brain.py validate passed it.

    Fail-closed: only the validator's own answer — exit 0 or 1, ending in its
    summary line — can let the page through, and every line of it is read
    (run_command returns the whole output). An error or warning line naming
    OUR page refuses it; problems on other pages are not ours to fix. A
    timeout (124), a script that cannot start (127), a crash or any other exit
    is no answer, and no answer is not a pass.
    """
    if not (clone / _BRAIN).is_file():
        return f"Brain main has no {_BRAIN} to validate the incident page with, not pushed"
    code, output = await git_repo.run_command(
        [sys.executable, _BRAIN, "validate"], cwd=clone, secrets=[token], timeout=VALIDATE_TIMEOUT_SECONDS
    )
    if code not in (0, 1) or not _VALIDATE_SUMMARY.search(output):
        return f"Brain's validator did not answer (exit {code}), the incident page was not pushed: {git_repo.tail(output)}"
    ours = [line for line in output.splitlines() if relative in line and line.lstrip().lower().startswith(("error", "warning"))]
    if ours:
        return f"Brain's validator refused the incident page, not pushed: {git_repo.tail('; '.join(ours))}"
    return None


async def write_incident(
    *, repo_url: str, token: str, request: IncidentRequest, now: datetime | None = None
) -> IncidentRecord:
    """Commit the page to Brain main and push, from ONE fresh clone, as git_repo.UPGRADE_AUTHOR; never forced.

    A rejected push raises the retryable ValuesRepoGitError: the retry
    re-clones and finds the page if that push landed after all.
    """
    when = now or datetime.now(UTC)
    page = render(request, when)
    with tempfile.TemporaryDirectory(prefix="brain-incident-") as tmp:
        clone = Path(tmp) / "repo"
        await git_repo.clone_main(repo_url=repo_url, token=token, dest=clone, repo="Brain")
        existing = _existing(clone, request)
        if existing is not None:
            return IncidentRecord(path=existing)
        relative = incident_path(request, when)
        target = clone / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page, encoding="utf-8")
        refused = await _why_not_pushed(clone, relative, token)
        if refused is not None:
            return IncidentRecord(path="", note=refused)
        code, output = await git_repo.run_command(
            [sys.executable, _BRAIN, "index"], cwd=clone, secrets=[token], timeout=VALIDATE_TIMEOUT_SECONDS
        )
        if code != 0:
            # INDEX.md is generated, never hand-edited: a page pushed without
            # it would leave Brain's own CI failing on a stale index.
            return IncidentRecord(
                path="",
                note=f"{_BRAIN} index failed (exit {code}), the incident page was not pushed: {git_repo.tail(output)}",
            )
        subject = _one_line(scrub(f"incident({_AREA}): upgrade agent {request.verdict.verdict} at {request.phase}", request))
        await git_repo.commit_and_push(
            clone,
            repo_url=repo_url,
            token=token,
            branch=git_repo.MAIN,
            paths=[relative, "INDEX.md"],
            message=subject,
            author=git_repo.UPGRADE_AUTHOR,
        )
        return IncidentRecord(path=relative)
