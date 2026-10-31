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
INDEX.md (never edited by hand). A page Brain's validator refuses is not
pushed: the run gets a note instead, because an upgrade never waits on Brain.

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
from activities.upgrade_gitops.day1_repo import GitIdentity, run_command
from shared.exceptions import ValuesRepoGitError
from shared.models.cluster_upgrade import IncidentRecord, IncidentRequest

BRANCH = "main"
_MISSING_MAIN = "the Brain repo has no `main` branch"
PUSH_ATTEMPTS = 3
VALIDATE_TIMEOUT_SECONDS = 60.0
_BRAIN = "scripts/brain.py"
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


async def _validator_refuses(clone: Path, relative: str, token: str) -> str | None:
    """What brain.py validate says about OUR page, or None; problems elsewhere are not ours to fix."""
    if not (clone / _BRAIN).is_file():
        return None
    _, output = await run_command(
        [sys.executable, _BRAIN, "validate"], cwd=clone, secrets=[token], timeout=VALIDATE_TIMEOUT_SECONDS
    )
    ours = [line for line in output.splitlines() if relative in line and line.lstrip().lower().startswith(("error", "warning"))]
    return "; ".join(ours) or None


async def write_incident(
    *, repo_url: str, token: str, request: IncidentRequest, author: GitIdentity, now: datetime | None = None
) -> IncidentRecord:
    """Commit the page to Brain main and push, retried from a fresh clone; never forced."""
    when = now or datetime.now(UTC)
    page = render(request, when)
    for attempt in range(1, PUSH_ATTEMPTS + 1):
        with tempfile.TemporaryDirectory(prefix="brain-incident-") as tmp:
            clone = Path(tmp) / "repo"
            await git_repo.clone(repo_url=repo_url, branch=BRANCH, token=token, dest=clone, missing_message=_MISSING_MAIN)
            existing = _existing(clone, request)
            if existing is not None:
                return IncidentRecord(path=existing)
            relative = incident_path(request, when)
            target = clone / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(page, encoding="utf-8")
            refused = await _validator_refuses(clone, relative, token)
            if refused is not None:
                return IncidentRecord(path="", note=f"Brain's validator refused the incident page, not pushed: {refused}")
            paths = [relative]
            if (clone / _BRAIN).is_file():
                code, output = await run_command(
                    [sys.executable, _BRAIN, "index"], cwd=clone, secrets=[token], timeout=VALIDATE_TIMEOUT_SECONDS
                )
                if code == 0:
                    paths.append("INDEX.md")
            secrets = [token]
            await git_repo.run_git(["add", *paths], cwd=clone, secrets=secrets)
            subject = _one_line(scrub(f"incident({_AREA}): upgrade agent {request.verdict.verdict} at {request.phase}", request))
            await git_repo.run_git(
                ["-c", f"user.name={author.name}", "-c", f"user.email={author.email}", "commit", "-m", subject],
                cwd=clone, secrets=secrets,
            )
            code, _, stderr = await git_repo.exec_git(["push", "origin", f"HEAD:{BRANCH}"], cwd=clone, secrets=secrets)
            if code == 0:
                return IncidentRecord(path=relative)
            if attempt == PUSH_ATTEMPTS:
                raise ValuesRepoGitError(f"git push to Brain main was rejected {PUSH_ATTEMPTS} times (exit {code}): {stderr}")
    raise AssertionError("unreachable")  # pragma: no cover
