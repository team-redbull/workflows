#!/usr/bin/env python3
"""Convention checker for this Temporal monorepo — the mechanical half of CLAUDE.md.

CLAUDE.md explains WHY each rule exists; this script checks the rules that can be
checked from source, so a slip is caught at the edit (Claude Code PostToolUse
hook), on `pytest` (tests/test_conventions.py) and in CI — not in production as a
run that sits RUNNING forever or a worker that crash-loops.

Standard library only, on purpose: it runs from a hook with whatever `python3`
the machine has, before any venv exists.

Modes
  python3 scripts/check_conventions.py               whole repo (cross-file rules too)
  python3 scripts/check_conventions.py --file PATH   one file's intra-file rules
  python3 scripts/check_conventions.py --hook        Claude Code hook: reads the
                                                     PostToolUse JSON on stdin

Exit codes: 0 clean (warnings allowed), 1 errors found; in --hook mode errors
exit 2 so the message is fed back to Claude. The hook also reminds Claude which
docs pages describe the edited code, on every edit of workflow, activity or
contract code: docs follow the code in the same change.

Every rule carries an id (WF/RT/AC/IF/SH/EP/DL) and cites the CLAUDE.md
section it enforces. Cross-file rules (registration, contract pairing, CI jobs,
docs pages) run ONLY in whole-repo mode so a half-written new workflow is not
flagged between one edit and the next.

A DOCUMENTED exception is written next to the code, never by loosening a rule:

    # conventions: allow WF007 — the one bounded retry, CLAUDE.md §5

on the flagged line or in the comment block directly above it. The reason is
required; a bare `allow` suppresses nothing.
"""

from __future__ import annotations

import ast
import builtins
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

WORKFLOW_DOMAINS_DIR = "workflow_domains"
ACTIVITIES_DIR = "activities"
SHARED_DIR = "shared"

# --- what workflow-side code may import (CLAUDE.md §5: the sandbox) ----------
# Applies to workflow modules AND the sandbox helpers beside them in
# workflow_domains/<domain>/ (dispatch, gates, policy modules), which load in
# the sandbox with the workflow that imports them.
WORKFLOW_FORBIDDEN_IMPORTS = {
    "shared.settings": "settings are read by entrypoints/activities, never inside a workflow (§8)",
    "shared.temporal_client": "a workflow never opens a client (§5)",
    "activities": "a workflow never imports activity code — call it via shared/interfaces (§1)",
    "workflow_domains.api": "the API is not sandbox-safe; a workflow never imports it (§2)",
    "workflow_domains.routers": "routers are not sandbox-safe; a workflow never imports them (§2)",
    "workflow_domains.main_worker_init": "a workflow never imports the worker entrypoint (§1)",
    "fastapi": "not sandbox-safe (§2)",
    "httpx": "I/O belongs in an activity (§5)",
    "requests": "I/O belongs in an activity (§5)",
    "kubernetes": "I/O belongs in an activity (§5)",
    "pydantic_settings": "settings are read by entrypoints/activities, never inside a workflow (§8)",
    "os": "non-deterministic / side effects — move it into an activity (§5)",
    "time": "use workflow.sleep / workflow.now() — durable, replay-safe (§5)",
    "random": "use workflow.random() (§5)",
    "uuid": "use workflow.uuid4() (§5)",
    "subprocess": "I/O belongs in an activity (§5)",
    "socket": "I/O belongs in an activity (§5)",
    "threading": "not deterministic (§5)",
}
# A domain module that is an API surface, never sandbox-safe.
NON_SANDBOX_DOMAIN_MODULES = {"router", "ui"}

# Attribute calls that break determinism inside a workflow.
WORKFLOW_FORBIDDEN_CALLS = {
    ("time", "sleep"): "use `workflow.sleep(...)`, the durable replay-safe timer (§5)",
    ("asyncio", "sleep"): "use `workflow.sleep(...)`, the durable replay-safe timer (§5)",
    ("datetime", "now"): "use `workflow.now()` (§5)",
    ("datetime", "utcnow"): "use `workflow.now()` (§5)",
    ("datetime", "today"): "use `workflow.now()` (§5)",
    ("date", "today"): "use `workflow.now()` (§5)",
    ("uuid", "uuid4"): "use `workflow.uuid4()` (§5)",
    ("random", "random"): "use `workflow.random()` (§5)",
    ("random", "choice"): "use `workflow.random()` (§5)",
    ("random", "randint"): "use `workflow.random()` (§5)",
    ("os", "environ"): "config never enters a workflow; pass it as typed input (§8)",
}
ACTIVITY_CALL_NAMES = {
    "execute_activity",
    "start_activity",
    "execute_activity_method",
    "start_activity_method",
    "execute_activity_class",
    "start_activity_class",
}
CHILD_CALL_NAMES = {"execute_child_workflow", "start_child_workflow"}
REQUIRED_ACTIVITY_KWARGS = ("task_queue", "start_to_close_timeout", "retry_policy")
REQUIRED_CHILD_KWARGS = ("task_queue", "id")
START_CALL_NAMES = {"start_workflow", "execute_workflow"}
# Handlers that make a capped activity call a deliberate best-effort step: the
# workflow catches the exhaustion and carries on (CLAUDE.md §4, remove-server-from-mce).
BEST_EFFORT_HANDLERS = {"ActivityError", "Exception", "BaseException"}

BUILTIN_EXCEPTIONS = {
    name for name in dir(builtins)
    if isinstance(getattr(builtins, name), type) and issubclass(getattr(builtins, name), BaseException)
}

# shared/ is the lightweight contract (§1): stdlib + these only.
SHARED_ALLOWED_TOP_LEVEL = {"temporalio", "pydantic", "pydantic_settings", "shared"}
INTERFACE_ALLOWED_TOP_LEVEL = {"temporalio", "shared", "typing", "datetime", "enum"}

GLOBAL_CONFIGMAP_KEYS = {"TEMPORAL_HOST", "TEMPORAL_NAMESPACE", "SEGMENTS_MANAGER_URL"}
TEMPORAL_CLIENT_MODULE = ("shared", "temporal_client.py")

_PRAGMA = re.compile(
    r"#\s*conventions:\s*allow\s+((?:[A-Z]{2}\d{3})(?:\s*,\s*[A-Z]{2}\d{3})*)\s*(?:—|–|-|:)+\s*\S"
)


@dataclass(frozen=True)
class Violation:
    severity: str  # "error" | "warning"
    path: str  # repo-relative
    line: int
    rule: str
    message: str

    def format(self) -> str:
        return f"{self.path}:{self.line}: [{self.rule}] {self.severity}: {self.message}"


class _Report:
    def __init__(self) -> None:
        self.violations: list[Violation] = []
        self.sources: dict[str, list[str]] = {}

    def _suppressed(self, rel: str, line: int, rule: str) -> bool:
        """A `# conventions: allow <RULE> — <reason>` on the line or in the
        comment block directly above it."""
        lines = self.sources.get(rel)
        if not lines or line < 1 or line > len(lines):
            return False
        candidates = [lines[line - 1]]
        index = line - 2
        # The comment block may sit above a decorated class or function.
        while index >= 0 and lines[index].lstrip().startswith(("#", "@")):
            candidates.append(lines[index])
            index -= 1
        for text in candidates:
            match = _PRAGMA.search(text)
            if match and rule in re.findall(r"[A-Z]{2}\d{3}", match.group(1)):
                return True
        return False

    def _add(self, severity: str, path: Path, line: int, rule: str, message: str) -> None:
        rel = _rel(path)
        if not self._suppressed(rel, line, rule):
            self.violations.append(Violation(severity, rel, line, rule, message))

    def error(self, path: Path, line: int, rule: str, message: str) -> None:
        self._add("error", path, line, rule, message)

    def warning(self, path: Path, line: int, rule: str, message: str) -> None:
        self._add("warning", path, line, rule, message)

    @property
    def errors(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == "error"]

    @property
    def warnings(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == "warning"]


# --- small helpers ------------------------------------------------------------


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _parse(path: Path) -> ast.Module | None:
    source = _read(path)
    if source is None:
        return None
    try:
        return ast.parse(source, filename=str(path))
    except SyntaxError:
        return None  # a syntax error is the editor's problem, not a convention


def _call_name(node: ast.Call) -> str:
    """`workflow.execute_activity` -> "execute_activity"; `Foo(` -> "Foo"."""
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _dotted(node: ast.AST) -> str:
    """`a.b.c` -> "a.b.c" for Attribute/Name chains, else ""."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return ""


def _kwargs(node: ast.Call) -> dict[str, ast.expr]:
    return {kw.arg: kw.value for kw in node.keywords if kw.arg is not None}


def _has_decorator(node: ast.AST, dotted: str) -> bool:
    """True when a decorator is `dotted` or `dotted(...)` (e.g. "workflow.defn")."""
    for decorator in getattr(node, "decorator_list", []):
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if _dotted(target) == dotted:
            return True
    return False


def _imported_names(tree: ast.Module) -> dict[str, str]:
    """Local name -> dotted origin for every import in the module."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                names[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return names


def _top_level_imports(tree: ast.Module) -> list[tuple[str, int]]:
    """Every imported top-level package name with its line (all scopes)."""
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((alias.name.split(".")[0], node.lineno))
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.append((node.module.split(".")[0], node.lineno))
    return found


def _module_imports(tree: ast.Module) -> list[tuple[str, int, ast.AST]]:
    """Every dotted module imported (with `from pkg import mod` expanded to
    `pkg.mod` as well), with line + the import node."""
    found: list[tuple[str, int, ast.AST]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((alias.name, node.lineno, node))
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.append((node.module, node.lineno, node))
            for alias in node.names:
                found.append((f"{node.module}.{alias.name}", node.lineno, node))
    return found


def _passed_through_import_nodes(tree: ast.Module) -> set[int]:
    """ids of import nodes inside `with workflow.unsafe.imports_passed_through():`."""
    inside: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            expr = item.context_expr
            target = expr.func if isinstance(expr, ast.Call) else expr
            if _dotted(target) == "workflow.unsafe.imports_passed_through":
                for child in ast.walk(node):
                    if isinstance(child, (ast.Import, ast.ImportFrom)):
                        inside.add(id(child))
    return inside


def _parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    return {id(child): parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}


def _inside_best_effort_try(node: ast.AST, parents: dict[int, ast.AST]) -> bool:
    """True when `node` sits in the body of a `try` whose handler catches the
    activity's failure — the run deliberately carries on without it."""
    current = node
    while id(current) in parents:
        parent = parents[id(current)]
        if isinstance(parent, ast.Try) and any(current is stmt for stmt in parent.body):
            for handler in parent.handlers:
                if handler.type is None:
                    return True
                types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
                if any(_dotted(t).split(".")[-1] in BEST_EFFORT_HANDLERS for t in types):
                    return True
        current = parent
    return False


def _stdlib_names() -> set[str]:
    return set(getattr(sys, "stdlib_module_names", ()))


def _kebab(snake: str) -> str:
    return snake.replace("_", "-")


def _has_docstring(node: ast.AST) -> bool:
    body = getattr(node, "body", [])
    return bool(body) and isinstance(body[0], ast.Expr) and isinstance(
        getattr(body[0], "value", None), ast.Constant
    ) and isinstance(body[0].value.value, str)


def _is_workflow_class(node: ast.AST) -> bool:
    return isinstance(node, ast.ClassDef) and _has_decorator(node, "workflow.defn")


def _is_legacy_subclass(cls: ast.ClassDef) -> bool:
    """A renamed workflow's OLD type, kept to replay runs started under it
    (CLAUDE.md §9): a subclass of the new workflow class."""
    return any(isinstance(base, ast.Name) and base.id.endswith("Workflow") for base in cls.bases)


def _relparts(path: Path) -> tuple[str, ...]:
    try:
        return path.resolve().relative_to(REPO_ROOT).parts
    except ValueError:
        return ()


# --- rule sets -------------------------------------------------------------------


def check_workflow_side_module(path: Path, tree: ast.Module, report: _Report) -> None:
    """WF rules — CLAUDE.md §5 (sandbox, determinism, routing, retries).

    Run on every module in workflow_domains/<domain>/ that is not the domain's
    router.py or ui.py: workflow modules and the sandbox helpers beside them."""
    passed_through = _passed_through_import_nodes(tree)
    seen: set[tuple[int, str]] = set()
    for module, line, node in _module_imports(tree):
        top = module.split(".")[0]
        if top == "shared" and id(node) not in passed_through and (line, "WF001") not in seen:
            seen.add((line, "WF001"))
            report.error(
                path, line, "WF001",
                f"`{module}` imported outside `with workflow.unsafe."
                "imports_passed_through():` — Pydantic C-extensions crash the sandbox (§5)",
            )
        parts = module.split(".")
        if top == WORKFLOW_DOMAINS_DIR and len(parts) >= 3 and parts[2] in NON_SANDBOX_DOMAIN_MODULES:
            report.error(path, line, "WF003", f"workflow-side code imports `{module}` — the domain's API surface is not sandbox-safe (§2)")
        for forbidden, why in WORKFLOW_FORBIDDEN_IMPORTS.items():
            if (module == forbidden or module.startswith(forbidden + ".")) and (line, forbidden) not in seen:
                seen.add((line, forbidden))
                report.error(path, line, "WF003", f"workflow-side code imports `{module}`: {why}")

    imported = _imported_names(tree)
    parents = _parent_map(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = _dotted(node.func)
            for (mod, attr), why in WORKFLOW_FORBIDDEN_CALLS.items():
                if target in (f"{mod}.{attr}", f"datetime.{mod}.{attr}"):
                    report.error(path, node.lineno, "WF004", f"`{target}` in workflow-side code — {why}")
            if target == "logging.getLogger":
                report.error(path, node.lineno, "WF013", "use `workflow.logger` — no replay spam (§5)")
            if isinstance(node.func, ast.Name) and node.func.id == "print":
                report.warning(path, node.lineno, "WF013", "use `workflow.logger` instead of print (§5)")

            name = _call_name(node)
            if name in ACTIVITY_CALL_NAMES and target.startswith("workflow."):
                _check_activity_call(path, node, name, imported, parents, report)
            if name in CHILD_CALL_NAMES and target.startswith("workflow."):
                kwargs = _kwargs(node)
                for required in REQUIRED_CHILD_KWARGS:
                    if required not in kwargs:
                        report.error(
                            path, node.lineno, "WF016",
                            f"`workflow.{name}` without `{required}=` — a child runs on its own "
                            "workflow queue under a deterministic id from shared/workflow_ids.py (§5)",
                        )
            if name == "RetryPolicy":
                kwargs = _kwargs(node)
                if "maximum_attempts" in kwargs:
                    report.error(
                        path, node.lineno, "WF007",
                        "`maximum_attempts` on a RetryPolicy — retries are UNBOUNDED; classify "
                        "permanent errors in `non_retryable_error_types` instead (§5). A documented "
                        "exception carries `# conventions: allow WF007 — <reason>`",
                    )
                if "maximum_interval" not in kwargs:
                    report.error(path, node.lineno, "WF007", "RetryPolicy without a capped `maximum_interval` (§5)")
                _check_non_retryable_names(path, kwargs.get("non_retryable_error_types"), report)
            if name == "ApplicationError":
                type_kw = _kwargs(node).get("type")
                if isinstance(type_kw, ast.Constant) and isinstance(type_kw.value, str):
                    report.warning(
                        path, node.lineno, "WF012",
                        f"`type=\"{type_kw.value}\"` is a literal — name the class once: "
                        "`type=<Class>.__name__` with the class in shared/exceptions.py (§5)",
                    )

        elif _is_workflow_class(node):
            _check_workflow_class(path, node, imported, report)


def _check_activity_call(path: Path, node: ast.Call, name: str, imported: dict[str, str],
                         parents: dict[int, ast.AST], report: _Report) -> None:
    kwargs = _kwargs(node)
    for required in REQUIRED_ACTIVITY_KWARGS:
        if required not in kwargs:
            report.error(
                path, node.lineno, "WF005",
                f"`workflow.{name}` without `{required}=` — every activity call routes to its "
                "limb's queue with a bounded attempt and an unbounded RetryPolicy (§5)",
            )
    if "schedule_to_close_timeout" in kwargs and not _inside_best_effort_try(node, parents):
        report.error(
            path, node.lineno, "WF005",
            "`schedule_to_close_timeout` caps retries — transient outages are out-waited and only "
            "classified errors stop retries (§5). A best-effort step may cap itself only where the "
            "workflow catches the ActivityError and carries on",
        )
    queue = kwargs.get("task_queue")
    if isinstance(queue, (ast.Constant, ast.JoinedStr, ast.BinOp)):
        report.error(
            path, node.lineno, "WF005",
            "`task_queue=` written inline — use a constant or queue builder from shared/consts.py (§5)",
        )
    if node.args:
        first = node.args[0]
        if isinstance(first, ast.Constant):
            report.error(
                path, node.lineno, "WF006",
                f"`workflow.{name}` by string name — pass the stub from shared/interfaces/ so the call "
                "is routed by exact signature (§1)",
            )
        elif isinstance(first, ast.Name) and first.id in imported:
            origin = imported[first.id]
            if not origin.startswith("shared.interfaces."):
                report.error(
                    path, node.lineno, "WF006",
                    f"`{first.id}` comes from `{origin}` — activities are called through their stub in "
                    "shared/interfaces/, never the implementation (§1)",
                )


def _check_non_retryable_names(path: Path, value: ast.expr | None, report: _Report) -> None:
    """WF008 — every classified error type must exist in shared/exceptions.py."""
    if value is None or not isinstance(value, (ast.List, ast.Tuple)):
        return
    known = _exception_class_names()
    if known is None:
        return
    for element in value.elts:
        if isinstance(element, ast.Constant) and isinstance(element.value, str):
            if element.value not in known:
                report.error(
                    path, element.lineno, "WF008",
                    f"`{element.value}` is not a class in shared/exceptions.py — a misspelt "
                    "non_retryable type retries every minute forever; build the list from the "
                    "classes (`[e.__name__ for e in ...]`) (§5)",
                )


_EXCEPTION_NAMES_CACHE: dict[str, set[str] | None] = {}


def _exception_class_names() -> set[str] | None:
    if "names" not in _EXCEPTION_NAMES_CACHE:
        tree = _parse(REPO_ROOT / SHARED_DIR / "exceptions.py")
        _EXCEPTION_NAMES_CACHE["names"] = (
            None if tree is None else {n.name for n in tree.body if isinstance(n, ast.ClassDef)}
        )
    return _EXCEPTION_NAMES_CACHE["names"]


def _check_workflow_class(path: Path, cls: ast.ClassDef, imported: dict[str, str], report: _Report) -> None:
    if not cls.name.endswith("Workflow"):
        report.error(path, cls.lineno, "WF011", f"workflow class `{cls.name}` must end with `Workflow` (§2)")
    legacy = _is_legacy_subclass(cls)

    has_progress = False
    for item in cls.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _has_decorator(item, "workflow.query") and item.name == "progress":
                has_progress = True
            if _has_decorator(item, "workflow.run"):
                params = [a for a in item.args.args if a.arg != "self"]
                params += item.args.kwonlyargs
                if item.args.vararg or item.args.kwarg or len(params) != 1:
                    report.error(
                        path, item.lineno, "WF009",
                        "`run()` takes exactly ONE Pydantic argument (a RunArgs wrapper) — "
                        "typed conversion is silently skipped otherwise (§5)",
                    )
    if not has_progress and not legacy:
        report.warning(
            path, cls.lineno, "WF014",
            f"`{cls.name}` exposes no `@workflow.query def progress` — "
            "GET /workflows/runs/{id} degrades to status-only (§2)",
        )

    for node in ast.walk(cls):
        if not (isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)):
            continue
        raised = _dotted(node.exc.func) or _call_name(node.exc)
        short = raised.split(".")[-1]
        if short == "ApplicationError":
            if "non_retryable" in _kwargs(node.exc):
                report.error(
                    path, node.lineno, "WF010",
                    "`non_retryable=` on an ApplicationError raised FROM workflow code is inert "
                    "— workflow failures are never retried; drop it (§5)",
                )
            continue
        origin = imported.get(raised.split(".")[0], "")
        if short in BUILTIN_EXCEPTIONS or origin.startswith("shared.exceptions"):
            report.error(
                path, node.lineno, "WF010",
                f"`raise {raised}(...)` in workflow code — a plain exception fails the workflow "
                "TASK (retried forever, run stuck RUNNING); raise "
                f"`ApplicationError(..., type={short}.__name__)` (§5)",
            )
        # Module-local sentinels caught inside run() and helpers that BUILD an
        # ApplicationError (`raise _fail(...)`) are the sanctioned shapes.


def _functions_calling(tree: ast.Module, call_names: set[str]) -> set[str]:
    """Names of module-level functions that call one of `call_names`, directly
    or through another module-level function."""
    functions = {
        node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    direct = {
        name for name, fn in functions.items()
        if any(isinstance(n, ast.Call) and _call_name(n) in call_names for n in ast.walk(fn))
    }
    result = set(direct)
    changed = True
    while changed:
        changed = False
        for name, fn in functions.items():
            if name in result:
                continue
            if any(isinstance(n, ast.Call) and _call_name(n) in result for n in ast.walk(fn)):
                result.add(name)
                changed = True
    return result


def check_router_module(path: Path, tree: ast.Module, report: _Report, kind: str) -> None:
    """RT rules — CLAUDE.md §2 (paths), §6 (deterministic ids), §9 (async trigger).

    kind: "router" (workflow_domains/<domain>/router.py), "ui"
    (workflow_domains/<domain>/ui.py) or "shared" (workflow_domains/routers/)."""
    parts = _relparts(path) or tuple(path.parts[-3:])
    domain_kebab = _kebab(parts[1]) if len(parts) == 3 else ""

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name in START_CALL_NAMES:
            kwargs = _kwargs(node)
            if "id" not in kwargs:
                report.error(path, node.lineno, "RT001", f"`{name}` without `id=` — ids are deterministic dedup keys (§6)")
            elif isinstance(kwargs["id"], (ast.Constant, ast.JoinedStr, ast.BinOp)):
                report.error(path, node.lineno, "RT002", "workflow id built inline — ONE builder per scheme in shared/workflow_ids.py (§2, §6)")
            if "task_queue" not in kwargs:
                report.error(path, node.lineno, "RT001", f"`{name}` without `task_queue=` (§5)")
            elif isinstance(kwargs["task_queue"], (ast.Constant, ast.JoinedStr, ast.BinOp)):
                report.error(path, node.lineno, "RT003", "`task_queue=` must come from shared/consts.py (§5)")
            if node.args and isinstance(node.args[0], ast.Constant):
                report.warning(path, node.lineno, "RT008", "start workflows by `<WorkflowClass>.run`, not by name (§1)")
        elif name == "APIRouter":
            prefix = _kwargs(node).get("prefix")
            value = prefix.value if isinstance(prefix, ast.Constant) else None
            if kind == "router" and value != f"/workflows/{domain_kebab}":
                report.error(path, node.lineno, "RT004", f"a domain router's prefix is `/workflows/{domain_kebab}` — the folder name, kebab-cased (§2)")
            if kind == "ui" and value != f"/ui/{domain_kebab}":
                report.error(path, node.lineno, "RT009", f"a domain's temporary page lives at `/ui/{domain_kebab}` (§2)")

    if kind != "router":
        return
    starters = _functions_calling(tree, START_CALL_NAMES)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            method = _dotted(decorator.func)
            if not method.startswith("router."):
                continue
            route = decorator.args[0] if decorator.args else None
            route_str = route.value if isinstance(route, ast.Constant) and isinstance(route.value, str) else None
            if route_str is not None and route_str.strip("/") == "":
                report.error(path, decorator.lineno, "RT005", "the bare domain prefix is never an endpoint — routes are `/<workflow>` under it (§2)")
            if method == "router.get" and route_str is not None and re.fullmatch(r"/\{[^}]+\}", route_str):
                report.error(
                    path, decorator.lineno, "RT006",
                    "a `/{id}` catch-all under the domain swallows sibling paths — status lives on "
                    "/workflows/runs/{workflow_id} (§2)",
                )
            if method == "router.post":
                starts = any(
                    isinstance(n, ast.Call) and (_call_name(n) == "start_workflow" or _call_name(n) in starters)
                    for n in ast.walk(node)
                )
                status = _kwargs(decorator).get("status_code")
                if starts and not (isinstance(status, ast.Constant) and status.value == 202):
                    report.error(
                        path, decorator.lineno, "RT007",
                        "a route that STARTS work is asynchronous: 202 + workflow id, the caller polls "
                        "/workflows/runs/{id} (§2, §9)",
                    )


def check_activity_module(path: Path, tree: ast.Module, report: _Report) -> None:
    """AC rules — CLAUDE.md §5 (httpx timeout/per-invocation, logging), §6."""
    module_level_ids = {id(n) for n in tree.body}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = _dotted(node.func)
            if target in ("httpx.AsyncClient", "httpx.Client"):
                if "timeout" not in _kwargs(node):
                    report.error(
                        path, node.lineno, "AC001",
                        "httpx client without an explicit `timeout=` strictly below the activity's "
                        "start_to_close_timeout (§5)",
                    )
            if target == "logging.getLogger":
                report.error(path, node.lineno, "AC003", "use `activity.logger` in activities (§5)")
            if isinstance(node.func, ast.Name) and node.func.id == "print":
                report.warning(path, node.lineno, "AC003", "use `activity.logger` instead of print (§5)")
            if target == "time.sleep":
                report.error(path, node.lineno, "AC004", "`time.sleep` blocks the worker's event loop — `await asyncio.sleep` (§5)")
        elif isinstance(node, ast.Assign) and id(node) in module_level_ids:
            for child in ast.walk(node.value):
                if isinstance(child, ast.Call) and _dotted(child.func) in ("httpx.AsyncClient", "httpx.Client"):
                    report.error(
                        path, node.lineno, "AC002",
                        "module-level HTTP client — create it INSIDE the activity with `async with` "
                        "so tokens/cookies scope to one invocation (§5)",
                    )
        elif isinstance(node, ast.FunctionDef) and _has_decorator(node, "activity.defn"):
            report.error(
                path, node.lineno, "AC005",
                f"`{node.name}` is a sync activity — the workers configure no activity_executor, "
                "so it would fail at startup; make it `async def` (§5)",
            )
        unbounded = None
        if isinstance(node, ast.FormattedValue) and _is_response_text(node.value):
            unbounded = "put into an f-string"
        elif isinstance(node, ast.Return) and node.value is not None and _is_response_text(node.value):
            unbounded = "returned"
        if unbounded:
            report.error(
                path, node.lineno, "AC007",
                f"an HTTP response's whole `.text` is {unbounded} — it ends up in an activity "
                "error, which history records verbatim and Temporal fails as non-retryable "
                "\"Failure exceeds size limit.\" over 2 MiB; slice it or pass it through a "
                "bounding helper (§5)",
            )


def _is_response_text(node: ast.expr) -> bool:
    """`resp.text` / `response.text` itself — not sliced, not passed to a helper."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "text"
        and isinstance(node.value, ast.Name)
        and node.value.id in ("resp", "response")
    )


def check_interface_module(path: Path, tree: ast.Module, report: _Report) -> None:
    """IF rules — CLAUDE.md §1 (signatures only, lightweight)."""
    for top, line in _top_level_imports(tree):
        if top not in INTERFACE_ALLOWED_TOP_LEVEL and top not in _stdlib_names():
            report.error(path, line, "IF003", f"shared/interfaces imports `{top}` — signatures only, no implementation deps (§1)")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and _has_decorator(node, "activity.defn"):
            report.error(path, node.lineno, "IF002", f"`{node.name}` must be `async def` to match its implementation (§5)")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _has_decorator(node, "activity.defn"):
            body = node.body[1:] if _has_docstring(node) else node.body
            is_stub = (
                len(body) == 1
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and body[0].value.value is Ellipsis
            )
            if not is_stub:
                report.error(
                    path, node.lineno, "IF001",
                    f"`{node.name}` has a body — interfaces hold signatures with `...` bodies; the "
                    "implementation lives in activities/<limb>/ (§1)",
                )
            if not _has_docstring(node):
                report.warning(path, node.lineno, "IF004", f"`{node.name}` has no docstring — state what it does, its idempotency and which classified errors it raises")


def check_shared_module(path: Path, tree: ast.Module, report: _Report) -> None:
    """SH rules — CLAUDE.md §1 (lightweight contract), §5 (model evolution), §2 (naming)."""
    stdlib = _stdlib_names()
    for top, line in _top_level_imports(tree):
        if top not in SHARED_ALLOWED_TOP_LEVEL and top not in stdlib and top != "__future__":
            report.error(
                path, line, "SH001",
                f"shared/ imports `{top}` — the contract layer is temporalio + pydantic + "
                "pydantic-settings only; heavy deps live in activities/<limb>/ (§1)",
            )
    parts = _relparts(path) or tuple(path.parts[-2:])
    if len(parts) >= 2 and parts[1] == "models":
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "extra" and isinstance(node.value, ast.Constant) and node.value.value == "forbid":
                report.error(
                    path, node.value.lineno, "SH002",
                    "`extra=\"forbid\"` on a boundary model — old payloads in history/lagging limbs "
                    "carry removed fields; put strictness on an API-edge subclass in the router (§5)",
                )
    if parts[-1:] == ("consts.py",):
        _check_consts(path, tree, report)
    if parts[-1:] == ("exceptions.py",):
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and not _has_docstring(node):
                report.error(
                    path, node.lineno, "SH005",
                    f"`{node.name}` has no docstring — say whether it is deterministic "
                    "(non-retryable), transient (retried) or WORKFLOW-RAISED (§5)",
                )
    if parts[-1:] == ("settings.py",):
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and any(_dotted(b) == "BaseSettings" for b in node.bases):
                config = next(
                    (n for n in node.body if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == "model_config" for t in n.targets)),
                    None,
                )
                extra_ignore = config is not None and any(
                    isinstance(k, ast.keyword) and k.arg == "extra" and isinstance(k.value, ast.Constant) and k.value.value == "ignore"
                    for k in ast.walk(config)
                )
                if not extra_ignore:
                    report.error(
                        path, node.lineno, "SH006",
                        f"`{node.name}` needs `extra=\"ignore\"` in its model_config — pods read one flat merged env (§8)",
                    )
    if parts[-1:] == ("workflow_ids.py",):
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_") and not node.name.endswith("_workflow_id"):
                report.warning(path, node.lineno, "SH004", f"`{node.name}`: id builders are named `<workflow>_workflow_id` (§6)")


def _check_consts(path: Path, tree: ast.Module, report: _Report) -> None:
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            name, value = target.id, node.value.value
            if not isinstance(value, str):
                continue
            if name.endswith("_WORKFLOW_QUEUE"):
                if not value.endswith("-workflow"):
                    report.error(path, node.lineno, "SH003", f"`{name}` = {value!r}: workflow queues are `<workflow>-workflow` (§2)")
            elif name.endswith("_ACTIVITY_QUEUE"):
                if not value.endswith("-activity"):
                    report.error(path, node.lineno, "SH003", f"`{name}` = {value!r}: activity queues are `<limb>-activity` (§2)")
            elif name.endswith("_QUEUE"):
                report.error(path, node.lineno, "SH003", f"`{name}`: queue constants end in `_WORKFLOW_QUEUE` or `_ACTIVITY_QUEUE` (§2)")
            if name.endswith("_QUEUE") and not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", value):
                report.error(path, node.lineno, "SH003", f"`{name}` = {value!r}: queue names are kebab-case (§2)")


def check_client_connect(path: Path, tree: ast.Module, report: _Report) -> None:
    """EP001/EP005 — CLAUDE.md §5: every connection goes through
    shared/temporal_client.connect_temporal, which owns the Pydantic data
    converter and TLS."""
    parts = _relparts(path) or tuple(path.parts[-2:])
    is_client_module = tuple(parts[-2:]) == TEMPORAL_CLIENT_MODULE
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _dotted(node.func) == "Client.connect":
            if not is_client_module:
                report.error(
                    path, node.lineno, "EP005",
                    "`Client.connect(...)` outside shared/temporal_client.py — connect through "
                    "`connect_temporal`, which sets the Pydantic converter and TLS (§5)",
                )
            elif "data_converter" not in _kwargs(node):
                report.error(path, node.lineno, "EP001", "`Client.connect(...)` without `data_converter=pydantic_data_converter` (§5)")


# --- per-file dispatch --------------------------------------------------------------


def check_file(path: Path, report: _Report) -> None:
    """Every intra-file rule that applies to `path`. Silent for files outside scope."""
    parts = _relparts(path)
    if not parts or path.suffix != ".py":
        return
    source = _read(path)
    if source is None:
        return
    check_source(path, parts, source, report)


def check_source(path: Path, parts: tuple[str, ...], source: str, report: _Report) -> None:
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return
    report.sources[_rel(path)] = source.splitlines()
    check_tree(path, parts, tree, report)


def check_tree(path: Path, parts: tuple[str, ...], tree: ast.Module, report: _Report) -> None:
    """Dispatch on where the file sits in the repo (`parts` are its repo-relative parts)."""
    if parts[0] in ("tests", "scripts", "docs", "deploy", ".venv", ".agents", ".claude"):
        return

    check_client_connect(path, tree, report)

    if parts[0] == SHARED_DIR:
        check_shared_module(path, tree, report)
        if len(parts) >= 2 and parts[1] == "interfaces":
            check_interface_module(path, tree, report)
    elif parts[0] == ACTIVITIES_DIR and parts[-1] != "worker_init.py":
        # worker_init.py is an entrypoint outside any activity context (plain
        # logging is right there); the EP rules cover it in whole-repo mode.
        check_activity_module(path, tree, report)
    elif parts[0] == WORKFLOW_DOMAINS_DIR:
        if len(parts) == 3 and parts[1] not in ("routers", "__pycache__"):
            stem = Path(parts[2]).stem
            if stem == "router":
                check_router_module(path, tree, report, "router")
            elif stem == "ui":
                check_router_module(path, tree, report, "ui")
            elif stem != "__init__":
                check_workflow_side_module(path, tree, report)
        elif len(parts) == 3 and parts[1] == "routers":
            check_router_module(path, tree, report, "shared")


# --- whole-repo (cross-file) rules --------------------------------------------------


def _domain_dirs() -> list[Path]:
    root = REPO_ROOT / WORKFLOW_DOMAINS_DIR
    return sorted(p for p in root.iterdir() if p.is_dir() and p.name not in ("routers", "__pycache__"))


def _workflow_classes(include_legacy: bool = True) -> list[tuple[Path, str, ast.ClassDef]]:
    """(module path, domain dir, class) for every @workflow.defn under workflow_domains/."""
    found: list[tuple[Path, str, ast.ClassDef]] = []
    for domain in _domain_dirs():
        for path in sorted(domain.glob("*.py")):
            tree = _parse(path)
            if tree is None:
                continue
            for node in tree.body:
                if _is_workflow_class(node) and (include_legacy or not _is_legacy_subclass(node)):
                    found.append((path, domain.name, node))
    return found


def _activity_domains() -> list[str]:
    root = REPO_ROOT / ACTIVITIES_DIR
    return sorted(p.name for p in root.iterdir() if p.is_dir() and (p / "activities.py").exists())


def _activity_defn_names(path: Path) -> set[str]:
    tree = _parse(path)
    if tree is None:
        return set()
    return {
        n.name for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and _has_decorator(n, "activity.defn")
    }


_CONSTS_CACHE: dict[str, set[str]] = {}


def _consts_names() -> set[str]:
    """Every module-level name in shared/consts.py: constants and queue builders."""
    if "names" not in _CONSTS_CACHE:
        tree = _parse(REPO_ROOT / SHARED_DIR / "consts.py")
        names: set[str] = set()
        if tree is not None:
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    names.update(t.id for t in node.targets if isinstance(t, ast.Name))
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    names.add(node.name)
        _CONSTS_CACHE["names"] = names
    return _CONSTS_CACHE["names"]


def check_brain_registration(report: _Report) -> None:
    """EP002 — §5: every workflow in `_WORKER_SPECS`, on a `_WORKFLOW_QUEUE` constant."""
    path = REPO_ROOT / WORKFLOW_DOMAINS_DIR / "main_worker_init.py"
    tree = _parse(path)
    if tree is None:
        report.error(path, 1, "EP002", "workflow_domains/main_worker_init.py missing or unparsable")
        return
    specs = next(
        (n.value for n in tree.body if isinstance(n, (ast.Assign, ast.AnnAssign))
         and any(isinstance(t, ast.Name) and t.id == "_WORKER_SPECS" for t in
                 ([n.target] if isinstance(n, ast.AnnAssign) else n.targets))),
        None,
    )
    if not isinstance(specs, ast.List):
        report.error(path, 1, "EP002", "no `_WORKER_SPECS` list in main_worker_init.py (§5)")
        return
    registered: dict[str, int] = {}
    queues: list[tuple[str, int]] = []
    for entry in specs.elts:
        if isinstance(entry, ast.Tuple) and len(entry.elts) == 2:
            queue, classes = entry.elts
            if isinstance(queue, ast.Name):
                queues.append((queue.id, entry.lineno))
            else:
                report.error(path, entry.lineno, "EP002", "queue in _WORKER_SPECS must be a `*_WORKFLOW_QUEUE` constant from shared/consts.py (§5)")
            if isinstance(classes, ast.List):
                for cls in classes.elts:
                    if isinstance(cls, ast.Name):
                        registered[cls.id] = registered.get(cls.id, 0) + 1
    consts = _consts_names()
    for queue, line in queues:
        if not queue.endswith("_WORKFLOW_QUEUE"):
            report.error(path, line, "EP002", f"`{queue}` in _WORKER_SPECS is not a `*_WORKFLOW_QUEUE` — workflow queues are per workflow (§5)")
        if queue not in consts:
            report.error(path, line, "EP002", f"`{queue}` is not defined in shared/consts.py (§5)")
    seen_queues = [q for q, _ in queues]
    for queue in set(seen_queues):
        if seen_queues.count(queue) > 1:
            report.error(path, 1, "EP002", f"`{queue}` appears twice in _WORKER_SPECS — a Worker polls exactly one queue (§5)")
    imported = _imported_names(tree)
    for module_path, domain, cls in _workflow_classes():
        if cls.name not in registered:
            report.error(
                module_path, cls.lineno, "EP002",
                f"`{cls.name}` is not registered in main_worker_init.py `_WORKER_SPECS` — "
                "one entry per workflow on its own queue (§5)",
            )
        elif registered[cls.name] > 1:
            report.error(path, 1, "EP002", f"`{cls.name}` registered more than once (§5)")
        expected = f"{WORKFLOW_DOMAINS_DIR}.{domain}.{module_path.stem}.{cls.name}"
        if cls.name in registered and imported.get(cls.name) != expected:
            report.error(path, 1, "EP002", f"`{cls.name}` must be imported from `{expected}` (§1)")


def _queue_expression_ok(expr: ast.expr, module_assignments: dict[str, ast.expr], imported: dict[str, str]) -> bool:
    """A limb's queue is an `*_ACTIVITY_QUEUE` constant or a `*_activity_queue(...)`
    builder from shared/consts.py, possibly through one module-level name."""
    if isinstance(expr, ast.Name):
        if expr.id.endswith("_ACTIVITY_QUEUE") and expr.id in _consts_names():
            return True
        if expr.id in module_assignments:
            return _queue_expression_ok(module_assignments.pop(expr.id), module_assignments, imported)
        return False
    if isinstance(expr, ast.Call):
        name = _call_name(expr)
        return name.endswith("_activity_queue") and imported.get(name, "").startswith("shared.consts.")
    return False


def check_limb_registration(report: _Report) -> None:
    """EP003 + AC006/IF005 — §1/§5: interface ↔ impl ↔ worker registration per limb."""
    for domain in _activity_domains():
        impl_path = REPO_ROOT / ACTIVITIES_DIR / domain / "activities.py"
        iface_path = REPO_ROOT / SHARED_DIR / "interfaces" / f"{domain}.py"
        worker_path = REPO_ROOT / ACTIVITIES_DIR / domain / "worker_init.py"
        impl = _activity_defn_names(impl_path)
        if not iface_path.exists():
            report.error(iface_path, 1, "IF005", f"no shared/interfaces/{domain}.py — every limb declares its signatures there (§1, §2)")
            iface: set[str] = set()
        else:
            iface = _activity_defn_names(iface_path)
        for name in sorted(impl - iface):
            report.error(impl_path, 1, "AC006", f"activity `{name}` has no stub in shared/interfaces/{domain}.py (§1)")
        for name in sorted(iface - impl):
            report.error(iface_path, 1, "IF005", f"stub `{name}` has no implementation in activities/{domain}/activities.py (§1)")

        if not worker_path.exists():
            report.error(worker_path, 1, "EP003", f"no activities/{domain}/worker_init.py — the limb entrypoint (§1)")
            continue
        tree = _parse(worker_path)
        if tree is None:
            continue
        imported = _imported_names(tree)
        module_assignments = {
            t.id: n.value for n in tree.body if isinstance(n, ast.Assign)
            for t in n.targets if isinstance(t, ast.Name)
        }
        registered: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_name(node) == "Worker":
                kwargs = _kwargs(node)
                acts = kwargs.get("activities")
                if isinstance(acts, ast.List):
                    registered.update(e.id for e in acts.elts if isinstance(e, ast.Name))
                elif isinstance(acts, ast.Name) and isinstance(module_assignments.get(acts.id), (ast.List, ast.Tuple)):
                    registered.update(e.id for e in module_assignments[acts.id].elts if isinstance(e, ast.Name))
                else:
                    registered.update(impl)  # built dynamically — tests/test_worker_registration.py owns it
                queue = kwargs.get("task_queue")
                if queue is None or not _queue_expression_ok(queue, dict(module_assignments), imported):
                    report.error(
                        worker_path, node.lineno, "EP003",
                        "the limb Worker polls an `*_ACTIVITY_QUEUE` constant or an `*_activity_queue(...)` "
                        "builder from shared/consts.py (§2)",
                    )
                if "workflows" in kwargs:
                    report.error(worker_path, node.lineno, "EP003", "a limb never registers workflows — the brain does (§1)")
                if "graceful_shutdown_timeout" not in kwargs:
                    report.warning(worker_path, node.lineno, "EP003", "set `graceful_shutdown_timeout` below the pod's terminationGracePeriodSeconds so rollouts finish in-flight activities")
        for name in sorted(impl - registered):
            report.error(worker_path, 1, "EP003", f"activity `{name}` is implemented but not in the Worker's `activities=[...]` (§1)")


def check_api_mounts(report: _Report) -> None:
    """EP004 — §1/§2: api.py mounts every domain router and temporary page."""
    api_path = REPO_ROOT / WORKFLOW_DOMAINS_DIR / "api.py"
    tree = _parse(api_path)
    if tree is None:
        report.error(api_path, 1, "EP004", "workflow_domains/api.py missing or unparsable")
        return
    imported = _imported_names(tree)
    mounted_origins = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "include_router" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Name):
                mounted_origins.add(imported.get(arg.id, ""))
            elif isinstance(arg, ast.Attribute):
                mounted_origins.add(f"{imported.get(_dotted(arg.value), _dotted(arg.value))}.{arg.attr}")
    for domain_dir in _domain_dirs():
        for module in ("router", "ui"):
            if not (domain_dir / f"{module}.py").exists():
                continue
            expected = f"{WORKFLOW_DOMAINS_DIR}.{domain_dir.name}.{module}.router"
            if expected not in mounted_origins:
                report.error(api_path, 1, "EP004", f"`{expected}` is not mounted with `app.include_router` (§1)")
    if f"{WORKFLOW_DOMAINS_DIR}.routers.runs.router" not in mounted_origins:
        report.error(api_path, 1, "EP004", "the domain-agnostic runs router is not mounted (§2)")


def _docs_pages(domain_kebab: str) -> list[Path]:
    folder = REPO_ROOT / "docs" / "site" / domain_kebab
    return sorted(folder.glob("*.html")) if folder.is_dir() else []


def _pages_for_limb(limb: str) -> list[Path]:
    """The docs pages of every domain whose workflow-side code calls this limb's
    stubs (a limb may serve another domain: upgrade_gitops serves cluster-upgrade)."""
    pages: list[Path] = []
    needle = f"shared.interfaces.{limb}"
    for domain in _domain_dirs():
        if any(needle in (_read(f) or "") for f in domain.glob("*.py")):
            pages += _docs_pages(_kebab(domain.name))
    return pages or _docs_pages(_kebab(limb))


def check_delivery(report: _Report) -> None:
    """DL rules — §1/§9: Dockerfile CMDs, CI jobs, docs pages, .env.example."""
    build_yml = REPO_ROOT / ".github" / "workflows" / "build.yml"
    build_text = _read(build_yml) or ""
    brain_docker = REPO_ROOT / WORKFLOW_DOMAINS_DIR / "Dockerfile"
    if brain_docker.exists() and f"{WORKFLOW_DOMAINS_DIR}.main_worker_init" not in (_read(brain_docker) or ""):
        report.error(brain_docker, 1, "DL001", "brain Dockerfile CMD must run `workflow_domains.main_worker_init` (§1)")

    for domain in _activity_domains():
        kebab = _kebab(domain)
        dockerfile = REPO_ROOT / ACTIVITIES_DIR / domain / "Dockerfile"
        if not dockerfile.exists():
            report.error(dockerfile, 1, "DL001", f"activities/{domain}/ has no Dockerfile — each limb is its own image (§1)")
        elif f"{ACTIVITIES_DIR}.{domain}.worker_init" not in (_read(dockerfile) or ""):
            report.error(dockerfile, 1, "DL001", f"limb Dockerfile CMD must run `activities.{domain}.worker_init` (§1)")
        if not (REPO_ROOT / ACTIVITIES_DIR / domain / "requirements.txt").exists():
            report.error(REPO_ROOT / ACTIVITIES_DIR / domain / "requirements.txt", 1, "DL001", f"activities/{domain}/ has no requirements.txt — the limb owns its dependency set (§2)")
        if not build_text:
            continue
        for message in limb_build_problems(domain, build_text):
            report.error(build_yml, 1, "DL002", message)

    nav = REPO_ROOT / "docs" / "site" / "assets" / "nav.js"
    nav_text = _read(nav) or ""
    for module_path, domain, cls in _workflow_classes(include_legacy=False):
        _check_workflow_docs(module_path, domain, cls, nav, nav_text, report)

    env_example = REPO_ROOT / ".env.example"
    settings_tree = _parse(REPO_ROOT / SHARED_DIR / "settings.py")
    if env_example.exists() and settings_tree is not None:
        documented = set(re.findall(r"^#?\s*([A-Z][A-Z0-9_]*)=", _read(env_example) or "", re.M))
        for node in settings_tree.body:
            if isinstance(node, ast.ClassDef) and any(_dotted(b) == "BaseSettings" for b in node.bases):
                for item in node.body:
                    if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.target.id != "model_config":
                        key = item.target.id.upper()
                        if key not in documented:
                            report.warning(env_example, 1, "DL004", f"`{key}` ({node.name}) is not documented in .env.example (§8)")

    _check_charts(report)


def limb_build_problems(domain: str, build_text: str) -> list[str]:
    """DL002 for one limb, from build.yml's text: a job on the reusable workflow, so a
    main build gets a vX.Y.Z tag and its redbull-platform bump (§9). An image-only job
    (`docker/build-push-action` straight on the Dockerfile, tagged by commit sha) is
    refused: the operator's rule since 2026-11-02 is version tags, never commit shas."""
    kebab = _kebab(domain)
    reusable = re.search(rf"dockerfile:\s*{ACTIVITIES_DIR}/{domain}/Dockerfile\b", build_text)
    image_only = re.search(rf"\bfile:\s*{ACTIVITIES_DIR}/{domain}/Dockerfile\b", build_text)
    if not (reusable or image_only):
        return [f"no build job for activities/{domain}/Dockerfile — add `build-{kebab}-worker` (§1, §9)"]
    problems = []
    if image_only:
        problems.append(
            f"activities/{domain}/Dockerfile is built image-only (a commit-sha tag) — use the reusable "
            f"ghcr-build-push.yml so main builds are tagged vX.Y.Z; vendor its chart in redbull-platform first (§9)"
        )
    if not re.search(rf"(image-name:\s*{kebab}-worker\b|/{kebab}-worker:)", build_text):
        problems.append(f"the {domain} limb image is named `{kebab}-worker` (§2)")
    if reusable and f"helm-values-path: gitops/charts/{kebab}-worker/values.yaml" not in build_text:
        problems.append(f"the reusable build bumps the tag into redbull-platform `gitops/charts/{kebab}-worker/values.yaml` (§1)")
    return problems


def _check_workflow_docs(module_path: Path, domain: str, cls: ast.ClassDef, nav: Path, nav_text: str, report: _Report) -> None:
    """DL003 — docs follow the code. A workflow a caller STARTS (its class is used in
    the domain's router) has its own page and a CATALOGUE entry; a workflow only
    another workflow starts (a child, an internal step) is described on its
    domain's pages. Warnings: a page is written after the code it describes."""
    domain_kebab = _kebab(domain)
    workflow_kebab = _kebab(module_path.stem)
    router_text = _read(module_path.parent / "router.py") or ""
    routed = re.search(rf"\b{re.escape(cls.name)}\b", router_text) is not None
    pages = _docs_pages(domain_kebab)
    if routed:
        page = REPO_ROOT / "docs" / "site" / domain_kebab / f"{workflow_kebab}.html"
        if not page.exists():
            report.warning(module_path, cls.lineno, "DL003", f"no docs page docs/site/{domain_kebab}/{workflow_kebab}.html for a workflow the API starts (docs/README.md)")
        entry = re.search(rf'name:\s*"{re.escape(workflow_kebab)}"\s*,\s*href:\s*("([^"]*)"|null)', nav_text)
        if entry is None:
            report.warning(nav, 1, "DL003", f"`{workflow_kebab}` is missing from CATALOGUE in docs/site/assets/nav.js")
        elif entry.group(1) == "null" and page.exists():
            report.warning(nav, 1, "DL003", f"`{workflow_kebab}` is listed as planned (href: null) but its page exists")
    elif not any(workflow_kebab in (_read(p) or "") for p in pages):
        report.warning(
            module_path, cls.lineno, "DL003",
            f"`{workflow_kebab}` is not described on any page under docs/site/{domain_kebab}/ — "
            "a child or internal workflow is documented on its parent's page",
        )


def _check_charts(report: _Report) -> None:
    """DL005 — a chart per limb: in this repo's deploy/charts/ (per-MCE limbs) or the
    sibling redbull-platform checkout's gitops/charts/ (hub limbs). Warnings only:
    the sibling path is an assumption, and that repo has its own review."""
    platform = REPO_ROOT.parent / "redbull-platform" / "gitops"
    local = REPO_ROOT / "deploy" / "charts"
    settings_tree = _parse(REPO_ROOT / SHARED_DIR / "settings.py")
    for domain in _activity_domains():
        kebab = _kebab(domain)
        marker = REPO_ROOT / ACTIVITIES_DIR / domain / "worker_init.py"
        charts = [c for c in (platform / "charts" / f"{kebab}-worker", local / f"{kebab}-worker") if c.is_dir()]
        if not charts:
            if platform.is_dir():
                report.warning(marker, 1, "DL005", f"no chart `{kebab}-worker` in redbull-platform/gitops/charts/ or deploy/charts/ — each limb is its own release (§1, §9)")
            continue
        for chart in charts:
            if (chart / "templates" / "namespace.yaml").exists():
                report.warning(marker, 1, "DL005", f"{_rel(chart)} templates a Namespace — a workflow chart never does (§9)")
        rendered = "".join(
            _read(p) or "" for chart in charts for p in list((chart / "templates").glob("*")) + [chart / "values.yaml"] if p.is_file()
        )
        if settings_tree is None:
            continue
        class_name = "".join(part.capitalize() for part in domain.split("_")) + "ActivitySettings"
        for node in settings_tree.body:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for item in node.body:
                    # Only REQUIRED keys (no default): a field with a default runs
                    # without the chart rendering it (IDRAC_USERNAME is "root").
                    if (isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
                            and item.target.id != "model_config" and item.value is None):
                        key = item.target.id.upper()
                        if key not in GLOBAL_CONFIGMAP_KEYS and key not in rendered:
                            report.warning(marker, 1, "DL005", f"`{key}` is a REQUIRED {class_name} field but no `{kebab}-worker` chart renders it — the worker crash-loops; the chart ships the key BEFORE the image that requires it (§8)")


def run_full(root: Path | None = None) -> list[Violation]:
    """Every rule, intra- and cross-file. `root` defaults to this checkout."""
    global REPO_ROOT
    if root is not None:
        REPO_ROOT = root.resolve()
        _EXCEPTION_NAMES_CACHE.clear()
        _CONSTS_CACHE.clear()
    report = _Report()
    for top in (SHARED_DIR, WORKFLOW_DOMAINS_DIR, ACTIVITIES_DIR):
        for path in sorted((REPO_ROOT / top).rglob("*.py")):
            if "__pycache__" not in path.parts:
                check_file(path, report)
    check_brain_registration(report)
    check_limb_registration(report)
    check_api_mounts(report)
    check_delivery(report)
    return report.violations


def run_file(path: Path) -> list[Violation]:
    report = _Report()
    check_file(path, report)
    return report.violations


def run_source(rel_path: str, source: str) -> list[Violation]:
    """Intra-file rules for `source` AS IF it lived at repo-relative `rel_path`.

    For the checker's own tests: nothing is written to disk, and the file kind
    is inferred from the path exactly as it is for a real file."""
    report = _Report()
    check_source(REPO_ROOT / rel_path, Path(rel_path).parts, source, report)
    return report.violations


# --- docs reminder (hook) ---------------------------------------------------------------


def docs_reminder(path: Path) -> str | None:
    """Which docs describe the code at `path` — returned on every hook run for
    workflow, activity and contract code, so docs are updated in the SAME change."""
    parts = _relparts(path)
    if len(parts) < 2 or path.suffix != ".py":
        return None
    targets: list[str] = []
    if parts[0] == WORKFLOW_DOMAINS_DIR and len(parts) == 3 and parts[1] not in ("routers", "__pycache__"):
        domain_kebab = _kebab(parts[1])
        own = REPO_ROOT / "docs" / "site" / domain_kebab / f"{_kebab(Path(parts[2]).stem)}.html"
        pages = [own] if own.exists() else _docs_pages(domain_kebab)
        targets += [_rel(p) for p in pages] or [f"docs/site/{domain_kebab}/<workflow>.html (none yet)"]
        if Path(parts[2]).stem == "router":
            targets.append("README.md (API table)")
    elif parts[0] == WORKFLOW_DOMAINS_DIR:
        targets.append("README.md (API table)")
    elif parts[0] == ACTIVITIES_DIR and len(parts) >= 3:
        pages = _pages_for_limb(parts[1])
        targets += [_rel(p) for p in pages] or ["the docs page of every workflow that calls these activities (docs/site/<domain>/)"]
        targets.append(f"the limb chart's README if it changed RBAC, config or a call ({_kebab(parts[1])}-worker)")
    elif parts[0] == SHARED_DIR:
        stem = Path(parts[-1]).stem
        if parts[1] in ("models", "interfaces"):
            pages = _pages_for_limb(stem) if parts[1] == "interfaces" else _docs_pages(_kebab(stem))
            targets += [_rel(p) for p in pages] or ["the docs page of every workflow using this contract (docs/site/<domain>/)"]
        elif stem == "settings":
            targets += [".env.example", "README.md (configuration table)", "the limb chart's values.yaml + ConfigMap"]
        else:
            targets.append("the docs page of every workflow this changes (docs/site/<domain>/)")
    else:
        return None
    return (
        "Docs follow the code (redbull-workflows skill, step 3): if this edit changes anything a caller, "
        "operator or reviewer reads — a route, request field or status code, a phase, a failure type, a "
        "retry or wait, a config key, an external call, a deploy-order constraint — update in the SAME "
        "change: " + "; ".join(targets) + "; docs/site/assets/nav.js if a workflow was added, renamed or "
        "removed; CLAUDE.md if a rule or decision changed. A pure refactor needs none."
    )


# --- entrypoints ---------------------------------------------------------------------


def _print(violations: list[Violation], stream) -> None:
    for v in sorted(violations, key=lambda v: (v.severity != "error", v.path, v.line)):
        print(v.format(), file=stream)


def _main_hook() -> int:
    """Claude Code PostToolUse hook (Edit|Write). Exit 2 feeds stderr back to Claude;
    exit 0 with JSON adds warnings and the docs reminder as context."""
    try:
        payload = json.load(sys.stdin)
    except (ValueError, OSError):
        return 0
    file_path = (payload.get("tool_input") or {}).get("file_path")
    if not file_path:
        return 0
    path = Path(file_path)
    if not path.is_absolute():
        path = Path(payload.get("cwd") or REPO_ROOT) / path
    if not path.exists() or not _relparts(path):
        return 0
    violations = run_file(path)
    errors = [v for v in violations if v.severity == "error"]
    warnings = [v for v in violations if v.severity == "warning"]
    reminder = docs_reminder(path)
    if errors:
        print(
            f"Convention check FAILED for {_rel(path)} — fix these before moving on "
            "(rules: .claude/rules/, rationale: CLAUDE.md; whole-repo run: "
            "`python3 scripts/check_conventions.py`):",
            file=sys.stderr,
        )
        _print(errors + warnings, sys.stderr)
        if reminder:
            print(reminder, file=sys.stderr)
        return 2
    context = []
    if warnings:
        context.append("Convention warnings for " + _rel(path) + ":\n" + "\n".join(v.format() for v in warnings))
    if reminder:
        context.append(reminder)
    if context:
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": "\n\n".join(context)}}))
    return 0


def main(argv: list[str]) -> int:
    if "--hook" in argv:
        return _main_hook()
    if "--file" in argv:
        index = argv.index("--file")
        if index + 1 >= len(argv):
            print("usage: check_conventions.py --file PATH", file=sys.stderr)
            return 64
        violations = run_file(Path(argv[index + 1]).resolve())
    else:
        violations = run_full()
    _print(violations, sys.stdout)
    errors = [v for v in violations if v.severity == "error"]
    warnings = [v for v in violations if v.severity == "warning"]
    print(f"{len(errors)} error(s), {len(warnings)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
