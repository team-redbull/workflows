"""Every activity a workflow dispatches must be registered on the worker that serves its queue.

This is the gap the workflow tests structurally cannot see: their harness
registers mock activities on both queues, so a workflow reaches activities the
real deployment never registered. Temporal treats an unregistered activity as a
RETRYABLE failure, so the symptom in production is not a crash but a run that
sits RUNNING forever in its first phase.

The check is static — it reads the activity names out of each worker module's
source rather than importing it, because importing a worker module instantiates
settings and opens a Temporal connection.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent

# What names a queue in workflow code -> the worker module that must poll it.
# Either a module-level const, or the helper that builds a per-MCE queue name
# (server-lifecycle activities are routed to the MCE they write to, so their
# queue is computed rather than constant).
_WORKERS = {
    "SEGMENT_LIFECYCLE_ACTIVITY_QUEUE": "activities/segment_lifecycle/worker_init.py",
    "SERVER_LIFECYCLE_ACTIVITY_QUEUE": "activities/server_lifecycle/worker_init.py",
    "server_lifecycle_activity_queue": "activities/server_lifecycle/worker_init.py",
    "SERVER_PROVISIONING_ACTIVITY_QUEUE": "activities/server_provisioning/worker_init.py",
}

# the workflow modules that dispatch activities, and the interface module each
# activity name must come from
_WORKFLOW_MODULES = [
    "workflow_domains/server_lifecycle/install_server.py",
    "workflow_domains/segment_lifecycle/allocate_segment.py",
    "workflow_domains/segment_lifecycle/initialize_segment.py",
    "workflow_domains/server_provisioning/provision_dell_server.py",
    "workflow_domains/segment_lifecycle/release_segment.py",
]


def _tree(relative: str) -> ast.Module:
    return ast.parse((REPO / relative).read_text())


def _registered_activities(worker_module: str) -> set[str]:
    """Names passed to Worker(..., activities=[...]) in a worker module."""
    for node in ast.walk(_tree(worker_module)):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Worker"):
            continue
        for keyword in node.keywords:
            if keyword.arg == "activities" and isinstance(keyword.value, ast.List):
                return {e.id for e in keyword.value.elts if isinstance(e, ast.Name)}
    raise AssertionError(f"{worker_module} has no Worker(activities=[...]) call")


def _interface_activities(tree: ast.Module) -> set[str]:
    """Activity names the workflow imported from shared/interfaces/*."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "shared.interfaces"
        ):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _helper_arguments(tree: ast.Module, helper: str, activities: set[str]) -> set[str]:
    """Activities passed as the first argument to a named helper method.

    The three resource creates go through one `self._create(activity, request)`
    helper, so the execute_activity call inside it names a PARAMETER rather than
    an activity. Following that one hop is what keeps this check precise instead
    of falling back to "registered on some worker somewhere".
    """
    passed: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if getattr(node.func, "attr", None) != helper:
            continue
        if isinstance(node.args[0], ast.Name) and node.args[0].id in activities:
            passed.add(node.args[0].id)
    return passed


def _enclosing_function(tree: ast.Module, param: str):
    """The function declaring `param`, if any."""
    return next(
        (
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
            and any(a.arg == param for a in n.args.args)
        ),
        None,
    )


def _argument_passed_for(tree: ast.Module, function: ast.AST, param: str):
    """What callers of `function` pass for `param`, as an AST node."""
    index = [a.arg for a in function.args.args].index(param)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "attr", None) != function.name:
            continue
        # `self`/`cls` is not in the call's positional args.
        offset = 1 if function.args.args and function.args.args[0].arg in {"self", "cls"} else 0
        position = index - offset
        if 0 <= position < len(node.args):
            return node.args[position]
    return None


def _resolve_queue(tree: ast.Module, name: str) -> str:
    """A task_queue name reduced to something _WORKERS can be keyed on.

    Three shapes reach this, and only the first is literal:

      * a module-level const — resolves to itself;
      * a local holding a computed queue
        (`activity_queue = server_lifecycle_activity_queue(mce)`) — resolves to
        the helper that built it, since that helper is what pairs with the
        worker polling the same name;
      * a PARAMETER of a helper method (`_create(..., task_queue)`) — resolved
        by following what callers pass, the same hop activity names take.
    """
    seen: set[str] = set()
    while name not in _WORKERS and name not in seen:
        seen.add(name)
        assigned = next(
            (
                node.value
                for node in ast.walk(tree)
                if isinstance(node, ast.Assign)
                and any(t.id == name for t in node.targets if isinstance(t, ast.Name))
            ),
            None,
        )
        if isinstance(assigned, ast.Call) and getattr(assigned.func, "id", None):
            name = assigned.func.id
            continue
        function = _enclosing_function(tree, name)
        if function is None:
            break
        passed = _argument_passed_for(tree, function, name)
        if not isinstance(passed, ast.Name):
            break
        name = passed.id
    return name


def _dispatched(workflow_module: str) -> list[tuple[str, str]]:
    """(activity name, queue const) for each activity a workflow dispatches."""
    tree = _tree(workflow_module)
    activities = _interface_activities(tree)
    dispatched: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "attr", None) != "execute_activity":
            continue
        if not node.args or not isinstance(node.args[0], ast.Name):
            continue
        queue = next(
            (
                _resolve_queue(tree, k.value.id)
                for k in node.keywords
                if k.arg == "task_queue" and isinstance(k.value, ast.Name)
            ),
            None,
        )
        name = node.args[0].id
        if name in activities:
            dispatched.append((name, queue))
            continue
        # Not an activity: a parameter of the helper this call sits in. Resolve
        # it to what callers actually pass.
        helper = next(
            (
                n.name
                for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
                and any(a.arg == name for a in n.args.args)
            ),
            None,
        )
        assert helper, f"{workflow_module}: cannot resolve dispatched name {name!r}"
        resolved = _helper_arguments(tree, helper, activities)
        assert resolved, f"{workflow_module}: {helper}() is never passed an activity"
        dispatched.extend((a, queue) for a in sorted(resolved))
    return dispatched


@pytest.mark.parametrize("workflow_module", _WORKFLOW_MODULES)
def test_every_dispatched_activity_is_registered_on_its_queues_worker(workflow_module):
    for activity_name, queue in _dispatched(workflow_module):
        assert queue in _WORKERS, (
            f"{workflow_module} dispatches {activity_name} to {queue}, which no "
            "worker module in this test is known to serve"
        )
        registered = _registered_activities(_WORKERS[queue])
        assert activity_name in registered, (
            f"{workflow_module} dispatches {activity_name!r} to {queue}, but "
            f"{_WORKERS[queue]} does not register it. Temporal retries an "
            "unregistered activity forever, so the run would sit RUNNING rather "
            "than fail"
        )


def test_the_install_server_workflow_reaches_both_queues():
    """Guards the premise of the test above: install-server really does span two limbs."""
    queues = {queue for _, queue in _dispatched(_WORKFLOW_MODULES[0])}
    assert queues == {
        # The Segments Manager credential lives on the segment-lifecycle limb,
        # on the hub...
        "SEGMENT_LIFECYCLE_ACTIVITY_QUEUE",
        # ...while everything that writes to a cluster is routed to the worker
        # inside the target MCE, by a queue name computed from the request.
        "server_lifecycle_activity_queue",
    }


def _workflow_classes(relative: str) -> set[str]:
    """Classes decorated @workflow.defn in a module."""
    return {
        node.name
        for node in ast.walk(_tree(relative))
        if isinstance(node, ast.ClassDef)
        and any(
            getattr(decorator, "attr", None) == "defn"
            and getattr(getattr(decorator, "value", None), "id", None) == "workflow"
            for decorator in node.decorator_list
        )
    }


def test_every_workflow_is_registered_in_the_brain():
    """The brain-side twin of the test above: a workflow class missing from
    main_worker_init's _WORKER_SPECS has no worker polling its queue, so a run
    started through the API sits RUNNING with nothing to execute it. Parsing
    the module also fails loudly if it does not parse at all — no other test
    imports it, because importing it needs a Temporal environment."""
    registry = next(
        node.value
        for node in ast.walk(_tree("workflow_domains/main_worker_init.py"))
        if isinstance(node, ast.AnnAssign)
        and getattr(node.target, "id", None) == "_WORKER_SPECS"
    )
    registered = {
        element.id
        for spec in registry.elts
        for element in spec.elts[1].elts
        if isinstance(element, ast.Name)
    }
    for module in sorted(str(path.relative_to(REPO)) for path in (REPO / "workflow_domains").rglob("*.py")):
        for name in _workflow_classes(module):
            assert name in registered, f"{module}: {name} is not in _WORKER_SPECS"
