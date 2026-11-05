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
    "upgrade_agent_activity_queue": "activities/upgrade_agent/worker_init.py",
    "UPGRADE_GITOPS_ACTIVITY_QUEUE": "activities/upgrade_gitops/worker_init.py",
    "VM_LIFECYCLE_ACTIVITY_QUEUE": "activities/vm_lifecycle/worker_init.py",
}

# the workflow modules that dispatch activities, and the interface module each
# activity name must come from
_WORKFLOW_MODULES = [
    "workflow_domains/server_lifecycle/add_server_to_mce.py",
    "workflow_domains/server_lifecycle/remove_server_from_mce.py",
    "workflow_domains/server_lifecycle/uninstall_server_from_hc.py",
    "workflow_domains/server_lifecycle/publish_infraenv_to_pxe_server.py",
    "workflow_domains/segment_lifecycle/allocate_segment.py",
    "workflow_domains/segment_lifecycle/initialize_segment.py",
    "workflow_domains/server_provisioning/provision_dell_server.py",
    "workflow_domains/segment_lifecycle/release_segment.py",
    "workflow_domains/vm_lifecycle/create_vm.py",
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


def test_the_add_server_to_mce_workflow_reaches_both_queues():
    """Guards the premise of the test above: add-server-to-mce really does span two limbs."""
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


# cluster-upgrade dispatches through workflow_domains/cluster_upgrade/dispatch.py:
# each helper is bound to ONE limb's queue and takes the activity as an
# argument, `on_mce(mce, activity, request)`. Helper -> (the queue builder its
# body must use, the worker module polling that queue).
_CLUSTER_UPGRADE_DISPATCH = "workflow_domains/cluster_upgrade/dispatch.py"
_CLUSTER_UPGRADE_ROUTES = {
    "on_mce": ("cluster_upgrade_activity_queue", "activities/cluster_upgrade/worker_init.py"),
    "start_watch": ("cluster_upgrade_activity_queue", "activities/cluster_upgrade/worker_init.py"),
}


def _cluster_upgrade_dispatches(helper: str) -> set[str]:
    """Every activity a cluster-upgrade workflow module hands to a dispatch helper."""
    names: set[str] = set()
    for path in sorted((REPO / "workflow_domains" / "cluster_upgrade").glob("*.py")):
        tree = ast.parse(path.read_text())
        activities = _interface_activities(tree)
        for node in ast.walk(tree):
            called = getattr(node, "func", None)
            if not isinstance(node, ast.Call) or getattr(called, "id", getattr(called, "attr", None)) != helper:
                continue
            if len(node.args) >= 2 and isinstance(node.args[1], ast.Name) and node.args[1].id in activities:
                names.add(node.args[1].id)
    return names


@pytest.mark.parametrize("helper", sorted(_CLUSTER_UPGRADE_ROUTES))
def test_every_cluster_upgrade_dispatch_lands_on_a_worker_registering_it(helper):
    queue_builder, worker_module = _CLUSTER_UPGRADE_ROUTES[helper]
    # The premise: the helper really routes to that limb's queue.
    function = next(
        node for node in ast.walk(_tree(_CLUSTER_UPGRADE_DISPATCH))
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == helper
    )
    queues = {
        keyword.value.func.id
        for call in ast.walk(function) if isinstance(call, ast.Call)
        for keyword in call.keywords
        if keyword.arg == "task_queue" and isinstance(keyword.value, ast.Call)
    }
    assert queues == {queue_builder}, f"dispatch.{helper} routes to {queues}, not {queue_builder}"
    dispatched = _cluster_upgrade_dispatches(helper)
    assert dispatched, f"no cluster-upgrade workflow calls {helper}: the scan is broken"
    missing = dispatched - _registered_activities(worker_module)
    assert not missing, (
        f"cluster-upgrade sends {sorted(missing)} through {helper} to {queue_builder}(<mce>), but "
        f"{worker_module} does not register them: the run would sit RUNNING on them"
    )


def test_the_cluster_upgrade_mce_worker_registers_its_whole_interface():
    """Nothing in shared/interfaces/cluster_upgrade.py is left unpolled, used today or not."""
    interface = {
        node.name for node in ast.walk(_tree("shared/interfaces/cluster_upgrade.py"))
        if isinstance(node, ast.AsyncFunctionDef)
    }
    assert _registered_activities("activities/cluster_upgrade/worker_init.py") == interface


def test_uninstall_server_from_hc_dispatches_everything_to_the_mces_own_worker():
    """No Segments Manager read of its own: the site lookup is remove-server-from-mce's, in the child."""
    dispatched = _dispatched("workflow_domains/server_lifecycle/uninstall_server_from_hc.py")
    assert {queue for _, queue in dispatched} == {"server_lifecycle_activity_queue"}
    assert {name for name, _ in dispatched} >= {
        "lookup_server_membership", "inspect_bound_host", "reserve_server",
        "ensure_machine_deleting", "get_machine_state", "get_agent_binding",
        "get_agent_machine_state", "check_hosted_node", "get_baremetal_host",
        "find_hosted_nodes", "drain_hosted_node", "delete_hosted_node", "release_agent",
        "release_machine_hook", "clean_up_agent_machines",
    }


def test_the_agent_call_lands_on_the_upgrade_agent_worker():
    """cluster-upgrade starts the agent with start_activity (cancellable), which _dispatched does not read."""
    tree = _tree("workflow_domains/cluster_upgrade/dispatch.py")
    started = [
        (node.args[0].id, next(k.value.func.id for k in node.keywords if k.arg == "task_queue"))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "start_activity"
        and node.args and getattr(node.args[0], "id", None) == "invoke_upgrade_agent"
    ]
    assert started == [("invoke_upgrade_agent", "upgrade_agent_activity_queue")]
    assert "invoke_upgrade_agent" in _registered_activities(_WORKERS["upgrade_agent_activity_queue"])


def _sent_to_the_hub(relative: str) -> set[str]:
    """Activities a cluster-upgrade module hands to dispatch.on_hub, its one route to the hub limb."""
    return {
        node.args[0].id
        for node in ast.walk(_tree(relative))
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "on_hub"
        and node.args and isinstance(node.args[0], ast.Name)
    }


def test_every_activity_sent_to_the_hub_is_registered_on_the_hub_worker():
    """cluster-upgrade reaches UPGRADE_GITOPS_ACTIVITY_QUEUE only through on_hub, which _dispatched cannot follow."""
    dispatch = _tree("workflow_domains/cluster_upgrade/dispatch.py")
    on_hub = next(n for n in ast.walk(dispatch) if isinstance(n, ast.AsyncFunctionDef) and n.name == "on_hub")
    queues = {
        keyword.value.id
        for node in ast.walk(on_hub) if isinstance(node, ast.Call)
        for keyword in node.keywords if keyword.arg == "task_queue"
    }
    assert queues == {"UPGRADE_GITOPS_ACTIVITY_QUEUE"}

    modules = sorted(str(p.relative_to(REPO)) for p in (REPO / "workflow_domains/cluster_upgrade").glob("*.py"))
    sent = set().union(*(_sent_to_the_hub(module) for module in modules))
    registered = _registered_activities(_WORKERS["UPGRADE_GITOPS_ACTIVITY_QUEUE"])
    assert sent, "no cluster-upgrade module sends anything to the hub"
    assert sent <= registered, sorted(sent - registered)
    # And the worker registers the hub's whole interface, nothing more.
    import shared.interfaces.upgrade_gitops as hub_interface

    interface = {
        name for name, value in vars(hub_interface).items() if hasattr(value, "__temporal_activity_definition")
    }
    assert registered == interface


def test_runs_started_under_the_old_names_still_have_a_worker():
    """The four server-lifecycle workflows were renamed on 2026-11-01. A run
    started before replays ONLY as its old workflow type on its old queue, so
    the brain must keep exactly those pairs until no such run is open —
    registered anywhere else, the run would sit RUNNING with nothing to replay
    it."""
    import shared.consts as consts

    registry = next(
        node.value
        for node in ast.walk(_tree("workflow_domains/main_worker_init.py"))
        if isinstance(node, ast.AnnAssign)
        and getattr(node.target, "id", None) == "_WORKER_SPECS"
    )
    pairs = {
        (spec.elts[0].id, element.id)
        for spec in registry.elts
        for element in spec.elts[1].elts
        if isinstance(element, ast.Name)
    }
    legacy = {
        "LEGACY_INSTALL_SERVER_WORKFLOW_QUEUE": ("InstallServerWorkflow", "install-server-workflow"),
        "LEGACY_REMOVE_SERVER_WORKFLOW_QUEUE": ("RemoveServerWorkflow", "remove-server-workflow"),
        "LEGACY_UNINSTALL_SERVER_WORKFLOW_QUEUE": ("UninstallServerWorkflow", "uninstall-server-workflow"),
        "LEGACY_PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE": (
            "PublishInfraEnvBootWorkflow", "publish-infraenv-boot-workflow",
        ),
    }
    for const, (workflow_type, queue) in legacy.items():
        assert (const, workflow_type) in pairs, f"{workflow_type} is not polled on {const}"
        # The names every in-flight run's history points at: never to be "tidied".
        assert getattr(consts, const) == queue
