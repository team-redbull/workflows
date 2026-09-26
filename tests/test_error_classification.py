"""The invariants that keep a permanent failure from retrying forever.

install-server runs with UNBOUNDED activity retries, so a failure Temporal does
not recognise as permanent does not fail the run — it retries every minute with
the run sitting RUNNING and the error buried on the activity. Two things have to
hold for that not to happen, and neither is visible in a behavioural test:

  * every type named in `non_retryable_error_types` must be a type something can
    actually raise — Temporal matches the list against the error's type NAME, so
    a misspelled entry silently means "retryable";
  * nothing raised from WORKFLOW code may be in that list, because it is inert
    there (Temporal never retries a workflow failure) and its presence suggests
    a protection that does not exist.

The checks below make both structural, so the compiler and this file — rather
than a reviewer's memory — are what enforce them.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from shared import exceptions
from shared.bmc_address import (
    BMC_VENDORS,
    build_bmc_address,
    is_k8s_resource_name,
    k8s_resource_name,
)
from shared.exceptions import InvalidServerNameError, OrchestratorError
from shared.models.server_lifecycle import BmcEndpoint
from workflow_domains.server_lifecycle.install_server import (
    _REJECTION_SUMMARY,
    _REJECTION_TYPE,
    _RETRY_POLICY,
)

_INSTALL_SERVER = (
    pathlib.Path(__file__).resolve().parent.parent
    / "workflow_domains/server_lifecycle/install_server.py"
)


def _workflow_raised_errors() -> set[str]:
    """Error classes whose docstring marks them as raised from workflow code."""
    return {
        name
        for name, obj in vars(exceptions).items()
        if isinstance(obj, type)
        and issubclass(obj, OrchestratorError)
        and (obj.__doc__ or "").startswith("WORKFLOW-RAISED")
    }


class TestTheNonRetryableList:
    def test_every_entry_names_a_real_error_class(self):
        for name in _RETRY_POLICY.non_retryable_error_types or []:
            assert isinstance(getattr(exceptions, name, None), type), (
                f"{name!r} is in non_retryable_error_types but is not a class in "
                "shared/exceptions.py — Temporal would treat that failure as "
                "retryable and the run would sit RUNNING forever"
            )

    def test_no_workflow_raised_failure_is_listed(self):
        # Listing one is inert, and an inert entry reads as a protection.
        workflow_raised = _workflow_raised_errors()
        assert workflow_raised, "the WORKFLOW-RAISED marker matched nothing"
        assert not workflow_raised & set(_RETRY_POLICY.non_retryable_error_types or [])

    def test_the_list_is_derived_from_classes_not_written_out(self):
        # A string literal here is the whole failure mode: it type-checks, reads
        # correctly and does nothing.
        tree = ast.parse(_INSTALL_SERVER.read_text())
        listed = next(
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.keyword) and node.arg == "non_retryable_error_types"
        )
        assert not isinstance(listed, ast.List), (
            "non_retryable_error_types is a literal list; build it from the "
            "exception classes so a wrong name fails at import"
        )


class TestWorkflowRaisedTypes:
    def test_every_application_error_takes_its_type_from_a_class(self):
        """No `type="SomeError"` literals: the name lives with the class.

        A literal drifts silently — the class can be renamed or deleted and the
        run keeps failing under the old name, which is what the status endpoint
        and any alerting key on.
        """
        tree = ast.parse(_INSTALL_SERVER.read_text())
        literals = [
            ast.unparse(keyword.value)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type" and isinstance(keyword.value, ast.Constant)
        ]
        assert literals == []

    # One `type=` is resolved through _REJECTION_TYPE rather than naming a
    # class directly — the failure for a draw where nothing was installable
    # takes the reason's own type. TestRejectionReasons covers that table.
    _RESOLVED_VIA_TABLE = {"error"}

    def test_each_type_used_resolves_to_a_class_in_shared_exceptions(self):
        tree = ast.parse(_INSTALL_SERVER.read_text())
        used = {
            keyword.value.value.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type"
            and isinstance(keyword.value, ast.Attribute)
            and keyword.value.attr == "__name__"
            and isinstance(keyword.value.value, ast.Name)
        }
        assert used, "install_server raises no classified ApplicationError"
        for name in used - self._RESOLVED_VIA_TABLE:
            assert isinstance(getattr(exceptions, name, None), type), name


class TestRejectionReasons:
    """The two tables a rejected candidate is reported through.

    A reason is a bare string in the selection loop, and it is looked up in
    BOTH tables when a draw yields nothing. A reason missing from either is a
    KeyError raised in WORKFLOW code — which does not fail the run, it fails
    the workflow task and retries forever, so the run hangs at exactly the
    moment it was trying to explain itself.
    """

    def test_the_two_tables_describe_the_same_reasons(self):
        assert set(_REJECTION_TYPE) == set(_REJECTION_SUMMARY)

    def test_every_reason_the_loop_records_is_in_both_tables(self):
        tree = ast.parse(_INSTALL_SERVER.read_text())
        recorded = {
            node.args[0].elts[0].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "append"
            and node.args
            and isinstance(node.args[0], ast.Tuple)
            and node.args[0].elts
            and isinstance(node.args[0].elts[0], ast.Constant)
            and isinstance(node.args[0].elts[0].value, str)
        }
        assert recorded, "the selection loop records no rejection reasons"
        assert recorded <= set(_REJECTION_TYPE), sorted(recorded - set(_REJECTION_TYPE))

    def test_no_reason_is_left_described_but_unreachable(self):
        # The other direction: a reason nothing records any more is dead
        # weight that reads as a case the loop still handles.
        tree = ast.parse(_INSTALL_SERVER.read_text())
        recorded = {
            node.args[0].elts[0].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "append"
            and node.args
            and isinstance(node.args[0], ast.Tuple)
            and node.args[0].elts
            and isinstance(node.args[0].elts[0], ast.Constant)
            and isinstance(node.args[0].elts[0].value, str)
        }
        assert set(_REJECTION_TYPE) == recorded

    def test_every_reason_maps_to_a_real_error_class(self):
        for reason, error in _REJECTION_TYPE.items():
            assert issubclass(error, OrchestratorError), reason
            assert getattr(exceptions, error.__name__, None) is error, reason


class TestTheBmcVendorGate:
    """The workflow's vendor gate and the driver map must be one thing.

    Two lists would let a vendor be drivable and still refused, with nothing to
    notice until a run failed on hardware that was supported all along.
    """

    @pytest.mark.parametrize("vendor", sorted(BMC_VENDORS))
    def test_every_vendor_the_workflow_admits_has_a_driver(self, vendor):
        assert build_bmc_address(vendor, BmcEndpoint(host="10.0.0.5"))

    def test_a_vendor_outside_the_set_has_no_driver(self):
        with pytest.raises(KeyError):
            build_bmc_address("SUPERMICRO", BmcEndpoint(host="10.0.0.5"))


class TestTheResourceNamePredicate:
    """The predicate and the converter must agree exactly.

    The workflow uses the predicate to SKIP a candidate; the activity uses the
    converter and lets it raise. If they disagreed, a candidate the workflow
    accepted would fail the run inside an activity — the outcome the predicate
    exists to prevent.
    """

    @pytest.mark.parametrize(
        "name",
        [
            "ocp-hp-gen11-nyc-64c-128gb-HP0001592",
            "ocp-dell-r650-tlv-64c-1024gb-del0000485",
            "a",
            "a.b.c",
            "ocp_underscores_are_illegal",
            "-leading-dash",
            "trailing-dash-",
            "UPPER_AND_UNDERSCORE",
            "x" * 254,
        ],
    )
    def test_the_predicate_answers_exactly_when_the_conversion_succeeds(self, name):
        if is_k8s_resource_name(name):
            assert k8s_resource_name(name) == name.lower()
        else:
            with pytest.raises(InvalidServerNameError):
                k8s_resource_name(name)

    def test_a_long_name_is_rejected_by_length_not_by_characters(self):
        assert is_k8s_resource_name("x" * 253)
        assert not is_k8s_resource_name("x" * 254)
