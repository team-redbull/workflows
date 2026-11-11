"""The invariants that keep a permanent failure from retrying forever.

add-server-to-mce runs with UNBOUNDED activity retries, so a failure Temporal does
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
from workflow_domains.server_lifecycle.add_server_to_mce import (
    _REJECTION_SUMMARY,
    _REJECTION_TYPE,
    _RETRY_POLICY,
)

_ADD_SERVER_TO_MCE = (
    pathlib.Path(__file__).resolve().parent.parent
    / "workflow_domains/server_lifecycle/add_server_to_mce.py"
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
        tree = ast.parse(_ADD_SERVER_TO_MCE.read_text())
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
        tree = ast.parse(_ADD_SERVER_TO_MCE.read_text())
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
        tree = ast.parse(_ADD_SERVER_TO_MCE.read_text())
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
        assert used, "add_server_to_mce raises no classified ApplicationError"
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
        tree = ast.parse(_ADD_SERVER_TO_MCE.read_text())
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
        tree = ast.parse(_ADD_SERVER_TO_MCE.read_text())
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


class TestTheClusterUpgradeMceLimbsErrors:
    """Every error the per-MCE cluster-upgrade limb raises is classified on purpose.

    The upgrade's activities retry without bound (dispatch.RETRY_POLICY), so an
    error left out of PERMANENT_ACTIVITY_ERRORS is waited on, the run RUNNING
    with it on the activity. For a weeks-long upgrade that is the point for the
    ones below: a hosted API that is down or a kubeconfig not published yet
    clears, and a run must not fail mid-upgrade on it. A new error raised in
    activities/cluster_upgrade/ must be put in one set or the other.
    """

    _PERMANENT = {"ArgoSyncRefusedError", "ProposedActionRefusedError", "AdminGateNotFoundError"}
    # Waited on: a run that may already have committed must never fail on them;
    # the hosted reads turn access errors into `reachable: False` for a person.
    _WAITED_ON = {
        "ArgoSyncFailedError", "HostedClusterAccessError", "HostedClusterUnreachableError",
        "ArgoOperationInProgressError", "AmbiguousHostedClusterError", "HostedClusterNotFoundError",
        "AdminAckNotAppliedError",
    }

    def _named_in_the_limb(self) -> set[str]:
        limb = pathlib.Path(__file__).resolve().parent.parent / "activities" / "cluster_upgrade"
        names = {
            node.id
            for module in limb.glob("*.py")
            for node in ast.walk(ast.parse(module.read_text()))
            if isinstance(node, ast.Name)
        }
        return {
            name for name in names
            if isinstance(getattr(exceptions, name, None), type) and issubclass(getattr(exceptions, name), Exception)
        }

    def test_every_error_the_limb_names_is_classified(self):
        assert self._named_in_the_limb() <= self._PERMANENT | self._WAITED_ON

    def test_the_permanent_ones_stop_the_retries_and_the_others_do_not(self):
        from workflow_domains.cluster_upgrade.dispatch import RETRY_POLICY

        listed = set(RETRY_POLICY.non_retryable_error_types or [])
        assert self._PERMANENT <= listed
        assert not self._WAITED_ON & listed


class TestTheUpgradeHubLimbsErrors:
    """Every shared error the hub limb raises is either permanent for cluster-upgrade or retried on purpose.

    The domain's activities retry without bound (dispatch.RETRY_POLICY), so an
    error neither listed nor meant to be retried would leave a run RUNNING on a
    failure no retry can fix.
    """

    # Retried by design: an outage, a rejected push, a registry credential being
    # fixed. A missing `main` (ValuesBranchNotFoundError) is retried too: failing
    # the run would strand an upgrade mid-way over something a person repairs.
    RETRIED = {"ValuesRepoGitError", "ValuesBranchNotFoundError", "ReleaseRegistryError"}

    def _raised(self) -> set[str]:
        root = pathlib.Path(__file__).resolve().parent.parent / "activities"
        modules = [*sorted((root / "upgrade_gitops").glob("*.py")), root / "git_repo.py"]
        names = set()
        for module in modules:
            for node in ast.walk(ast.parse(module.read_text())):
                call = node.exc if isinstance(node, ast.Raise) else node.value if isinstance(node, ast.Return) else None
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                    names.add(call.func.id)
        return {name for name in names if isinstance(getattr(exceptions, name, None), type)}

    def test_each_is_permanent_or_retried_on_purpose(self):
        from workflow_domains.cluster_upgrade.dispatch import PERMANENT_ACTIVITY_ERRORS

        permanent = {error.__name__ for error in PERMANENT_ACTIVITY_ERRORS}
        raised = self._raised()
        assert raised, "the scan found no shared error raised by the hub limb"
        assert raised <= permanent | self.RETRIED, sorted(raised - permanent - self.RETRIED)
        assert not self.RETRIED & permanent

    def test_the_hub_limbs_permanent_errors_are_all_listed(self):
        from workflow_domains.cluster_upgrade.dispatch import PERMANENT_ACTIVITY_ERRORS

        assert {
            "UpgradeRulesInvalidError", "ClusterFileNotFoundError", "AmbiguousClusterFileError",
            "Day1FileChangedError", "Day1ChangeRejectedError",
        } <= {error.__name__ for error in PERMANENT_ACTIVITY_ERRORS}


class TestCreateVmClassification:
    """The same invariants for create-vm (workflow_domains/vm_lifecycle/create_vm.py) and its limb."""

    _CREATE_VM = pathlib.Path(__file__).resolve().parent.parent / "workflow_domains/vm_lifecycle/create_vm.py"
    # Retried by design: an agent outage, a day1 clone that fails (unreachable is not missing).
    RETRIED = {"KvCapacityUnavailableError", "KvVmApiUnavailableError", "ValuesRepoGitError", "ValuesBranchNotFoundError"}

    def test_every_entry_names_a_real_error_class_and_none_is_workflow_raised(self):
        from workflow_domains.vm_lifecycle.create_vm import _RETRY_POLICY as policy

        listed = set(policy.non_retryable_error_types or [])
        assert listed, "create-vm lists no permanent error"
        for name in listed:
            assert isinstance(getattr(exceptions, name, None), type), name
        assert not _workflow_raised_errors() & listed

    def test_the_list_is_derived_from_classes_and_every_type_is_a_class(self):
        tree = ast.parse(self._CREATE_VM.read_text())
        listed = next(
            node.value for node in ast.walk(tree)
            if isinstance(node, ast.keyword) and node.arg == "non_retryable_error_types"
        )
        assert not isinstance(listed, ast.List)
        literals = [
            keyword for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords if keyword.arg == "type" and isinstance(keyword.value, ast.Constant)
        ]
        assert literals == []
        used = {
            keyword.value.value.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "__name__"
        }
        assert used == {"NoKvClusterError", "NoKvClusterFitsError", "VmNameExhaustedError", "VmRootDiskNotBoundError"}
        assert used <= _workflow_raised_errors()

    def _raised_by_the_limb(self) -> set[str]:
        root = pathlib.Path(__file__).resolve().parent.parent / "activities"
        modules = [*sorted((root / "vm_lifecycle").glob("*.py")), root / "git_repo.py"]
        names = set()
        for module in modules:
            for node in ast.walk(ast.parse(module.read_text())):
                call = node.exc if isinstance(node, ast.Raise) else None
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                    names.add(call.func.id)
        return {name for name in names if isinstance(getattr(exceptions, name, None), type)}

    def test_every_error_the_limb_raises_is_permanent_or_retried_on_purpose(self):
        from workflow_domains.vm_lifecycle.create_vm import _PERMANENT_ACTIVITY_ERRORS

        permanent = {error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS}
        raised = self._raised_by_the_limb()
        assert raised, "the scan found no shared error raised by the vm-lifecycle limb"
        assert raised <= permanent | self.RETRIED, sorted(raised - permanent - self.RETRIED)
        assert not self.RETRIED & permanent
        assert {"KvAgentAuthError", "VmIdentityConflictError", "VmNamespaceNotFoundError", "VmRequestRejectedError",
                "VmDisappearedError"} <= permanent


class TestProvisionClusterClassification:
    """The same invariants for provision-cluster (workflow_domains/cluster_lifecycle/
    provision_cluster.py), its limb, and the refactored allocate-segment."""

    _PROVISION_CLUSTER = pathlib.Path(__file__).resolve().parent.parent / "workflow_domains/cluster_lifecycle/provision_cluster.py"
    _ALLOCATE_SEGMENT = pathlib.Path(__file__).resolve().parent.parent / "workflow_domains/segment_lifecycle/allocate_segment.py"
    # Retried by design: a day1 or sig-repo clone/push that fails (a rejected
    # push is re-done from a fresh clone; a missing `main` is unreachable, not
    # missing), the git provider down, a 5xx, a merge request born in between.
    RETRIED = {"ValuesRepoGitError", "ValuesBranchNotFoundError", "GitProviderError"}

    def test_every_entry_names_a_real_error_class_and_none_is_workflow_raised(self):
        from workflow_domains.cluster_lifecycle.provision_cluster import _RETRY_POLICY as policy

        listed = set(policy.non_retryable_error_types or [])
        assert listed, "provision-cluster lists no permanent error"
        for name in listed:
            assert isinstance(getattr(exceptions, name, None), type), name
        assert not _workflow_raised_errors() & listed

    @pytest.mark.parametrize("module", [_PROVISION_CLUSTER, _ALLOCATE_SEGMENT], ids=["provision-cluster", "allocate-segment"])
    def test_the_list_is_derived_from_classes_and_every_type_is_a_workflow_raised_class(self, module):
        tree = ast.parse(module.read_text())
        listed = next(
            node.value for node in ast.walk(tree)
            if isinstance(node, ast.keyword) and node.arg == "non_retryable_error_types"
        )
        assert not isinstance(listed, ast.List)
        literals = [
            keyword for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords if keyword.arg == "type" and isinstance(keyword.value, ast.Constant)
        ]
        assert literals == []
        used = {
            keyword.value.value.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "__name__"
        }
        assert used, f"{module.name} raises no classified ApplicationError"
        assert used <= _workflow_raised_errors(), sorted(used - _workflow_raised_errors())

    def test_provision_cluster_raises_exactly_its_four_workflow_errors(self):
        tree = ast.parse(self._PROVISION_CLUSTER.read_text())
        used = {
            keyword.value.value.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "__name__"
        }
        assert used == {"AllocateSegmentAlreadyRunningError", "AllocateSegmentFailedError", "MergeRequestClosedError", "Day2EnvMissingError"}

    def test_allocate_segment_raises_exactly_its_three_workflow_errors(self):
        tree = ast.parse(self._ALLOCATE_SEGMENT.read_text())
        used = {
            keyword.value.value.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
            for keyword in node.keywords
            if keyword.arg == "type" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "__name__"
        }
        assert used == {"UnsupportedSegmentType", "SiteMissingError", "UnknownSite", "AllocationNotConfirmed"}

    def _raised_by_the_limb(self) -> set[str]:
        root = pathlib.Path(__file__).resolve().parent.parent / "activities"
        modules = [*sorted((root / "cluster_lifecycle").glob("*.py")), root / "git_repo.py", root / "day1_repo.py"]
        names = set()
        for module in modules:
            for node in ast.walk(ast.parse(module.read_text())):
                call = node.exc if isinstance(node, ast.Raise) else None
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                    names.add(call.func.id)
        return {name for name in names if isinstance(getattr(exceptions, name, None), type)}

    def test_every_error_the_limb_raises_is_permanent_or_retried_on_purpose(self):
        from workflow_domains.cluster_lifecycle.provision_cluster import _PERMANENT_ACTIVITY_ERRORS

        permanent = {error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS}
        raised = self._raised_by_the_limb()
        assert raised, "the scan found no shared error raised by the cluster-lifecycle limb"
        assert raised <= permanent | self.RETRIED, sorted(raised - permanent - self.RETRIED)
        assert not self.RETRIED & permanent
        assert {"MceFolderNotFoundError", "AmbiguousMceFolderError", "ClusterAlreadyProvisionedError",
                "ClusterValuesRejectedError", "ClusterFileConflictError", "GitProviderAuthError",
                "GitProviderRequestRejectedError"} <= permanent

    def test_allocate_segments_permanent_errors_are_the_segments_managers_answers(self):
        from workflow_domains.segment_lifecycle.allocate_segment import _PERMANENT_ACTIVITY_ERRORS, _RETRY_POLICY as policy

        assert {error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS} == {
            "SegmentsManagerAuthError", "SegmentNotFoundError", "SegmentValidationError", "SegmentPoolExhaustedError",
        }
        assert not _workflow_raised_errors() & set(policy.non_retryable_error_types or [])
