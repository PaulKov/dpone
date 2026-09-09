"""Producer domain policy preserves the observable acquisition/validation order."""

from dataclasses import replace
from tempfile import TemporaryDirectory

import pytest

from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.readiness.dbt_sqlserver_project_policy import DbtSqlserverProjectPolicyValidator
from dpone.services.dbt_release_builder import build_release_inputs
from tests.test_dbt_project_artifact_projection import FixtureSelection
from tests.test_dbt_publish_atomicity import DEMO, _compile


def _build(report, resolver, bundles=None, *, wire_contract=DBT_RUNTIME_WIRE_V1):
    bundles = bundles or RuntimeDbtProjectBundleOperations()
    return build_release_inputs(
        report,
        project_root=DEMO,
        selection_resolver=resolver,
        bundle_builder=bundles,
        bundle_extractor=bundles,
        bundle_verifier=bundles,
        project_policy=DbtSqlserverProjectPolicyValidator(),
        wire_contract=wire_contract,
    )


@pytest.mark.parametrize("setting", ["dbt_timeout_seconds", "dbt_threads", "dbt_warning_policy"])
@pytest.mark.parametrize("resolver_fails", [False, True])
def test_option_failures_keep_their_position_relative_to_selection(setting, resolver_fails):
    events = []
    report = _compile()
    profile = report.workflows[0].models[0].profile
    if setting == "dbt_warning_policy":
        profile.quality[setting] = "invalid"
    else:
        profile.runtime[setting] = 0

    class Resolver(FixtureSelection):
        def resolve(self, **kwargs):
            events.append("selection")
            if resolver_fails:
                raise RuntimeError("selection sentinel")
            return super().resolve(**kwargs)

    # An error in the first workflow must not begin the second workflow.
    report = replace(report, workflows=(*report.workflows, replace(report.workflows[0], workflow="second")))
    if setting != "dbt_timeout_seconds" and resolver_fails:
        with pytest.raises(RuntimeError, match="selection sentinel"):
            _build(report, Resolver())
    else:
        message = "warning policy must" if setting == "dbt_warning_policy" else "integer setting must"
        with pytest.raises(ValueError, match=message):
            _build(report, Resolver())
    assert events == ([] if setting == "dbt_timeout_seconds" else ["selection"])


def test_bundle_failure_precedes_exact_toolchain_rejection():
    events = []

    class Bundles(RuntimeDbtProjectBundleOperations):
        def build(self, project_root):
            events.append("bundle")
            raise RuntimeError("bundle sentinel")

    with pytest.raises(RuntimeError, match="bundle sentinel"):
        _build(replace(_compile(), dbt_version="unsupported"), FixtureSelection(), Bundles())
    assert events == ["bundle"]


def test_selection_observes_runtime_snapshot_but_quality_is_checked_after_callback():
    report = _compile()
    profile = report.workflows[0].models[0].profile

    class Resolver(FixtureSelection):
        def resolve(self, **kwargs):
            profile.runtime["dbt_threads"] = 0
            profile.quality["dbt_warning_policy"] = "allow"
            return super().resolve(**kwargs)

    inputs = _build(report, Resolver())
    pack = next(iter(inputs.execution_packs.values()))
    assert pack.profile.threads > 0
    assert pack.dbt_warning_policy == "allow"


def test_workflow_plan_is_owned_by_project_contract_and_preserves_option_imports():
    from dpone.contracts.dbt_project_artifacts import DbtWorkflowReleasePlan, dbt_warning_policy, positive_int
    from dpone.services import dbt_release_runtime_options as options

    plan = DbtWorkflowReleasePlan.prepare(_compile().workflows[0])
    assert plan.selection_arguments()["selected_unique_ids"] == plan.selected_models
    assert options.positive_int is positive_int
    assert options.dbt_warning_policy is dbt_warning_policy


@pytest.mark.parametrize("invalid_lock", [False, True])
def test_selection_lock_failure_precedes_missing_workspace_invocation_target(invalid_lock):
    class Resolver(FixtureSelection):
        def resolve(self, **kwargs):
            selection = replace(super().resolve(**kwargs), invocation_target=None)
            if invalid_lock:
                # Structurally valid resolver result, but the publishing models
                # are no longer in its expected results: lock construction fails.
                selection = replace(
                    selection,
                    selected_graph_unique_ids=("model.fixture.unrelated",),
                    expected_run_result_unique_ids=("model.fixture.unrelated",),
                )
            return selection

    error = DbtPublishingError if invalid_lock else ValueError
    message = "publish models must" if invalid_lock else "dbt-rendered invocation target"
    with pytest.raises(error, match=message):
        _build(_compile(), Resolver(), wire_contract=DBT_RUNTIME_WIRE_V2)


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_cleanup_precedes_ownership_which_precedes_mixed_authorities(cleanup_fails, monkeypatch):
    class Snapshot(TemporaryDirectory):
        def __exit__(self, *args):
            super().__exit__(*args)
            if cleanup_fails:
                raise RuntimeError("cleanup sentinel")

    class Resolver(FixtureSelection):
        calls = 0

        def resolve(self, **kwargs):
            self.calls += 1
            selection = super().resolve(**kwargs)
            return replace(selection, authority="dbt_cli" if self.calls == 1 else "manifest_preview")

    monkeypatch.setattr("dpone.services.dbt_release_builder.TemporaryDirectory", Snapshot)
    report = _compile()
    report = replace(report, workflows=(*report.workflows, replace(report.workflows[0], workflow="second")))
    resolver = Resolver()
    error = RuntimeError if cleanup_fails else ValueError
    message = "cleanup sentinel" if cleanup_fails else "multiple workflow owners"
    with pytest.raises(error, match=message):
        _build(report, resolver)
    assert resolver.calls == 2


def test_resolver_receives_original_nonlexicographic_publish_order():
    report = _compile()
    workflow = report.workflows[0]
    models = tuple(sorted(workflow.models, key=lambda item: item.model.unique_id, reverse=True))
    original = tuple(item.model.unique_id for item in models)
    assert len(original) > 1 and original != tuple(sorted(original))

    class Resolver(FixtureSelection):
        def resolve(self, **kwargs):
            assert kwargs["selected_unique_ids"] == original
            return super().resolve(**kwargs)

    _build(replace(report, workflows=(replace(workflow, models=models),)), Resolver())


def test_incomplete_logical_target_is_rejected_before_selection():
    report = _compile()
    workflow = report.workflows[0]
    models = tuple(replace(item, model=replace(item.model, schema="")) for item in workflow.models)
    report = replace(report, workflows=(replace(workflow, models=models),))

    class Resolver(FixtureSelection):
        def resolve(self, **kwargs):
            pytest.fail("selection must not start for an incomplete logical target")

    with pytest.raises(ValueError, match="logical target is incomplete"):
        _build(report, Resolver())
