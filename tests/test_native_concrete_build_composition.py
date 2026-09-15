"""Concrete runtime composition with synthetic external ports; no live qualification."""

import pytest

from dpone.runtime.commit_unknown import CommitUnknownError
from tests.native_build_composition_fixtures import concrete_build


def test_concrete_build_retains_profile_identity_through_receipt(tmp_path):
    with concrete_build(tmp_path) as fixture:
        receipt = fixture.bridge(fixture.reservation, fixture.invocation.executor)
        assert receipt.outcome == "SUCCEEDED"
        fixture.require_clean_profile()
        completion = fixture.writer.require_build_completion()
        assert receipt.build_evidence == completion.build_evidence
        assert receipt.artifact_inventory == completion.artifact_inventory
        assert receipt.termination == completion.termination
        assert fixture.admission.states == ["RUNNING"]
        assert fixture.reads == [fixture.request]
        assert len(fixture.runner.calls) == 3
        assert fixture.publication_kinds == [
            "dbt_build_evidence_v1",
            "dbt_build_manifest_v1",
            "dbt_build_run_results_v1",
            "native_build_artifact_inventory_v1",
        ]
        assert fixture.delegate_writes == ["passed"]
        directory = fixture.profile_path.parent
    assert not directory.exists()


def test_concrete_build_missing_manifest_cannot_publish_positive_cohort(tmp_path):
    with concrete_build(tmp_path, missing_manifest=True) as fixture:
        with pytest.raises(CommitUnknownError) as failure:
            fixture.bridge(fixture.reservation, fixture.invocation.executor)
        assert failure.value.outcome.checkpoint_state == "not_advanced"
        fixture.require_clean_profile()
        assert fixture.admission.states == ["RUNNING"]
        assert fixture.reads == [fixture.request]
        assert len(fixture.runner.calls) == 3
        assert fixture.publication_kinds == []
        assert fixture.delegate_writes == []
        with pytest.raises(ValueError, match="no complete positive cohort"):
            fixture.writer.require_build_completion()
        directory = fixture.profile_path.parent
    assert not directory.exists()
