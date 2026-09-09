"""Legacy metadata policy remains distinct from actual mirror acquisition."""

import pytest

from dpone.contracts.airflow_deployment import release_id
from dpone.services.dbt_promotion_verification import DbtPromotionVerificationService
from tests.test_dbt_promotion_verification import _promotion_inputs, _service


def test_report_is_reexported_from_pure_promotion_contract():
    from dpone.contracts.dbt_promotion import DbtPromotionVerificationReport
    from dpone.services.dbt_promotion_verification import DbtPromotionVerificationReport as LegacyReport

    assert LegacyReport is DbtPromotionVerificationReport


def test_legacy_promotion_does_not_adopt_workspace_membership_rules(tmp_path):
    root, release, snapshot = _promotion_inputs(tmp_path)
    release["artifacts"]["runtime_payloads"].append({"id": "unrelated-legacy-object"})
    release["release_id"] = release_id(release)
    assert _service().verify(project_root=root, release_set=release, source_snapshot=snapshot).passed


@pytest.mark.parametrize("failure", ["wrong-type", "byte-count", "exception"])
def test_builder_failure_keeps_pinned_identity_without_claiming_observed_success(tmp_path, failure):
    from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle

    root, release, snapshot = _promotion_inputs(tmp_path)
    artifact = build_dbt_project_bundle(root)
    if failure == "byte-count":
        release["artifacts"]["runtime_payloads"][0]["bytes"] += 1
        release["release_id"] = release_id(release)

    class Builder:
        def build(self, project_root):
            assert project_root == root
            if failure == "exception":
                raise OSError("fixture acquisition failed")
            if failure == "wrong-type":
                return object()
            return artifact

    report = DbtPromotionVerificationService(bundle_builder=Builder()).verify(
        project_root=root, release_set=release, source_snapshot=snapshot
    )
    assert not report.passed
    assert report.release_id == release["release_id"]
    assert report.expected_project_bundle_sha256 == snapshot["project_bundle_sha256"]
    expected_observed = snapshot["project_bundle_sha256"] if failure == "byte-count" else None
    assert report.observed_project_bundle_sha256 == expected_observed
