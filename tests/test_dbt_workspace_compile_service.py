"""One check and one injected publication; success and failure reports stay honest."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceDiscoveryReport
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactOutputConflict, DbtArtifactPublicationError
from dpone.services.dbt_workspace import DbtWorkspaceService
from tests.test_dbt_workspace_release_assembly import _assemble, _project


def _service(tmp_path, *, failure=None, empty=False):
    projects = [] if empty else [_project(tmp_path, name) for name in ("alpha", "beta")]
    check = DbtWorkspaceCheckReport(
        DbtWorkspaceDiscoveryReport(tuple(item.check.project for item in projects)),
        tuple(item.check for item in projects),
    )
    calls = []
    tree = None if empty else replace(_assemble(projects), files={"release-subjects.sha256": b"exact subject fixture"})

    class Writer:
        def write(self, received, *, root, output_dir):
            calls.append(("write", received, root, output_dir))
            if failure:
                raise failure
            return tree

    def writer_factory():
        calls.append(("factory",))
        return Writer()

    class Service(DbtWorkspaceService):
        def check(self, root):
            calls.append(("check", root))
            return check

    return Service(discovery=object(), compiler_factory=object(), writer_factory=writer_factory), check, calls


def test_compile_checks_once_and_reports_published_identities(tmp_path: Path):
    service, check, calls = _service(tmp_path)
    result = service.compile(tmp_path, output_dir=tmp_path / "out")
    assert [call[0] for call in calls] == ["check", "factory", "write"]
    assert result.passed and result.exit_code == 0 and result.check == check
    assert result.release_id.startswith("sha256:") and result.source_snapshot_sha256.startswith("sha256:")
    from dpone.contracts.dbt_contract_validation import sha256_bytes

    assert result.subject_sha256 == sha256_bytes(b"exact subject fixture")
    assert set(result.to_dict()) == {
        "schema",
        "passed",
        "check",
        "output_dir",
        "release_id",
        "source_snapshot_sha256",
        "subject_sha256",
        "blockers",
    }


def test_empty_compile_is_not_a_successful_removal_release(tmp_path: Path):
    service, _, calls = _service(tmp_path, empty=True)
    result = service.compile(tmp_path, output_dir=tmp_path / "out")
    assert not result.passed and result.exit_code == 2
    assert [call[0] for call in calls] == ["check"]
    assert result.blockers[0].code == "DPONE_DBT_NO_PUBLISH_MODELS"


@pytest.mark.parametrize(
    "failure,code,exit_code",
    [
        (
            DbtPublishingError(
                "DPONE_DBT_MANIFEST_STALE", "Regenerate the manifest", path="beta", remediation="Run dbt parse"
            ),
            "DPONE_DBT_MANIFEST_STALE",
            2,
        ),
        (DbtArtifactOutputConflict("SECRET"), "DPONE_DBT_PUBLISH_OUTPUT_CONFLICT", 2),
        (DbtArtifactPublicationError("SECRET"), "DPONE_DBT_OUTPUT_WRITE_FAILED", 5),
        (ValueError("SECRET"), "DPONE_DBT_COMPILE_FAILED", 2),
        (RuntimeError("SECRET"), "DPONE_DBT_INTERNAL", 5),
    ],
)
def test_writer_failure_retains_all_checks_without_success_ids(tmp_path: Path, failure, code, exit_code):
    service, check, _ = _service(tmp_path, failure=failure)
    result = service.compile(tmp_path, output_dir=tmp_path / "out")
    assert not result.passed and result.exit_code == exit_code and result.check == check
    assert len(result.check.projects) == 2 and result.blockers[0].code == code
    assert result.release_id is result.source_snapshot_sha256 is result.subject_sha256 is None
    assert "SECRET" not in str(result.to_dict())
    if code == "DPONE_DBT_OUTPUT_WRITE_FAILED":
        assert "may already" in result.blockers[0].message
        assert "identical" in result.blockers[0].remediation
