from __future__ import annotations

import argparse
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from dpone import __version__
from dpone.commands import dbt_promotion_cmd
from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_project_bundle import DbtProjectBundleArtifact
from dpone.contracts.dbt_release import DBT_RELEASE_WIRE_CONTRACT
from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle
from dpone.services.dbt_promotion_verification import (
    DBT_PROMOTION_SOURCE_DRIFT,
    DBT_PROMOTION_SOURCE_VERIFIED,
    DbtPromotionVerificationReport,
    DbtPromotionVerificationService,
)
from dpone.services.dbt_release_assets import build_source_snapshot


def test_promotion_verification_proves_prod_mirror_matches_pinned_release(
    tmp_path: Path,
) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)

    report = _service().verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=source_snapshot,
    )

    assert report.passed
    assert report.code == DBT_PROMOTION_SOURCE_VERIFIED
    assert report.release_id == release_set["release_id"]
    assert report.source_snapshot_sha256 == source_snapshot["snapshot_sha256"]
    assert report.expected_project_bundle_sha256 == source_snapshot["project_bundle_sha256"]
    assert report.observed_project_bundle_sha256 == source_snapshot["project_bundle_sha256"]
    assert report.to_dict() == {
        "schema": "dpone.dbt-promotion-verification.v1",
        "status": "passed",
        "passed": True,
        "code": DBT_PROMOTION_SOURCE_VERIFIED,
        "release_id": release_set["release_id"],
        "source_snapshot_sha256": source_snapshot["snapshot_sha256"],
        "expected_project_bundle_sha256": source_snapshot["project_bundle_sha256"],
        "observed_project_bundle_sha256": source_snapshot["project_bundle_sha256"],
    }


def test_promotion_verification_rejects_changed_prod_source(tmp_path: Path) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)
    (project_root / "models" / "orders.sql").write_text("select 2 as order_id\n", encoding="utf-8")

    report = _service().verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=source_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert report.expected_project_bundle_sha256 == source_snapshot["project_bundle_sha256"]
    assert report.observed_project_bundle_sha256 != source_snapshot["project_bundle_sha256"]


def test_promotion_verification_rejects_missing_prod_source(tmp_path: Path) -> None:
    _, release_set, source_snapshot = _promotion_inputs(tmp_path)

    report = _service().verify(
        project_root=tmp_path / "missing-mirror",
        release_set=release_set,
        source_snapshot=source_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert report.observed_project_bundle_sha256 is None


@pytest.mark.parametrize(
    "mutation",
    [
        lambda snapshot: snapshot.pop("snapshot_sha256"),
        lambda snapshot: snapshot.update(project_bundle_sha256="sha256:" + "f" * 64),
        lambda snapshot: snapshot.update(snapshot_sha256="sha256:" + "f" * 64),
        lambda snapshot: snapshot.update(unexpected="value"),
    ],
)
def test_promotion_verification_rejects_malformed_or_tampered_snapshot_before_rebuild(
    tmp_path: Path,
    mutation: Any,
) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)
    mutation(source_snapshot)
    builder = _UnexpectedBundleBuilder()

    report = DbtPromotionVerificationService(bundle_builder=builder).verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=source_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert builder.calls == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda release: release.update(schema="dpone.release-set.v1"),
        lambda release: release.update(release_id="sha256:" + "f" * 64),
        lambda release: release.pop("release_id"),
        lambda release: release["artifacts"].update(runtime_payloads=[]),
        lambda release: release["artifacts"]["runtime_payloads"][0].pop("sha256"),
    ],
)
def test_promotion_verification_rejects_invalid_release_before_rebuild(
    tmp_path: Path,
    mutation: Any,
) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)
    mutation(release_set)
    builder = _UnexpectedBundleBuilder()

    report = DbtPromotionVerificationService(bundle_builder=builder).verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=source_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert builder.calls == 0


def test_promotion_verification_rejects_snapshot_for_another_release_bundle(
    tmp_path: Path,
) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)
    other_bundle_sha256 = "sha256:" + "f" * 64
    other_snapshot = build_source_snapshot(
        project_bundle_sha256=other_bundle_sha256,
        manifest_sha256=str(source_snapshot["manifest_sha256"]),
    )
    builder = _UnexpectedBundleBuilder()

    report = DbtPromotionVerificationService(bundle_builder=builder).verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=other_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert builder.calls == 0


def test_promotion_verification_binds_snapshot_manifest_to_release_payload(
    tmp_path: Path,
) -> None:
    project_root, release_set, source_snapshot = _promotion_inputs(tmp_path)
    tampered_snapshot = build_source_snapshot(
        project_bundle_sha256=str(source_snapshot["project_bundle_sha256"]),
        manifest_sha256="sha256:" + "f" * 64,
    )
    release_set["provenance"]["source_snapshot_sha256"] = tampered_snapshot["snapshot_sha256"]
    builder = _UnexpectedBundleBuilder()

    report = DbtPromotionVerificationService(bundle_builder=builder).verify(
        project_root=project_root,
        release_set=release_set,
        source_snapshot=tampered_snapshot,
    )

    assert not report.passed
    assert report.code == DBT_PROMOTION_SOURCE_DRIFT
    assert builder.calls == 0


def test_promotion_cli_rejects_explicitly_empty_trust_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = DbtPromotionVerificationReport(
        code=DBT_PROMOTION_SOURCE_VERIFIED,
        release_id="sha256:" + "a" * 64,
    )
    monkeypatch.setattr(
        dbt_promotion_cmd,
        "build_dbt_promotion_verification_service",
        lambda: _StaticPromotionService(report),
    )
    args = argparse.Namespace(
        compiled_root=str(tmp_path),
        project=str(tmp_path),
        repository_root="",
        descriptor_path="",
        expected_dev_deployment_id="",
        expected_dev_evidence_ref="",
        expected_dev_evidence_subject_sha256="",
        expected_dev_evidence_artifact_name="",
        expected_dev_evidence_producer_workflow="",
        expected_dev_evidence_source_commit="",
        format="json",
    )

    exit_code = dbt_promotion_cmd.cmd_verify_promotion(
        args,
        ctx=object(),
        logger=logging.getLogger(__name__),
    )

    assert exit_code == 2


def _service() -> DbtPromotionVerificationService:
    return DbtPromotionVerificationService(bundle_builder=_BundleBuilder())


def _promotion_inputs(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, str]]:
    project_root = tmp_path / "prod-mirror"
    (project_root / "models").mkdir(parents=True)
    (project_root / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    (project_root / "models" / "orders.sql").write_text("select 1 as order_id\n", encoding="utf-8")
    bundle = build_dbt_project_bundle(project_root)
    manifest_bytes = b'{"metadata": {}}\n'
    source_snapshot = build_source_snapshot(
        project_bundle_sha256=bundle.bundle.archive_sha256,
        manifest_sha256=sha256_bytes(manifest_bytes),
    )
    release_set: dict[str, Any] = {
        "schema": "dpone.release-set.v2",
        "release_id": "",
        "producer": {
            "dpone_version": __version__,
            "wire_contract": DBT_RELEASE_WIRE_CONTRACT,
        },
        "artifacts": {
            "runtime_payloads": [
                {
                    "id": "dbt_project",
                    "kind": "dbt_project_bundle",
                    "path": "runtime/dbt/project.tar.gz",
                    "sha256": bundle.bundle.archive_sha256,
                    "bytes": bundle.bundle.archive_bytes,
                    "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
                },
                {
                    "id": "dbt_manifest",
                    "kind": "dbt_manifest",
                    "path": "runtime/dbt/manifest.json",
                    "sha256": sha256_bytes(manifest_bytes),
                    "bytes": len(manifest_bytes),
                    "media_type": "application/vnd.dbt.manifest+json",
                },
            ]
        },
        "provenance": {
            "source": "dpone dbt compile",
            "source_snapshot_sha256": source_snapshot["snapshot_sha256"],
        },
    }
    release_set["release_id"] = release_id(release_set)
    return project_root, deepcopy(release_set), deepcopy(source_snapshot)


class _UnexpectedBundleBuilder:
    calls = 0

    def build(self, project_root: Path) -> DbtProjectBundleArtifact:
        self.calls += 1
        raise AssertionError(f"bundle rebuild must not run for invalid metadata: {project_root}")


class _BundleBuilder:
    def build(self, project_root: Path) -> DbtProjectBundleArtifact:
        return build_dbt_project_bundle(project_root)


class _StaticPromotionService:
    def __init__(self, report: DbtPromotionVerificationReport) -> None:
        self._report = report

    def verify(self, **_kwargs: object) -> DbtPromotionVerificationReport:
        return self._report
