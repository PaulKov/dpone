from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import dpone.services.dbt_prod_mirror_transaction as mirror_transaction
from dpone.adapters.dbt_workflow_selection import (
    DbtCliSelectionResolver,
    ManifestPreviewSelectionResolver,
)
from dpone.app.dbt_promotion_composition import (
    RuntimeDbtProjectBundleOperations,
    build_dbt_prod_mirror_service,
)
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle
from dpone.services.dbt_prod_mirror import (
    DbtProdMirrorError,
    DbtProdMirrorService,
)
from dpone.services.dbt_prod_mirror_journal import TRANSACTION_DIRECTORY
from dpone.services.dbt_prod_promotion_metadata import (
    DbtProdPromotionMetadataVerifier,
)
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"
DEV_EVIDENCE_SUBJECT_SHA256 = "sha256:" + "e" * 64
DEV_EVIDENCE_ARTIFACT_NAME = "dpone-dbt-dev-evidence"
DEV_EVIDENCE_PRODUCER_WORKFLOW = ".github/workflows/dbt-self-service-dev-activation.yml"
DEV_EVIDENCE_SOURCE_COMMIT = "c" * 40
DEV_EVIDENCE_SET_ID = "sha256:" + "f" * 64
DEV_EVIDENCE_CAMPAIGN_REQUEST_SHA256 = "sha256:" + "9" * 64


def test_prod_mirror_is_byte_identical_idempotent_and_replaces_manual_drift(
    tmp_path: Path,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()

    created = _prepare(service, compiled, repository, release_id)
    expected = build_dbt_project_bundle(DEMO)
    observed = build_dbt_project_bundle(repository / "dbt-mirror")
    assert observed.archive == expected.archive
    descriptor = json.loads((repository / ".dpone/dbt/promotion.json").read_text(encoding="utf-8"))
    assert descriptor["schema"] == "dpone.dbt-prod-promotion.v1"
    assert descriptor["release_id"] == release_id
    assert descriptor["dev_evidence_subject_sha256"] == DEV_EVIDENCE_SUBJECT_SHA256
    assert descriptor["dev_evidence_artifact_name"] == DEV_EVIDENCE_ARTIFACT_NAME
    assert descriptor["dev_evidence_producer_workflow"] == DEV_EVIDENCE_PRODUCER_WORKFLOW
    assert descriptor["dev_evidence_source_commit"] == DEV_EVIDENCE_SOURCE_COMMIT
    unsigned_descriptor = dict(descriptor)
    unsigned_descriptor.pop("promotion_id")
    assert descriptor["promotion_id"] == canonical_fingerprint(unsigned_descriptor)
    assert descriptor["promotion_id"] == created.promotion_id
    assert _prepare(service, compiled, repository, release_id).no_op is True
    DbtProdPromotionMetadataVerifier().verify(
        compiled_root=compiled,
        repository_root=repository,
        descriptor_path=".dpone/dbt/promotion.json",
        expected_release_id=release_id,
        expected_dev_deployment_id="sha256:" + "d" * 64,
        expected_dev_evidence_ref="github-actions://example/dev/actions/runs/123",
        expected_dev_evidence_subject_sha256=DEV_EVIDENCE_SUBJECT_SHA256,
        expected_dev_evidence_artifact_name=DEV_EVIDENCE_ARTIFACT_NAME,
        expected_dev_evidence_producer_workflow=DEV_EVIDENCE_PRODUCER_WORKFLOW,
        expected_dev_evidence_source_commit=DEV_EVIDENCE_SOURCE_COMMIT,
    )

    (repository / "dbt-mirror/models/competitive_pricing.sql").write_text(
        "select 'manual drift'\n",
        encoding="utf-8",
    )
    replaced = _prepare(service, compiled, repository, release_id)
    assert replaced.no_op is False
    assert build_dbt_project_bundle(repository / "dbt-mirror").archive == expected.archive


def test_prod_mirror_v2_binds_dev_evidence_campaign(
    tmp_path: Path,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_set_id"] = DEV_EVIDENCE_SET_ID
    arguments["dev_evidence_campaign_request_sha256"] = DEV_EVIDENCE_CAMPAIGN_REQUEST_SHA256

    build_dbt_prod_mirror_service().prepare(**arguments)

    descriptor = json.loads((repository / ".dpone/dbt/promotion.json").read_text(encoding="utf-8"))
    assert descriptor["schema"] == "dpone.dbt-prod-promotion.v2"
    assert descriptor["dev_evidence_set_id"] == DEV_EVIDENCE_SET_ID
    assert descriptor["dev_evidence_campaign_request_sha256"] == DEV_EVIDENCE_CAMPAIGN_REQUEST_SHA256
    DbtProdPromotionMetadataVerifier().verify(
        compiled_root=compiled,
        repository_root=repository,
        descriptor_path=".dpone/dbt/promotion.json",
        **{
            **_metadata_expectations(release_id),
            "expected_dev_evidence_set_id": DEV_EVIDENCE_SET_ID,
            "expected_dev_evidence_campaign_request_sha256": (DEV_EVIDENCE_CAMPAIGN_REQUEST_SHA256),
        },
    )
    with pytest.raises(DbtProdMirrorError):
        DbtProdPromotionMetadataVerifier().verify(
            compiled_root=compiled,
            repository_root=repository,
            descriptor_path=".dpone/dbt/promotion.json",
            **{
                **_metadata_expectations(release_id),
                "expected_dev_evidence_set_id": "sha256:" + "0" * 64,
                "expected_dev_evidence_campaign_request_sha256": (DEV_EVIDENCE_CAMPAIGN_REQUEST_SHA256),
            },
        )


def test_prod_mirror_rejects_incomplete_campaign_identity(
    tmp_path: Path,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_set_id"] = DEV_EVIDENCE_SET_ID

    with pytest.raises(DbtProdMirrorError, match="incomplete"):
        build_dbt_prod_mirror_service().prepare(**arguments)


def test_prod_mirror_metadata_verification_rejects_manual_edit(
    tmp_path: Path,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()
    _prepare(service, compiled, repository, release_id)
    descriptor_path = repository / ".dpone/dbt/promotion.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["dev_evidence_ref"] = "github-actions://attacker/run/1"
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with pytest.raises(DbtProdMirrorError):
        DbtProdPromotionMetadataVerifier().verify(
            compiled_root=compiled,
            repository_root=repository,
            descriptor_path=".dpone/dbt/promotion.json",
            expected_release_id=release_id,
            expected_dev_deployment_id="sha256:" + "d" * 64,
            expected_dev_evidence_ref="github-actions://example/dev/actions/runs/123",
            expected_dev_evidence_subject_sha256=DEV_EVIDENCE_SUBJECT_SHA256,
            expected_dev_evidence_artifact_name=DEV_EVIDENCE_ARTIFACT_NAME,
            expected_dev_evidence_producer_workflow=DEV_EVIDENCE_PRODUCER_WORKFLOW,
            expected_dev_evidence_source_commit=DEV_EVIDENCE_SOURCE_COMMIT,
        )


def test_prod_mirror_transaction_rolls_back_all_outputs_on_partial_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()
    _prepare(service, compiled, repository, release_id)
    mirror_file = repository / "dbt-mirror/models/competitive_pricing.sql"
    descriptor_file = repository / ".dpone/dbt/promotion.json"
    mirror_file.write_text("select 'existing manual state'\n", encoding="utf-8")
    before_mirror = mirror_file.read_bytes()
    before_descriptor = descriptor_file.read_bytes()
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_subject_sha256"] = "sha256:" + "f" * 64
    original_replace = mirror_transaction._replace
    calls = 0

    def fail_fourth_replace(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 4:
            raise OSError("injected transaction failure")
        original_replace(source, destination)

    monkeypatch.setattr(mirror_transaction, "_replace", fail_fourth_replace)

    with pytest.raises(DbtProdMirrorError):
        service.prepare(**arguments)

    assert mirror_file.read_bytes() == before_mirror
    assert descriptor_file.read_bytes() == before_descriptor
    assert not list(repository.glob(".dpone-dbt-promotion*"))


def test_prod_mirror_rolls_back_when_directory_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()
    _prepare(service, compiled, repository, release_id)
    mirror_file = repository / "dbt-mirror/models/competitive_pricing.sql"
    descriptor_file = repository / ".dpone/dbt/promotion.json"
    snapshot_file = repository / ".dpone/dbt/source-snapshot.json"
    mirror_file.write_text("select 'existing manual state'\n", encoding="utf-8")
    before = (
        mirror_file.read_bytes(),
        descriptor_file.read_bytes(),
        snapshot_file.read_bytes(),
    )
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_subject_sha256"] = "sha256:" + "f" * 64
    original = mirror_transaction.fsync_directory
    failed = False

    def fail_once(directory: Path) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise DbtProdMirrorError("simulated directory fsync failure")
        original(directory)

    monkeypatch.setattr(mirror_transaction, "fsync_directory", fail_once)

    with pytest.raises(DbtProdMirrorError, match="simulated directory fsync failure"):
        service.prepare(**arguments)

    assert (
        mirror_file.read_bytes(),
        descriptor_file.read_bytes(),
        snapshot_file.read_bytes(),
    ) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


def test_prod_mirror_recovers_durable_journal_after_process_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SimulatedProcessCrash(BaseException):
        pass

    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()
    _prepare(service, compiled, repository, release_id)
    mirror_file = repository / "dbt-mirror/models/competitive_pricing.sql"
    mirror_file.write_text("select 'state before interrupted transaction'\n", encoding="utf-8")
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_subject_sha256"] = "sha256:" + "f" * 64
    original_replace = mirror_transaction._replace
    calls = 0

    def crash_after_first_install(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        original_replace(source, destination)
        if calls == 2:
            raise SimulatedProcessCrash

    monkeypatch.setattr(mirror_transaction, "_replace", crash_after_first_install)
    with pytest.raises(SimulatedProcessCrash):
        service.prepare(**arguments)

    transaction_root = repository / TRANSACTION_DIRECTORY
    assert transaction_root.is_dir()

    monkeypatch.setattr(mirror_transaction, "_replace", original_replace)
    recovered = service.prepare(**arguments)

    assert recovered.no_op is False
    assert not transaction_root.exists()
    expected = build_dbt_project_bundle(DEMO)
    observed = build_dbt_project_bundle(repository / "dbt-mirror")
    assert observed.archive == expected.archive


def test_prod_mirror_recovers_new_destination_after_process_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SimulatedProcessCrash(BaseException):
        pass

    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    service = build_dbt_prod_mirror_service()
    original_replace = mirror_transaction._replace

    def crash_after_new_destination_install(source: Path, destination: Path) -> None:
        original_replace(source, destination)
        raise SimulatedProcessCrash

    monkeypatch.setattr(
        mirror_transaction,
        "_replace",
        crash_after_new_destination_install,
    )
    with pytest.raises(SimulatedProcessCrash):
        _prepare(service, compiled, repository, release_id)

    transaction_root = repository / TRANSACTION_DIRECTORY
    assert transaction_root.is_dir()

    monkeypatch.setattr(mirror_transaction, "_replace", original_replace)
    recovered = _prepare(service, compiled, repository, release_id)

    assert recovered.no_op is False
    assert not transaction_root.exists()
    assert (repository / ".dpone/dbt/source-snapshot.json").is_file()
    assert (repository / ".dpone/dbt/promotion.json").is_file()


def test_prod_mirror_fails_closed_on_corrupt_recovery_journal(
    tmp_path: Path,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    transaction_root = repository / TRANSACTION_DIRECTORY
    transaction_root.mkdir()
    (transaction_root / "journal.json").write_text(
        '{"schema":"dpone.dbt-prod-mirror-journal.v1","replacements":[',
        encoding="utf-8",
    )

    with pytest.raises(DbtProdMirrorError, match="recovery journal is invalid"):
        _prepare(
            build_dbt_prod_mirror_service(),
            compiled,
            repository,
            release_id,
        )

    assert transaction_root.is_dir()
    assert not (repository / "dbt-mirror").exists()
    assert not (repository / ".dpone/dbt/promotion.json").exists()


@pytest.mark.parametrize(
    ("field", "unexpected"),
    [
        ("expected_dev_evidence_subject_sha256", "sha256:" + "f" * 64),
        ("expected_dev_evidence_artifact_name", "foreign-evidence"),
        (
            "expected_dev_evidence_producer_workflow",
            ".github/workflows/foreign.yml",
        ),
        ("expected_dev_evidence_source_commit", "b" * 40),
    ],
)
def test_prod_mirror_metadata_verification_rejects_trust_descriptor_mismatch(
    tmp_path: Path,
    field: str,
    unexpected: str,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    _prepare(build_dbt_prod_mirror_service(), compiled, repository, release_id)
    expectations = _metadata_expectations(release_id)
    expectations[field] = unexpected

    with pytest.raises(DbtProdMirrorError):
        DbtProdPromotionMetadataVerifier().verify(
            compiled_root=compiled,
            repository_root=repository,
            descriptor_path=".dpone/dbt/promotion.json",
            **expectations,
        )


@pytest.mark.parametrize(
    "field",
    [
        "dev_evidence_subject_sha256",
        "dev_evidence_artifact_name",
        "dev_evidence_producer_workflow",
        "dev_evidence_source_commit",
    ],
)
def test_prod_mirror_metadata_verification_rejects_missing_trust_descriptor_field(
    tmp_path: Path,
    field: str,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    _prepare(build_dbt_prod_mirror_service(), compiled, repository, release_id)
    descriptor_path = repository / ".dpone/dbt/promotion.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor.pop(field)
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with pytest.raises(DbtProdMirrorError):
        DbtProdPromotionMetadataVerifier().verify(
            compiled_root=compiled,
            repository_root=repository,
            descriptor_path=".dpone/dbt/promotion.json",
            **_metadata_expectations(release_id),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mirror_path", "../outside"),
        ("source_snapshot_path", "dbt-mirror/snapshot.json"),
        ("descriptor_path", ".git/promotion.json"),
    ],
)
def test_prod_mirror_rejects_unsafe_or_overlapping_paths(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    arguments = _arguments(compiled, repository, release_id)
    arguments[field] = value

    with pytest.raises(DbtProdMirrorError):
        build_dbt_prod_mirror_service().prepare(**arguments)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_prod_mirror_rejects_symlinked_output_parent(tmp_path: Path) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, repository / "unsafe")
    arguments = _arguments(compiled, repository, release_id)
    arguments["descriptor_path"] = "unsafe/promotion.json"

    with pytest.raises(DbtProdMirrorError):
        build_dbt_prod_mirror_service().prepare(**arguments)


@pytest.mark.parametrize(
    "reference",
    (
        "github-actions://trusted/run/\x7f",
        "x" * 2049,
    ),
)
def test_prod_mirror_rejects_unsafe_evidence_reference(
    tmp_path: Path,
    reference: str,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    arguments = _arguments(compiled, repository, release_id)
    arguments["dev_evidence_ref"] = reference

    with pytest.raises(DbtProdMirrorError):
        build_dbt_prod_mirror_service().prepare(**arguments)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dev_evidence_subject_sha256", "e" * 64),
        ("dev_evidence_artifact_name", " evidence-artifact "),
        ("dev_evidence_producer_workflow", "workflow\ninjection"),
        ("dev_evidence_source_commit", "c" * 39),
        ("dev_evidence_source_commit", "C" * 40),
    ],
)
def test_prod_mirror_rejects_malformed_trust_descriptor(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    compiled, release_id = _compiled_release(tmp_path)
    repository = _repository(tmp_path)
    arguments = _arguments(compiled, repository, release_id)
    arguments[field] = value

    with pytest.raises(DbtProdMirrorError):
        build_dbt_prod_mirror_service().prepare(**arguments)


def test_prod_promotion_schema_publishes_required_trust_descriptor() -> None:
    generated = dbt_schema_contracts()["dpone.dbt-prod-promotion.v1"]
    persisted = json.loads(
        (ROOT / "docs/schemas/dbt/dpone.dbt-prod-promotion.v1.schema.json").read_text(encoding="utf-8")
    )
    trust_fields = {
        "dev_evidence_subject_sha256",
        "dev_evidence_artifact_name",
        "dev_evidence_producer_workflow",
        "dev_evidence_source_commit",
    }

    assert persisted == generated
    assert trust_fields <= set(generated["required"])
    assert generated["properties"]["dev_evidence_source_commit"]["pattern"] == "^[0-9a-f]{40}$"


def _prepare(
    service: DbtProdMirrorService,
    compiled: Path,
    repository: Path,
    release_id: str,
):
    return service.prepare(**_arguments(compiled, repository, release_id))


def _arguments(
    compiled: Path,
    repository: Path,
    release_id: str,
) -> dict[str, Any]:
    return {
        "compiled_root": compiled,
        "repository_root": repository,
        "mirror_path": "dbt-mirror",
        "source_snapshot_path": ".dpone/dbt/source-snapshot.json",
        "descriptor_path": ".dpone/dbt/promotion.json",
        "expected_release_id": release_id,
        "dev_deployment_id": "sha256:" + "d" * 64,
        "dev_evidence_ref": "github-actions://example/dev/actions/runs/123",
        "dev_evidence_subject_sha256": DEV_EVIDENCE_SUBJECT_SHA256,
        "dev_evidence_artifact_name": DEV_EVIDENCE_ARTIFACT_NAME,
        "dev_evidence_producer_workflow": DEV_EVIDENCE_PRODUCER_WORKFLOW,
        "dev_evidence_source_commit": DEV_EVIDENCE_SOURCE_COMMIT,
    }


def _metadata_expectations(release_id: str) -> dict[str, str]:
    return {
        "expected_release_id": release_id,
        "expected_dev_deployment_id": "sha256:" + "d" * 64,
        "expected_dev_evidence_ref": "github-actions://example/dev/actions/runs/123",
        "expected_dev_evidence_subject_sha256": DEV_EVIDENCE_SUBJECT_SHA256,
        "expected_dev_evidence_artifact_name": DEV_EVIDENCE_ARTIFACT_NAME,
        "expected_dev_evidence_producer_workflow": (DEV_EVIDENCE_PRODUCER_WORKFLOW),
        "expected_dev_evidence_source_commit": DEV_EVIDENCE_SOURCE_COMMIT,
    }


def _repository(tmp_path: Path) -> Path:
    repository = tmp_path / "prod"
    (repository / ".git").mkdir(parents=True)
    return repository


def _compiled_release(tmp_path: Path) -> tuple[Path, str]:
    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )
    models = tuple(
        replace(
            item,
            route_capability={
                **dict(item.route_capability),
                "certification_level": "production-certified",
                "evidence_status": "PASS",
                "variant_id": (str(item.route_capability["route_id"]).replace(":", "_") + "_airflow_kpo"),
                "transport": "native_bcp_to_clickhouse",
                "schema_evolution": "widening",
                "airflow_runtime_mode": "kpo",
                "evidence_refs": ["sha256:" + "e" * 64],
                "evidence_reason_codes": [],
            },
        )
        for item in report.models
    )
    by_id = {item.model.unique_id: item for item in models}
    report = replace(
        report,
        models=models,
        workflows=tuple(
            replace(
                workflow,
                models=tuple(by_id[item.model.unique_id] for item in workflow.models),
            )
            for workflow in report.workflows
        ),
    )
    compiled = tmp_path / "compiled"
    written = DbtArtifactWriter(
        selection_resolver=_authoritative_selection_resolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
    ).write(report, compiled, project_root=DEMO)
    assert written.passed and written.release_id is not None
    DbtReleaseIntegrityService().write(compiled)
    return compiled, written.release_id


def _authoritative_selection_resolver() -> DbtCliSelectionResolver:
    manifest_bytes = (DEMO / "fixtures" / "manifest.v12.json").read_bytes()
    manifest = json.loads(manifest_bytes)

    def runner(args, **_kwargs):
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True)
            (target / "manifest.json").write_bytes(manifest_bytes)
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        selectors = args[args.index("--select") + 1 :]
        selected_fqns = {selector.removeprefix("+fqn:") for selector in selectors}
        selected_unique_ids = tuple(
            sorted(unique_id for unique_id, node in manifest["nodes"].items() if ".".join(node["fqn"]) in selected_fqns)
        )
        selection = ManifestPreviewSelectionResolver().resolve(
            project_root=DEMO,
            manifest_bytes=manifest_bytes,
            selected_unique_ids=selected_unique_ids,
            profiles_dir=None,
            profile_name="dpone_runtime",
            target_name="runtime",
            dbt_core_version="1.12.3",
            dbt_adapter="sqlserver",
            dbt_adapter_version="1.11.1",
        )
        stdout = b"".join(
            json.dumps({"unique_id": unique_id}).encode() + b"\n" for unique_id in selection.selected_graph_unique_ids
        )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    versions = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    return DbtCliSelectionResolver(
        runner=runner,
        package_version=versions.__getitem__,
    )
