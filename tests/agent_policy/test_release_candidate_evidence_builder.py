from __future__ import annotations

import copy
from pathlib import Path

import pytest
from tests.agent_policy import _release_commit_gate_helpers as exact_checks_producer
from tests.agent_policy._release_candidate_evidence_helpers import (
    COMMIT_SHA,
    RELEASE,
    REPOSITORY,
    ROOT,
    canonical,
    codec,
    load_module,
    policy,
    valid_source_payloads,
    write_valid_sources,
)

builder = load_module(
    "dpone_release_candidate_evidence_builder_test",
    "tools/agent_policy/release_candidate_evidence_builder.py",
)


def _build(tmp_path: Path, *, repository: str = REPOSITORY) -> tuple[dict[str, object], Path]:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "bundle"
    write_valid_sources(input_root, repository=repository)
    result = builder.build_bundle(
        root=ROOT,
        input_root=input_root,
        output_dir=output_dir,
        repository=repository,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )
    return result, output_dir


def _read(path: Path) -> dict[str, object]:
    payload, _raw = codec.read_strict_json(path, field=path.name)
    return payload


def test_builds_closed_bundle_with_exact_identity_and_source_digests(tmp_path: Path) -> None:
    result, output = _build(tmp_path)

    manifest = _read(output / builder.MANIFEST_NAME)
    pack = _read(output / builder.PACK_NAME)
    receipt = _read(output / builder.RECEIPT_NAME)

    assert result["status"] == "PASS"
    assert result["source_count"] == len(policy.SOURCE_PATHS)
    assert manifest["repository"] == pack["repository"] == receipt["repository"] == REPOSITORY
    assert manifest["commit_sha"] == pack["commit_sha"] == receipt["commit_sha"] == COMMIT_SHA
    assert manifest["release"] == pack["release"] == receipt["release"] == RELEASE
    assert manifest["profile"] == pack["profile"] == receipt["profile"] == policy.PROFILE
    assert manifest["policy_sha256"] == pack["policy_sha256"] == receipt["policy_sha256"]
    assert manifest["policy_sha256"] == policy.POLICY_SHA256
    assert codec.DIGEST.fullmatch(str(manifest["policy_sha256"])) is not None
    assert manifest["run_id"] == receipt["run_id"] == 101
    assert manifest["run_attempt"] == receipt["run_attempt"] == 2
    assert pack["status"] == "PASS"
    assert pack["decision"] == "GO"
    assert pack["required_roles"] == sorted(policy.SOURCE_PATHS)
    assert set(pack["source_digests"]) == set(policy.SOURCE_PATHS)
    assert set(pack["observations"]) == set(policy.SOURCE_PATHS)
    assert set(pack["checklist"]) == set(policy.CHECKLIST_ROLES)
    assert set(pack["checklist"].values()) == {"PASS"}
    assert receipt["manifest_sha256"] == result["manifest_sha256"]
    assert receipt["pack_sha256"] == result["pack_sha256"]
    assert receipt["binding_id"] == builder.receipt_binding_id(
        {key: value for key, value in receipt.items() if key != "binding_id"}
    )
    assert (output / builder.EXIT_NAME).read_bytes() == b"0\n"
    assert {entry["role"] for entry in manifest["entries"]} == set(policy.SOURCE_PATHS)
    for entry in manifest["entries"]:
        source = output / entry["path"]
        assert source.is_file()
        assert source.stat().st_size == entry["size_bytes"]
        assert codec.sha256_bytes(source.read_bytes()) == entry["sha256"]


def test_builds_from_actual_exact_commit_gate_report(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    report = exact_checks_producer.evaluate(exact_checks_producer.snapshot())._replace(
        policy_sha256=exact_checks_producer.POLICY_DIGEST
    )
    paths["exact_commit_checks"].write_bytes(canonical(report.to_payload()))

    result = builder.build_bundle(
        root=ROOT,
        input_root=input_root,
        output_dir=tmp_path / "bundle",
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )

    assert result["status"] == "PASS"


@pytest.mark.parametrize("status", ["UNVERIFIED", "SKIP", "FAIL"])
def test_rejects_non_pass_junit_evidence_and_leaves_no_partial_output(
    tmp_path: Path,
    status: str,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payloads = valid_source_payloads()
    payload = copy.deepcopy(payloads["service_markers"])
    payload["status"] = status
    paths["service_markers"].write_bytes(canonical(payload))
    output = tmp_path / "bundle"

    with pytest.raises(ValueError, match="identity or status"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=output,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )

    assert not output.exists()


@pytest.mark.parametrize(
    "role",
    [
        "exact_commit_checks",
        "merge_receipt",
        "mssql_clickhouse_execution",
        "mssql_clickhouse_verification",
    ],
)
def test_unverified_status_never_satisfies_a_status_bearing_source_role(
    tmp_path: Path,
    role: str,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payload = valid_source_payloads()[role]
    payload["status"] = "UNVERIFIED"
    paths[role].write_bytes(canonical(payload))

    with pytest.raises(ValueError, match="PASS|successful|state-promotable|bind"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


@pytest.mark.parametrize(
    "role",
    ["mssql_clickhouse_execution", "mssql_clickhouse_verification"],
)
def test_route_source_profile_must_match_frozen_native_transfer_campaign(
    tmp_path: Path,
    role: str,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payload = valid_source_payloads()[role]
    payload["profile"] = "real_local"
    paths[role].write_bytes(canonical(payload))

    with pytest.raises(ValueError, match="profile|identity"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_route_verification_dataset_must_match_bound_execution_bytes(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    verification = valid_source_payloads()["mssql_clickhouse_verification"]
    verification["dataset"] = "different-dataset"
    paths["mssql_clickhouse_verification"].write_bytes(canonical(verification))

    with pytest.raises(ValueError, match="dataset"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


@pytest.mark.parametrize("field", ["profile", "commit_sha"])
def test_junit_identity_must_match_frozen_campaign_and_exact_commit(
    tmp_path: Path,
    field: str,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payload = valid_source_payloads()["cdc_state_junit"]
    payload[field] = "real_local" if field == "profile" else "b" * 40
    paths["cdc_state_junit"].write_bytes(canonical(payload))

    with pytest.raises(ValueError, match="identity or status"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_rejects_missing_required_role_instead_of_shrinking_role_set(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    paths["cdc_state_junit"].unlink()

    with pytest.raises(ValueError, match="cdc_state_junit.*unavailable"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


@pytest.mark.parametrize("role", ["exact_commit_checks", "merge_receipt"])
def test_rejects_source_repository_mismatch(tmp_path: Path, role: str) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payload = valid_source_payloads()[role]
    payload["repository"] = "attacker/fork"
    paths[role].write_bytes(canonical(payload))

    with pytest.raises(ValueError, match="repositories must match"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("check_run_id", 0, "positive integer"),
        ("workflow_run_attempt", True, "positive integer"),
        ("artifact_size_bytes", -1, "positive integer"),
        ("github_app_id", 1, "application is invalid"),
    ],
)
def test_rejects_invalid_merge_receipt_provider_identity(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    payload = valid_source_payloads()["merge_receipt"]
    payload[field] = value
    paths["merge_receipt"].write_bytes(canonical(payload))

    with pytest.raises(ValueError, match=message):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_rejects_verification_not_bound_to_exact_execution_bytes(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    verification = valid_source_payloads()["mssql_clickhouse_verification"]
    verification["execution_sha256"] = "0" * 64
    paths["mssql_clickhouse_verification"].write_bytes(canonical(verification))

    with pytest.raises(ValueError, match="does not bind execution bytes"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_duplicate_metric_names_cannot_replace_a_required_observation(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    stress = valid_source_payloads()["stress_benchmark"]
    stress["metrics"].append(copy.deepcopy(stress["metrics"][0]))
    paths["stress_benchmark"].write_bytes(canonical(stress))

    with pytest.raises(ValueError, match="duplicate|inventory"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_same_named_junit_case_from_wrong_file_cannot_satisfy_frozen_inventory(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    paths = write_valid_sources(input_root)
    junit = valid_source_payloads()["cdc_state_junit"]
    original = junit["cases"][0]["node_id"]
    function = original.rsplit("::", 1)[-1]
    junit["cases"][0]["node_id"] = f"tests/attacker/test_false_green.py::{function}"
    paths["cdc_state_junit"].write_bytes(canonical(junit))

    with pytest.raises(ValueError, match="case inventory"):
        builder.build_bundle(
            root=ROOT,
            input_root=input_root,
            output_dir=tmp_path / "bundle",
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_does_not_overwrite_existing_authority_directory(tmp_path: Path) -> None:
    _result, output = _build(tmp_path)
    sentinel = output / "operator-note.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    with pytest.raises(ValueError, match="already exists"):
        builder.build_bundle(
            root=ROOT,
            input_root=tmp_path / "inputs",
            output_dir=output,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )

    assert sentinel.read_text(encoding="utf-8") == "preserve"
