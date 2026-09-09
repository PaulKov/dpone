from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "feature-design-ci-shadow-pr5b-completed-producer-verifier.md"
PARENT = ROOT / "docs" / "feature-design-ci-pr-gate-exact-sha-evidence.md"
PR5A = ROOT / "docs" / "feature-design-ci-shadow-pr5a-candidate-manifest.md"
ADR_0048 = ROOT / "docs" / "adr" / "0048-exact-sha-readiness-evidence.md"
TASK = ROOT / "test_artifacts" / "agent-policy" / "ci-shadow-pr5b-design-task-contract.yml"
POLICY = ROOT / ".agents" / "policy" / "github-branch-protection.yml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _normalized(path: Path) -> str:
    return " ".join(_read(path).split())


def _domain_digest(domain: str, value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return f"sha256:{hashlib.sha256(domain.encode('ascii') + bytes([0]) + payload).hexdigest()}"


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")


def _markdown_table_rows_after(path: Path, anchor: str) -> list[tuple[str, ...]]:
    tail = _read(path).split(anchor, maxsplit=1)[1]
    lines = tail.splitlines()
    first = next(index for index, line in enumerate(lines) if line.startswith("|"))
    rows: list[tuple[str, ...]] = []
    for line in lines[first + 2 :]:
        if not line.startswith("|"):
            break
        rows.append(tuple(cell.strip() for cell in line.strip("|").split("|")))
    return rows


def test_pr5b_design_lifecycle_and_links_are_explicit() -> None:
    spec = _read(SPEC)
    parent = _read(PARENT)

    assert "- Status: RESEARCHED" in spec
    assert "2046eab67b432d8caf0fb615f142fae2a8d75d54" in spec
    assert "two P0 implementability gaps" in spec
    assert "a fresh exact-head approval is required" in spec
    assert "- [ ] Maintainer changes status to `APPROVED`" in spec
    assert "feature-design-ci-pr-gate-exact-sha-evidence.md" in spec
    assert "feature-design-ci-shadow-pr5a-candidate-manifest.md" in spec
    assert "feature-design-ci-shadow-pr5b-completed-producer-verifier.md" in parent
    assert "- Implementation status: IN PROGRESS" in parent


def test_pr5b_workflow_triggers_names_and_boundaries_are_frozen() -> None:
    spec = _normalized(SPEC)

    for expected in (
        ".github/workflows/exact-sha-candidate.yml",
        "name: Exact SHA candidate",
        "push.branches: [master]",
        "Build exact SHA candidate",
        ".github/workflows/exact-sha-compatibility.yml",
        "name: Exact SHA compatibility",
        "workflow_run.workflows: [Exact SHA candidate]",
        "types: [completed]",
        "Authenticate completed candidate",
        "Evaluate exact SHA compatibility",
        "fail-fast:false",
        "max-parallel:2",
        "max-parallel:1",
        "enable-cache:false",
        "UV_NO_CACHE=1",
        "PIP_NO_CACHE_DIR=1",
    ):
        assert expected in spec

    assert "trusted data-only" in spec
    assert "unprivileged cacheless" in spec
    assert "no secret, inherited secret, write permission, OIDC" in spec
    assert "run with `GITHUB_TOKEN`/`GH_TOKEN` absent" in spec
    assert "trigger exactly `workflow_dispatch`" in spec
    assert "does not extend the one-hop candidate chain" in spec
    assert "workflow_run.workflows: [Exact SHA compatibility]" not in spec


def test_pr5b_profile_is_exactly_eight_airflow_and_two_runtime_cases() -> None:
    spec = _read(SPEC)
    exact_rows = (
        "| `airflow-2.10.5-py3.11` | 2.10.5 | 3.11 | 10.1.0 | N/A | N/A | compatibility |",
        "| `airflow-2.10.5-py3.12` | 2.10.5 | 3.12 | 10.1.0 | N/A | N/A | compatibility |",
        "| `airflow-2.11.0-py3.11` | 2.11.0 | 3.11 | 10.5.0 | 4.7.0 | 1.15.0 | compatibility |",
        "| `airflow-2.11.0-py3.12` | 2.11.0 | 3.12 | 10.5.0 | 4.7.0 | N/A | compatibility |",
        "| `airflow-3.2.0-py3.11` | 3.2.0 | 3.11 | 10.14.0 | 4.7.0 | N/A | primary |",
        "| `airflow-3.2.0-py3.12` | 3.2.0 | 3.12 | 10.14.0 | 4.7.0 | N/A | primary |",
        "| `airflow-3.3.0-py3.11` | 3.3.0 | 3.11 | 10.20.0 | 4.7.0 | N/A | latest |",
        "| `airflow-3.3.0-py3.12` | 3.3.0 | 3.12 | 10.20.0 | 4.7.0 | 1.15.0 | latest |",
        "| `runtime-wheel-smoke-py3.11` | N/A | 3.11 | N/A | N/A | N/A | runtime | N/A |",
        "| `runtime-wheel-smoke-py3.12` | N/A | 3.12 | N/A | N/A | N/A | runtime | N/A |",
    )

    for row in exact_rows:
        assert row in spec
    assert spec.count("peeled official constraint tag commit") == 1
    assert "5d78b83985da76882c7581e8b3829b8abf0f33cc" in spec
    assert "dce316a589d156b364eda656b65ee197298c5bfd" in spec
    assert "sha256:be9e20197c2a398101bd15b58e943403dd1bc8ef8bc8ece46969c424205cc853" in spec


def test_pr5b_raw_tar_and_subject_identity_amendment_preserve_pr5a_v1() -> None:
    combined = " ".join((_normalized(SPEC), _normalized(PR5A), _normalized(ADR_0048)))

    for expected in (
        "dpone.compatibility-candidate.v1",
        "raw USTAR",
        "archive:false",
        "overwrite:false",
        "retention-days:90",
        "exactly four",
        "1,073,872,896",
        "provider-authenticated",
        "preserves manifest v1",
        "does not use an embedded SHA as authority",
        "Exactly two all-zero terminal records",
        "base-256 numeric fields",
    ):
        assert expected in combined

    assert "only the unprivileged" in combined
    assert "trusted preflight and evaluation remain data-only" in combined
    assert "embedded\nsubject SHA" not in _read(PARENT)


def test_pr5b_cli_schemas_identity_and_create_new_semantics_are_closed() -> None:
    spec = _normalized(SPEC)

    for command in (
        "build_candidate_archive.py",
        "preflight_exact_sha_compatibility.py",
        "download_exact_sha_preflight.py",
        "emit_exact_sha_preflight_outputs.py",
        "run_exact_sha_compatibility_case.py",
        "evaluate_exact_sha_compatibility.py",
    ):
        assert command in spec
    for schema in (
        "dpone.exact-sha-candidate-preflight.v1",
        "dpone.exact-sha-compatibility-case.v1",
        "dpone.exact-sha-compatibility-receipt.v1",
        "dpone.exact-sha-current-workflow-coordinate.v1",
        "dpone.exact-sha-certification-request.v1",
        "dpone.exact-sha-certification-coordinate-set.v1",
        "dpone.exact-sha-compatibility-certification.v1",
        "dpone.exact-sha-compatibility-campaign.v1",
    ):
        assert schema in spec
    for path in (
        "exact-sha-candidate-preflight-v1.schema.json",
        "exact-sha-compatibility-case-v1.schema.json",
        "exact-sha-compatibility-receipt-v1.schema.json",
        "exact-sha-current-workflow-coordinate-v1.schema.json",
        "exact-sha-certification-request-v1.schema.json",
        "exact-sha-certification-coordinate-set-v1.schema.json",
        "exact-sha-compatibility-certification-v1.schema.json",
        "exact-sha-compatibility-campaign-v1.schema.json",
        "exact-sha-candidate-preflight-v1.json",
        "exact-sha-compatibility-case-v1.json",
        "exact-sha-compatibility-receipt-v1.json",
        "exact-sha-compatibility-certification-v1.json",
        "exact-sha-compatibility-campaign-v1.json",
    ):
        assert path in spec
    for identity in (
        "`repository_id`",
        "`producer_workflow_id`",
        "`producer_workflow_revision_sha`",
        "`producer_workflow_blob_sha256`",
        "`artifact_id`",
        "`provider_digest`",
        "`verifier_workflow_id`",
        "`verifier_revision_sha`",
        "`profile_digest`",
    ):
        assert identity in spec

    assert "Usage errors exit 2" in spec
    assert "exit 0 with empty stdout/stderr" in spec
    assert "domain non-pass exits 1" in spec
    assert "No command overwrites" in spec
    assert "canonical UTF-8 JSON plus one newline" in spec
    assert "write_candidate_archive(" in spec
    assert "extract_candidate_archive(" in spec
    assert "preflight_completed_producer(" in spec
    assert "acquire_candidate_artifact(" in spec
    assert "execute_compatibility_case(" in spec
    assert "fold_compatibility_decision(" in spec
    assert "evaluate_attempt(" in spec
    assert "certify_compatibility_attempt(" in spec
    assert "collect_compatibility_campaign(" in spec


def test_pr5b_normative_json_examples_are_parseable_and_complete() -> None:
    blocks = re.findall(r"```json\n(.*?)\n```", _read(SPEC), flags=re.DOTALL)

    assert len(blocks) == 5
    preflight, case, final, unverified_preflight, unverified_final = (json.loads(block) for block in blocks)
    assert preflight["schema_version"] == "dpone.exact-sha-candidate-preflight.v1"
    assert preflight["candidate"]["inventory_digest"] is None
    assert case["schema_version"] == "dpone.exact-sha-compatibility-case.v1"
    assert case["candidate"]["inventory_digest"].startswith("sha256:")
    assert case["dependency_plan_digest"].startswith("sha256:")
    assert case["dependency_inventory_digest"].startswith("sha256:")
    assert case["materialized_plan_digest"].startswith("sha256:")
    assert case["materialized_plan_observation_status"] == "VERIFIED"
    assert case["dependency_observation_status"] == "VERIFIED"
    assert case["resolver_input_observation_status"] == "VERIFIED"
    assert case["resolver_input_inventory_digest"].startswith("sha256:")
    assert [item["resolution_environment_id"] for item in case["resolver_sessions"]] == [
        "airflow-base",
        "scheduler-extras",
        "dbt",
    ]
    assert all(item["status"] == "VERIFIED" for item in case["resolver_sessions"])
    resolver_session_fields = {
        "bootstrap_digest",
        "bootstrap_observation",
        "candidate_inventory_digest",
        "cleanup_journal_digest",
        "cleanup_observation_digest",
        "cleanup_status",
        "dependency_plan_digest",
        "dependency_resolution_digest",
        "exit_code",
        "launcher_sha256",
        "leaf_bytes",
        "leaf_count",
        "materialized_plan_digest",
        "output_inventory_digest",
        "protocol_digest",
        "provisioner_sha256",
        "process_observation_digest",
        "recorded_mount_inventory_digest",
        "recorded_process_inventory_digest",
        "retired_cleanup_journal_digest",
        "rendezvous_auth_digest",
        "resolution_environment_id",
        "resolver_input_inventory_digest",
        "resource_limit",
        "root_lifecycle_evidence_digest",
        "root_pid",
        "root_start_time",
        "runtime_inventory_digest",
        "sequence",
        "signal",
        "staging_id",
        "staging_launcher_inode",
        "status",
        "supervisor_pid",
        "supervisor_start_time",
        "timed_out",
    }
    assert all(set(item) == resolver_session_fields for item in case["resolver_sessions"])
    assert all(item["bootstrap_digest"].startswith("sha256:") for item in case["resolver_sessions"])
    assert all(
        item["bootstrap_observation"]["schema_version"] == "dpone.exact-sha-resolver-bootstrap-observation.v1"
        for item in case["resolver_sessions"]
    )
    assert all(
        item["resolver_input_inventory_digest"] == case["resolver_input_inventory_digest"]
        for item in case["resolver_sessions"]
    )
    assert case["runtime_observation_status"] == "VERIFIED"
    assert case["verifier_snapshot_observation_status"] == "VERIFIED"
    assert case["launcher_sources_observation_status"] == "VERIFIED"
    assert [item["relative_path"] for item in case["launcher_sources"]] == [
        "tools/ci/exact_sha_dependency_root_launcher.py",
        "tools/ci/exact_sha_sandbox_root_launcher.py",
    ]
    assert all(
        item["verifier_inventory_digest"] == case["verifier_inventory_digest"] for item in case["launcher_sources"]
    )
    assert case["session_observation_status"] == "VERIFIED"
    assert case["failed_operation_id"] is None
    assert case["failure_class"] is None
    assert case["runtime"]["major_minor"] == "3.11"
    assert case["session"]["terminal_status"] == "VERIFIED"
    assert case["session"]["close_reason"] == "COMPLETE"
    assert case["session"]["product_stop_sequence"] is None
    assert case["session"]["bootstrap_observation"]["schema_version"] == (
        "dpone.exact-sha-sandbox-bootstrap-observation.v1"
    )
    assert [(item["attempt_kind"], item["attempt_ordinal"]) for item in case["root_lifecycle_evidence"]] == [
        ("RESOLVER", 0),
        ("RESOLVER", 1),
        ("RESOLVER", 2),
        ("SANDBOX", 0),
    ]
    process_fields = {
        "schema_version",
        "stage_pid",
        "stage_start_time",
        "stage_wait_status",
        "outer_pid",
        "outer_start_time",
        "outer_wait_status",
        "root_peer",
        "namespace",
        "journal_status",
        "staging_identity",
        "cleanup_journal_digest",
        "recorded_process_inventory_digest",
        "recorded_mount_inventory_digest",
        "recorded_privileged_descendant_count",
        "reaped_privileged_descendant_count",
        "nonzero_privileged_descendant_count",
        "terminate_helper_pid",
        "terminate_helper_start_time",
        "terminate_helper_wait_status",
        "stage_term_sent",
        "stage_kill_sent",
        "stage_reaped",
        "stage_group_absent",
        "outer_term_sent",
        "outer_kill_sent",
        "outer_reaped",
        "outer_group_absent",
        "status",
        "diagnostic_code",
    }
    cleanup_fields = {
        "schema_version",
        "staging_nonce",
        "staging_identity",
        "status",
        "process_observation_digest",
        "cleanup_journal_digest",
        "recorded_process_inventory_digest",
        "recorded_mount_inventory_digest",
        "recorded_privileged_descendant_count",
        "reaped_privileged_descendant_count",
        "nonzero_privileged_descendant_count",
        "retired_cleanup_journal_digest",
        "retired_stage",
        "root_peer",
        "namespace",
        "recorded_processes_absent",
        "recorded_descendants_absent",
        "recorded_mounts_absent",
        "mounts_unmounted",
        "stage_absent",
        "retired_stage_present",
        "cleanup_helper_response_digest",
        "cleanup_helper_attempts",
        "diagnostic_code",
    }
    lifecycle_digest_references = [
        *(item["root_lifecycle_evidence_digest"] for item in case["resolver_sessions"]),
        case["session"]["root_lifecycle_evidence_digest"],
    ]
    for lifecycle, lifecycle_digest_reference in zip(
        case["root_lifecycle_evidence"], lifecycle_digest_references, strict=True
    ):
        assert set(lifecycle) == {
            "schema_version",
            "attempt_kind",
            "attempt_ordinal",
            "process_observation",
            "process_observation_digest",
            "cleanup_observation",
            "cleanup_observation_digest",
        }
        assert set(lifecycle["process_observation"]) == process_fields
        assert set(lifecycle["cleanup_observation"]) == cleanup_fields
        process_observation = lifecycle["process_observation"]
        cleanup_observation = lifecycle["cleanup_observation"]
        assert set(process_observation["root_peer"]) == {
            "role",
            "pid",
            "start_time",
            "process_group",
            "wait_status",
            "state",
        }
        assert set(process_observation["namespace"]) == set(process_observation["root_peer"])
        assert set(process_observation["staging_identity"]) == {
            "staging_id",
            "staging_directory_device",
            "staging_directory_inode",
            "cleanup_journal_device",
            "cleanup_journal_inode",
            "cleanup_journal_genesis_digest",
        }
        assert set(cleanup_observation["retired_stage"]) == {
            "retired_basename",
            "directory_device",
            "directory_inode",
            "journal_device",
            "journal_inode",
        }
        assert len(cleanup_observation["cleanup_helper_attempts"]) == 1
        cleanup_helper_attempt = cleanup_observation["cleanup_helper_attempts"][0]
        assert set(cleanup_helper_attempt) == {
            "attempt_ordinal",
            "pid",
            "start_time",
            "wait_status",
            "response_status",
            "reported_status",
            "response_digest",
        }
        assert cleanup_helper_attempt["attempt_ordinal"] == 0
        assert cleanup_helper_attempt["pid"] > 0
        assert cleanup_helper_attempt["start_time"] > 0
        assert cleanup_helper_attempt["wait_status"] == 0
        assert cleanup_helper_attempt["response_status"] == "VERIFIED"
        assert cleanup_helper_attempt["reported_status"] == "CLEANED"
        assert cleanup_helper_attempt["response_digest"] == cleanup_observation["cleanup_helper_response_digest"]
        helper_response = {
            key: value
            for key, value in cleanup_observation.items()
            if key not in {"schema_version", "cleanup_helper_response_digest", "cleanup_helper_attempts"}
        }
        helper_response["schema_version"] = "dpone.exact-sha-root-cleanup-helper-response.v1"
        assert cleanup_observation["cleanup_helper_response_digest"] == _domain_digest(
            "dpone.exact-sha-root-cleanup-helper-response.v1", helper_response
        )
        assert process_observation["root_peer"]["process_group"] == process_observation["outer_pid"]
        assert process_observation["namespace"]["process_group"] == process_observation["outer_pid"]
        assert cleanup_observation["process_observation_digest"] == lifecycle["process_observation_digest"]
        assert lifecycle["process_observation_digest"] == _domain_digest(
            "dpone.exact-sha-root-process-observation.v1", process_observation
        )
        assert lifecycle["cleanup_observation_digest"] == _domain_digest(
            "dpone.exact-sha-root-cleanup-observation.v1", cleanup_observation
        )
        assert lifecycle_digest_reference == _domain_digest(
            "dpone.exact-sha-root-attempt-lifecycle-evidence.v1", lifecycle
        )
    for resolver_session, lifecycle in zip(case["resolver_sessions"], case["root_lifecycle_evidence"][:3], strict=True):
        assert resolver_session["process_observation_digest"] == lifecycle["process_observation_digest"]
        assert resolver_session["cleanup_observation_digest"] == lifecycle["cleanup_observation_digest"]
        assert (
            resolver_session["retired_cleanup_journal_digest"]
            == lifecycle["cleanup_observation"]["retired_cleanup_journal_digest"]
        )
    assert (
        case["session"]["process_observation_digest"]
        == case["root_lifecycle_evidence"][-1]["process_observation_digest"]
    )
    assert (
        case["session"]["cleanup_observation_digest"]
        == case["root_lifecycle_evidence"][-1]["cleanup_observation_digest"]
    )
    assert set(case["session"]) == {
        "bootstrap_digest",
        "bootstrap_observation",
        "bootstrap_status",
        "candidate_inventory_digest",
        "cleanup_journal_digest",
        "cleanup_observation_digest",
        "cleanup_status",
        "close_reason",
        "command_count",
        "dependency_inventory_digest",
        "launcher_sha256",
        "materialized_plan_digest",
        "product_stop_sequence",
        "protocol_digest",
        "provisioner_sha256",
        "process_observation_digest",
        "recorded_mount_inventory_digest",
        "recorded_process_inventory_digest",
        "retired_cleanup_journal_digest",
        "rendezvous_auth_digest",
        "root_lifecycle_evidence_digest",
        "root_pid",
        "root_start_time",
        "runtime_inventory_digest",
        "sequence",
        "staging_id",
        "staging_launcher_inode",
        "supervisor_pid",
        "supervisor_start_time",
        "terminal_status",
        "verifier_inventory_digest",
        "verifier_tree_oid",
    }
    assert case["session"]["verifier_inventory_digest"] == case["verifier_inventory_digest"]
    assert [item["command_id"] for item in case["commands"]] == [
        "verify-constraint",
        "install-airflow",
        "pip-check-airflow",
        "install-candidate-provider",
        "pip-check-candidate-provider",
        "verify-provider-origins",
        "airflow-pytest",
        "airflow-parse-slo",
        "install-candidate-dbt",
        "pip-check-candidate-dbt",
        "verify-dbt-runtime",
    ]
    assert all(item["sandbox_status"] == "VERIFIED" for item in case["commands"])
    assert all(len(item["argv_digest"]) == 71 for item in case["commands"])
    assert all(len(item["operation_plan_digest"]) == 71 for item in case["commands"])
    assert final["schema_version"] == "dpone.exact-sha-compatibility-receipt.v1"
    assert unverified_preflight["producer"] is None
    assert unverified_preflight["producer_observation"]["event"] == "workflow_dispatch"
    assert unverified_final["decision"] == "UNVERIFIED"
    assert unverified_final["cases"] == []
    assert len(final["cases"]) == 10
    assert [item["case_id"] for item in final["cases"]] == [
        "airflow-2.10.5-py3.11",
        "airflow-2.10.5-py3.12",
        "airflow-2.11.0-py3.11",
        "airflow-2.11.0-py3.12",
        "airflow-3.2.0-py3.11",
        "airflow-3.2.0-py3.12",
        "airflow-3.3.0-py3.11",
        "airflow-3.3.0-py3.12",
        "runtime-wheel-smoke-py3.11",
        "runtime-wheel-smoke-py3.12",
    ]


def test_pr5b_uncertainty_precedence_and_full_rerun_are_non_negotiable() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "Uncertainty always outranks authenticated failure",
        "missing executor receipt is infrastructure uncertainty",
        "must rerun **all jobs**",
        "selective/failed-only reruns",
        "prior-attempt receipts are `UNVERIFIED`",
        "`VERIFIER_DECISION_CONFLICT/INVESTIGATE_AND_CREATE_FRESH_PRODUCER_ATTEMPT`",
        "Receipt bytes or a workflow conclusion alone never become authority",
        "classifies the authenticated terminal conclusion before querying candidate artifacts",
        "One PR5B evaluator observes only its own verifier run/attempt",
        "Shared `producer_observation` always exists",
        "`MISSING|INVALID|VERIFIED`",
        "When preflight is not `READY`, `cases` is empty",
    ):
        assert expected in spec


def test_pr5b_freezes_actions_jobs_harness_and_capability_separation() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",
        "astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "`actions/download-artifact` is deliberately not used",
        "download_exact_sha_candidate.py",
        "needs: [preflight, airflow-compat, runtime-wheel-smoke]",
        "write case receipt → upload case receipt → enforce",
        "write final receipt → upload final receipt → enforce",
        "dpone.ports.github_ci_shadow_compatibility_metadata",
        "dpone.ports.candidate_artifact_reader",
        "dpone.ports.closed_json_evidence_reader",
        "dpone.ports.fixed_public_http",
        "cannot be constructed with a candidate-byte capability",
        "profile_digest` is `sha256(domain || canonical_json",
        "verifier_tree_oid",
        "There is no partial handwritten harness allowlist",
        "VerifierJobObservationV1",
        "WorkflowRunEventV1",
        "eleven thin internal composition roots",
        "CandidateAcquisitionSnapshotV1",
        "CandidateSandboxV1",
        "CandidateSandboxFactoryV1",
        "CandidateCasePlanMaterializerV1",
        "RootLauncherProvisionerV1",
        "CandidateDependencyPreparerV1",
        "CandidateDependencySnapshotV1",
        "WorkflowCommandOutputWriterV1",
        "emit_preflight_job_outputs",
        "stream_exact_artifact",
        "read_exact_receipt",
        "seal_read_only",
        "case_command_plans",
        "dpone.exact-sha-command.v1",
        "exact_sha_case_step.py",
        "exact_sha_sandbox_root_launcher.py",
        "Airflow 2.11.0-py3.11",
        "Airflow 3.3.0-py3.11",
        "Runtime py3.11/py3.12",
    ):
        assert expected in spec
    assert "--case-receipts" not in spec
    assert "| `tools/ci/**` |" not in _read(SPEC)


def test_pr5b_preflight_handoff_and_dependency_preparation_are_implementable() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "DirectJsonArtifactCoordinateV1",
        "DirectJsonArtifactCoordinateFieldsV1",
        "PreflightAcquisitionV1",
        "acquire_preflight_receipt(",
        "`VERIFIED|MISSING|INVALID`",
        "any non-empty proper subset means `INVALID` without a provider read",
        "before any reader call",
        "preflight_artifact_id",
        "preflight_payload_sha256",
        "Neither state falls back to another artifact or attempt",
        "prepare_and_seal_dependencies_without_provider_token",
        "CandidateExtractionSnapshotV1",
        "`archive` and `directory`",
        "dependency_plan_digest",
        "dependency_inventory_digest",
        "dpone.exact-sha-dependency-plan.v1",
        "dpone.exact-sha-dependency-inventory.v1",
        "https://pypi.org/simple",
        "`resolver_version` (const `26.2.1`)",
        "pip-26.2.1-py3-none-any.whl",
        "sha256:71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e",
        "1,816,632",
        "No executable is resolved through `PATH`",
        "--isolated download",
        "rejected direct references",
        "one monotonic per-case dependency-preparation budget",
        "counters never reset per resolution",
        "--no-index --find-links /opt/dependencies",
        "tokenless dependency preparer",
        "post-upload step invokes `download_exact_sha_preflight.py`",
        "/usr/bin/sudo --non-interactive",
        "exact_sha_sandbox_root_launcher.py",
        "AF_UNIX|SOCK_SEQPACKET",
        "exactly five `SCM_RIGHTS` descriptors",
        "/opt/dependencies",
        "/opt/verifier",
        "SandboxSessionObservationV1",
        "import rules forbid candidate-dependencies or I/O-port imports",
        "preflight producer gets 16 and its separate post-upload direct-JSON readback gets 8",
        "gets 54",
        "direct-JSON/metadata partitions sum to 128",
        "complete Workflow B ceiling is exactly 178",
        "unavailable sandbox remains `UNVERIFIED`",
    ):
        assert expected in spec

    assert (
        "--preflight <preflight.json>"
        not in _read(SPEC).split("python tools/ci/evaluate_exact_sha_compatibility.py", maxsplit=1)[1]
    )
    assert "CandidateDependencyPreparerV1.prepare(" in spec
    assert "inspections: CandidateWheelInspectionSetV1" in spec
    assert ") -> CandidateDependencySnapshotV1 | CandidateDependencyFailureV1" in spec
    assert "CandidateSandboxFactoryV1.create( *, case_id: str, profile: CompatibilityProfileV1" in spec
    assert "verifier: VerifierTreeSnapshotV1" in spec
    assert "runtime: PythonRuntimeSnapshotV1" in spec
    assert (
        "mutable path"
        not in spec.split("`CandidateExtractionSnapshotV1` has exactly", maxsplit=1)[1].split(
            "`CandidateDependencyPreparerV1`", maxsplit=1
        )[0]
    )


def test_pr5b_dependency_resolutions_split_constraints_and_name_exact_targets() -> None:
    spec = _normalized(SPEC)
    dependency_table = spec.split(
        "| Case | Ordered resolution; exact candidate targets; ordered requirements; constraint |",
        maxsplit=1,
    )[1].split("The `plan_digest`", maxsplit=1)[0]

    for expected in (
        "`airflow-base`; targets `()`",
        "`cncf-negative-control`; targets (`apache-airflow-providers-dpone` extras `()`, `dpone-airflow-pack` extras `()`)",
        "`scheduler-extras`; targets (`apache-airflow-providers-dpone` extras `()`, `dpone-airflow-pack` extras `()`)",
        'targets (`dpone` extras `("dbt-mssql",)`, `dpone-airflow-pack` extras `()`)',
        "constraint derived",
        "constraint null",
        "Only `airflow-base` and `cncf-negative-control` resolutions",
        "official Airflow constraint pins",
    ):
        assert expected in dependency_table

    assert "; provider, pack;" not in dependency_table
    assert "the same tuple" not in dependency_table
    assert "Every Airflow scheduler/negative-control resolution" not in dependency_table

    descriptor = spec.split("Airflow plans use, in order,", maxsplit=1)[1].split("Runtime plans use", maxsplit=1)[0]
    negative_order = (
        "`install-negative-control-base`",
        "`pip-check-negative-control-base`",
        "`install-candidate-negative-control`",
        "`pip-check-candidate-negative-control`",
        "`cncf-negative-control`",
        "`install-airflow`",
    )
    assert [descriptor.index(value) for value in negative_order] == sorted(
        descriptor.index(value) for value in negative_order
    )


def test_pr5b_runtime_constraints_and_root_protocol_are_capability_complete() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "VerifierTreeSnapshotV1",
        "PythonRuntimeSnapshotV1",
        "VerifiedCheckoutSnapshotFactoryV1.capture",
        "CurrentPythonRuntimeSnapshotFactoryV1.capture",
        "system `/usr/bin/python` is never substituted",
        "exactly five `SCM_RIGHTS` descriptors",
        "sealed setup-python runtime",
        "/opt/python",
        "constraint-source.txt",
        "constraint-derived.txt",
        "pip-26.2.1-py3-none-any.whl",
        "dpone.exact-sha-sandbox-ready.v1",
        "dpone.exact-sha-sandbox-command-observation.v1",
        "dpone.exact-sha-sandbox-close.v1",
        "unsigned eight-byte big-endian canonical-JSON length",
        "MSG_TRUNC",
        "MSG_CTRUNC",
        "authenticated rendezvous descriptor across the full session",
        "candidate child closes it",
        "runtime digest is bound into the persisted dependency inventory digest",
    ):
        assert expected in spec

    assert "exactly three `SCM_RIGHTS` descriptors" not in spec
    assert "over a dedicated pipe" not in spec
    assert "closes every inherited descriptor except" not in spec


def test_pr5b_resolver_metadata_and_operation_plans_are_executable() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "BoundedResolverProcessV1",
        "exact_sha_dependency_root_launcher.py",
        "--retries 0 --timeout 60",
        "Pip's internal HTTPS requests and deleted temporary files are deliberately not claimed",
        "size=5GiB",
        "nr_inodes=8192",
        "Equality is accepted; `N+1` is rejected",
        "CandidateWheelInspectorV1",
        "Exactly one ASCII `<normalized-distribution>-<version>.dist-info/METADATA`",
        "each ratio is at most 20:1",
        "DEPENDENCY_METADATA_INVALID",
        "SandboxOperationV1",
        "operation_plan_digest",
        "INFRASTRUCTURE|PRODUCT",
        "There is no cleanup or continue-on-failure command",
        "nonzero `PRODUCT` operation stops immediately",
    ):
        assert expected in spec

    assert "runs `uv pip check`" not in spec
    assert "every HTTP dispatch/redirect/retry and response body" not in spec


def test_pr5b_resolver_inputs_launchers_and_wire_are_explicit_capabilities() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "ResolverInputSnapshotV1",
        "ResolverInputPreparerV1",
        "dpone.exact-sha-resolver-input-inventory.v1",
        "VerifiedRootLauncherV1",
        "VerifiedRootLauncherFactoryV1.capture",
        "RootLauncherProvisionerV1",
        "root-owned staging",
        "sealed-memfd namespace re-exec",
        "/proc/self/fd/3",
        "dpone.exact-sha-resolver-bootstrap.v1",
        "dpone.exact-sha-resolver-leaf.v1",
        "dpone.exact-sha-resolver-ack.v1",
        "dpone.exact-sha-resolver-terminal.v1",
        "dpone.exact-sha-resolver-protocol.v1",
        "ResolvedDependencyStreamV1.abort()",
        "proves `/proc` has no owned survivor",
        "RLIMIT_AS=12GiB",
        "RLIMIT_NPROC=256",
        "RLIMIT_NOFILE=512",
        "logical admission ceiling is 2 GiB",
    ):
        assert expected in spec

    assert "receives sealed candidate and Python-runtime descriptors plus the verified constraint leaves" not in spec
    assert "/usr/bin/sudo --non-interactive --preserve-fds=3" not in spec


def test_pr5b_operation_and_snapshot_evidence_is_unambiguous() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "exactly one closed `SandboxOperationV1`",
        "`operation_id` (equal to `command_id`)",
        "Pre-READY venv, ensurepip, pip bootstrap and origin checks",
        "Invalid short aliases such as `dpone.whl` are forbidden",
        "MaterializedCasePlanV1",
        "compatibility-profile-v1.json",
        "has no repository-package import",
        "nullable `failed_operation_id`",
        "nullable `failure_class`",
        "nullable closed `session`",
        "nullable `verifier_inventory_digest`",
        "dpone.exact-sha-tree-inventory.v1",
        "Verifier capture permits at most 32,768 files",
        "runtime capture permits at most 65,536 files",
        "packaging>=25,<26",
        "exact 25.0",
    ):
        assert expected in spec

    assert "ordered non-empty `operations` tuple" not in spec
    assert "verifier_destination: ConfinedOutputDirectoryV1" in spec
    assert "runtime_destination: ConfinedOutputDirectoryV1" in spec
    assert "verifier_factory: VerifiedCheckoutSnapshotFactoryV1" in spec
    assert "runtime_factory: CurrentPythonRuntimeSnapshotFactoryV1" in spec


def test_pr5b_post_upload_certification_has_a_trusted_producer() -> None:
    spec = _normalized(SPEC)

    for expected in (
        ".github/workflows/exact-sha-compatibility-certify.yml",
        "Exact SHA compatibility certification",
        "certify_exact_sha_compatibility.py",
        "collect_exact_sha_compatibility_campaign.py",
        "dpone.exact-sha-compatibility-certification.v1",
        "dpone.exact-sha-compatibility-campaign.v1",
        "certify_compatibility_attempt",
        "collect_compatibility_campaign",
        "sole producers of certification receipt/manifest candidates",
        "trigger exactly `workflow_dispatch`",
        "exactly eight `coordinates`",
        "provider-created next attempt necessarily contains only that job and its dependents",
        "never selects latest",
    ):
        assert expected in spec


def test_pr5b_architecture_removes_manifest_port_cycle_and_names_certification() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "dpone.contracts.ci_shadow_candidate",
        "do not import the manifest implementation",
        "VerifiedCheckoutSnapshotFactoryV1",
        "WorkflowArtifactObservationV1",
        "tests/test_ci_shadow_pr5b_dependencies.py",
        "tests/test_ci_shadow_pr5b_preflight_outputs.py",
        "tests/test_ci_shadow_pr5b_candidate_metadata.py",
        "tests/test_ci_shadow_pr5b_resolver_protocol.py",
        "tests/test_ci_shadow_pr5b_runtime_snapshot.py",
        "tests/test_ci_shadow_pr5b_sandbox_protocol.py",
        "tests/test_ci_shadow_pr5b_certification.py",
        "tests/test_ci_shadow_pr5b_campaign.py",
        "tests/test_ci_shadow_pr5b_certification_readback.py",
        "machine-readable full ten-case golden projection",
        "verifier whole-run attempt 1→2 with ten fresh cells",
        "exact-job selective rerun",
        "certification-manifest.json",
    ):
        assert expected in spec

    architecture = spec.split("## Architecture", maxsplit=1)[1].split("### Trust and data flow", maxsplit=1)[0]
    assert "manifest extraction DTO" not in architecture


def test_pr5b_preflight_output_emitter_is_fail_closed_and_only_job_output_source() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "emit_exact_sha_preflight_outputs.py",
        "exactly seven ASCII `key=value` records",
        "`preflight_artifact_id`, `preflight_artifact_name`, "
        "`preflight_provider_digest`, `preflight_provider_size_bytes`, "
        "`preflight_payload_sha256`, `profile_digest`, and `execute_candidate`",
        "`execute_candidate` last",
        "O_WRONLY|O_APPEND|O_NOFOLLOW|O_CLOEXEC",
        "performs one bounded write",
        "reopens/revalidates the same inode and exact appended suffix",
        "needs.preflight.result == 'success'",
        "all-or-none optional coordinate group",
        "classifies any partial group as `INVALID`",
        "raw decision, upload and readback step outputs are not job outputs",
    ):
        assert expected in spec


def test_pr5b_adr_amendment_freezes_handoff_dependencies_and_capabilities() -> None:
    adr = _normalized(ADR_0048)

    for expected in (
        "post-upload current-attempt readback",
        "missing, partial, stale or invalid coordinates are `UNVERIFIED`",
        "separate tokenless trusted capability",
        "descriptor-confined sealed candidate extraction",
        "Case evidence binds plan, resolver-input, dependency and runtime inventory digests",
        "The minimal launcher",
        "No mutable path reacquisition, hidden global, degraded sandbox",
        "does not claim to count opaque pip HTTPS bodies",
        "sealed candidate, dependency, verifier-tree, setup-python runtime and materialized-plan",
        "root-owned file",
        "manually dispatched certification workflow",
    ):
        assert expected in adr


def test_pr5b_candidate_execution_is_confined_and_receipt_is_trusted() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "dedicated numeric UID/GID `20001:20001`",
        "private mount, PID and network namespace",
        "authenticated rendezvous bootstrap packet",
        "one session per case",
        "`pivot_root`s into the tmpfs root",
        "authenticated rendezvous descriptor across the full session",
        "candidate child closes it",
        "--no-new-privs",
        "GitHub command-file paths",
        "captured stdout and stderr, each capped at 1 MiB",
        "never replayed to the workflow command parser",
        "lies outside every sandbox mount",
        "Candidate code can neither create nor modify a receipt",
        "protected-master review and required-check policy",
        "not a malware verdict",
        "tests/test_ci_shadow_pr5b_linux_sandbox.py",
    ):
        assert expected in spec


def test_pr5b_successor_head_closes_launcher_materialization_and_di_gaps() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "Ubuntu Noble `sudo` closes descriptors above 2",
        "RootLauncherProvisionerV1",
        "/run/dpone-exact-sha",
        "authenticated Unix rendezvous",
        "sealed `memfd`",
        "RootLauncherAttemptV1.cleanup",
        "MaterializedCasePlanV1",
        "original PEP 427 basename",
        "/opt/candidate-wheels/<normalized-distribution>/<original_filename>",
        "--materialized-plan-digest {materialized_plan_digest}",
        "--argv-digest {argv_digest}",
        "--operation-plan-digest {operation_plan_digest}",
        "FixedPublicHttpReaderV1",
        "DependencyBudgetChargeKindV1",
        "python_location: str",
        "HOME=/work/home",
        "AIRFLOW_HOME=/work/airflow",
        "dpone.ports.candidate_case_plan_materializer",
        "dpone.ports.root_launcher_provisioner",
        "STAGED -> LAUNCHED -> CONNECTED -> CLEANED",
        "25, 50, 100, 200, 400, 800, 1,600 and 3,200",
        "ROOT_LAUNCHER_CLEANUP_V1_SOURCE",
        "HMAC-SHA256 keyed by the rendezvous nonce",
        "RootLauncherCleanupObservationV1",
        "never mounts the candidate extraction directory as a visible tree",
        "/opt/verifier-input/materialized-case-plan-v1.json",
        "unmounted in exact reverse construction order",
        "FixedPublicArtifactRequestV1",
        "RootLauncherSourceObservationV1",
        "launcher_sources_observation_status",
        "VerifiedRootLauncherV1.observation()",
    ):
        assert expected in spec

    assert "/usr/bin/sudo --non-interactive --preserve-fds=3" not in spec
    assert "bind-mounts them at the stable aliases" not in spec
    assert "candidate extraction at `/opt/candidate-wheels`" not in spec
    assert "from dpone.ports.candidate_snapshot_factories import (\n    CandidateCasePlanMaterializerV1" not in _read(
        SPEC
    )


def test_pr5b_successor_head_closes_incomplete_evidence_and_chain_depth_gaps() -> None:
    spec = _normalized(SPEC)
    parent = _normalized(PARENT)

    for expected in (
        "workflow_dispatch` Workflow C",
        "does not extend the one-hop candidate chain",
        "dpone.exact-sha-current-workflow-coordinate.v1",
        "dpone.exact-sha-certification-request.v1",
        "dpone.exact-sha-certification-coordinate-set.v1",
        "exactly eight `coordinates`",
        "DISTINCT_SUBJECT_A",
        "CANCELLED_RUN_CONTROL",
        "pairwise distinct",
        "One certification may satisfy only its declared role",
        "dependency_observation_status",
        "session_observation_status",
        "MISSING means the stage was not attempted",
        "certification_status=PASS` iff",
        "It is `FAIL` iff",
        "certification-navigation.md",
        "non-authoritative",
        "tests/test_ci_shadow_pr5b_certification.py",
        "tests/test_ci_shadow_pr5b_campaign.py",
        "capture domain exit → if: always() upload candidate → if: always()",
        "github.ref=refs/heads/master",
        "HEAD == github.sha == provider head_sha",
        "DPONE_COORDINATES_JSON",
        "49,152 ASCII bytes",
        "CertifierTerminalObservationV1",
        "completed/failure",
        "completed/cancelled",
        "producer_attempt_replay_status=PROVEN",
        "exact-sha-campaign-<collector-run-id>-<collector-attempt>.json",
        "terminal `completed/success`",
        "readback_exact_sha_certification_evidence.py",
        "shadow_compatibility_certification_readback",
        "VerifiedDirectJsonUploadV1",
        "verifier_terminal_observation` is exact `completed/success`",
        "verifier_terminal_observation` is exact `completed/failure`",
        "already-uploaded PASS receipt followed by verifier cancellation/failure",
    ):
        assert expected in spec

    assert "PR5B post-upload certification is a separate explicit read-only" in parent
    assert "workflow_run.workflows: [Exact SHA compatibility]" not in spec


def test_pr5b_successor_head_closes_partial_state_and_operation_contradictions() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "materialized_plan_observation_status",
        "MISSING|INVALID|PARTIAL|VERIFIED",
        "SandboxSessionPartialObservationV1",
        "SandboxNamespaceTerminalV1",
        "SandboxOuterTerminalV1",
        "materialized_plan_digest`, `output_inventory_digest`, `provisioner_sha256`",
        "--cold-samples 30 --warm-samples 30",
        "--constraint /opt/dependencies/constraint-derived.txt --find-links /opt/dependencies",
        "The three-iteration/120-second template",
        "materialize_exact_sha_certification_inputs.py",
    ):
        assert expected in spec

    assert "--iterations 3 --max-seconds 120" not in spec
    assert "five sealed directory descriptors" in spec


def test_pr5b_successor_head_closes_readback_budget_and_exact_execution_gaps() -> None:
    spec = _normalized(SPEC)
    raw = _read(SPEC)

    readback_cli = raw.split("python tools/ci/readback_exact_sha_certification_evidence.py", maxsplit=1)[1].split(
        "```", maxsplit=1
    )[0]
    for expected in (
        "--local-candidate <trusted-create-only-candidate.json>",
        "LocalDirectJsonPayloadSnapshotV1",
        "LocalDirectJsonPayloadSnapshotFactoryV1.capture(",
        "dpone.exact-sha-verified-direct-json-upload.v1",
        "exact-sha-verified-direct-json-upload-v1.schema.json",
        "the post-upload locally recomputed snapshot",
        "ProviderDispatchBudgetFactoryV1.create(",
        "`CERTIFICATION_PRODUCER` | 56 | 12,582,912 | 100",
        "`CERTIFICATION_READBACK` | 8 | 4,194,304 | 20",
        "`PREFLIGHT_PRODUCER` | 16 | 12,582,912 | 100",
        "`PREFLIGHT_READBACK` | 8 | 4,194,304 | 20",
        "`EXECUTOR_PREFLIGHT` | 5 | 65,537 | 120",
        "`EXECUTOR_CANDIDATE` | 5 | 1,073,872,897 | 600",
        "attempt 55/56 is admitted and 57 is refused",
        "attempt 7/8 is admitted and 9 is refused",
        "ProviderDispatchBudgetV1.consume_before_dispatch",
        "ProviderDispatchBudgetV1.admit_response_bytes",
        "12 MiB/4 MiB response bytes and 100/20 provider-wall seconds",
        "get_run_attempt(",
        "fixed_public_http: FixedPublicHttpReaderV1",
        "CandidateArchiveBuildSnapshotV1",
        "ConfinedFileIdentityV1",
        "CandidateArchiveMemberInputV1",
        "swapped or N+1 reader bytes",
        "CandidateInventoryDigestRefV1",
        "CANDIDATE_INVENTORY_DIGEST",
        "DependencyPreparationBudgetFactoryV1.create(",
        "resolver_input_observation_status",
        "CandidateDependencyFailureV1",
        "RootLauncherAttemptV1.close()` is legal only after",
        "LocalDirectJsonWriterCommitmentV1",
        "emit_local_writer_commitment",
        "four independent immutable views",
        "--writer-kind <CERTIFICATION-or-CAMPAIGN>",
        "--writer-inode <positive-int>",
    ):
        assert expected in spec
    assert "--payload-sha256" not in readback_cli
    assert "ConfinedFileSnapshotV1" not in spec
    candidate_contract_imports = raw.split("from dpone.contracts.ci_shadow_candidate import (", maxsplit=1)[1].split(
        ")", maxsplit=1
    )[0]
    for value_model in (
        "CandidateDependencySnapshotV1",
        "ResolverSessionObservationV1",
        "SandboxCommandObservationV1",
        "SandboxSessionObservationV1",
    ):
        assert value_model in candidate_contract_imports

    for expected in (
        "--dag-count 100 --workloads-per-dag 5",
        "--cold-p95-limit-seconds 5.5",
        "--warm-p95-limit-seconds 3.0",
        "--rss-limit-bytes 262144000",
        "samples[ceil(0.95 * count) - 1]",
        "install-negative-control-base",
        "install-candidate-negative-control",
        "pip-check-candidate-negative-control",
        "DURABLE_KPO_CAPABILITY_MISSING",
    ):
        assert expected in spec
    assert "--threshold-fixture /opt/verifier/docs/benchmarks/quality_budgets.yml" not in spec

    api = spec.split("These repository-internal APIs are supported", maxsplit=1)[1].split("Pages expose", maxsplit=1)[0]
    for signature in (
        "extract_candidate_archive(",
        "CandidateWheelInspectorV1.inspect_all(",
        "VerifiedCheckoutSnapshotFactoryV1.capture(",
        "CurrentPythonRuntimeSnapshotFactoryV1.capture(",
        "VerifiedRootLauncherFactoryV1.capture(",
        "CandidateCasePlanMaterializerV1.materialize(",
        "FixedPublicHttpReaderV1.read_exact(",
    ):
        block = api.split(signature, maxsplit=1)[1].split(") ->", maxsplit=1)[0]
        assert "deadline: CaseExecutionDeadlineV1" in block


def test_pr5b_successor_head_closes_terminal_digest_and_retry_identity_gaps() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "dpone.exact-sha-sandbox-bootstrap.v1",
        "dpone.exact-sha-sandbox-ready-binding.v1",
        "dpone.exact-sha-resolver-bootstrap.v1",
        "dpone.exact-sha-resolver-ready-binding.v1",
        "dpone.exact-sha-resolver-output-inventory.v1",
        "dpone.exact-sha-resolver-input-inventory.v1",
        "dpone.exact-sha-dependency-inventory.v1",
        "dpone.exact-sha-mounted-inventory.v1",
        "dpone.exact-sha-sandbox-protocol-prefix.v1",
        "dpone.exact-sha-sandbox-namespace-terminal.v1",
        "dpone.exact-sha-sandbox-outer-terminal.v1",
        "dpone.exact-sha-verifier-job-set.v1",
        "dpone.exact-sha-certifier-job-set.v1",
        "dpone.exact-sha-certification-coordinate-set.v1",
        "SandboxNamespaceTerminalV1` has exactly",
        "SandboxOuterTerminalV1` has exactly",
        "unchanged inner bytes",
        "ProviderRequestIdentityV1` has exactly",
        "r` as unsigned eight-byte big-endian",
        "contains no hostname/raw URL",
        "signed-query changes that must not change identity",
        "cleanup_status` (`VERIFIED|UNVERIFIED`)",
        "SandboxBootstrapObservationV1",
        "ResolverBootstrapObservationV1",
        "SandboxCloseReasonV1",
        "PRODUCT_STOP",
        "session_output_inventory_digests",
    ):
        assert expected in spec

    assert "nullable `cleanup_status`" not in spec


def test_pr5b_root_cleanup_journal_golden_vectors_are_independent_and_exact() -> None:
    entry_domain = "dpone.exact-sha-root-cleanup-journal-entry.v1"
    journal_domain = "dpone.exact-sha-root-cleanup-journal.v1"
    process_inventory_domain = "dpone.exact-sha-root-recorded-process-inventory.v1"
    mount_inventory_domain = "dpone.exact-sha-root-recorded-mount-inventory.v1"
    genesis_payload = {
        "staging_nonce": "1" * 64,
        "rendezvous_nonce": "2" * 64,
        "staging_id": "stage-1111111111111111",
        "staging_directory_device": 2049,
        "staging_directory_inode": 9001,
        "config_device": 2049,
        "config_inode": 9002,
        "config_sha256": f"sha256:{'3' * 64}",
        "launcher_device": 2049,
        "launcher_inode": 9003,
        "launcher_size_bytes": 4096,
        "launcher_sha256": f"sha256:{'4' * 64}",
        "cleanup_journal_device": 2049,
        "cleanup_journal_inode": 9004,
        "provisioner_sha256": f"sha256:{'5' * 64}",
        "supervisor_uid": 501,
        "supervisor_gid": 20,
        "supervisor_pid": 7000,
        "supervisor_start_time": 1000,
    }
    process_intent = {
        "pid": 7001,
        "start_time": 1001,
        "process_group": 7001,
        "role": "ROOT_PEER",
        "wait_status": None,
    }
    process_reaped = {**process_intent, "wait_status": 0}
    mounted_inventory_active = {
        "target_path": "/run/dpone-root",
        "kind": "TMPFS",
        "source_device": None,
        "source_inode": None,
        "source_size_bytes": None,
        "source_sha256": None,
        "source_inventory_digest": None,
        "mount_options": ["mode=0700", "nodev", "nosuid"],
    }
    mounted_inventory_absent = {**mounted_inventory_active, "target_path": "/run/dpone-empty"}
    mount_active_transition = {"mount_operation_id": 1, "inventory": mounted_inventory_active}
    mount_absent_transition = {"mount_operation_id": 2, "inventory": mounted_inventory_absent}
    payloads: list[tuple[str, object]] = [
        ("GENESIS", genesis_payload),
        ("PROCESS_INTENT", process_intent),
        ("PROCESS_REAPED", process_reaped),
        ("MOUNT_INTENT", mount_active_transition),
        ("MOUNT_ACTIVE", mount_active_transition),
        ("MOUNT_INTENT", mount_absent_transition),
        ("MOUNT_ABSENT", mount_absent_transition),
    ]
    entries: list[dict[str, object]] = []
    entry_digests: list[str] = []

    for ordinal, (kind, payload) in enumerate(payloads):
        entry = {
            "schema_version": "dpone.exact-sha-root-cleanup-journal-entry.v1",
            "ordinal": ordinal,
            "previous_entry_digest": None if ordinal == 0 else entry_digests[-1],
            "kind": kind,
            "payload": payload,
        }
        entries.append(entry)
        entry_digests.append(_domain_digest(entry_domain, entry))

    process_inventory_digest = _domain_digest(process_inventory_domain, [process_intent])
    mount_inventory_digest = _domain_digest(
        mount_inventory_domain,
        [mount_active_transition, mount_absent_transition],
    )
    pre_retirement_journal_digest = _domain_digest(journal_domain, entry_digests)
    staging_identity = {
        "staging_id": "stage-1111111111111111",
        "staging_directory_device": 2049,
        "staging_directory_inode": 9001,
        "cleanup_journal_device": 2049,
        "cleanup_journal_inode": 9004,
        "cleanup_journal_genesis_digest": entry_digests[0],
    }
    retirement_payloads = [
        (
            "STAGE_RETIRE_INTENT",
            {
                "staging_identity": staging_identity,
                "process_observation_digest": f"sha256:{'7' * 64}",
                "cleanup_journal_digest": pre_retirement_journal_digest,
                "recorded_process_inventory_digest": process_inventory_digest,
                "recorded_mount_inventory_digest": mount_inventory_digest,
                "retired_basename": f"retired-stage-{'1' * 64}",
            },
        ),
    ]
    for kind, payload in retirement_payloads:
        entry = {
            "schema_version": "dpone.exact-sha-root-cleanup-journal-entry.v1",
            "ordinal": len(entries),
            "previous_entry_digest": entry_digests[-1],
            "kind": kind,
            "payload": payload,
        }
        entries.append(entry)
        entry_digests.append(_domain_digest(entry_domain, entry))
    retired_entry = {
        "schema_version": "dpone.exact-sha-root-cleanup-journal-entry.v1",
        "ordinal": len(entries),
        "previous_entry_digest": entry_digests[-1],
        "kind": "STAGE_RETIRED",
        "payload": {
            "retire_intent_entry_digest": entry_digests[-1],
            "retired_basename": f"retired-stage-{'1' * 64}",
            "directory_device": 2049,
            "directory_inode": 9001,
            "journal_device": 2049,
            "journal_inode": 9004,
        },
    }
    entries.append(retired_entry)
    entry_digests.append(_domain_digest(entry_domain, retired_entry))

    observed_vectors = [
        (
            entry["kind"],
            len(_canonical_json_bytes(entry)),
            len(_canonical_json_bytes(entry)).to_bytes(8, "big").hex(),
            digest,
        )
        for entry, digest in zip(entries, entry_digests, strict=True)
    ]
    assert observed_vectors == [
        ("GENESIS", 947, "00000000000003b3", "sha256:eb7ee7b5422481cd86577e9de5952847c7d413ef0ccb3cf9149b98d9cc601f2a"),
        (
            "PROCESS_INTENT",
            300,
            "000000000000012c",
            "sha256:0362dd4605b74849542c865c76e2529821a39dfbd4990c445bf39667d198c964",
        ),
        (
            "PROCESS_REAPED",
            297,
            "0000000000000129",
            "sha256:c6e48a0c413defc8eb60c1a1d627c0cba9de4e211d609bcb65d9b676f9afd5d3",
        ),
        (
            "MOUNT_INTENT",
            459,
            "00000000000001cb",
            "sha256:e0ecf2dc21443ba1ef8781dfa3c8087ca8abc31316817cd6676811f0e8f06ff9",
        ),
        (
            "MOUNT_ACTIVE",
            459,
            "00000000000001cb",
            "sha256:5c5747edab31de649be342a86e291ed4a32c5130e790cdf9252ce38f1b2b5bc2",
        ),
        (
            "MOUNT_INTENT",
            460,
            "00000000000001cc",
            "sha256:6be732aee7380d38cafd3da7046956f252ac78dcce31cb269574f07019927b26",
        ),
        (
            "MOUNT_ABSENT",
            460,
            "00000000000001cc",
            "sha256:0b4b3fc835681ae58f2e7859e81692e5c2f969634cae06b4cd3e408a5451ef03",
        ),
        (
            "STAGE_RETIRE_INTENT",
            1025,
            "0000000000000401",
            "sha256:8e391c9e97c699ce0b71e533779acecba3db88a1df4a118447fdd62f4a40a01f",
        ),
        (
            "STAGE_RETIRED",
            504,
            "00000000000001f8",
            "sha256:1263de7603e6966ae11fc88b9fe7bd84899995198b60ed76ca0b1fa5114a147e",
        ),
    ]
    assert pre_retirement_journal_digest == "sha256:1f0f1d72536e4dff55d677fcc37066342359c98a86575e19cab30f3bf3f9d4a9"
    assert _domain_digest(journal_domain, entry_digests) == (
        "sha256:f98b024ec05a18536780a5d590b0fe2943f45f67a35c1f95b77d0ccfe1a817e3"
    )
    assert process_inventory_digest == "sha256:ffc764cd6ffc1240c05741771da6fc211198ca88e306f4f6d34894d31a89b96a"
    assert mount_inventory_digest == "sha256:8fd06b28f4c8459e7088a17382b9e49c86b4323bd8899705464108716b1ebcf9"
    assert sum(8 + len(_canonical_json_bytes(entry)) for entry in entries) == 4983
    assert [(size, 8 + size <= 4104) for size in (4095, 4096, 4097)] == [
        (4095, True),
        (4096, True),
        (4097, False),
    ]
    assert 2 * (8 + 4096) == 8208
    assert 262_144 - 8208 == 253_936
    assert 1024 - 2 == 1022


def test_pr5b_successor_head_closes_helper_budget_order_and_dependency_gaps() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "failure/deadline before either helper yields a PID",
        "started-but-not-reaped is non-null PID/start with null wait",
        "started-but-unreaped final helper uses DEADLINE_EXHAUSTED",
        "process_observation_digest",
        "recorded_descendants_absent",
        "RootCleanupJournalEntryV1",
        "dpone.exact-sha-root-cleanup-journal.v1",
        "recorded_process_inventory_digest",
        "recorded_mount_inventory_digest",
        "unsigned eight-byte big-endian canonical-JSON byte length",
        "reserves exactly the final two entry slots and 8,208 framed bytes",
        "253,936 framed bytes",
        "sum(8 + canonical_json_size(entry) for entry in entries) <= 262144",
        "previous_entry_digest` is exactly lowercase",
        "The GENESIS payload has exactly the JSON keys",
        "STAGE_RETIRED whose payload has exactly",
        "mount_operation_id",
        "recorded_privileged_descendant_count",
        "RootJournalTerminalObservationV1",
        "outer_group_absent=true",
        "STAGE_RETIRE_INTENT",
        "renameat2(RENAME_NOREPLACE)",
        "retired_cleanup_journal_digest",
        "ResolverStartOutcomeV1",
        "ResolverTerminalOutcomeV1",
        "CandidateSandboxStartOutcomeV1",
        "SandboxTerminalOutcomeV1",
        "DISPATCHED_PRE_READY",
        "failure_kind",
        "gapless and unique separately within each `attempt_kind`",
        "fork-before-record (the closed gate prevents effect)",
        "record-before-effect crash recovery",
        "missing/torn/stale/substituted journals",
        "RootPrivilegedLifecycleObservationV1",
        "UNKNOWN_NOT_YET_AUTHENTICATED",
        "RootProcessGroupAbsenceProverV1",
        "RootProcessGroupReapOutcomeV1",
        "waitid(P_PIDFD, leader_pidfd, WEXITED|WNOWAIT)",
        "waitid(P_PIDFD, ..., WEXITED|WNOWAIT|WNOHANG)",
        "waitpid(leader_pid, 0)",
        "stage_group_absent|outer_group_absent",
        "a reaped group leader with a live descendant",
        "RootCleanupHelperResponseV1",
        "dpone.exact-sha-root-cleanup-helper-response.v1",
        "RootCleanupHelperAttemptObservationV1",
        "cleanup_helper_attempts",
        "already sealed response loss",
        "Retry accepts exactly these five states",
        "exact raw-zero-MISSING-only retry predicate",
        "nonzero/signalled/unreaped/invalid/UNVERIFIED branches",
        "ResolverInputPreparationOutcomeV1",
        "ResolverInputPreparationOutcomeV1.close() -> None",
        "ResolverStartOutcomeV1.close( ) -> RootAttemptLifecycleEvidenceV1 | ResolverTerminalOutcomeV1 | None",
        "CandidateSandboxStartOutcomeV1.close( ) -> RootAttemptLifecycleEvidenceV1 | SandboxTerminalOutcomeV1 | None",
        "it alone constructs",
        "tests/test_ci_shadow_pr5b_root_cleanup_journal.py",
        "independent golden byte vectors for every entry",
        "STARTED_UNREAPED|REAPED",
        "PROCESS_EXIT_NONZERO",
        "namespace_wait_status=0",
        "NAMESPACE_EXIT_NONZERO",
        "Every `UNVERIFIED` result has that same mandatory exact",
        "RootAttemptLifecycleEvidenceV1",
        "pre-READY root-peer",
        "root_lifecycle_evidence",
        "never claims `waitpid`/reaping of a nonchild",
        "nullable nonnegative `namespace_wait_status`",
        "After both terminal observations return",
        "CandidateExtractionSnapshotV1.archive.inventory_digest",
        "preflight producer gets 16 and its separate post-upload direct-JSON readback gets 8",
        "Their exact static sums are 24, 16 MiB and 120 seconds",
        "producer attempt 15/16 is admitted and 17 refused",
        "materializes the plan before it captures the verifier snapshot",
        "materialized-plan snapshots plus the authenticated sandbox-launcher",
        "consume the sole inspector result",
        "candidate contracts + confined I/O + monotonic-deadline ports + PR5A contract",
        "contracts + provider-budget + confined-I/O ports",
        "fixed-public-HTTP + root-launcher-provisioner ports",
    ):
        assert expected in spec

    assert "CandidateExtractionSnapshotV1.inventory_digest" not in spec
    assert "immediately captures both snapshots" not in spec
    assert "| `PREFLIGHT` | 24 |" not in _read(SPEC)
    assert "`descendants_reaped`" not in spec


def test_pr5b_root_failure_retry_and_outcome_ownership_matrices_are_closed() -> None:
    prover_rows = _markdown_table_rows_after(SPEC, "The prover terminal matrix is exhaustive")
    assert prover_rows == [
        ("WNOWAIT terminal, complete scan absent", "true", "WAIT", "true", "null"),
        ("WNOWAIT terminal, live member remains", "true", "WAIT", "false", "PROCESS_UNREAPED"),
        ("WNOWAIT terminal, scan/protocol failure", "true", "WAIT", "false", "PROTOCOL_INVALID"),
        (
            "WNOWAIT terminal, absent scan, waitpid deadline",
            "false",
            "null",
            "false",
            "DEADLINE_EXHAUSTED",
        ),
        (
            "WNOWAIT terminal, absent scan, waitpid protocol failure",
            "false",
            "null",
            "false",
            "PROTOCOL_INVALID",
        ),
        (
            "WNOWAIT terminal, live member and waitpid deadline",
            "false",
            "null",
            "false",
            "DEADLINE_EXHAUSTED",
        ),
        (
            "WNOWAIT terminal, live member and waitpid protocol failure",
            "false",
            "null",
            "false",
            "PROTOCOL_INVALID",
        ),
        (
            "WNOWAIT terminal, scan/protocol failure and waitpid deadline",
            "false",
            "null",
            "false",
            "DEADLINE_EXHAUSTED",
        ),
        (
            "WNOWAIT terminal, scan/protocol and waitpid protocol failure",
            "false",
            "null",
            "false",
            "PROTOCOL_INVALID",
        ),
        ("leader not terminal before deadline", "false", "null", "false", "DEADLINE_EXHAUSTED"),
        ("abort observes terminal child", "true", "WAIT", "false", "FAILURE"),
        ("abort observes terminal child but waitpid fails", "false", "null", "false", "FAILURE"),
        ("abort observes live/unknown child", "false", "null", "false", "FAILURE"),
    ]
    for _, leader_reaped, wait_status, group_absent, diagnostic in prover_rows:
        assert (wait_status != "null") is (leader_reaped == "true")
        assert (group_absent == "true") is (diagnostic == "null")
        if group_absent == "true":
            assert leader_reaped == "true"

    process_priority_rows = _markdown_table_rows_after(SPEC, "one by the global priority shown here")
    assert process_priority_rows == [
        ("1", "JOURNAL_INVALID", "every lower row"),
        ("2", "DEADLINE_EXHAUSTED", "protocol, unreaped, nonzero, helper"),
        ("3", "PROTOCOL_INVALID", "unreaped, nonzero, helper"),
        ("4", "PROCESS_UNREAPED", "nonzero, helper"),
        ("5", "PROCESS_EXIT_NONZERO", "helper"),
        ("6", "HELPER_FAILED", "none"),
    ]
    process_projection_rows = _markdown_table_rows_after(
        SPEC, "For each stage/outer prover outcome the public projection is exhaustive"
    )
    assert process_projection_rows == [
        ("reaped, raw zero, group absent", "true / 0 / true", "none"),
        ("reaped, nonzero, group absent", "true / RAW / true", "PROCESS_EXIT_NONZERO"),
        ("reaped, raw zero, group false", "true / 0 / false", "prover diagnostic"),
        (
            "reaped, nonzero, group false",
            "true / RAW / false",
            "prover diagnostic + PROCESS_EXIT_NONZERO",
        ),
        (
            "unreaped, null wait, group false",
            "false / null / false",
            "DEADLINE_EXHAUSTED or PROTOCOL_INVALID or PROCESS_UNREAPED",
        ),
    ]

    priority = [row[1] for row in process_priority_rows]

    def fold_process_diagnostic(candidates: set[str]) -> str | None:
        return next((code for code in priority if code in candidates), None)

    allowed = set(priority)
    role_candidate_sets: list[set[str]] = []
    for _, leader_reaped, wait_token, group_absent, diagnostic_token in prover_rows:
        diagnostics = (
            ("DEADLINE_EXHAUSTED", "PROTOCOL_INVALID") if diagnostic_token == "FAILURE" else (diagnostic_token,)
        )
        waits = (0, 9) if wait_token == "WAIT" else (None,)
        for diagnostic in diagnostics:
            for wait_status in waits:
                candidates = set()
                if diagnostic != "null":
                    candidates.add(diagnostic)
                if wait_status not in (None, 0):
                    candidates.add("PROCESS_EXIT_NONZERO")
                selected = fold_process_diagnostic(candidates)
                assert selected is None or selected in allowed
                assert (wait_status is not None) is (leader_reaped == "true")
                if leader_reaped == "false":
                    assert selected in {"PROCESS_UNREAPED", "DEADLINE_EXHAUSTED", "PROTOCOL_INVALID"}
                if group_absent == "true" and wait_status == 0:
                    assert selected is None
                role_candidate_sets.append(candidates)

    for stage_candidates in role_candidate_sets:
        for outer_candidates in role_candidate_sets:
            for mask in range(1 << len(priority)):
                other_candidates = {code for index, code in enumerate(priority) if mask & (1 << index)}
                combined = stage_candidates | outer_candidates | other_candidates
                assert fold_process_diagnostic(combined) == next((code for code in priority if code in combined), None)
    assert fold_process_diagnostic({"HELPER_FAILED", "PROCESS_EXIT_NONZERO"}) == "PROCESS_EXIT_NONZERO"
    assert fold_process_diagnostic({"PROCESS_UNREAPED", "PROTOCOL_INVALID"}) == "PROTOCOL_INVALID"
    assert fold_process_diagnostic({"DEADLINE_EXHAUSTED", "JOURNAL_INVALID"}) == "JOURNAL_INVALID"

    attempt_rows = _markdown_table_rows_after(SPEC, "The attempt fold table is exhaustive")
    assert attempt_rows == [
        ("raw-zero MISSING, slots/time remain", "yes", "not terminal", "N/A", "next attempt reauthenticates"),
        (
            "raw-zero MISSING, fourth slot and time remains",
            "no",
            "UNVERIFIED",
            "HELPER_FAILED",
            "conservative",
        ),
        (
            "raw-zero MISSING, no time remains (any slot)",
            "no",
            "UNVERIFIED",
            "DEADLINE_EXHAUSTED",
            "conservative",
        ),
        ("raw-zero INVALID", "no", "UNVERIFIED", "PROTOCOL_INVALID", "conservative"),
        ("nonzero or signalled wait", "no", "UNVERIFIED", "HELPER_FAILED", "conservative"),
        ("started but unreaped", "no", "UNVERIFIED", "DEADLINE_EXHAUSTED", "conservative"),
        ("spawn failure before deadline", "no", "UNVERIFIED", "HELPER_FAILED", "conservative"),
        ("deadline before spawn", "no", "UNVERIFIED", "DEADLINE_EXHAUSTED", "conservative"),
        (
            "raw-zero VERIFIED UNVERIFIED",
            "no",
            "UNVERIFIED",
            "exact helper diagnostic",
            "accepted response",
        ),
        ("raw-zero VERIFIED CLEANED", "no", "CLEANED", "null", "accepted response"),
    ]
    assert [row for row in attempt_rows if row[1] == "yes"] == [attempt_rows[0]]
    assert all(row[4] == "conservative" for row in attempt_rows[1:8])
    assert all(row[4] == "accepted response" for row in attempt_rows[8:])

    response_rows = _markdown_table_rows_after(SPEC, "classification is exhaustive and precedes retry folding")
    assert response_rows == [
        ("spawn failed, no PID/start", "none possible", "MISSING", "null / null"),
        ("started but unreaped", "any partial or complete bytes ignored", "MISSING", "null / null"),
        ("reaped nonzero or signalled", "any bytes ignored", "INVALID", "null / null"),
        (
            "raw-zero reaped",
            "no complete EOF response by deadline (none or partial)",
            "MISSING",
            "null / null",
        ),
        (
            "raw-zero reaped",
            "invalidity established before deadline: overflow, trailing, malformed, noncanonical or binding-invalid",
            "INVALID",
            "null / null",
        ),
        ("raw-zero reaped", "complete canonical bound CLEANED", "VERIFIED", "CLEANED / exact digest"),
        (
            "raw-zero reaped",
            "complete canonical bound UNVERIFIED",
            "VERIFIED",
            "UNVERIFIED / exact digest",
        ),
    ]

    outcome_rows = _markdown_table_rows_after(SPEC, "The resolver-input ownership matrix is exhaustive")
    assert outcome_rows == [
        ("INVALID", "no child operation", "none"),
        ("VERIFIED, take succeeds", "empty wrapper only", "case service"),
        ("VERIFIED, close/cancel before take", "contained snapshot and directory closed", "none"),
        ("second close", "no operation", "unchanged"),
        ("take after close or second take", "closed-capability error", "unchanged"),
    ]

    start_rows = _markdown_table_rows_after(SPEC, "The resolver/sandbox start-outcome close matrix is exhaustive")
    assert start_rows == [
        ("NOT_DISPATCHED", "null", "none"),
        ("DISPATCHED_FAILURE", "same RootAttemptLifecycleEvidenceV1", "exactly once by owner"),
        ("READY, not taken", "same matching terminal outcome", "exactly once by owner"),
        ("READY, taken", "null; transferred product untouched", "terminal owner appends later"),
    ]
    normalized = _normalized(SPEC)
    assert "`CandidateDependencyPreparerV1` alone owns every `ResolverStartOutcomeV1`" in normalized
    assert "`execute_compatibility_case` never receives a resolver start outcome" in normalized
    assert "The case service alone owns `CandidateSandboxStartOutcomeV1`" in normalized
    assert "no separate consume state exists" in normalized

    authority = f"{_normalized(ADR_0048)} {_normalized(TASK)}"
    assert "only after a raw-zero MISSING response" in authority
    assert "unreaped, invalid or UNVERIFIED attempt is sticky" not in authority
    assert "then KILL, proves the root process groups absent" not in normalized
    assert "The runner alone applies `RootProcessGroupAbsenceProverV1`" in normalized
    assert 262_144 + 4_096 + 262_144 == 528_384
    assert 528_384 * 64 == 33_816_576
    assert "528,384 bytes (516 KiB) per tombstone and 33,816,576 bytes across 64" in normalized


def test_pr5b_multi_attempt_helper_response_and_no_response_folds_are_executable() -> None:
    blocks = re.findall(r"```json\n(.*?)\n```", _read(SPEC), flags=re.DOTALL)
    case = json.loads(blocks[1])
    cleanup = dict(case["root_lifecycle_evidence"][0]["cleanup_observation"])
    base_response = {
        key: value
        for key, value in cleanup.items()
        if key not in {"schema_version", "cleanup_helper_response_digest", "cleanup_helper_attempts"}
    }
    base_response["schema_version"] = "dpone.exact-sha-root-cleanup-helper-response.v1"

    def response_for(status: str) -> dict[str, object]:
        response = dict(base_response)
        response["status"] = status
        response["diagnostic_code"] = None if status == "CLEANED" else "MOUNT_UNVERIFIED"
        if status == "UNVERIFIED":
            response.update(
                recorded_processes_absent=0,
                recorded_descendants_absent=0,
                recorded_mounts_absent=0,
                mounts_unmounted=0,
                stage_absent=False,
                retired_stage_present=False,
                retired_cleanup_journal_digest=None,
                retired_stage=None,
            )
        return response

    def accepted_observation(*, predecessor_count: int, status: str) -> dict[str, object]:
        response = response_for(status)
        digest = _domain_digest("dpone.exact-sha-root-cleanup-helper-response.v1", response)
        observation = dict(response)
        observation["schema_version"] = "dpone.exact-sha-root-cleanup-observation.v1"
        observation["cleanup_helper_response_digest"] = digest
        attempts: list[dict[str, object]] = [
            {
                "attempt_ordinal": ordinal,
                "pid": 8100 + ordinal,
                "start_time": 2100 + ordinal,
                "wait_status": 0,
                "response_status": "MISSING",
                "reported_status": None,
                "response_digest": None,
            }
            for ordinal in range(predecessor_count)
        ]
        attempts.append(
            {
                "attempt_ordinal": predecessor_count,
                "pid": 8100 + predecessor_count,
                "start_time": 2100 + predecessor_count,
                "wait_status": 0,
                "response_status": "VERIFIED",
                "reported_status": status,
                "response_digest": digest,
            }
        )
        observation["cleanup_helper_attempts"] = attempts
        return observation

    for predecessor_count in range(1, 4):
        for status in ("CLEANED", "UNVERIFIED"):
            observation = accepted_observation(predecessor_count=predecessor_count, status=status)
            attempts = observation["cleanup_helper_attempts"]
            assert isinstance(attempts, list)
            assert [item["attempt_ordinal"] for item in attempts] == list(range(predecessor_count + 1))
            assert all(item["response_status"] == "MISSING" for item in attempts[:-1])
            assert attempts[-1]["response_status"] == "VERIFIED"
            assert attempts[-1]["reported_status"] == status
            reconstructed = {
                key: value
                for key, value in observation.items()
                if key not in {"schema_version", "cleanup_helper_response_digest", "cleanup_helper_attempts"}
            }
            reconstructed["schema_version"] = "dpone.exact-sha-root-cleanup-helper-response.v1"
            assert observation["cleanup_helper_response_digest"] == _domain_digest(
                "dpone.exact-sha-root-cleanup-helper-response.v1", reconstructed
            )
            assert attempts[-1]["response_digest"] == observation["cleanup_helper_response_digest"]

    def retry_allowed(attempt: dict[str, object], *, count: int, time_remaining: float, pidfd_closed: bool) -> bool:
        return (
            attempt["pid"] is not None
            and attempt["start_time"] is not None
            and attempt["wait_status"] == 0
            and attempt["response_status"] == "MISSING"
            and attempt["reported_status"] is None
            and attempt["response_digest"] is None
            and pidfd_closed
            and count < 4
            and time_remaining > 0
        )

    predecessor = accepted_observation(predecessor_count=1, status="CLEANED")["cleanup_helper_attempts"][0]
    assert retry_allowed(predecessor, count=1, time_remaining=1, pidfd_closed=True)
    assert not retry_allowed(predecessor, count=1, time_remaining=1, pidfd_closed=False)
    assert not retry_allowed(predecessor, count=4, time_remaining=1, pidfd_closed=True)
    assert not retry_allowed(predecessor, count=1, time_remaining=0, pidfd_closed=True)
    for changed in (
        {"wait_status": None},
        {"wait_status": 9},
        {"response_status": "INVALID"},
        {
            "response_status": "VERIFIED",
            "reported_status": "UNVERIFIED",
            "response_digest": "sha256:" + "a" * 64,
        },
    ):
        attempt = {**predecessor, **changed}
        assert not retry_allowed(attempt, count=1, time_remaining=1, pidfd_closed=True)

    conservative_fields = {
        "recorded_processes_absent": 0,
        "recorded_descendants_absent": 0,
        "recorded_mounts_absent": 0,
        "mounts_unmounted": 0,
        "stage_absent": False,
        "retired_stage_present": False,
        "retired_cleanup_journal_digest": None,
        "retired_stage": None,
        "cleanup_helper_response_digest": None,
    }
    missing_attempts = [
        {
            "attempt_ordinal": ordinal,
            "pid": 8300 + ordinal,
            "start_time": 2300 + ordinal,
            "wait_status": 0,
            "response_status": "MISSING",
            "reported_status": None,
            "response_digest": None,
        }
        for ordinal in range(4)
    ]
    no_response_cases: tuple[tuple[str, str, list[dict[str, object]]], ...] = (
        ("deadline before spawn", "DEADLINE_EXHAUSTED", []),
        (
            "spawn failure",
            "HELPER_FAILED",
            [
                {
                    "attempt_ordinal": 0,
                    "pid": None,
                    "start_time": None,
                    "wait_status": None,
                    "response_status": "MISSING",
                    "reported_status": None,
                    "response_digest": None,
                }
            ],
        ),
        (
            "started unreaped",
            "DEADLINE_EXHAUSTED",
            [
                {
                    "attempt_ordinal": 0,
                    "pid": 8200,
                    "start_time": 2200,
                    "wait_status": None,
                    "response_status": "MISSING",
                    "reported_status": None,
                    "response_digest": None,
                }
            ],
        ),
        (
            "nonzero/signalled",
            "HELPER_FAILED",
            [
                {
                    "attempt_ordinal": 0,
                    "pid": 8201,
                    "start_time": 2201,
                    "wait_status": 9,
                    "response_status": "INVALID",
                    "reported_status": None,
                    "response_digest": None,
                }
            ],
        ),
        (
            "raw-zero invalid",
            "PROTOCOL_INVALID",
            [
                {
                    "attempt_ordinal": 0,
                    "pid": 8202,
                    "start_time": 2202,
                    "wait_status": 0,
                    "response_status": "INVALID",
                    "reported_status": None,
                    "response_digest": None,
                }
            ],
        ),
        ("raw-zero missing at deadline", "DEADLINE_EXHAUSTED", missing_attempts[:1]),
        ("raw-zero missing fourth slot", "HELPER_FAILED", missing_attempts),
    )
    for _, diagnostic, attempts in no_response_cases:
        observation = dict(base_response)
        observation.update(
            schema_version="dpone.exact-sha-root-cleanup-observation.v1",
            status="UNVERIFIED",
            diagnostic_code=diagnostic,
        )
        observation.update(conservative_fields)
        observation["cleanup_helper_attempts"] = attempts
        assert observation["schema_version"] == "dpone.exact-sha-root-cleanup-observation.v1"
        assert observation["status"] == "UNVERIFIED"
        assert observation["diagnostic_code"] == diagnostic
        assert all(observation[key] == value for key, value in conservative_fields.items())
        assert [attempt["attempt_ordinal"] for attempt in attempts] == list(range(len(attempts)))
        assert all(attempt["response_status"] in {"MISSING", "INVALID"} for attempt in attempts)
        assert all(attempt["reported_status"] is None for attempt in attempts)
        assert all(attempt["response_digest"] is None for attempt in attempts)


def test_pr5b_successor_head_closes_ready_product_stop_and_retry_matrix() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "`request` (the complete bootstrap request) and `observation`",
        "SandboxEnvironmentBootstrapObservationV1` has exactly",
        "ResolverSystemLeafObservationV1` has exactly",
        "The parent validates the closed observation",
        "COMPLETE has the complete profile command count",
        "PRODUCT_STOP has a strict profile prefix",
        "A clean early product stop is therefore terminally VERIFIED",
        "RUN_ATTEMPT = `run_id,run_attempt`",
        "ATTEMPT_JOBS_PAGE = `run_id,run_attempt,page`",
        "ARTIFACT_BYTES = `artifact_id`",
        "Every nullable field not named for that kind is exactly null",
        "zero-based integer `0|1|2|3`",
        "before provider attempt `2|3|4|5`",
    ):
        assert expected in spec


def test_pr5b_successor_head_closes_certification_commitment_and_cjm_gaps() -> None:
    spec = _normalized(SPEC)

    for expected in (
        "writer-derived kind/schema/size/digest/device/inode",
        "before upload",
        "four independent immutable views",
        "writer-time commitment",
        "post-upload locally recomputed snapshot",
        "provider-returned immutable artifact coordinate",
        "freshly downloaded canonical bytes",
        "independently discovers/authenticates its final artifact and producer subject",
        "request containing only verifier run ID, run attempt",
        "`repository_id`, `repository`, `workflow_id`",
        "`head_repository_id`, `head_branch`, `head_sha`",
    ):
        assert expected in spec

    assert "request supplies the final artifact" not in spec


def test_pr5b_exact_executor_coverage_is_named() -> None:
    spec = _read(SPEC)

    for path in (
        "tests/test_airflow_pack_cache.py",
        "tests/test_airflow_provider_parse_benchmark.py",
        "tests/test_kpo_live_base_container_logs.py",
        "tests/test_airflow_runtime_init_fetch_cli.py",
        "tests/test_airflow_cache_materializer.py",
        "tests/test_dbt_runtime_execution.py",
        "tests/fixtures/dbt-runtime-correctness-v1",
    ):
        assert path in spec
    assert "CNCF\nprovider 10.19.0" in spec
    assert "dbt-core 1.12.3" in spec
    assert "dbt-sqlserver 1.11.1" in spec
    assert "devmajor and devminor are each exact ASCII `0000000` plus NUL" in spec


def test_pr5b_is_diagnostic_and_cannot_weaken_current_authority() -> None:
    spec = _normalized(SPEC)
    policy = yaml.safe_load(_read(POLICY))

    required_contexts = policy["ruleset"]["required_status_checks"]["checks"]
    assert "Doctor import Windows (3.11)" in required_contexts
    assert "Doctor import Windows (3.12)" in required_contexts
    assert len(required_contexts) == len(set(required_contexts))
    assert "resolved from the canonical active branch-protection policy" in spec
    assert "cannot authorize a merge, readiness `PASS`, release, publication" in spec
    assert (
        "There is deliberately no arrow from the receipt to merge, readiness, release or publication authority" in spec
    )


def test_pr5b_design_task_contract_is_bound_and_production_paths_are_forbidden() -> None:
    task = yaml.safe_load(_read(TASK))

    assert task["task_id"] == "DPONE-CI-SHADOW-PR5B-DESIGN"
    assert task["base_commit"] == "816e0f9906a07dadeb44cdb958647109d592b6e9"
    assert task["specification"] == SPEC.relative_to(ROOT).as_posix()
    assert task["shared_file_owner"] == "/root"
    assert ".github/workflows" in task["forbidden_paths"]
    assert "src" in task["forbidden_paths"]
    assert "tools" in task["forbidden_paths"]
    assert any("cumulative per-case resource budget" in item for item in task["acceptance_criteria"])
    assert any("verified fail-closed emitter" in item for item in task["acceptance_criteria"])
    assert any("exact setup-python runtime capabilities" in item for item in task["acceptance_criteria"])
    assert any("Opaque pip HTTP activity" in item for item in task["acceptance_criteria"])
    assert any("ResolverInputSnapshotV1" in item for item in task["acceptance_criteria"])
    assert any("closed sequenced protocol" in item for item in task["acceptance_criteria"])
    assert any("post-upload certifier" in item for item in task["acceptance_criteria"])
    assert any("Ubuntu-compatible" in item for item in task["acceptance_criteria"])
    assert any("original PEP 427 filenames" in item for item in task["acceptance_criteria"])
    assert any("closed observation states" in item for item in task["acceptance_criteria"])
    assert any("8-role campaign coordinate schemas" in item for item in task["acceptance_criteria"])
    assert any("preflight producer and post-upload readback" in item for item in task["acceptance_criteria"])
    assert any("clean early nonzero PRODUCT stop" in item for item in task["acceptance_criteria"])
    assert any("before helper PID acquisition" in item for item in task["acceptance_criteria"])
    assert any("started-unreaped and reaped" in item for item in task["acceptance_criteria"])
    assert any("Maintainer approval evidence does not bind" in item for item in task["stop_conditions"])
