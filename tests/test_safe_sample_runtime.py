from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dpone.commands.run_safe_sample_cmd import SafeSampleCommandResult
from dpone.security_redaction import REDACTION_TOKEN, redact_text
from dpone.services.safe_sample_execution_plan import (
    AirflowDeploymentContext,
    SafeSampleExecutionPlanBuilder,
    SafeSampleSourceSnapshot,
)
from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    TemporaryTargetPlan,
)
from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

_CREDENTIAL_URI = "postgresql://runtime-user:runtime-password@db.internal/dwh"


@pytest.mark.parametrize(
    ("uri", "secret", "expected"),
    [
        (
            "redis://:xvqzx-qzxv-zxvqzx@cache.internal/0",
            "xvqzx-qzxv-zxvqzx",
            f"redis://{REDACTION_TOKEN}@cache.internal/0",
        ),
        (
            "https://vqzxv-zxvq-xvqzxv@api.internal/path",
            "vqzxv-zxvq-xvqzxv",
            f"https://{REDACTION_TOKEN}@api.internal/path",
        ),
        (
            "postgresql://user%40example.com:zxvqzxv%3Avqzxvq@[2001:db8::1]/dwh",
            "zxvqzxv%3Avqzxvq",
            f"postgresql://{REDACTION_TOKEN}@[2001:db8::1]/dwh",
        ),
        (
            "custom://malformed-user:qzxvqzxvq-xvqzxv@@host.internal/path",
            "qzxvqzxvq-xvqzxv",
            f"custom://{REDACTION_TOKEN}@host.internal/path",
        ),
    ],
)
def test_shared_redactor_removes_complete_uri_authority_userinfo(
    uri: str,
    secret: str,
    expected: str,
) -> None:
    redacted = redact_text(uri)

    assert redacted == expected
    assert secret not in redacted


def test_safe_sample_command_result_redacts_programmatic_payload_and_final_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    token_uri = "https://final-json-token@api.internal/v1"
    result = SafeSampleCommandResult(
        exit_code=1,
        payload={
            "safe_sample": {
                "endpoint": token_uri,
                "diagnostics": [{"password": "final-json-password"}],
            }
        },
        text=f"failed at {token_uri}\n",
        markdown=f"# failed at {token_uri}\n",
    )

    assert "final-json-token" not in json.dumps(result.payload)
    assert "final-json-password" not in json.dumps(result.payload)
    assert "final-json-token" not in result.text
    assert "final-json-token" not in result.markdown

    assert result.write(format="json") == 1
    output = capsys.readouterr().out
    assert "final-json-token" not in output
    assert "final-json-password" not in output
    assert REDACTION_TOKEN in output


def _execution_plan() -> Any:
    target_plan = TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_dev",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
        ttl_seconds=86400,
        cleanup_required=True,
        pii_policy="masked",
    )
    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=target_plan,
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery={},
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256="sha256:" + "c" * 64,
        ),
    )


def test_safe_sample_runtime_redacts_uri_userinfo_before_report_and_evidence(tmp_path: Path) -> None:
    class ArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            del plan
            return {
                "schema": "dpone.init-fetch-result.v1",
                "passed": True,
                "endpoint": _CREDENTIAL_URI,
            }

    class TargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            return {"backend": "clickhouse", "endpoint": _CREDENTIAL_URI}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            return {"backend": "clickhouse", "dropped": True}

    class DataCopier:
        def copy(
            self,
            *,
            plan: Any,
            target_plan: TemporaryTargetPlan,
            init_fetch: dict[str, Any],
        ) -> dict[str, object]:
            del plan, target_plan, init_fetch
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "failed",
                "diagnostics": {
                    "attempts": [
                        f"connection refused at {_CREDENTIAL_URI}",
                        {
                            "detail": "retry budget exhausted for db.internal",
                            "password": "mapping-secret",
                        },
                    ]
                },
                "errors": [
                    {
                        "schema": "dpone.error.v1",
                        "code": "DPONE_SAFE_SAMPLE_SOURCE_READ_FAILED",
                        "stage": "data_copy",
                        "severity": "error",
                        "message": "source failed with token=assignment-secret",
                        "fixes": [],
                    }
                ],
            }

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=ArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=TargetAdapter()),
        data_copier=DataCopier(),
    ).execute(_execution_plan())

    runtime_text = json.dumps(result.to_dict(), sort_keys=True)
    assert "runtime-password" not in runtime_text
    assert "mapping-secret" not in runtime_text
    assert "assignment-secret" not in runtime_text
    assert REDACTION_TOKEN in runtime_text
    assert "db.internal" in runtime_text
    assert "retry budget exhausted" in runtime_text

    output_dir = tmp_path / "evidence"
    report = SafeSampleRuntimeEvidenceWriter().write(result, output_dir)
    evidence_text = (output_dir / "safe-sample-runtime-execution.json").read_text(encoding="utf-8")
    assert report.path == "$OUTPUT_ROOT/safe-sample-runtime-execution.json"
    assert "runtime-password" not in evidence_text
    assert "mapping-secret" not in evidence_text
    assert "assignment-secret" not in evidence_text
    assert REDACTION_TOKEN in evidence_text
    assert "db.internal" in evidence_text
    assert "retry budget exhausted" in evidence_text


def test_safe_sample_evidence_redacts_uri_userinfo_at_persistence_boundary(tmp_path: Path) -> None:
    output_dir = tmp_path / "evidence"
    report = SafeSampleRuntimeEvidenceWriter().write(
        {
            "execution_status": "failed",
            "data_outcome": "unknown",
            "diagnostics": {
                "endpoint": _CREDENTIAL_URI,
                "attempts": [
                    f"connection refused at {_CREDENTIAL_URI}",
                    {"detail": "host db.internal remains actionable", "api_key": "mapping-secret"},
                ],
                "tuple_attempts": (f"tuple connection refused at {_CREDENTIAL_URI}",),
            },
        },
        output_dir,
    )

    evidence_text = (output_dir / "safe-sample-runtime-execution.json").read_text(encoding="utf-8")
    assert report.path == "$OUTPUT_ROOT/safe-sample-runtime-execution.json"
    assert "runtime-password" not in evidence_text
    assert "mapping-secret" not in evidence_text
    assert "tuple connection refused" in evidence_text
    assert REDACTION_TOKEN in evidence_text
    assert "db.internal" in evidence_text
    assert "remains actionable" in evidence_text


def test_safe_sample_evidence_redacts_all_uri_userinfo_forms(
    tmp_path: Path,
) -> None:
    credential_uris = (
        "redis://:empty-user-evidence-secret@cache.internal/0",
        "https://token-only-evidence-secret@api.internal/path",
        "postgresql://user%40example.com:percent%3Aevidence-secret@[2001:db8::1]/dwh",
    )

    output_dir = tmp_path / "evidence"
    report = SafeSampleRuntimeEvidenceWriter().write(
        {
            "execution_status": "failed",
            "data_outcome": "unknown",
            "diagnostics": {"endpoints": credential_uris},
        },
        output_dir,
    )

    evidence_text = (output_dir / "safe-sample-runtime-execution.json").read_text(encoding="utf-8")
    assert report.path == "$OUTPUT_ROOT/safe-sample-runtime-execution.json"
    assert "empty-user-evidence-secret" not in evidence_text
    assert "token-only-evidence-secret" not in evidence_text
    assert "percent%3Aevidence-secret" not in evidence_text
    assert "cache.internal" in evidence_text
    assert "api.internal" in evidence_text
    assert "2001:db8::1" in evidence_text
    assert evidence_text.count(REDACTION_TOKEN) == len(credential_uris)
