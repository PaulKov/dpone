from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.airflow_live_preflight import LivePreflightContext, build_live_preflight_report


class _ReturnedSecretRunner:
    def run(self, context: LivePreflightContext) -> dict[str, object]:
        return {
            "probes": [
                {
                    "connection_ref": context.resolved_connection_refs[0],
                    "probe": "credential_resolution",
                    "status": "failed",
                }
            ],
            "errors": [
                {
                    "code": "DPONE_LIVE_CHECK_PROBE_FAILED",
                    "message": ("postgresql://user:uri-password@db.internal Authorization: Bearer bearer-secret"),
                    "details": {"vault_token": "nested-secret"},
                }
            ],
        }


class _RaisedSecretRunner:
    def run(self, context: LivePreflightContext) -> dict[str, object]:
        del context
        raise RuntimeError("postgresql://user:uri-password@db.internal token=plain-secret")


def _report(runner: object) -> dict[str, object]:
    return build_live_preflight_report(
        connection_details={
            "connection_refs": ["pg_source"],
            "resolved_connection_refs": ["pg_source"],
        },
        environment="dev",
        source_path=Path("/private/project/pipelines/orders/pipeline.yaml"),
        source_label="pipelines/orders/pipeline.yaml",
        runner=runner,  # type: ignore[arg-type]
    )


def test_returned_runner_errors_use_shared_recursive_redaction() -> None:
    rendered = json.dumps(_report(_ReturnedSecretRunner()), sort_keys=True)

    assert "uri-password" not in rendered
    assert "bearer-secret" not in rendered
    assert "nested-secret" not in rendered
    assert "pg_source" in rendered


def test_raised_runner_error_redacts_uri_userinfo_and_assignments() -> None:
    rendered = json.dumps(_report(_RaisedSecretRunner()), sort_keys=True)

    assert "uri-password" not in rendered
    assert "plain-secret" not in rendered
    assert "pipelines/orders/pipeline.yaml" in rendered
