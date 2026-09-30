from __future__ import annotations

from pathlib import Path

import yaml


def test_native_sqlclient_certification_workflow_is_exact_commit_and_fail_closed() -> None:
    path = Path(".github/workflows/mssql-sqlclient-certification.yml")
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    dispatch = workflow[True]["workflow_dispatch"]["inputs"]
    job = workflow["jobs"]["certify"]
    steps = "\n".join(str(step.get("run", "")) for step in job["steps"])

    assert dispatch["expected_commit_sha"]["required"] is True
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] >= 60
    assert "uname -m" in steps and "x86_64" in steps
    assert "git rev-parse HEAD" in steps
    assert 'test "${EXPECTED_COMMIT_SHA}" = "${GITHUB_SHA}"' in steps
    assert "mssql_sqlclient_certification_image.py" in steps
    assert "mssql_sqlclient_certification_runner.py" in steps
    assert "mssql_sqlclient_transport_campaign.py" in steps
    assert "DPONE_IT_MSSQL_MEMORY_LIMIT_MB=3072" in steps
    assert "up -d --wait clickhouse mssql\n" in steps
    assert "run --rm mssql-init" in steps
    assert "test_clickhouse_mssql_target_local_runtime_live.py" in steps
    assert "test_clickhouse_mssql_default_native_runtime_live.py" in steps
    assert "DPONE_SQLCLIENT_CERT_LAYOUT_VERSION" in steps
    assert "DPONE_CERTIFICATION_IMAGE_SHA256" in steps
    assert '--user "$(id -u):$(id -g)"' in steps
    assert "assert_junit_executed.py" in steps
    assert "--min-passed 1 --max-skipped 0" in steps
    assert "--min-passed 13 --max-skipped 0" in steps
    assert any(step.get("uses", "").startswith("actions/upload-artifact@") for step in job["steps"])
