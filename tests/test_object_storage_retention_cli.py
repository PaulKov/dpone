from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.storage import ObjectStorageUri


def test_object_storage_lifecycle_render_cli_outputs_prefix_scoped_rule(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "object-storage",
                "lifecycle",
                "render",
                "--uri-prefix",
                "s3://example-data-bucket/dpone-stage/",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.object_storage.lifecycle_readiness.v1"
    assert payload["rules"][0]["Filter"]["Prefix"] == "dpone-stage/"


def test_object_storage_budget_cli_uses_local_inventory(capsys, tmp_path: Path) -> None:
    object_path = tmp_path / "store" / "s3" / "example-data-bucket" / "dpone-stage" / "prod" / "mart" / "table" / "run-1"
    object_path.mkdir(parents=True)
    (object_path / "chunk.parquet").write_bytes(b"x" * 10)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "object-storage",
                "budget",
                "--uri-prefix",
                "s3://example-data-bucket/dpone-stage/prod/",
                "--limit",
                "100",
                "--local-root-dir",
                str(tmp_path / "store"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.object_storage.budget_guard.v1"
    assert payload["used_bytes"] == 10
    assert payload["status"] == "green"


def test_object_storage_cleanup_cli_dry_run_does_not_delete(capsys, tmp_path: Path) -> None:
    prefix = tmp_path / "store" / "s3" / "example-data-bucket" / "dpone-stage" / "prod" / "mart" / "table" / "run-1"
    prefix.mkdir(parents=True)
    marker = {
        "schema_version": "dpone.object_storage.run_marker.v1",
        "run_id": "run-1",
        "workload_id": "workload",
        "table": "table",
        "created_at": "2026-06-29T00:00:00+00:00",
        "expires_at": "2026-06-29T01:00:00+00:00",
        "status": "failed",
    }
    (prefix / "__dpone_run_marker.json").write_text(json.dumps(marker), encoding="utf-8")
    (prefix / "chunk.parquet").write_bytes(b"x")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "object-storage",
                "cleanup",
                "--uri-prefix",
                "s3://example-data-bucket/dpone-stage/prod/",
                "--mode",
                "dry-run",
                "--local-root-dir",
                str(tmp_path / "store"),
                "--now",
                "2026-06-30T00:00:00+00:00",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.object_storage.cleanup_sweep.v1"
    assert payload["candidate_count"] == 1
    assert ObjectStorageUri.parse(payload["candidates"][0]["prefix"]).key.endswith("run-1/")
    assert (prefix / "chunk.parquet").exists()
