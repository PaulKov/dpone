from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_wide_release_authority.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_wide_release_authority", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Mssql:
    def fetch_schema(self, schema: str, table: str):
        assert (schema, table) == ("dbt_calc", "wide_dbt_result")
        return [(f"column_{index:03d}", "int nullable") for index in range(202)]

    def close(self) -> None:
        return None


def test_authority_is_create_once_self_hashed_and_exact_snapshot_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_tool_module()
    snapshot = module.capture_local_source_snapshot()
    snapshot = type(snapshot)(snapshot.git_head_sha, snapshot.source_snapshot_sha256, False)
    monkeypatch.setattr(module, "capture_local_source_snapshot", lambda: snapshot)
    monkeypatch.setattr(module.base, "_mssql_connector", lambda config: _Mssql())
    args = module.parse_args(
        [
            "--release-id",
            "0.74.0",
            "--source-schema",
            "dbt_calc",
            "--source-table",
            "wide_dbt_result",
            "--mssql-password",
            "secret",
            "--output",
            str(tmp_path / "authority.json"),
        ]
    )

    path = module.write_authority(args.output, module.build_authority(args))
    value = module.load_authority(
        path,
        release_id="0.74.0",
        source_relation="dbt_calc.wide_dbt_result",
    )

    schema = json.loads(
        Path("docs/schemas/dpone.mssql-clickhouse-wide-release-authority.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(value)
    assert value["target_schema_sha256_by_slot"]["bcp_native_required"]
    assert value["target_schema_sha256_by_slot"]["parquet_s3_pull"]
    with pytest.raises(FileExistsError):
        module.write_authority(path, value)

    tampered = dict(value)
    tampered["s3_bucket"] = "forged"
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match="digest_mismatch"):
        module.load_authority(
            path,
            release_id="0.74.0",
            source_relation="dbt_calc.wide_dbt_result",
        )
