from __future__ import annotations

from pathlib import Path

import pytest

from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.manifest.authoring import AuthoringCompiler
from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.errors import ManifestConfigurationError
from dpone.runtime.governance.service import LoadGovernanceService


def test_load_config_builder_preserves_root_quality_gates_for_runtime() -> None:
    load_config = LoadConfigBuilder().build(
        {
            "name": "orders",
            "source": {
                "type": "mssql",
                "connection_id": "source",
                "table": {"schema": "dbo", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_id": "sink",
                "table": {"schema": "raw", "name": "orders"},
            },
            "quality": {
                "gates": [
                    {
                        "id": "row_count_reconciliation",
                        "type": "row_count_reconciliation",
                        "severity": "error",
                    }
                ]
            },
        }
    )

    assert load_config.options["quality"]["gates"][0]["type"] == "row_count_reconciliation"


def test_flow_root_quality_reaches_compiled_process_load_config_and_runtime(tmp_path: Path) -> None:
    source_path = tmp_path / "pipelines/orders/pipeline.yaml"
    payload = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipelines/orders/pipeline.yaml"},
        "metadata": {"id": "orders", "domain": "sales"},
        "quality": {
            "mode": "fail",
            "checks": [{"type": "min_rows", "threshold": 1, "side": "target"}],
        },
        "processes": [
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_id": "source",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_id": "sink",
                    "table": {"schema": "raw", "name": "orders"},
                },
            }
        ],
    }

    compilation = AuthoringCompiler().compile(payload, source_path=source_path, project_root=tmp_path)
    load_config = LoadConfigBuilder().build(compilation.processes[0])

    assert load_config.options["quality"] == payload["quality"]

    class _Extract:
        artifact = type("Artifact", (), {"estimated_rows": 0})()
        typed_hash = None

    class _Load:
        staging_rows = 0
        total_rows = 0
        typed_hash = None

    try:
        LoadGovernanceService().run_quality_gates(
            load_config=load_config,
            extract_result=_Extract(),
            load_result=_Load(),
        )
    except RuntimeError as exc:
        assert str(exc) == "quality gates failed"
    else:
        raise AssertionError("root quality policy must fail an empty target")


def test_explicit_batch_root_quality_replaces_legacy_defaults_quality(tmp_path: Path) -> None:
    root_quality = {
        "gates": [
            {
                "id": "root_rows",
                "type": "min_rows",
                "side": "target",
                "threshold": 1,
            }
        ]
    }
    compiled = BatchManifestCompiler().compile(
        {
            "kind": "dpone.batch.v1",
            "quality": root_quality,
            "defaults": {
                "name": "orders",
                "source": {"type": "mssql", "connection_id": "source"},
                "sink": {"type": "clickhouse", "connection_id": "sink"},
                "quality": {
                    "gates": [
                        {
                            "id": "legacy_rows",
                            "type": "min_rows",
                            "side": "target",
                            "threshold": 999,
                        }
                    ]
                },
            },
            "schemas": {"dbo": {"tables": ["orders"]}},
        },
        manifest_path=tmp_path / "pipeline.yaml",
    )

    assert compiled[0].raw_config["quality"] == root_quality


@pytest.mark.parametrize("endpoint", ["source", "sink"])
def test_load_config_builder_rejects_misplaced_endpoint_quality(endpoint: str) -> None:
    config = {
        "name": "orders",
        "source": {
            "type": "mssql",
            "connection_id": "source",
            "table": {"schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "sink",
            "table": {"schema": "raw", "name": "orders"},
        },
        "quality": {
            "gates": [
                {
                    "id": "authoritative",
                    "type": "min_rows",
                    "threshold": 1,
                }
            ]
        },
    }
    config[endpoint]["options"] = {
        "quality": {
            "gates": [
                {
                    "id": "shadow",
                    "type": "min_rows",
                    "threshold": 999,
                }
            ]
        }
    }

    with pytest.raises(DagConfigurationError, match=rf"{endpoint}\.options\.quality"):
        LoadConfigBuilder().build(config)


def test_process_quality_atomically_overrides_root_compatibility_checks(tmp_path: Path) -> None:
    process_quality = {
        "gates": [
            {
                "id": "process_target_rows",
                "type": "min_rows",
                "side": "target",
                "threshold": 2,
            }
        ]
    }
    payload = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipelines/orders/pipeline.yaml"},
        "metadata": {"id": "orders", "domain": "sales"},
        "quality": {
            "mode": "fail",
            "checks": [{"type": "min_rows", "threshold": 999}],
        },
        "processes": [
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_id": "source",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_id": "sink",
                    "table": {"schema": "raw", "name": "orders"},
                },
                "quality": process_quality,
            }
        ],
    }

    compilation = AuthoringCompiler().compile(
        payload,
        source_path=tmp_path / "pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
    )
    compiled_quality = compilation.processes[0]["quality"]
    load_config = LoadConfigBuilder().build(dict(compilation.processes[0]))

    assert compiled_quality == process_quality
    assert "checks" not in compiled_quality
    assert load_config.options["quality"] == process_quality


@pytest.mark.parametrize("quality", ["invalid", [], 1])
def test_batch_compiler_rejects_non_mapping_process_quality(tmp_path: Path, quality: object) -> None:
    payload = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipelines/orders/pipeline.yaml"},
        "metadata": {"id": "orders", "domain": "sales"},
        "processes": [
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_id": "source",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_id": "sink",
                    "table": {"schema": "raw", "name": "orders"},
                },
                "quality": quality,
            }
        ],
    }

    with pytest.raises(ManifestConfigurationError, match="quality"):
        AuthoringCompiler().compile(
            payload,
            source_path=tmp_path / "pipelines/orders/pipeline.yaml",
            project_root=tmp_path,
        )
