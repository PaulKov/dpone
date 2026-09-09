from __future__ import annotations

import json
from pathlib import Path

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.dbt_sqlserver_graph_policy import (
    DbtSqlserverPreviewGraphPolicyValidator,
    semantic_refresh_preview_selected_graph,
)
from dpone.contracts.dbt_semantic_refresh_source_proof import prove_raw_jinja_closure

MODEL_ID = "model.analytics.events"


def _model(*, compiled: str | None = None, dependency: str = "source.analytics.events") -> dict[str, object]:
    query = compiled or "select event_id, occurred_at from DWH.raw.events"
    return {
        "unique_id": MODEL_ID,
        "name": "events",
        "alias": "events",
        "resource_type": "model",
        "language": "sql",
        "database": "DWH",
        "schema": "mart",
        "fqn": ["analytics", "events"],
        "original_file_path": "models/events.sql",
        "raw_code": "select event_id, occurred_at from {{ source('raw', 'events') }}",
        "compiled_code": query,
        "meta": {"dpone": {"publish": {"enabled": True}}},
        "config": {
            "enabled": True,
            "materialized": "incremental",
            "on_schema_change": "fail",
            "contract": {"enforced": True},
            "pre-hook": [],
            "post-hook": [],
        },
        "depends_on": {"nodes": [dependency], "macros": []},
    }


def _manifest(model: dict[str, object], *, extra: dict[str, object] | None = None) -> dict[str, object]:
    nodes = {MODEL_ID: model, **(extra or {})}
    return {
        "nodes": nodes,
        "unit_tests": {},
        "macros": {},
        "parent_map": {unique_id: [] for unique_id in nodes},
        "child_map": {unique_id: [] for unique_id in nodes},
    }


class _AcceptOfficialSchema:
    def validate(self, _payload: object, *, version: int) -> tuple[()]:
        del version
        return ()


def _artifact_manifest(model: dict[str, object], macros: dict[str, object]) -> dict[str, object]:
    return {
        **_manifest(model),
        "metadata": {
            "adapter_type": "sqlserver",
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.12.3",
            "invocation_id": "semantic-refresh-test",
            "project_name": "analytics",
        },
        "macros": macros,
    }


def _read_artifact(tmp_path: Path, payload: dict[str, object]):
    path = tmp_path / "artifact-manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return DbtArtifactReader(validator=_AcceptOfficialSchema()).read(path)


def _validate(
    tmp_path: Path,
    manifest: dict[str, object],
    *,
    require_target_independence: bool = False,
) -> tuple[str, ...]:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    issues = DbtSqlserverPreviewGraphPolicyValidator().validate_semantic_refresh(
        path,
        {"daily": (MODEL_ID,)},
        require_target_independence=require_target_independence,
    )
    return tuple(issue.code for issue in issues)


def test_v2_preview_uses_exact_roots_and_checks_read_only_sql(tmp_path: Path) -> None:
    manifest = _manifest(_model())

    assert semantic_refresh_preview_selected_graph(manifest, (MODEL_ID,)) == (MODEL_ID,)
    assert _validate(tmp_path, manifest) == ()


def test_v2_production_proof_requires_two_named_target_compilations(tmp_path: Path) -> None:
    codes = _validate(
        tmp_path,
        _manifest(_model()),
        require_target_independence=True,
    )

    assert "DPONE_DBT_V2_COMPILE_UNVERIFIED" in codes


def test_v2_production_proves_identical_named_target_compilations(tmp_path: Path) -> None:
    model = _model()
    model["compiled_code_by_target"] = {
        "certified_a": model["compiled_code"],
        "certified_b": model["compiled_code"],
    }

    assert (
        _validate(
            tmp_path,
            _manifest(model),
            require_target_independence=True,
        )
        == ()
    )


def test_v2_preview_blocks_compiled_target_read(tmp_path: Path) -> None:
    codes = _validate(
        tmp_path,
        _manifest(_model(compiled="select event_id, occurred_at from DWH.mart.events")),
    )

    assert "DPONE_DBT_V2_TARGET_READ_UNSUPPORTED" in codes


def test_v2_preview_blocks_transitive_ephemeral_dependency(tmp_path: Path) -> None:
    ephemeral_id = "model.analytics.ephemeral_events"
    ephemeral = {
        "unique_id": ephemeral_id,
        "resource_type": "model",
        "config": {"materialized": "ephemeral"},
        "depends_on": {"nodes": [], "macros": []},
    }
    codes = _validate(
        tmp_path,
        _manifest(_model(dependency=ephemeral_id), extra={ephemeral_id: ephemeral}),
    )

    assert "DPONE_DBT_V2_EPHEMERAL_UNSUPPORTED" in codes


def test_artifact_reader_resolves_complete_transitive_macro_source_closure(tmp_path: Path) -> None:
    model = _model()
    model["depends_on"] = {
        "nodes": ["source.analytics.events"],
        "macros": ["macro.analytics.direct"],
    }
    payload = _artifact_manifest(
        model,
        {
            "macro.analytics.direct": {
                "macro_sql": "{% macro direct() %}{{ transitive() }}{% endmacro %}",
                "depends_on": {"macros": ["macro.analytics.transitive"]},
            },
            "macro.analytics.transitive": {
                "macro_sql": "{% macro transitive() %}event_id{% endmacro %}",
                "depends_on": {"macros": []},
            },
        },
    )

    artifact, issues = _read_artifact(tmp_path, payload)

    assert issues == ()
    assert artifact is not None
    assert tuple(artifact.models[0].semantic_refresh_macro_sources) == (
        "macro.analytics.direct",
        "macro.analytics.transitive",
    )
    assert artifact.models[0].semantic_refresh_macro_closure_complete is True
    assert tuple(artifact.models[0].macro_sources) == ("macro.analytics.direct",)


def test_transitive_macro_missing_or_forbidden_source_never_proves(tmp_path: Path) -> None:
    model = _model()
    model["depends_on"] = {
        "nodes": ["source.analytics.events"],
        "macros": ["macro.analytics.direct"],
    }
    missing, missing_issues = _read_artifact(
        tmp_path,
        _artifact_manifest(
            model,
            {
                "macro.analytics.direct": {
                    "macro_sql": "{% macro direct() %}{{ transitive() }}{% endmacro %}",
                    "depends_on": {"macros": ["macro.analytics.transitive"]},
                }
            },
        ),
    )
    assert missing is not None
    assert missing_issues == ()
    assert missing.models[0].semantic_refresh_macro_closure_complete is False
    assert missing.models[0].semantic_refresh_macro_sources == {}

    forbidden, forbidden_issues = _read_artifact(
        tmp_path,
        _artifact_manifest(
            model,
            {
                "macro.analytics.direct": {
                    "macro_sql": "{% macro direct() %}{{ transitive() }}{% endmacro %}",
                    "depends_on": {"macros": ["macro.analytics.transitive"]},
                },
                "macro.analytics.transitive": {
                    "macro_sql": "{% macro transitive() %}{{ run_query('delete from dbo.x') }}{% endmacro %}",
                    "depends_on": {"macros": []},
                },
            },
        ),
    )
    assert forbidden_issues == ()
    assert forbidden is not None
    macro_sources = forbidden.models[0].semantic_refresh_macro_sources
    proof = prove_raw_jinja_closure(
        model_raw_sql=forbidden.models[0].raw_code,
        macro_sources=macro_sources,
        required_macro_ids=tuple(macro_sources),
        allowed_vars=("dpone_data_interval_end", "dpone_data_interval_start"),
        maximum_source_bytes=1024 * 1024,
    )
    assert proof.status == "NONCONFORMANT"
    assert "DPONE_DBT_V2_DYNAMIC_MODEL_SOURCE_UNSUPPORTED" in {issue.code for issue in proof.issues}


def test_v1_artifact_reader_preserves_direct_macro_compatibility(tmp_path: Path) -> None:
    model = _model()
    model["depends_on"] = {
        "nodes": ["source.analytics.events"],
        "macros": ["macro.analytics.direct"],
    }
    artifact, issues = _read_artifact(
        tmp_path,
        _artifact_manifest(
            model,
            {
                "macro.analytics.direct": {
                    "macro_sql": "{% macro direct() %}event_id{% endmacro %}",
                    "depends_on": {"macros": ["macro.analytics.unavailable"]},
                }
            },
        ),
    )

    assert issues == ()
    assert artifact is not None
    assert artifact.models[0].macro_sources == {"macro.analytics.direct": "{% macro direct() %}event_id{% endmacro %}"}
