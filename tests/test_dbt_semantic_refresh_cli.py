from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

import pytest

from dpone.commands import dbt_publish_cmd
from dpone.commands.dbt_publish_cli_support import emit_model
from tests.test_dbt_semantic_refresh_plan_compiler import _template_pack


def test_v2_text_explain_is_the_exact_documented_projection(capsys) -> None:
    emit_model(
        {
            "model": "model.project.competitive_pricing",
            "source_relation": {"database": "DWH", "schema": "mart", "name": "competitive_pricing"},
            "workload_id": "competitive_pricing",
            "resolved_strategy": {"mode": "semantic_refresh_v2"},
            "resolved_physical_design": {},
            "semantic_refresh": {
                "workflow_mode": "normal",
                "model_archetype": "scope_stable_event_fact",
                "scope": "UTC day [start,end)",
                "static_dependency_closure": "UNVERIFIED",
                "runtime_dependency_closure": "RUNTIME_REQUIRED",
                "ephemeral_nodes": "none",
                "adapter_lifecycle_policy": "UNVERIFIED",
                "runtime_adapter_lifecycle": "RUNTIME_REQUIRED",
                "dbt_core": "1.12.3",
                "dbt_sqlserver": "1.11.1",
                "writer_assurance": "RUNTIME_REQUIRED",
                "source_side_pruning": "NOT_PROVEN",
                "event_time_policy": "immutable_effective_key_member",
                "effective_key_policy": "non_null_injective_exact",
                "utc_assurance": "RUNTIME_REQUIRED",
                "publication": "sequential_model_atomic",
                "recovery": "evidence_driven",
                "replay_mutation": "upsert_only",
                "row_removal": "unsupported",
                "profile_sha256": "sha256:" + "a" * 64,
            },
        }
    )
    assert capsys.readouterr().out == (
        "Asset: model.project.competitive_pricing\n"
        "Model: scope_stable_event_fact\n"
        "Workflow mode: normal\n"
        "Scope: UTC day [start,end)\n"
        "Static policy: dependency closure UNVERIFIED; adapter lifecycle UNVERIFIED\n"
        "Runtime proof: dependency closure RUNTIME_REQUIRED; adapter lifecycle RUNTIME_REQUIRED\n"
        "Toolchain: dbt Core 1.12.3; dbt-sqlserver 1.11.1\n"
        "Assurance: writer RUNTIME_REQUIRED; source-side pruning NOT_PROVEN\n"
        "Key policy: event time immutable_effective_key_member; effective key non_null_injective_exact\n"
        "UTC assurance: RUNTIME_REQUIRED\n"
        "Publication: sequential_model_atomic; recovery evidence_driven\n"
        "Recovery: replay is upsert-only; row removal unsupported\n"
        "Profile: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
    )


def test_execute_pack_rejects_a_non_executable_semantic_release_template(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    template = tmp_path / "semantic-template.json"
    template.write_text(json.dumps(_template_pack()), encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = dbt_publish_cmd.cmd_execute_pack(
        Namespace(relative_pack_path=template.name, format="json"),
        ctx=object(),
        logger=logging.getLogger("test.dbt.semantic-template"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["code"] == "DPONE_DBT_V2_TEMPLATE_NOT_EXECUTABLE"
    assert payload["stage"] == "dbt_execute_pack"
