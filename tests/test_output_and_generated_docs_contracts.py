from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.output import (
    OutputFormat,
    dumps_json,
    dumps_yaml,
    is_machine_output,
    resolve_output_format,
    write_json,
    write_line,
    write_text,
    write_text_file,
    write_yaml,
)
from dpone.output_table import render_table
from dpone.services.ci.yaml_update import (
    DEFAULT_INSTALL_MODE,
    DEFAULT_TARGET_DIR,
    SNAPSHOT_ROOT_KEY,
    render_airflow_dev_snapshot_override,
    update_airflow_dev_snapshot_file,
)
from dpone.services.docs.generated_block import (
    is_generated_doc_in_sync,
    replace_generated_block,
    sync_generated_doc,
)


def test_output_format_resolution_supports_format_output_json_flag_and_defaults() -> None:
    assert resolve_output_format(SimpleNamespace(format="yaml")) is OutputFormat.yaml
    assert resolve_output_format(SimpleNamespace(output="json", format="yaml")) is OutputFormat.json
    assert resolve_output_format(SimpleNamespace(output="", format="unknown", json=True)) is OutputFormat.json
    assert resolve_output_format(SimpleNamespace(), default=OutputFormat.yaml) is OutputFormat.yaml
    assert is_machine_output(OutputFormat.json) is True
    assert is_machine_output(OutputFormat.yaml) is True
    assert is_machine_output(OutputFormat.text) is False


def test_output_serializers_preserve_unicode_and_trailing_newlines(capsys: pytest.CaptureFixture[str]) -> None:
    payload = {"z": "последний", "a": [1, 2]}

    assert dumps_json(payload, sort_keys=True) == '{\n  "a": [\n    1,\n    2\n  ],\n  "z": "последний"\n}\n'
    assert dumps_yaml(payload, sort_keys=False) == "z: последний\na:\n- 1\n- 2\n"

    write_text("hello")
    write_line(" world")
    write_json({"ok": True}, indent=0)
    write_yaml({"ok": True})

    assert capsys.readouterr().out == 'hello world\n{\n"ok": true\n}\nok: true\n'


def test_write_text_file_creates_parent_directories(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "report.txt"

    write_text_file(target, "content")

    assert target.read_text(encoding="utf-8") == "content"


def test_render_table_outputs_headers_and_rows() -> None:
    rendered = render_table(["name", "count"], [["orders", 2], ["users", None]], align="r")

    assert "name" in rendered
    assert "count" in rendered
    assert "orders" in rendered
    assert "users" in rendered
    assert "None" in rendered or "" in rendered


def test_replace_generated_block_preserves_outer_content_and_replaces_exact_markers() -> None:
    original = "Intro\n\n<!-- START -->\nold\n<!-- END -->\n\nOutro\n"
    block = "<!-- START -->\nnew\n<!-- END -->"

    assert replace_generated_block(original, block, start_marker="<!-- START -->", end_marker="<!-- END -->") == (
        "Intro\n\n<!-- START -->\nnew\n<!-- END -->\nOutro\n"
    )

    with pytest.raises(ValueError, match="Generated block markers not found"):
        replace_generated_block("no markers", block, start_marker="<!-- START -->", end_marker="<!-- END -->")


def test_generated_doc_sync_writes_only_when_content_changes(tmp_path: Path) -> None:
    doc = tmp_path / "README.md"
    doc.write_text("Intro\n<!-- START -->\nold\n<!-- END -->\n", encoding="utf-8")
    block = "<!-- START -->\nnew\n<!-- END -->"

    changed, updated = sync_generated_doc(
        doc, rendered_block=block, start_marker="<!-- START -->", end_marker="<!-- END -->"
    )

    assert changed is True
    assert updated == "Intro\n\n<!-- START -->\nnew\n<!-- END -->\n"
    assert is_generated_doc_in_sync(doc, rendered_block=block, start_marker="<!-- START -->", end_marker="<!-- END -->")

    changed_again, _ = sync_generated_doc(
        doc,
        rendered_block=block,
        start_marker="<!-- START -->",
        end_marker="<!-- END -->",
    )
    assert changed_again is False


def test_airflow_dev_snapshot_override_validation_and_mapping() -> None:
    override = render_airflow_dev_snapshot_override("dpone==0.1.0")

    assert override.enabled is True
    assert override.install_mode == DEFAULT_INSTALL_MODE
    assert override.target_dir == DEFAULT_TARGET_DIR
    assert override.to_mapping() == {
        SNAPSHOT_ROOT_KEY: {
            "enabled": True,
            "installMode": "snapshot",
            "packageSpec": "dpone==0.1.0",
            "targetDir": "/opt/airflow/.dpone-pkgs",
        }
    }

    with pytest.raises(ValueError, match="package_spec must look like"):
        render_airflow_dev_snapshot_override("dpone")
    with pytest.raises(ValueError, match="install_mode must be non-empty"):
        render_airflow_dev_snapshot_override("dpone==0.1.0", install_mode="")
    with pytest.raises(ValueError, match="target_dir must be non-empty"):
        render_airflow_dev_snapshot_override("dpone==0.1.0", target_dir="")


def test_update_airflow_dev_snapshot_file_creates_and_merges_yaml(tmp_path: Path) -> None:
    target = tmp_path / "values" / "dev.yaml"

    created = update_airflow_dev_snapshot_file(target, "dpone==0.1.0")

    assert created.created is True
    assert created.changed is True
    assert yaml.safe_load(target.read_text(encoding="utf-8"))[SNAPSHOT_ROOT_KEY]["packageSpec"] == "dpone==0.1.0"

    target.write_text("existing:\n  keep: true\n", encoding="utf-8")
    updated = update_airflow_dev_snapshot_file(target, "dpone==0.2.0", enabled=False, target_dir="/tmp/dpone")

    assert updated.created is False
    assert updated.changed is True
    assert updated.payload["existing"] == {"keep": True}
    assert updated.payload[SNAPSHOT_ROOT_KEY] == {
        "enabled": False,
        "installMode": "snapshot",
        "packageSpec": "dpone==0.2.0",
        "targetDir": "/tmp/dpone",
    }

    unchanged = update_airflow_dev_snapshot_file(target, "dpone==0.2.0", enabled=False, target_dir="/tmp/dpone")
    assert unchanged.changed is False


def test_update_airflow_dev_snapshot_file_rejects_non_mapping_yaml(tmp_path: Path) -> None:
    target = tmp_path / "dev.yaml"
    target.write_text("- not\n- mapping\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Expected YAML mapping"):
        update_airflow_dev_snapshot_file(target, "dpone==0.1.0")
