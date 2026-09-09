from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator

from dpone.manifest.loader import ManifestLoaderRouter

ROOT = Path(__file__).resolve().parents[1]
GUIDE_ROOT = ROOT / "docs" / "source-sink"
EXAMPLE_ROOT = ROOT / "examples" / "source-sink"
SCHEMA_PATH = ROOT / "src" / "dpone" / "schema" / "etl-batch-manifest.schema.json"
GUIDES = tuple(sorted(GUIDE_ROOT.glob("*.md")))


def _copy_paste_manifest(path: Path) -> Mapping[str, Any]:
    content = path.read_text(encoding="utf-8")
    section = content.split("## Copy/paste manifest", 1)[1].split("\n## ", 1)[0]
    block = section.split("```yaml", 1)[1].split("```", 1)[0]
    manifest = yaml.safe_load(block)
    assert isinstance(manifest, Mapping), path
    return manifest


def _format_schema_errors(validator: Draft7Validator, manifest: Mapping[str, Any]) -> list[str]:
    return [
        f"{'/'.join(str(part) for part in error.absolute_path) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(manifest), key=lambda item: list(item.absolute_path))
    ]


def _endpoint(manifest: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    defaults = manifest.get("defaults")
    assert isinstance(defaults, Mapping)
    endpoint = defaults.get(name)
    assert isinstance(endpoint, Mapping)
    return endpoint


def _strategy_mode(manifest: Mapping[str, Any]) -> str:
    strategy = _endpoint(manifest, "sink").get("strategy")
    assert isinstance(strategy, Mapping)
    mode = strategy.get("mode")
    assert isinstance(mode, str)
    return mode


def test_source_sink_guides_and_examples_are_one_to_one() -> None:
    assert len(GUIDES) == 30
    expected_examples = {f"{path.stem}.yaml" for path in GUIDES}
    actual_examples = {path.name for path in EXAMPLE_ROOT.glob("*.yaml")}

    assert actual_examples == expected_examples


def test_copy_paste_manifests_match_referenced_examples_semantically() -> None:
    for guide in GUIDES:
        example = EXAMPLE_ROOT / f"{guide.stem}.yaml"
        reference = f"examples/source-sink/{example.name}"
        content = guide.read_text(encoding="utf-8")

        assert reference in content, guide
        assert f"dpone plan {reference} --format md" in content, guide
        assert f"dpone run {reference}" in content, guide
        assert yaml.safe_load(example.read_text(encoding="utf-8")) == _copy_paste_manifest(guide), guide


def test_all_source_sink_examples_validate_and_load_without_external_io() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft7Validator.check_schema(schema)
    validator = Draft7Validator(schema)
    loader = ManifestLoaderRouter(registry_paths=())

    for guide in GUIDES:
        example = EXAMPLE_ROOT / f"{guide.stem}.yaml"
        manifest = yaml.safe_load(example.read_text(encoding="utf-8"))
        assert isinstance(manifest, Mapping), example
        assert _format_schema_errors(validator, manifest) == [], example

        loaded = loader.load(example, metadata_only=True)
        assert loaded.kind == "dpone.batch.v1", example
        assert len(loaded.processes) == 1, example

        source_name, sink_name = guide.stem.split("-to-", 1)
        process = loaded.processes[0]
        assert process.name == f"{source_name}_to_{sink_name}_example", example
        assert process.raw_config["source"]["type"] == source_name, example
        assert process.raw_config["sink"]["type"] == sink_name, example
        assert process.config.load_config.load_strategy.value == _strategy_mode(manifest), example


def test_examples_use_only_canonical_connector_strategy_and_export_values() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    fragment = schema["definitions"]["process_fragment"]["properties"]
    source_schema = fragment["source"]["properties"]
    sink_schema = fragment["sink"]["properties"]
    source_types = set(source_schema["type"]["enum"])
    sink_types = set(sink_schema["type"]["enum"])
    strategy_modes = set(sink_schema["strategy"]["properties"]["mode"]["enum"])
    export_formats = set(source_schema["options"]["properties"]["export_format"]["enum"])

    for guide in GUIDES:
        manifest = _copy_paste_manifest(guide)
        source = _endpoint(manifest, "source")
        sink = _endpoint(manifest, "sink")
        source_options = source.get("options", {})

        assert source.get("type") in source_types, guide
        assert sink.get("type") in sink_types, guide
        assert _strategy_mode(manifest) in strategy_modes, guide
        assert isinstance(source_options, Mapping), guide
        if "export_format" in source_options:
            assert source_options["export_format"] in export_formats, guide
        for endpoint in (source, sink):
            table = endpoint.get("table")
            if table is not None:
                assert isinstance(table, Mapping), guide
                assert set(table) <= {"schema", "name"}, guide


def test_kafka_sink_guides_describe_append_only_publication() -> None:
    for guide in GUIDES:
        manifest = _copy_paste_manifest(guide)
        if _endpoint(manifest, "sink").get("type") != "kafka":
            continue

        content = guide.read_text(encoding="utf-8")
        lowered = content.lower()
        assert _strategy_mode(manifest) == "incremental_append", guide
        assert "append-only message publication" in lowered, guide
        for unsupported_claim in (
            "replace the target",
            "table replacement",
            "staging swap",
            "shadow swap",
        ):
            assert unsupported_claim not in lowered, guide


def test_guides_separate_snapshot_diff_from_reconciliation_capability() -> None:
    for guide in GUIDES:
        content = guide.read_text(encoding="utf-8")
        strategy_section = content.split("## Supported load strategies", 1)[1].split("\n## ", 1)[0]
        behavior_section = content.split("## Strategy behavior", 1)[1].split("\n## ", 1)[0]

        assert "| `snapshot_diff` |" in strategy_section, guide
        assert "- `snapshot_diff`:" in behavior_section, guide
        assert "reconciliation.mode=snapshot" in content, guide
        assert "| `snapshot_reconciliation` |" not in strategy_section, guide
        assert "- `snapshot_reconciliation`:" not in behavior_section, guide


def test_full_refresh_transition_requires_an_explicit_non_empty_target_gate() -> None:
    for guide in GUIDES:
        content = guide.read_text(encoding="utf-8")

        assert "`0` source / `0` target" in content, guide
        assert "id: target_min_rows" in content, guide
        assert "type: min_rows" in content, guide
        assert "side: target" in content, guide
        assert "threshold: 1" in content, guide
