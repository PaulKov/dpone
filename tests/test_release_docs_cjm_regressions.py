from __future__ import annotations

import re
from pathlib import Path

import yaml

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.managed_planning import ExecutionPlanService

ROOT = Path(__file__).resolve().parents[1]
NESTED_DOC = ROOT / "docs" / "nested-normalization.md"
RUN_DOC = ROOT / "docs" / "run.md"
QUICKSTART_DOC = ROOT / "docs" / "getting-started" / "quickstart.md"
NESTED_EXAMPLES = (
    ROOT / "examples" / "nested" / "rest_api_to_clickhouse.yml",
    ROOT / "examples" / "nested" / "kafka_to_clickhouse.yml",
)


def _code_blocks(text: str, language: str) -> list[str]:
    return re.findall(rf"```{language}\n(.*?)\n```", text, re.DOTALL)


def test_nested_lifecycle_yaml_fragments_use_sink_options() -> None:
    document = NESTED_DOC.read_text(encoding="utf-8")
    section = document.split("## Child lifecycle:", maxsplit=1)[1].split(
        "### Live certification harness",
        maxsplit=1,
    )[0]

    fragments = [yaml.safe_load(block) for block in _code_blocks(section, "yaml")]

    assert fragments
    assert all("normalization" not in fragment for fragment in fragments)
    normalization_fragments = [
        fragment
        for fragment in fragments
        if isinstance(fragment.get("sink"), dict)
        and isinstance(fragment["sink"].get("options"), dict)
        and "normalization" in fragment["sink"]["options"]
    ]
    assert len(normalization_fragments) == 6


def test_python_quickstart_selector_matches_manifest_name() -> None:
    run_document = RUN_DOC.read_text(encoding="utf-8")
    quickstart_document = QUICKSTART_DOC.read_text(encoding="utf-8")
    manifest = yaml.safe_load(_code_blocks(quickstart_document, "yaml")[0])
    python_quickstart = _code_blocks(run_document, "python")[0]

    match = re.search(r'selector="([^"]+)"', python_quickstart)

    assert match is not None
    assert match.group(1) == manifest["name"]


def test_advertised_nested_manifests_are_canonical_executable_flows() -> None:
    expected_sources = ("api", "kafka")
    for example, expected_source in zip(NESTED_EXAMPLES, expected_sources, strict=True):
        payload = yaml.safe_load(example.read_text(encoding="utf-8"))

        assert payload["kind"] == "dpone.flow.v1"
        assert payload["authoring"] == {"mode": "flow", "source": example.relative_to(ROOT).as_posix()}
        assert payload["metadata"]["id"]
        assert payload["processes"]
        process = payload["processes"][0]
        assert process["source"]["connection_ref"]
        assert process["sink"]["type"] == "clickhouse"
        assert process["sink"]["connection_ref"]
        assert process["sink"]["options"]["normalization"]["nested"]["enabled"] is True

        loaded = ManifestLoaderRouter().load(example, metadata_only=True)
        assert loaded.source_kind == "dpone.flow.v1"
        assert len(loaded.processes) == 1
        loaded_process = loaded.processes[0].raw_config
        assert loaded_process["source"]["type"] == expected_source
        assert loaded_process["sink"]["type"] == "clickhouse"
        assert loaded_process["sink"]["options"]["normalization"]["nested"]["enabled"] is True

        plan = ExecutionPlanService().plan_manifest(example)
        assert plan["source"]["type"] == expected_source
        assert plan["sink"]["type"] == "clickhouse"
        assert plan["strategy"]["mode"] == process["sink"]["strategy"]["mode"]
