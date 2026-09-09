"""dbt artifact lineage export orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.dbt_artifacts import (
    MODEL_RESOURCE_TYPES,
    DbtArtifactGraph,
    DbtArtifactParser,
    DbtLineageEdge,
    DbtLineageNode,
)
from dpone.ops.dbt_openlineage import DbtOpenLineageEventBuilder


@dataclass(frozen=True, slots=True)
class DbtLineageReport:
    run_id: str
    invocation_id: str
    passed: bool
    blockers: tuple[str, ...]
    model_count: int
    source_count: int
    test_count: int
    edge_count: int
    lineage_path: str
    openlineage_path: str
    markdown_path: str
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "invocation_id": self.invocation_id,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "model_count": self.model_count,
            "source_count": self.source_count,
            "test_count": self.test_count,
            "edge_count": self.edge_count,
            "lineage_path": self.lineage_path,
            "openlineage_path": self.openlineage_path,
            "markdown_path": self.markdown_path,
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone dbt lineage export",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- dbt invocation ID: `{self.invocation_id}`",
            f"- Passed: `{self.passed}`",
            f"- Models: `{self.model_count}`",
            f"- Sources: `{self.source_count}`",
            f"- Tests: `{self.test_count}`",
            f"- Edges: `{self.edge_count}`",
            f"- Lineage graph: `{self.lineage_path}`",
            f"- OpenLineage events: `{self.openlineage_path}`",
        ]
        if self.blockers:
            lines.extend(["", "dbt lineage blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Run `dbt build` or `dbt run` before exporting lineage.",
                "2. Export `target/manifest.json` and `target/run_results.json` with `dpone ops dbt-lineage`.",
                "3. Send `dbt_openlineage.json` events to the same collector used for dpone run events.",
                "4. If dbt results are red, fix dbt models/tests before promoting dpone release evidence.",
                "",
            ]
        )
        return "\n".join(lines)


class DbtLineageService:
    """Builds graph and OpenLineage evidence from dbt artifacts."""

    def __init__(
        self,
        *,
        parser: DbtArtifactParser | None = None,
        event_builder: DbtOpenLineageEventBuilder | None = None,
    ) -> None:
        self._parser = parser or DbtArtifactParser()
        self._event_builder = event_builder or DbtOpenLineageEventBuilder()

    def export(
        self,
        *,
        output_dir: str | Path,
        manifest_path: str | Path,
        run_results_path: str | Path | None = None,
        run_registry_entry_path: str | Path | None = None,
        namespace: str = "dbt.local",
    ) -> DbtLineageReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        manifest_file = Path(manifest_path)
        run_results_file = Path(run_results_path) if run_results_path else None
        registry_file = Path(run_registry_entry_path) if run_registry_entry_path else None
        manifest = self._payload(manifest_file)
        run_results = self._payload(run_results_file) if run_results_file else {}
        registry = self._payload(registry_file) if registry_file else {}
        graph = self._parser.parse(manifest, run_results)
        blockers = self._blockers(manifest_file, manifest, graph, registry_file, registry)
        run_id = self._run_id(registry, graph.invocation_id)
        paths = self._paths(directory)
        self._write_graph(paths["lineage"], run_id, graph)
        self._write_events(paths["openlineage"], graph, run_id, namespace)
        report = self._report(
            directory=directory,
            paths=paths,
            run_id=run_id,
            graph=graph,
            blockers=blockers,
        )
        paths["markdown"].write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _paths(directory: Path) -> dict[str, Path]:
        return {
            "lineage": directory / "dbt_lineage.json",
            "openlineage": directory / "dbt_openlineage.json",
            "markdown": directory / "dbt_lineage.md",
        }

    def _write_graph(self, path: Path, run_id: str, graph: DbtArtifactGraph) -> None:
        payload = {
            "run_id": run_id,
            "invocation_id": graph.invocation_id,
            "nodes": {node.unique_id: node.to_dict() for node in graph.nodes.values()},
            "edges": [edge.to_dict() for edge in graph.edges],
        }
        self._write_json(path, payload)

    def _write_events(self, path: Path, graph: DbtArtifactGraph, run_id: str, namespace: str) -> None:
        events = self._event_builder.build_events(
            nodes=graph.nodes,
            run_id=run_id,
            invocation_id=graph.invocation_id,
            namespace=namespace,
        )
        self._write_json(path, {"events": events})

    def _report(
        self,
        *,
        directory: Path,
        paths: Mapping[str, Path],
        run_id: str,
        graph: DbtArtifactGraph,
        blockers: tuple[str, ...],
    ) -> DbtLineageReport:
        nodes = tuple(graph.nodes.values())
        return DbtLineageReport(
            run_id=run_id,
            invocation_id=graph.invocation_id,
            passed=not blockers,
            blockers=blockers,
            model_count=sum(1 for node in nodes if node.resource_type in MODEL_RESOURCE_TYPES),
            source_count=sum(1 for node in nodes if node.resource_type == "source"),
            test_count=sum(1 for node in nodes if node.resource_type == "test"),
            edge_count=len(graph.edges),
            lineage_path=str(paths["lineage"]),
            openlineage_path=str(paths["openlineage"]),
            markdown_path=str(paths["markdown"]),
            output_dir=str(directory),
        )

    def _blockers(
        self,
        manifest_file: Path,
        manifest: Mapping[str, Any],
        graph: DbtArtifactGraph,
        registry_file: Path | None,
        registry: Mapping[str, Any],
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        if not manifest_file.exists():
            blockers.append("dbt_manifest.missing")
        elif not manifest:
            blockers.append("dbt_manifest.invalid_json")
        if not graph.results_passed:
            blockers.append("dbt_results.not_passed")
        if registry_file and not registry_file.exists():
            blockers.append("run_registry.missing")
        elif registry_file and registry and not bool(registry.get("passed", False)):
            blockers.append("run_registry.not_passed")
        return tuple(blockers)

    @staticmethod
    def _payload(path: Path | None) -> Mapping[str, Any]:
        if path is None or not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _run_id(registry: Mapping[str, Any], invocation_id: str) -> str:
        registry_run_id = registry.get("run_id")
        if isinstance(registry_run_id, str) and registry_run_id:
            return registry_run_id
        return invocation_id

    @staticmethod
    def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = [
    "DbtLineageEdge",
    "DbtLineageNode",
    "DbtLineageReport",
    "DbtLineageService",
]
