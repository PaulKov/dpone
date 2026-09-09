"""Catalog publication artifacts for OpenLineage, dbt, and DataHub handoff."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class CatalogPublicationReport:
    passed: bool
    run_id: str
    dataset_count: int
    openlineage_path: str
    dbt_sources_path: str
    datahub_path: str
    json_path: str
    markdown_path: str
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return "\n".join(
            [
                "# dpone catalog publication pack",
                "",
                f"- Passed: `{self.passed}`",
                f"- Run ID: `{self.run_id}`",
                f"- Dataset count: `{self.dataset_count}`",
                f"- OpenLineage: `{self.openlineage_path}`",
                f"- dbt sources: `{self.dbt_sources_path}`",
                f"- DataHub MCP-like payload: `{self.datahub_path}`",
                "",
                "## Runbook",
                "",
                "1. Send the OpenLineage artifact to an OpenLineage-compatible collector.",
                "2. Copy `dbt_sources.yml` into the downstream dbt project and review naming.",
                "3. Publish `datahub_mcp.json` through your DataHub ingestion job.",
                "4. Keep `run_id` stable across run registry, lineage, and catalog artifacts.",
                "",
            ]
        )


class CatalogPublicationService:
    """Builds catalog handoff payloads from a run registry entry and dataset refs."""

    def publish(
        self,
        *,
        output_dir: str | Path,
        run_registry_entry_path: str | Path,
        input_datasets: Mapping[str, str],
        output_datasets: Mapping[str, str],
        namespace: str,
    ) -> CatalogPublicationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        registry = _read_json(Path(run_registry_entry_path))
        run_id = str(registry.get("run_id") or "unknown_run")
        all_datasets = {**dict(input_datasets), **dict(output_datasets)}
        openlineage_path = directory / "openlineage_catalog_event.json"
        dbt_sources_path = directory / "dbt_sources.yml"
        datahub_path = directory / "datahub_mcp.json"
        json_path = directory / "catalog_publication.json"
        markdown_path = directory / "catalog_publication.md"
        openlineage_path.write_text(
            json.dumps(
                _openlineage_event(
                    run_id=run_id,
                    namespace=namespace,
                    process=str(registry.get("process") or "unknown"),
                    input_datasets=input_datasets,
                    output_datasets=output_datasets,
                ),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        dbt_sources_path.write_text(_dbt_sources(all_datasets), encoding="utf-8")
        datahub_path.write_text(
            json.dumps(_datahub_payload(all_datasets), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        report = CatalogPublicationReport(
            passed=bool(input_datasets or output_datasets),
            run_id=run_id,
            dataset_count=len(all_datasets),
            openlineage_path=str(openlineage_path),
            dbt_sources_path=str(dbt_sources_path),
            datahub_path=str(datahub_path),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
            output_dir=str(directory),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _openlineage_event(
    *,
    run_id: str,
    namespace: str,
    process: str,
    input_datasets: Mapping[str, str],
    output_datasets: Mapping[str, str],
) -> dict[str, object]:
    return {
        "eventType": "COMPLETE",
        "eventTime": utc_now_iso(),
        "producer": "https://github.com/PaulKov/dpone",
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": process},
        "inputs": [_dataset(ns, name) for ns, name in sorted(input_datasets.items())],
        "outputs": [_dataset(ns, name) for ns, name in sorted(output_datasets.items())],
    }


def _dataset(namespace: str, name: str) -> dict[str, object]:
    return {"namespace": namespace, "name": name, "facets": {}}


def _dbt_sources(datasets: Mapping[str, str]) -> str:
    lines = ["version: 2", "", "sources:"]
    for namespace, name in sorted(datasets.items()):
        source_name = _safe_name(namespace)
        table_name = _safe_name(name.split(".")[-1])
        lines.extend(
            [
                f"  - name: {source_name}",
                "    tables:",
                f"      - name: {table_name}",
                f"        description: Generated by dpone catalog publication for `{namespace}.{name}`.",
            ]
        )
    return "\n".join(lines) + "\n"


def _datahub_payload(datasets: Mapping[str, str]) -> dict[str, object]:
    return {
        "entities": [
            {
                "urn": f"urn:li:dataset:(urn:li:dataPlatform:{_safe_name(namespace)},{name},PROD)",
                "name": name,
                "platform": namespace,
                "aspects": {"dponeManaged": True},
            }
            for namespace, name in sorted(datasets.items())
        ]
    }


def _safe_name(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value.lower())
    return safe.strip("_") or "dpone"


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, Mapping) else {}
