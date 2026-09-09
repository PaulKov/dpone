"""OpenLineage-compatible export from dpone run registry entries."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file

OPENLINEAGE_PRODUCER = "https://github.com/PaulKov/dpone"
OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json"
DPONE_AIRFLOW_CORRELATION_FACET_SCHEMA_URL = (
    "https://raw.githubusercontent.com/PaulKov/dpone/"
    "v0.73.0/docs/schemas/openlineage/dpone-airflow-correlation-run-facet-v1.schema.json"
)


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


@dataclass(frozen=True, slots=True)
class OpenLineageDataset:
    namespace: str
    name: str

    def to_dict(self) -> dict[str, object]:
        return {"namespace": self.namespace, "name": self.name, "facets": {}}


@dataclass(frozen=True, slots=True)
class OpenLineageExportReport:
    run_id: str
    job_name: str
    event_type: str
    passed: bool
    blockers: tuple[str, ...]
    dataset_count: int
    event_path: str
    markdown_path: str
    output_dir: str
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "event_type": self.event_type,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "dataset_count": self.dataset_count,
            "event_path": self.event_path,
            "markdown_path": self.markdown_path,
            "output_dir": self.output_dir,
            "correlation_id": self.correlation_id,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone OpenLineage export",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- Job: `{self.job_name}`",
            f"- Event type: `{self.event_type}`",
            f"- Passed: `{self.passed}`",
            f"- Dataset count: `{self.dataset_count}`",
            f"- Event path: `{self.event_path}`",
        ]
        if self.blockers:
            lines.extend(["", "OpenLineage export blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Send `openlineage_event.json` payloads to an OpenLineage-compatible collector.",
                "2. Use `correlation_id` for Airflow joins; the dpone run ID remains in the dpone run facet.",
                "3. If event type is `FAIL`, inspect the linked run registry entry first.",
                "4. If dataset lists are empty, re-export with `--input namespace=name` and `--output namespace=name`.",
                "",
            ]
        )
        return "\n".join(lines)


class OpenLineageExportService:
    """Builds OpenLineage-compatible run events from dpone registry entries."""

    def __init__(self, *, correlation_loader: Any | None = None) -> None:
        self._correlation_loader = correlation_loader or _symbol(
            "dpone.observability.correlation:load_airflow_correlation"
        )

    def export(
        self,
        *,
        output_dir: str | Path,
        run_registry_entry_path: str | Path,
        namespace: str,
        input_datasets: Mapping[str, str] | None = None,
        output_datasets: Mapping[str, str] | None = None,
        event_type: str = "auto",
        airflow_evidence_bundle_path: str | Path | None = None,
    ) -> OpenLineageExportReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        registry_path = Path(run_registry_entry_path)
        payload = self._payload(registry_path)
        run_id = self._value(payload, "run_id", "unknown_run")
        job_name = self._value(payload, "process", "unknown")
        resolved_event_type = self._event_type(payload, event_type)
        blockers = list(self._blockers(registry_path, payload))
        correlation_result = self._correlation_loader(airflow_evidence_bundle_path)
        blockers.extend(correlation_result.blockers)
        correlation = correlation_result.correlation
        event_path = directory / f"{run_id}__openlineage.json"
        markdown_path = directory / f"{run_id}__openlineage.md"
        inputs = tuple(OpenLineageDataset(ns, name) for ns, name in sorted((input_datasets or {}).items()))
        outputs = tuple(OpenLineageDataset(ns, name) for ns, name in sorted((output_datasets or {}).items()))
        event = self._event(
            payload=payload,
            namespace=namespace,
            run_id=run_id,
            job_name=job_name,
            event_type=resolved_event_type,
            inputs=inputs,
            outputs=outputs,
            registry_path=registry_path,
            correlation=correlation,
        )
        event_path.write_text(json.dumps(event, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report = OpenLineageExportReport(
            run_id=run_id,
            job_name=job_name,
            event_type=resolved_event_type,
            passed=not blockers,
            blockers=tuple(blockers),
            dataset_count=len(inputs) + len(outputs),
            event_path=str(event_path),
            markdown_path=str(markdown_path),
            output_dir=str(directory),
            correlation_id=correlation.correlation_id if correlation is not None else None,
        )
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _event(
        self,
        *,
        payload: Mapping[str, Any],
        namespace: str,
        run_id: str,
        job_name: str,
        event_type: str,
        inputs: tuple[OpenLineageDataset, ...],
        outputs: tuple[OpenLineageDataset, ...],
        registry_path: Path,
        correlation: Any | None,
    ) -> dict[str, object]:
        run_facets = self._run_facets(payload=payload, registry_path=registry_path, correlation=correlation)
        return {
            "eventType": event_type,
            "eventTime": datetime.now(UTC).isoformat(),
            "producer": OPENLINEAGE_PRODUCER,
            "schemaURL": OPENLINEAGE_SCHEMA_URL,
            "run": {
                "runId": correlation.openlineage_run_id if correlation is not None else run_id,
                "facets": run_facets,
            },
            "job": {
                "namespace": namespace,
                "name": job_name,
                "facets": {
                    "sourceCodeLocation": {
                        "_producer": OPENLINEAGE_PRODUCER,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
                        "type": "git",
                        "url": OPENLINEAGE_PRODUCER,
                    }
                },
            },
            "inputs": [dataset.to_dict() for dataset in inputs],
            "outputs": [dataset.to_dict() for dataset in outputs],
        }

    def _run_facets(
        self,
        *,
        payload: Mapping[str, Any],
        registry_path: Path,
        correlation: Any | None,
    ) -> dict[str, object]:
        facets: dict[str, object] = {
            "dpone_run": {
                "_producer": OPENLINEAGE_PRODUCER,
                "_schemaURL": "https://github.com/PaulKov/dpone/blob/v0.73.0/docs/lineage.md",
                "status": self._value(payload, "status", "unknown"),
                "passed": bool(payload.get("passed", False)),
                "manifest": self._value(payload, "manifest", "unknown"),
                "dponeRunId": self._value(payload, "run_id", "unknown"),
                "run_result_sha256": self._value(payload, "run_result_sha256", "unknown"),
                "run_registry_entry_sha256": sha256_file(registry_path) if registry_path.exists() else "0" * 64,
            },
            "dpone_artifacts": {
                "_producer": OPENLINEAGE_PRODUCER,
                "_schemaURL": "https://github.com/PaulKov/dpone/blob/v0.73.0/docs/ops-cli.md",
                "artifacts": self._artifacts(payload),
            },
        }
        if correlation is not None:
            facets["dpone_airflowCorrelation"] = _airflow_correlation_facet(correlation)
        return facets

    @staticmethod
    def _blockers(path: Path, payload: Mapping[str, Any]) -> tuple[str, ...]:
        if not path.exists():
            return ("run_registry.missing",)
        if not payload:
            return ("run_registry.invalid_json",)
        if not bool(payload.get("passed", False)):
            return ("run_registry.not_passed",)
        return tuple()

    @staticmethod
    def _event_type(payload: Mapping[str, Any], event_type: str) -> str:
        normalized = event_type.upper()
        if normalized != "AUTO":
            return normalized
        return "COMPLETE" if bool(payload.get("passed", False)) else "FAIL"

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _artifacts(payload: Mapping[str, Any]) -> list[dict[str, object]]:
        artifacts = payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            return []
        return [asdict_like(item) for item in artifacts if isinstance(item, Mapping)]

    @staticmethod
    def _value(payload: Mapping[str, Any], key: str, default: str) -> str:
        value = payload.get(key, default)
        if isinstance(value, str) and value:
            return value
        return default


def asdict_like(value: Mapping[str, Any]) -> dict[str, object]:
    return {str(key): item for key, item in value.items() if isinstance(key, str)}


def _airflow_correlation_facet(correlation: Any) -> dict[str, object]:
    return {
        "_producer": OPENLINEAGE_PRODUCER,
        "_schemaURL": DPONE_AIRFLOW_CORRELATION_FACET_SCHEMA_URL,
        "correlationId": correlation.correlation_id,
        "airflow": {
            "dagId": correlation.airflow.dag_id,
            "taskId": correlation.airflow.task_id,
            "runId": correlation.airflow.run_id,
            "tryNumber": correlation.airflow.try_number,
            "mapIndex": correlation.airflow.map_index,
        },
        "dpone": {"runId": correlation.dpone.run_id, "process": correlation.dpone.process},
        "artifacts": {
            "releaseId": correlation.artifacts.release_id,
            "deploymentId": correlation.artifacts.deployment_id,
            "workloadId": correlation.artifacts.workload_id,
            "workloadPackSha256": correlation.artifacts.workload_pack_sha256,
            "runtimeEvidenceSha256": correlation.artifacts.runtime_evidence_sha256,
        },
        "pod": {
            "name": correlation.pod.name,
            "uid": correlation.pod.uid,
            "namespace": correlation.pod.namespace,
            "imageDigest": correlation.pod.image_digest,
        },
    }
