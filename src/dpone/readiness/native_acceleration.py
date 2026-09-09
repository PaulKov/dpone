"""Readiness service for native acceleration diagnostics."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.direct_ingest import DirectIngestResolver, DirectIngestRouteRequest
from dpone.runtime.native_acceleration import NativeAccelerationRegistry


class NativeAccelerationReadinessService:
    """Expose native acceleration diagnostics without command/runtime coupling."""

    def __init__(self, registry: NativeAccelerationRegistry | None = None) -> None:
        self._registry = registry or NativeAccelerationRegistry()

    def doctor(self) -> dict[str, Any]:
        payload = self._registry.doctor()
        direct = (
            DirectIngestResolver()
            .decide(
                DirectIngestRouteRequest(
                    bulk_options=ClickHouseBulkOptionsResolver.resolve(
                        {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "auto"}}}
                    ),
                    input_format="Native",
                    route_certified=True,
                )
            )
            .to_evidence()
        )
        direct.update(_direct_ingest_provider_metadata())
        payload["direct_ingest"] = direct
        return payload

    def benchmark_plan(self, *, manifest: str, rows: int, output: str) -> dict[str, Any]:
        output_dir = Path(output)
        output_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "kind": "runtime.native_accel_benchmark",
            "manifest": manifest,
            "rows": rows,
            "output": output,
            "status": "planned",
            "message": "Use live MSSQL/ClickHouse certification tools for route benchmarks.",
            "doctor": self.doctor(),
        }
        json_path = output_dir / "native_accel_benchmark_plan.json"
        markdown_path = output_dir / "native_accel_benchmark_plan.md"
        payload["artifact_json"] = str(json_path)
        payload["artifact_markdown"] = str(markdown_path)
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        markdown_path.write_text(_benchmark_markdown(payload), encoding="utf-8")
        return payload


def _benchmark_markdown(payload: dict[str, Any]) -> str:
    doctor = payload.get("doctor") or {}
    return "\n".join(
        [
            "# dpone runtime native-accel benchmark",
            "",
            f"- manifest: `{payload['manifest']}`",
            f"- rows: `{payload['rows']}`",
            f"- output: `{payload['output']}`",
            f"- status: `{payload['status']}`",
            f"- selected_backend: `{doctor.get('selected_backend')}`",
            f"- fallback_reason: `{doctor.get('fallback_reason')}`",
            "",
        ]
    )


def _direct_ingest_provider_metadata() -> dict[str, Any]:
    try:
        provider = importlib.import_module("dpone_native_accel")
    except ImportError:
        return {}
    capabilities = getattr(provider, "direct_ingest_capabilities", lambda: {})()
    if not isinstance(capabilities, dict):
        return {}
    backends = capabilities.get("backends") or ()
    backend = next((item for item in backends if isinstance(item, dict)), {})
    return {
        "provider_version": capabilities.get("package_version"),
        "supported_compression": list(backend.get("compression") or ()),
        "protocol_revision": backend.get("protocol_revision"),
    }


__all__ = ["NativeAccelerationReadinessService"]
