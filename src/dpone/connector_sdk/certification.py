from __future__ import annotations

from typing import Any

_SOURCE_STRATEGIES = ["full_refresh", "incremental_append"]
_SINK_STRATEGIES = ["full_refresh", "incremental_append", "incremental_merge", "replace"]

_SOURCE_REQUIRED_TESTS = [
    "import_safety",
    "manifest_validation",
    "schema_inference",
    "full_refresh_extract",
    "incremental_append_extract",
    "quality_contracts",
    "run_artifact_evidence",
]
_SINK_REQUIRED_TESTS = [
    "import_safety",
    "manifest_validation",
    "staging_first_load",
    "full_refresh_load",
    "incremental_append_load",
    "incremental_merge_load",
    "replace_load",
    "schema_evolution_contracts",
    "quality_contracts",
    "run_artifact_evidence",
]
_STATE_REQUIRED_TESTS = [
    "import_safety",
    "state_read_write_round_trip",
    "state_commit_after_success",
    "state_no_advance_after_failure",
]

_NATIVE_FORMATS = ["tabseparated", "jsonl"]


class ConnectorCertificationTemplateService:
    """Build deterministic certification manifests for generated connectors."""

    def build(
        self,
        *,
        connector: str,
        connector_type: str,
        capabilities: tuple[str, ...],
        native_capabilities: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        normalized_capabilities = tuple(dict.fromkeys(capabilities))
        payload = {
            "contract_version": "1",
            "connector": connector,
            "connector_type": connector_type,
            "status": "community",
            "capabilities": self._capabilities_payload(normalized_capabilities),
            "quality_gates": {
                "unit_tests": True,
                "manifest_examples_parse": True,
                "docs_present": True,
                "run_artifacts_present": True,
                "secret_redaction": True,
            },
            "performance": {
                "benchmark_required": True,
                "minimum_rows": 10000,
                "wide_columns": 120,
            },
            "evidence": {
                "artifacts_dir": f"test_artifacts/connectors/{connector}",
                "markdown_report": "certification_report.md",
                "json_report": "certification_report.json",
            },
        }
        native_payload = self._native_transfer_payload(tuple(dict.fromkeys(native_capabilities)))
        if native_payload:
            payload["native_transfer"] = native_payload
        return payload

    def _capabilities_payload(self, capabilities: tuple[str, ...]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if "source" in capabilities:
            payload["source"] = {
                "strategies": _SOURCE_STRATEGIES,
                "required_tests": _SOURCE_REQUIRED_TESTS,
                "schema_evolution": "emits_extract_schema",
            }
        if "sink" in capabilities:
            payload["sink"] = {
                "strategies": _SINK_STRATEGIES,
                "required_tests": _SINK_REQUIRED_TESTS,
                "schema_evolution": "target_introspection_and_safe_plan",
                "staging_first": True,
            }
        if "state" in capabilities:
            payload["state"] = {
                "strategies": ["read", "write", "commit", "rollback"],
                "required_tests": _STATE_REQUIRED_TESTS,
            }
        return payload

    def _native_transfer_payload(self, native_capabilities: tuple[str, ...]) -> dict[str, Any]:
        capabilities: dict[str, Any] = {}
        for capability in native_capabilities:
            if capability.endswith("_export"):
                capabilities[capability] = {
                    "formats": list(_NATIVE_FORMATS),
                    "bounded": True,
                    "supports_checksum": True,
                    "supports_cleanup": True,
                }
            elif capability.endswith("_staging_load"):
                capabilities[capability] = {
                    "formats": list(_NATIVE_FORMATS),
                    "staging_safe": True,
                    "supports_abort": True,
                    "supports_idempotency_key": True,
                }
        return {"capabilities": capabilities} if capabilities else {}
