from __future__ import annotations

from typing import Any


def capabilities_payload() -> dict[str, Any]:
    return {
        "sources": ["postgres", "mssql", "clickhouse", "api", "kafka"],
        "sinks": ["mssql", "postgres", "clickhouse", "bigquery", "kafka"],
        "state_backends": ["bigquery", "postgres", "mssql"],
        "strategies": ["full_refresh", "incremental_append", "incremental_merge", "replace"],
        "quality_checks": [
            "not_null",
            "unique",
            "accepted_values",
            "freshness",
            "row_count_delta",
            "min_rows",
            "max_null_ratio",
            "checksum",
            "source_target_count",
        ],
        "manifest_quality_gates": {
            "executable": ["min_rows", "source_target_count"],
            "declared_fail_closed": ["not_null", "unique"],
        },
        "schema_evolution": {
            "default_enabled": True,
            "safe_modes": ["add_nullable_column", "widening", "generated_new_column"],
            "breaking_modes": ["drop", "rename", "narrowing", "nullable_to_not_null"],
        },
        "gitops": {
            "default_branch_prefix": "codex/",
            "required_evidence": ["dpone plan artifact", "quality gates", "CI status", "draft PR"],
        },
        "maturity": [
            "security_policy",
            "audit_events",
            "observability_slo",
            "deployment_guide",
            "schema_explorer",
            "reconciliation_preview",
        ],
    }


def security_policy_payload(*, token_required: bool = False) -> dict[str, Any]:
    mode = "token" if token_required else "local-only"
    return {
        "mode": mode,
        "auth": {
            "required": token_required,
            "headers": ["Authorization: Bearer <token>", "X-Dpone-Studio-Token: <token>"],
            "public_endpoints": ["/", "/healthz", "/openapi.json"],
        },
        "rbac": {
            "roles": [
                {"name": "viewer", "permissions": ["read:doctor", "read:plan", "read:runs", "read:state"]},
                {
                    "name": "operator",
                    "permissions": ["create:manifest", "run:quality", "prepare:gitops", "preview:reconciliation"],
                },
                {"name": "admin", "permissions": ["reset:state", "configure:studio", "manage:tokens"]},
            ],
            "default_role": "operator" if token_required else "local_operator",
        },
        "guardrails": [
            "No destructive state operation is executed without explicit --yes in CLI flows.",
            "Studio GitOps prepares review commands and PR metadata; it does not push without operator action.",
            "All diagnostics must redact secrets before being returned by API endpoints.",
        ],
    }


def observability_slo_payload() -> dict[str, Any]:
    return {
        "slo": {
            "freshness_minutes": 30,
            "successful_runs_ratio": 0.99,
            "row_loss_tolerance": 0,
            "schema_breaking_change_policy": "fail_closed",
        },
        "signals": [
            "run_duration_seconds",
            "rows_extracted_total",
            "rows_loaded_total",
            "rows_reconciled_total",
            "quality_failed_total",
            "state_lag_seconds",
        ],
        "telemetry": {
            "opentelemetry": "optional manifest observability.opentelemetry",
            "prometheus": "optional manifest observability.prometheus",
            "artifacts": ".dpone/runs by default, test_artifacts during certification",
        },
        "alerts": [
            {"severity": "critical", "condition": "quality_failed_total > 0"},
            {"severity": "critical", "condition": "source_target_count mismatch"},
            {"severity": "warning", "condition": "freshness_minutes > configured SLO"},
            {"severity": "warning", "condition": "schema evolution generated __dpone__nc__ column"},
        ],
    }


def deployment_guide_payload() -> dict[str, Any]:
    return {
        "topology": "framework_headless_plus_optional_studio_ui",
        "framework": {
            "local": "uv run dpone studio --serve --host 127.0.0.1 --port 8765",
            "container": (
                "docker run --rm -p 8765:8765 -e DPONE_STUDIO_TOKEN dpone:0.2.0 "
                "dpone studio --serve --host 0.0.0.0 --port 8765"
            ),
            "kubernetes": "Run dpone as an internal ClusterIP service and expose Studio UI separately.",
        },
        "studio": {
            "repository": "https://github.com/PaulKov/dpone-studio",
            "runtime_env": ["NUXT_PUBLIC_DPONE_API_BASE", "NUXT_PUBLIC_DPONE_API_TOKEN"],
            "recommended_boundary": "Keep Studio UI and framework API as independently deployable services.",
        },
        "gitops": [
            "Generate manifest draft.",
            "Run dpone plan and quality gates.",
            "Commit manifest and plan artifact.",
            "Open draft PR.",
            "Merge only after CI and connector certification gates pass.",
        ],
    }


def schema_explorer_payload(query: dict[str, str]) -> dict[str, Any]:
    source = query.get("source", "postgres")
    sink = query.get("sink", "mssql")
    type_notes = {
        "postgres": ["arrays and ranges require explicit mapping for non-Postgres sinks", "xmin state is supported"],
        "mssql": ["rowversion is metadata-only", "bcp is preferred for bulk paths"],
        "clickhouse": ["replace flows use staging/shadow tables", "mutations are avoided for heavy delete handling"],
        "bigquery": ["nullable additions are safe", "relax/widen follows BigQuery API limits"],
        "kafka": ["schema evolution is enforced through Schema Registry when enabled"],
        "api": ["schema is inferred from sampled JSON rows unless explicit schema is supplied"],
    }
    return {
        "source": source,
        "sink": sink,
        "schema_evolution_default": "enabled",
        "compatibility": {
            "safe": ["new_nullable_column", "widen_string", "widen_numeric", "framework_technical_column"],
            "guarded": ["incompatible_type_to___dpone__nc__", "nullable_to_not_null_with_default"],
            "manual": ["drop_column", "rename_column", "narrowing", "semantic_type_change"],
        },
        "source_notes": type_notes.get(source, []),
        "sink_notes": type_notes.get(sink, []),
        "generated_column_prefix": "__dpone__nc__",
    }


def reconciliation_preview_payload(payload: dict[str, Any]) -> dict[str, Any]:
    source_type = str(payload.get("source_type") or "postgres")
    sink_type = str(payload.get("sink_type") or "mssql")
    unique_key = str(payload.get("unique_key") or "id")
    apply_deletes = bool(payload.get("apply_deletes", False))
    return {
        "source_type": source_type,
        "sink_type": sink_type,
        "unique_key": unique_key,
        "apply_deletes": apply_deletes,
        "strategy": "snapshot_reconciliation",
        "safe_flow": [
            "extract source keys into staging",
            "load changes into staging table",
            "compare target keys against staging keys",
            "apply set-based delete or soft-delete only after staging validation",
            "commit state after target commit succeeds",
        ],
        "target_policy": {
            "mssql": "staging table plus set-based delete/merge in transaction",
            "postgres": "temporary/staging table plus set-based delete/merge in transaction",
            "clickhouse": "shadow-table replacement or ReplacingMergeTree tombstone model; no heavy ALTER UPDATE path",
            "bigquery": "staging table plus MERGE/DELETE where allowed by configured policy",
            "kafka": "emit keyed delete/tombstone events only when deletes.enabled and key is non-null",
        }.get(sink_type, "staging-first set-based reconciliation"),
        "warnings": [] if apply_deletes else ["physical deletes are previewed only; set apply_deletes=true to enable"],
    }
