"""GitOps workload-init templates for catalog and batch layouts."""

from __future__ import annotations

from typing import Any

PREVIEW_START_DATE = "2026-01-01"
DEFAULT_SCHEDULE = "0 6 * * *"
DEFAULT_TIMEZONE = "Europe/Moscow"

ENGINE_DEFAULTS: dict[str, dict[str, str]] = {
    "mssql": {"source_schema": "dbo", "source_table": "orders", "unique_key": "id"},
    "postgres": {"source_schema": "public", "source_table": "orders", "unique_key": "id"},
    "clickhouse": {"target_schema": "analytics", "target_table": "orders"},
}


def workload_init_options(*, source: str, sink: str, strategy: str) -> dict[str, Any]:
    """Resolve beginner-safe defaults for a source/sink/strategy triple."""

    source_engine = source.strip().lower()
    sink_engine = sink.strip().lower()
    source_defaults = dict(ENGINE_DEFAULTS.get(source_engine, {}))
    sink_defaults = dict(ENGINE_DEFAULTS.get(sink_engine, {}))
    return {
        "source_type": source_engine,
        "sink_type": sink_engine,
        "strategy": strategy.strip().lower(),
        "source_connection_ref": f"{source_engine}_dev",
        "sink_connection_ref": f"{sink_engine}_dev",
        "source_schema": source_defaults.get("source_schema", "public"),
        "source_table": source_defaults.get("source_table", "source_table"),
        "target_schema": sink_defaults.get("target_schema", "analytics"),
        "target_table": sink_defaults.get("target_table", "target_table"),
        "unique_key": source_defaults.get("unique_key"),
    }


def catalog_manifest_payload(
    *,
    workload_id: str,
    options: dict[str, Any],
    sql_repo_relative: str,
) -> dict[str, Any]:
    strategy = {"mode": options["strategy"]}
    if options.get("unique_key") and options["strategy"] == "incremental_merge":
        strategy["unique_key"] = options["unique_key"]
    return {
        "name": workload_id,
        "source": {
            "type": options["source_type"],
            "connection_ref": options["source_connection_ref"],
            "table": {"schema": options["source_schema"], "name": options["source_table"]},
            "sql_file": sql_repo_relative,
        },
        "sink": {
            "type": options["sink_type"],
            "connection_ref": options["sink_connection_ref"],
            "table": {"schema": options["target_schema"], "name": options["target_table"]},
            "mode": "append" if options["strategy"] == "incremental_merge" else "replace",
            "strategy": strategy,
        },
        "gitops": {
            "airflow": {
                "execution": {
                    "outlets": [f"{options['sink_type']}://{options['target_schema']}/{options['target_table']}"]
                }
            }
        },
    }


def batch_manifest_payload(*, workload_id: str, options: dict[str, Any], sql_repo_relative: str) -> dict[str, Any]:
    strategy = {"mode": options["strategy"]}
    if options.get("unique_key") and options["strategy"] == "incremental_merge":
        strategy["unique_key"] = options["unique_key"]
    return {
        "kind": "dpone.batch.v1",
        "metadata": {"id": workload_id, "domain": options["domain"], "tags": ["dpone", "airflow"]},
        "defaults": {
            "source": {
                "type": options["source_type"],
                "connection_ref": options["source_connection_ref"],
                "table": {"schema": "{{ src_schema }}", "name": "{{ src_table }}"},
                "sql_file": sql_repo_relative,
            },
            "sink": {
                "type": options["sink_type"],
                "connection_ref": options["sink_connection_ref"],
                "table": {"schema": options["target_schema"], "name": "{{ src_schema }}__{{ src_table }}"},
                "mode": "append" if options["strategy"] == "incremental_merge" else "replace",
                "strategy": strategy,
            },
        },
        "naming": {"process_name": "{{ src_schema }}__{{ src_table }}"},
        "schemas": {
            options["source_schema"]: {
                "tables": [{"table": options["source_table"]}],
            }
        },
    }


def sql_stub(*, workload_id: str, options: dict[str, Any]) -> str:
    schema = options["source_schema"]
    table = options["source_table"]
    return f"-- dpone workload init scaffold for {workload_id}\nSELECT *\nFROM {schema}.{table}\n"


def docs_stub(*, domain: str, workload_id: str, owner: str) -> str:
    return (
        f"# {domain}/{workload_id}\n\n"
        f"- Owner: {owner}\n"
        f"- Purpose: dpone GitOps workload scaffold\n"
        f"- Runbook: validate with `dpone gitops airflow reconcile --workload-set dpone_workloads/gitops/gitops.yaml`\n"
    )


def dag_declaration(
    *,
    dag_id: str,
    workload_id: str,
    schedule: str | None,
    owner: str,
    timezone: str,
) -> dict[str, Any]:
    return {
        "description": f"Governed sync for {workload_id}",
        "schedule": schedule,
        "start_date": PREVIEW_START_DATE,
        "timezone": timezone,
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["dpone", owner],
        "default_args": {"retries": 0, "owner": owner},
        "operator_overrides": {"in_cluster": True},
        "workloads": [workload_id],
        "wiring": {"mode": "waves", "max_parallel_workloads": 1},
    }


def ownership_entry(*, domain: str, workload_id: str, owner: str) -> dict[str, Any]:
    return {
        "domain": domain,
        "workload_id": workload_id,
        "owner": owner,
        "contacts": [],
    }


__all__ = [
    "DEFAULT_SCHEDULE",
    "DEFAULT_TIMEZONE",
    "PREVIEW_START_DATE",
    "batch_manifest_payload",
    "catalog_manifest_payload",
    "dag_declaration",
    "docs_stub",
    "ownership_entry",
    "sql_stub",
    "workload_init_options",
]
