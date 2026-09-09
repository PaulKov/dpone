"""Plan-time export_format vs sink compatibility checks."""

from __future__ import annotations

from typing import Any

from dpone.dag.errors import DagConfigurationError


def validate_export_format_for_sink(*, source_options: dict[str, Any], sink_cfg: dict[str, Any]) -> None:
    """Fail closed when source export wire cannot load into the declared sink."""

    export_format = str(source_options.get("export_format", "csv")).lower().replace("_", "-")
    sink_type = str(sink_cfg.get("type", "")).lower()
    if sink_type == "bigquery" and export_format == "binary":
        raise DagConfigurationError(
            "BigQuery sink не поддерживает PostgreSQL Binary формат.\n"
            "Измените конфигурацию:\n"
            "  source:\n"
            "    options:\n"
            "      export_format: csv  # ← Используйте CSV вместо binary\n\n"
            f"Текущая конфигурация: sink.type={sink_type}, source.options.export_format={export_format}"
        )
    if sink_type == "bigquery" and export_format in {"mssql-delimited", "clickhouse-tsv"}:
        raise DagConfigurationError(
            "BigQuery sink cannot load MySQL mssql-delimited or clickhouse-tsv artifacts.\n"
            "Change the configuration:\n"
            "  source:\n"
            "    options:\n"
            "      export_format: csv\n\n"
            f"Current configuration: sink.type={sink_type}, source.options.export_format={export_format}"
        )
    if sink_type in {"postgres", "postgresql"} and export_format == "mssql-delimited":
        raise DagConfigurationError(
            "Postgres sink cannot load MySQL mssql-delimited artifacts.\n"
            "Change the configuration:\n"
            "  source:\n"
            "    options:\n"
            "      export_format: csv\n\n"
            f"Current configuration: sink.type={sink_type}, source.options.export_format={export_format}"
        )
    if sink_type == "clickhouse" and export_format == "mssql-delimited":
        raise DagConfigurationError(
            "ClickHouse sink cannot load MySQL mssql-delimited (BCP) artifacts.\n"
            "Change the configuration:\n"
            "  source:\n"
            "    options:\n"
            "      export_format: csv  # produces ClickHouse TabSeparated wire\n\n"
            f"Current configuration: sink.type={sink_type}, source.options.export_format={export_format}"
        )
    if sink_type == "kafka" and export_format == "mssql-delimited":
        raise DagConfigurationError(
            "Kafka sink cannot load MySQL mssql-delimited (BCP) artifacts.\n"
            "Change the configuration:\n"
            "  source:\n"
            "    options:\n"
            "      export_format: csv\n\n"
            f"Current configuration: sink.type={sink_type}, source.options.export_format={export_format}"
        )
    compress = bool(source_options.get("compress_export", False))
    if sink_type == "kafka" and compress:
        raise DagConfigurationError(
            "Kafka sink cannot read gzip MySQL export artifacts.\n"
            "Change the configuration:\n"
            "  source:\n"
            "    options:\n"
            "      compress_export: false\n\n"
            f"Current configuration: sink.type={sink_type}, source.options.compress_export={compress}"
        )


__all__ = ["validate_export_format_for_sink"]
