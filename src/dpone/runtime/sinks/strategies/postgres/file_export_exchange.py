from __future__ import annotations

from typing import Any

from psycopg import sql

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sql_helpers import ExchangeQueries


def load_with_exchange(loader: Any, load_config: Any, payload: LoadPayload, artifact: FileExportArtifact) -> LoadResult:
    target_schema = load_config.target_schema
    target_table = load_config.target_table
    tmp_table = ExchangeQueries.get_tmp_table_name(target_table)
    backup_table = ExchangeQueries.get_backup_table_name(target_table)

    loader.connector.begin()
    inserted = 0
    try:
        check_sql = sql.SQL(ExchangeQueries.pg_check_table_exists())
        exists = loader.connector.get_records(check_sql, (target_schema, target_table))
        if not bool(exists and exists[0][0]):
            loader.connector.rollback()
            return loader.load_standard(load_config, payload, artifact)

        _log_exchange_start(loader, target_schema, target_table, tmp_table, artifact)
        schema_with_tech, _ = loader.target_table_manager.build_schema_with_technical_columns(
            load_config,
            payload.schema,
        )
        create_tmp_sql = sql.SQL("CREATE TABLE {}.{} ({});").format(
            sql.Identifier(target_schema),
            sql.Identifier(tmp_table),
            loader.target_table_manager.build_create_table_columns_sql(schema_with_tech),
        )
        loader.connector.execute_query(create_tmp_sql)
        inserted = loader.copy_from_artifact(
            target_schema=target_schema,
            target_table=tmp_table,
            schema=payload.schema,
            artifact=artifact,
        )
        _swap_tables(loader, target_schema, target_table, tmp_table, backup_table, inserted)
        loader.connector.commit_transaction()
        _log_sample_if_needed(loader, load_config, inserted)
        return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted, staging_rows=inserted)
    except Exception as error:
        try:
            loader.connector.rollback()
            loader._cleanup_exchange_failure(target_schema, target_table, tmp_table, backup_table, error)
        except Exception as rollback_error:
            loader.logger.log_etl_progress(
                "EXCHANGE_ROLLBACK_FAILED",
                {"Error": str(rollback_error), "Original_Error": str(error)},
            )
        raise
    finally:
        loader.cleanup_file(artifact)


def _log_exchange_start(
    loader: Any,
    target_schema: str,
    target_table: str,
    tmp_table: str,
    artifact: FileExportArtifact,
) -> None:
    loader.logger.log_etl_progress(
        "EXCHANGE_START",
        {
            "Target": f"{target_schema}.{target_table}",
            "Mode": "Exchange Pattern (атомарная замена)",
            "Source": "FileExport",
        },
    )
    loader.logger.log_etl_progress(
        "EXCHANGE_CREATE_TMP",
        {"TmpTable": f"{target_schema}.{tmp_table}", "Source": "file", "File": artifact.file_path},
    )


def _swap_tables(
    loader: Any,
    target_schema: str,
    target_table: str,
    tmp_table: str,
    backup_table: str,
    inserted: int,
) -> None:
    loader.logger.log_etl_progress(
        "EXCHANGE_TMP_CREATED", {"TmpTable": f"{target_schema}.{tmp_table}", "Inserted": inserted}
    )
    loader.connector.execute_query(
        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            sql.Identifier(target_schema),
            sql.Identifier(target_table),
            sql.Identifier(backup_table),
        )
    )
    loader.logger.log_etl_progress(
        "EXCHANGE_BACKUP", {"Target": f"{target_schema}.{target_table}", "Backup": f"{target_schema}.{backup_table}"}
    )
    loader.connector.execute_query(
        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            sql.Identifier(target_schema),
            sql.Identifier(tmp_table),
            sql.Identifier(target_table),
        )
    )
    loader.logger.log_etl_progress(
        "EXCHANGE_SWAP",
        {"Target": f"{target_schema}.{target_table}", "TmpTable": f"{target_schema}.{tmp_table}", "Status": "Swapped"},
    )
    loader.connector.execute_query(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(target_schema), sql.Identifier(backup_table))
    )
    loader.logger.log_etl_progress(
        "EXCHANGE_CLEANUP", {"Backup": f"{target_schema}.{backup_table}", "Status": "Dropped"}
    )
    loader.logger.log_etl_progress(
        "EXCHANGE_COMPLETE",
        {"Target": f"{target_schema}.{target_table}", "Inserted": inserted, "Mode": "Exchange Pattern (FileExport)"},
    )


def _log_sample_if_needed(loader: Any, load_config: Any, inserted: int) -> None:
    if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
        loader._log_target_sample(load_config, load_config.log_sample_rows)
