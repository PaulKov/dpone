"""GCS export helpers for ClickHouseConnector."""

from __future__ import annotations

from typing import Any


class ClickHouseGCSExportService:
    def __init__(self, connector: Any):
        self.connector = connector

    def export_to_gcs(
        self,
        query: str,
        gcs_uri: str,
        *,
        format: str = "parquet",
        chunk_rows: int | None = None,
        file_prefix: str = "data",
        extra_settings: dict[str, Any] | None = None,
    ) -> str:
        from dpone.runtime.support.gcs import (
            build_gcs_file_path,
            get_gcs_hmac_credentials,
            gs_to_https,
            normalize_gcs_path,
        )

        https_url = normalize_gcs_path(gs_to_https(gcs_uri))
        ext_map = {"parquet": "parquet", "csv": "csv", "orc": "orc"}
        ext = ext_map.get(format.lower(), "parquet")
        file_path = build_gcs_file_path(
            base_uri=https_url,
            file_prefix=file_prefix,
            extension=ext,
            partition_id_placeholder=(chunk_rows is not None and chunk_rows > 0),
        )

        hmac_key, hmac_secret = get_gcs_hmac_credentials(
            hmac_key=self.connector.gcs_hmac_key,
            hmac_secret=self.connector.gcs_hmac_secret,
            vault_path=self.connector.gcs_hmac_vault_path,
        )

        sql, params = self._build_export_sql(
            query=query,
            gcs_https_url=file_path,
            format=format,
            chunk_rows=chunk_rows,
            extra_settings=extra_settings,
        )
        params["ak"] = hmac_key
        params["sk"] = hmac_secret
        self.connector.execute_query(sql, params)
        return https_url

    def _build_export_sql(
        self,
        query: str,
        gcs_https_url: str,
        format: str = "parquet",
        chunk_rows: int | None = None,
        extra_settings: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        format_map = {
            "csv": "'CSVWithNames'",
            "parquet": "'Parquet'",
            "orc": "'ORC'",
        }
        format_lit = format_map.get(format.lower(), f"'{format.upper()}'")

        args = ["%(url)s", "%(ak)s", "%(sk)s"]
        params = {"url": gcs_https_url}
        partition_clause = ""
        if chunk_rows and chunk_rows > 0:
            partition_clause = f"PARTITION BY intDiv(rowNumberInAllBlocks(), {int(chunk_rows)})"

        all_settings = dict(extra_settings or {})
        all_settings.setdefault("s3_truncate_on_insert", 1)
        if format.lower() == "csv":
            all_settings.setdefault("format_csv_null_representation", "")

        settings_clause = ""
        if all_settings:
            settings_kv = ", ".join(f"{k} = %(set_{k})s" for k in all_settings.keys())
            for k, v in all_settings.items():
                params[f"set_{k}"] = v
            settings_clause = f" SETTINGS {settings_kv}"

        sql = f"""
            INSERT INTO FUNCTION s3({", ".join(args)}, {format_lit})
            {partition_clause}
            SELECT *
            FROM ({query}) AS sub
            {settings_clause}
        """.strip()
        return sql, params
