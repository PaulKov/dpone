"""Canonical runtime-facing GCS helpers.

This module keeps runtime imports free from ``dpone.lib`` in static import graphs
by resolving the legacy implementations lazily via ``importlib``.
"""

from __future__ import annotations

from importlib import import_module

from dpone.config.env import get_env_code as _get_env_code


def _pm():
    return import_module("dpone.lib.utils.gcs.path_manager")


def _uri():
    return import_module("dpone.lib.utils.gcs.uri")


def _cleaner():
    return import_module("dpone.lib.utils.gcs.cleaner").GCSCleaner


def _client_mod():
    return import_module("dpone.lib.utils.gcs.client")


def _hmac_mod():
    return import_module("dpone.lib.utils.gcs.hmac_credentials")


def parse_date_value(value):
    return _pm().parse_date_value(value)


def get_env_code() -> str:
    return _get_env_code()


def get_gcs_bucket_name(schema: str, env_code: str | None = None) -> str:
    return _pm().get_gcs_bucket_name(schema, env_code)


def get_gcs_table_path(
    database: str, table: str, partition_date=None, partition_by: str | None = None, target_schema: str | None = None
) -> str:
    return _pm().get_gcs_table_path(
        database, table, partition_date=partition_date, partition_by=partition_by, target_schema=target_schema
    )


def format_partition_date(date_value, partition_by: str = "day") -> str:
    return _pm().format_partition_date(date_value, partition_by=partition_by)


def parse_partition_date(partition_str: str):
    return _pm().parse_partition_date(partition_str)


def build_gcs_file_pattern(file_prefix: str, format: str, chunk_rows, partitioned: bool = False) -> str:
    return _pm().build_gcs_file_pattern(file_prefix, format, chunk_rows, partitioned=partitioned)


def gs_to_https(uri: str) -> str:
    return _uri().gs_to_https(uri)


def https_to_gs(uri: str) -> str:
    return _uri().https_to_gs(uri)


def normalize_gcs_path(path: str) -> str:
    return _uri().normalize_gcs_path(path)


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    return _uri().parse_gs_uri(gs_uri)


def build_gcs_file_path(
    base_uri: str, file_prefix: str, extension: str, *, partition_id_placeholder: bool = False
) -> str:
    return _uri().build_gcs_file_path(
        base_uri, file_prefix, extension, partition_id_placeholder=partition_id_placeholder
    )


def create_gcs_client(
    env_code: str | None = None, target_conn_id: str = "bigquery_default", vault_path: str | None = None
):
    return _client_mod().create_gcs_client(env_code=env_code, target_conn_id=target_conn_id, vault_path=vault_path)


def get_gcs_hmac_credentials(
    *,
    hmac_key: str | None = None,
    hmac_secret: str | None = None,
    vault_path: str | None = None,
    auto_generate_vault_path: bool = False,
):
    return _hmac_mod().get_gcs_hmac_credentials(
        hmac_key=hmac_key,
        hmac_secret=hmac_secret,
        vault_path=vault_path,
        auto_generate_vault_path=auto_generate_vault_path,
    )


class GCSCleaner:
    @staticmethod
    def delete_prefix(*args, **kwargs):
        return _cleaner().delete_prefix(*args, **kwargs)

    @staticmethod
    def cleanup_from_load_config(*args, **kwargs):
        return _cleaner().cleanup_from_load_config(*args, **kwargs)

    @staticmethod
    def cleanup_exact_file(*args, **kwargs):
        return _cleaner().cleanup_exact_file(*args, **kwargs)
