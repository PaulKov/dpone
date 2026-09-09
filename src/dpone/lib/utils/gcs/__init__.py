"""GCS utilities package for ClickHouse → BigQuery ETL."""

# Импортируем get_env_code из централизованного места
from dpone.config import get_env_code
from dpone.lib.utils.gcs.cleaner import GCSCleaner
from dpone.lib.utils.gcs.client import create_gcs_client
from dpone.lib.utils.gcs.hmac_credentials import get_gcs_hmac_credentials
from dpone.lib.utils.gcs.path_manager import (
    build_gcs_file_pattern,
    format_partition_date,
    get_gcs_bucket_name,
    get_gcs_table_path,
    parse_date_value,
    parse_partition_date,
)
from dpone.lib.utils.gcs.uri import (
    build_gcs_file_path,
    gs_to_https,
    https_to_gs,
    normalize_gcs_path,
    parse_gs_uri,
)

__all__ = [
    "GCSCleaner",
    "create_gcs_client",
    "get_gcs_hmac_credentials",
    # URI utilities
    "gs_to_https",
    "https_to_gs",
    "normalize_gcs_path",
    "parse_gs_uri",
    "build_gcs_file_path",
    "get_env_code",
    "parse_date_value",
    "get_gcs_bucket_name",
    "get_gcs_table_path",
    "format_partition_date",
    "parse_partition_date",
    "build_gcs_file_pattern",
]
