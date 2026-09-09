"""Public provider facade for dpone native acceleration."""

from __future__ import annotations

import tomllib
from collections.abc import Iterable, Mapping
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version
from pathlib import Path
from typing import Any

from dpone_native_accel._clickhouse_native_direct import ClickHouseNativeProtocolClient
from dpone_native_accel._mssql_clickhouse_native import MssqlBcpClickHouseNativeBackend


def _distribution_package_version() -> str:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if pyproject.exists():
        metadata = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        return str(metadata.get("project", {}).get("version", "0.0.0"))
    try:
        return _distribution_version("dpone-native-accel")
    except PackageNotFoundError:
        return "0.0.0"


__version__ = _distribution_package_version()
SCHEMA_VERSION = "dpone.native_transfer.acceleration.v1"
_BATCH_SCHEMA_VERSION = "dpone.native_transfer.acceleration-batch.v1"
DIRECT_INGEST_SCHEMA_VERSION = "dpone.native_transfer.direct_ingest.v1"
BACKEND_ID = "mssql_bcp_native_to_clickhouse_native"
SUPPORTED_TYPES = (
    "bit",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "real",
    "float",
    "money",
    "smallmoney",
    "decimal",
    "numeric",
    "date",
    "time",
    "datetime",
    "datetime2",
    "datetimeoffset",
    "smalldatetime",
    "uniqueidentifier",
    "binary",
    "varbinary",
    "char",
    "varchar",
    "nchar",
    "nvarchar",
)


def capabilities() -> dict[str, Any]:
    """Return certified native acceleration backends."""

    return {
        "schema_version": SCHEMA_VERSION,
        "package_version": __version__,
        "backends": [
            {
                "backend_id": BACKEND_ID,
                "source_format": "mssql-bcp-native",
                "target_format": "Native",
                "certified": True,
                "supported_platforms": ["linux_x86_64", "macos_arm64", "macos_x86_64"],
                "supported_types": list(SUPPORTED_TYPES),
                "native_wire_revision": 2,
            }
        ],
    }


def direct_ingest_capabilities() -> dict[str, Any]:
    """Return direct protocol capabilities."""

    return {
        "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
        "package_version": __version__,
        "backends": [
            {
                "backend_id": "clickhouse_native_tcp_direct",
                "sink": "clickhouse",
                "input_format": "Native",
                "certified": True,
                "protocol_revision": 54453,
                "compression": ["none", "lz4", "zstd"],
            }
        ],
    }


def transcode(request: Mapping[str, Any]) -> Iterable[bytes]:
    """Transcode one MSSQL BCP native artifact to ClickHouse Native blocks."""

    return MssqlBcpClickHouseNativeBackend.from_request(request).iter_blocks()


def transcode_batches(request: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    """Transcode with a closed encoder-authored row authority per Native block."""

    return (
        {"schema_version": _BATCH_SCHEMA_VERSION, "payload": payload, "rows": rows}
        for payload, rows in MssqlBcpClickHouseNativeBackend.from_request(request).iter_batches()
    )


def insert_clickhouse_native(request: Mapping[str, Any]) -> dict[str, Any]:
    """Insert pre-encoded ClickHouse Native blocks through direct TCP."""

    return ClickHouseNativeProtocolClient(provider_version=__version__).insert(request).to_provider_dict()
