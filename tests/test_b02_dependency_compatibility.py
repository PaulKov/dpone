"""Characterized public contracts survive the B02 responsibility extraction."""

import base64
import importlib
import inspect
import io
import json
import pickle
import re
import shlex
import sys
import typing
from dataclasses import FrozenInstanceError, asdict, replace
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.runtime.connectors import clickhouse_bulk as client
from dpone.runtime.connectors import clickhouse_http_bulk as http
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy
from dpone.runtime.support import bulk_text_file_reader as reader


def _symbol(path):
    module, qualified = path.split(":")
    value = importlib.import_module(module)
    for part in qualified.split("."):
        value = getattr(value, part)
    return value


def _contract(path):
    value = _symbol(path)
    try:
        hints = {key: str(hint) for key, hint in typing.get_type_hints(value).items()}
    except NameError as error:
        hints = {"existing_unresolved_annotation": str(error)}
    return {
        "module": value.__module__,
        "qualname": value.__qualname__,
        "signature": re.sub(r" at 0x[0-9a-f]+", " at <address>", str(inspect.signature(value))),
        "type_hints": hints,
    }


# Captured from unmodified production sources at 5c9d8ff before extraction.
BASELINE = {
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientCredentials": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientCredentials",
        "signature": "(host: 'str', port: 'int', "
        "database: 'str', user: 'str', "
        "password: 'str' = '', secure: "
        "'bool' = False) -> None",
        "type_hints": {
            "host": "<class 'str'>",
            "port": "<class 'int'>",
            "database": "<class 'str'>",
            "user": "<class 'str'>",
            "password": "<class 'str'>",
            "secure": "<class 'bool'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientCredentials.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientCredentials.__init__",
        "signature": "(self, host: 'str', "
        "port: 'int', "
        "database: 'str', "
        "user: 'str', "
        "password: 'str' = '', "
        "secure: 'bool' = "
        "False) -> None",
        "type_hints": {
            "host": "<class 'str'>",
            "port": "<class 'int'>",
            "database": "<class 'str'>",
            "user": "<class 'str'>",
            "password": "<class 'str'>",
            "secure": "<class 'bool'>",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientOptions": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientOptions",
        "signature": "(client_command: 'str' = "
        "'clickhouse-client', input_format: "
        "'str' = 'TabSeparated', "
        "timeout_seconds: 'int | None' = "
        "None, max_insert_block_size: 'int "
        "| None' = None, settings: "
        "'dict[str, Any]' = <factory>, "
        "query_id: 'str | None' = None, "
        "insert_deduplication_token: 'str | "
        "None' = None) -> None",
        "type_hints": {
            "client_command": "<class 'str'>",
            "input_format": "<class 'str'>",
            "timeout_seconds": "int | None",
            "max_insert_block_size": "int | None",
            "settings": "dict[str, typing.Any]",
            "query_id": "str | None",
            "insert_deduplication_token": "str | None",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientOptions.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientOptions.__init__",
        "signature": "(self, client_command: "
        "'str' = "
        "'clickhouse-client', "
        "input_format: 'str' = "
        "'TabSeparated', "
        "timeout_seconds: 'int | "
        "None' = None, "
        "max_insert_block_size: "
        "'int | None' = None, "
        "settings: 'dict[str, "
        "Any]' = <factory>, "
        "query_id: 'str | None' = "
        "None, "
        "insert_deduplication_token: "
        "'str | None' = None) -> "
        "None",
        "type_hints": {
            "client_command": "<class 'str'>",
            "input_format": "<class 'str'>",
            "timeout_seconds": "int | None",
            "max_insert_block_size": "int | None",
            "settings": "dict[str, typing.Any]",
            "query_id": "str | None",
            "insert_deduplication_token": "str | None",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientOptions.from_bulk_wire_contract": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientOptions.from_bulk_wire_contract",
        "signature": "(contract: 'Any', *, base: 'ClickHouseClientOptions | None' = None) -> 'ClickHouseClientOptions'",
        "type_hints": {
            "contract": "typing.Any",
            "base": "dpone.runtime.connectors.clickhouse_bulk.ClickHouseClientOptions | None",
            "return": "<class 'dpone.runtime.connectors.clickhouse_bulk.ClickHouseClientOptions'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientRunner.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientRunner.__init__",
        "signature": "(self, credentials: "
        "'ClickHouseClientCredentials', "
        "options: "
        "'ClickHouseClientOptions | "
        "None' = None, "
        "run=<function run at "
        "<address>>, popen=<class "
        "'subprocess.Popen'>) -> "
        "'None'",
        "type_hints": {
            "credentials": "<class 'dpone.runtime.connectors.clickhouse_bulk.ClickHouseClientCredentials'>",
            "options": "dpone.runtime.connectors.clickhouse_bulk.ClickHouseClientOptions | None",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientRunner.build_insert_command": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientRunner.build_insert_command",
        "signature": "(self, table: 'str', columns: 'Sequence[str]') -> 'list[str]'",
        "type_hints": {"table": "<class 'str'>", "columns": "collections.abc.Sequence[str]", "return": "list[str]"},
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientRunner.build_query_command": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientRunner.build_query_command",
        "signature": "(self, sql: 'str') -> 'list[str]'",
        "type_hints": {"sql": "<class 'str'>", "return": "list[str]"},
    },
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientRunner.redact_command": {
        "module": "dpone.runtime.connectors.clickhouse_bulk",
        "qualname": "ClickHouseClientRunner.redact_command",
        "signature": "(command: 'Sequence[str]') -> 'list[str]'",
        "type_hints": {"command": "collections.abc.Sequence[str]", "return": "list[str]"},
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpCredentials": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpCredentials",
        "signature": "(host: 'str', port: 'int', "
        "database: 'str', user: "
        "'str', password: 'str' = "
        "'', secure: 'bool' = False) "
        "-> None",
        "type_hints": {
            "host": "<class 'str'>",
            "port": "<class 'int'>",
            "database": "<class 'str'>",
            "user": "<class 'str'>",
            "password": "<class 'str'>",
            "secure": "<class 'bool'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpCredentials.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpCredentials.__init__",
        "signature": "(self, host: "
        "'str', port: "
        "'int', database: "
        "'str', user: "
        "'str', password: "
        "'str' = '', "
        "secure: 'bool' = "
        "False) -> None",
        "type_hints": {
            "host": "<class 'str'>",
            "port": "<class 'int'>",
            "database": "<class 'str'>",
            "user": "<class 'str'>",
            "password": "<class 'str'>",
            "secure": "<class 'bool'>",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpOptions": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpOptions",
        "signature": "(input_format: 'str' = "
        "'TabSeparated', "
        "timeout_seconds: 'int' = 3600, "
        "chunk_size: 'int' = 1048576, "
        "settings: 'dict[str, Any]' = "
        "<factory>, query_id: 'str | "
        "None' = None, "
        "insert_deduplication_token: "
        "'str | None' = None) -> None",
        "type_hints": {
            "input_format": "<class 'str'>",
            "timeout_seconds": "<class 'int'>",
            "chunk_size": "<class 'int'>",
            "settings": "dict[str, typing.Any]",
            "query_id": "str | None",
            "insert_deduplication_token": "str | None",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpOptions.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpOptions.__init__",
        "signature": "(self, input_format: "
        "'str' = "
        "'TabSeparated', "
        "timeout_seconds: 'int' "
        "= 3600, chunk_size: "
        "'int' = 1048576, "
        "settings: 'dict[str, "
        "Any]' = <factory>, "
        "query_id: 'str | None' "
        "= None, "
        "insert_deduplication_token: "
        "'str | None' = None) "
        "-> None",
        "type_hints": {
            "input_format": "<class 'str'>",
            "timeout_seconds": "<class 'int'>",
            "chunk_size": "<class 'int'>",
            "settings": "dict[str, typing.Any]",
            "query_id": "str | None",
            "insert_deduplication_token": "str | None",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpOptions.from_bulk_wire_contract": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpOptions.from_bulk_wire_contract",
        "signature": "(contract: 'Any', *, base: 'ClickHouseHttpOptions | None' = None) -> 'ClickHouseHttpOptions'",
        "type_hints": {
            "contract": "typing.Any",
            "base": "dpone.runtime.connectors.clickhouse_http_bulk.ClickHouseHttpOptions | None",
            "return": "<class 'dpone.runtime.connectors.clickhouse_http_bulk.ClickHouseHttpOptions'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpBulkRunner.__init__": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpBulkRunner.__init__",
        "signature": "(self, credentials: "
        "'ClickHouseHttpCredentials', "
        "options: "
        "'ClickHouseHttpOptions "
        "| None' = None, "
        "connection_factory: "
        "'Callable[..., "
        "http.client.HTTPConnection] "
        "| None' = None) -> "
        "'None'",
        "type_hints": {
            "credentials": "<class 'dpone.runtime.connectors.clickhouse_http_bulk.ClickHouseHttpCredentials'>",
            "options": "dpone.runtime.connectors.clickhouse_http_bulk.ClickHouseHttpOptions | None",
            "connection_factory": "collections.abc.Callable[..., http.client.HTTPConnection] | None",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpBulkRunner.build_insert_url": {
        "module": "dpone.runtime.connectors.clickhouse_http_bulk",
        "qualname": "ClickHouseHttpBulkRunner.build_insert_url",
        "signature": "(self, "
        "table: "
        "'str', "
        "columns: "
        "'Sequence[str]', "
        "*, "
        "include_password: "
        "'bool' = "
        "True, "
        "query: 'str "
        "| None' = "
        "None) -> "
        "'str'",
        "type_hints": {
            "table": "<class 'str'>",
            "columns": "collections.abc.Sequence[str]",
            "include_password": "<class 'bool'>",
            "query": "str | None",
            "return": "<class 'str'>",
        },
    },
    "dpone.runtime.connectors.bulk_text_codec:BulkTextCodec": {
        "module": "dpone.runtime.connectors.bulk_text_codec",
        "qualname": "BulkTextCodec",
        "signature": "(marker_prefix: 'str' = '\\x1d', "
        "empty_string_marker: 'str' = '\\x1dE', "
        "field_terminator: 'str' = '\\t', "
        "row_terminator: 'str' = '\\n') -> None",
        "type_hints": {
            "codec_id": "typing.ClassVar[str]",
            "codec_version": "typing.ClassVar[int]",
            "marker_prefix": "<class 'str'>",
            "empty_string_marker": "<class 'str'>",
            "field_terminator": "<class 'str'>",
            "row_terminator": "<class 'str'>",
        },
    },
    "dpone.runtime.support.bulk_text_file_reader:BulkTextFileReadError": {
        "module": "dpone.runtime.support.bulk_text_file_reader",
        "qualname": "BulkTextFileReadError",
        "signature": "(blocker: 'str') -> 'None'",
        "type_hints": {},
    },
    "dpone.runtime.support.bulk_text_file_reader:BulkTextFileReadError.__init__": {
        "module": "dpone.runtime.support.bulk_text_file_reader",
        "qualname": "BulkTextFileReadError.__init__",
        "signature": "(self, blocker: 'str') -> 'None'",
        "type_hints": {"blocker": "<class 'str'>", "return": "<class 'NoneType'>"},
    },
    "dpone.runtime.support.bulk_text_file_reader:iter_wire_rows": {
        "module": "dpone.runtime.support.bulk_text_file_reader",
        "qualname": "iter_wire_rows",
        "signature": "(stream: 'BinaryIO', width: 'int', "
        "codec: 'BulkTextCodec', *, "
        "max_record_bytes: 'int | None' = None) "
        "-> 'Iterator[tuple[bytes, ...]]'",
        "type_hints": {
            "stream": "<class 'typing.BinaryIO'>",
            "width": "<class 'int'>",
            "codec": "<class 'dpone.runtime.connectors.bulk_text_codec.BulkTextCodec'>",
            "max_record_bytes": "int | None",
            "return": "collections.abc.Iterator[tuple[bytes, ...]]",
        },
    },
    "dpone.runtime.support.bulk_text_file_reader:decode_wire_value": {
        "module": "dpone.runtime.support.bulk_text_file_reader",
        "qualname": "decode_wire_value",
        "signature": "(raw: 'bytes', *, dtype: 'str', codec: 'BulkTextCodec') -> 'object'",
        "type_hints": {
            "raw": "<class 'bytes'>",
            "dtype": "<class 'str'>",
            "codec": "<class 'dpone.runtime.connectors.bulk_text_codec.BulkTextCodec'>",
            "return": "<class 'object'>",
        },
    },
    "dpone.runtime.support.bulk_text_file_reader:iter_rows": {
        "module": "dpone.runtime.support.bulk_text_file_reader",
        "qualname": "iter_rows",
        "signature": "(stream: 'BinaryIO', schema: "
        "'tuple[tuple[str, str], ...]', codec: "
        "'BulkTextCodec', *, max_record_bytes: 'int | "
        "None' = None) -> 'Iterator[tuple[object, "
        "...]]'",
        "type_hints": {
            "stream": "<class 'typing.BinaryIO'>",
            "schema": "tuple[tuple[str, str], ...]",
            "codec": "<class 'dpone.runtime.connectors.bulk_text_codec.BulkTextCodec'>",
            "max_record_bytes": "int | None",
            "return": "collections.abc.Iterator[tuple[object, ...]]",
        },
    },
    "dpone.runtime.sinks.clickhouse_validated_file_models:ClickHouseValidatedFilePolicy": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_models",
        "qualname": "ClickHouseValidatedFilePolicy",
        "signature": "(work_directory: "
        "'Path', "
        "max_spool_bytes: "
        "'int', "
        "max_source_bytes: "
        "'int' = "
        "4294967296, "
        "max_record_bytes: "
        "'int' = "
        "16777216, "
        "min_free_bytes: "
        "'int' = "
        "1073741824, "
        "preparation_timeout_seconds: "
        "'int' = 3600, "
        "verification_timeout_seconds: "
        "'int' = 3600) -> "
        "None",
        "type_hints": {
            "work_directory": "<class 'pathlib.Path'>",
            "max_spool_bytes": "<class 'int'>",
            "max_source_bytes": "<class 'int'>",
            "max_record_bytes": "<class 'int'>",
            "min_free_bytes": "<class 'int'>",
            "preparation_timeout_seconds": "<class 'int'>",
            "verification_timeout_seconds": "<class 'int'>",
        },
    },
    "dpone.runtime.sinks.clickhouse_validated_file_models:ClickHouseValidatedFilePolicy.__init__": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_models",
        "qualname": "ClickHouseValidatedFilePolicy.__init__",
        "signature": "(self, "
        "work_directory: "
        "'Path', "
        "max_spool_bytes: "
        "'int', "
        "max_source_bytes: "
        "'int' = "
        "4294967296, "
        "max_record_bytes: "
        "'int' = "
        "16777216, "
        "min_free_bytes: "
        "'int' = "
        "1073741824, "
        "preparation_timeout_seconds: "
        "'int' = "
        "3600, "
        "verification_timeout_seconds: "
        "'int' = "
        "3600) "
        "-> None",
        "type_hints": {
            "work_directory": "<class 'pathlib.Path'>",
            "max_spool_bytes": "<class 'int'>",
            "max_source_bytes": "<class 'int'>",
            "max_record_bytes": "<class 'int'>",
            "min_free_bytes": "<class 'int'>",
            "preparation_timeout_seconds": "<class 'int'>",
            "verification_timeout_seconds": "<class 'int'>",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.sinks.clickhouse_validated_file_models:FileConsumptionError": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_models",
        "qualname": "FileConsumptionError",
        "signature": "(blocker: 'str', *, "
        "phase: 'str' = "
        "'checking', column: 'str "
        "| None' = None, "
        "row_ordinal: 'int | None' "
        "= None) -> 'None'",
        "type_hints": {},
    },
    "dpone.runtime.sinks.clickhouse_validated_file_models:FileConsumptionError.__init__": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_models",
        "qualname": "FileConsumptionError.__init__",
        "signature": "(self, blocker: "
        "'str', *, phase: "
        "'str' = "
        "'checking', "
        "column: 'str | "
        "None' = None, "
        "row_ordinal: "
        "'int | None' = "
        "None) -> 'None'",
        "type_hints": {
            "blocker": "<class 'str'>",
            "phase": "<class 'str'>",
            "column": "str | None",
            "row_ordinal": "int | None",
            "return": "<class 'NoneType'>",
        },
    },
    "dpone.runtime.sinks.clickhouse_validated_file_models:require_transport_profile": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_models",
        "qualname": "require_transport_profile",
        "signature": "(options: 'Mapping[str, Any]') -> 'tuple[str, int]'",
        "type_hints": {"options": "collections.abc.Mapping[str, typing.Any]", "return": "tuple[str, int]"},
    },
    "dpone.runtime.sinks.clickhouse_validated_file_journal:canonical_json": {
        "module": "dpone.runtime.sinks.clickhouse_validated_file_journal",
        "qualname": "canonical_json",
        "signature": "(value: 'object') -> 'bytes'",
        "type_hints": {"value": "<class 'object'>", "return": "<class 'bytes'>"},
    },
    "dpone.runtime.sinks.clickhouse_sink:ClickHouseSink.__init__": {
        "module": "dpone.runtime.sinks.clickhouse_sink",
        "qualname": "ClickHouseSink.__init__",
        "signature": "(self, connector: "
        "'ClickHouseConnectorPort', "
        "state_storage: 'Any' = None, logger: "
        "'Any | None' = None, *, "
        "client_runner_cls: 'Any | None' = None, "
        "http_runner_cls: 'Any | None' = None, "
        "physical_type_resolver: "
        "'ClickHousePhysicalColumnTypeResolver | "
        "None' = None, "
        "validated_file_runner_factory: "
        "'Callable[[LoadConfig, "
        "ClickHouseValidatedFilePolicy], "
        "ClickHouseFileStageRunner] | None' = "
        "None)",
        "type_hints": {"existing_unresolved_annotation": "name 'ClickHouseConnectorPort' is not defined"},
    },
    "dpone.runtime.sinks.clickhouse_sink:ClickHouseSink.stage_validated_file": {
        "module": "dpone.runtime.sinks.clickhouse_sink",
        "qualname": "ClickHouseSink.stage_validated_file",
        "signature": "(self, load_config: "
        "'LoadConfig', payload: "
        "'LoadPayload', *, policy: "
        "'ClickHouseValidatedFilePolicy') "
        "-> 'StagedLoadHandle'",
        "type_hints": {"existing_unresolved_annotation": "name 'LoadConfig' is not defined"},
    },
}


@pytest.mark.parametrize("path", BASELINE)
def test_public_metadata_signature_and_type_hints_match_characterized_contract(path):
    assert _contract(path) == BASELINE[path]


# Synthetic instance pickles produced by an isolated archive of the original commit.
LEGACY_PICKLES = {
    "dpone.runtime.connectors.bulk_text_codec:BulkTextCodec": "gASVoQAAAAAAAACMKGRwb25lLnJ1bnRpbWUuY29ubmVjdG9ycy5idWxrX3RleHRfY29kZWOUjA1CdWxrVGV4dENvZGVjlJOUKYGUfZQojA1tYXJrZXJfcHJlZml4lIwBHZSME2VtcHR5X3N0cmluZ19tYXJrZXKUjAIdRZSMEGZpZWxkX3Rlcm1pbmF0b3KUjAEJlIwOcm93X3Rlcm1pbmF0b3KUjAEKlHViLg==",
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientCredentials": "gASVmAAAAAAAAACMKGRwb25lLnJ1bnRpbWUuY29ubmVjdG9ycy5jbGlja2hvdXNlX2J1bGuUjBtDbGlja0hvdXNlQ2xpZW50Q3JlZGVudGlhbHOUk5QpgZR9lCiMBGhvc3SUaAWMBHBvcnSUTSgjjAhkYXRhYmFzZZSMAmRilIwEdXNlcpRoCYwIcGFzc3dvcmSUjACUjAZzZWN1cmWUiXViLg==",
    "dpone.runtime.connectors.clickhouse_bulk:ClickHouseClientOptions": "gASV/QAAAAAAAACMKGRwb25lLnJ1bnRpbWUuY29ubmVjdG9ycy5jbGlja2hvdXNlX2J1bGuUjBdDbGlja0hvdXNlQ2xpZW50T3B0aW9uc5STlCmBlH2UKIwOY2xpZW50X2NvbW1hbmSUjBFjbGlja2hvdXNlLWNsaWVudJSMDGlucHV0X2Zvcm1hdJSMDFRhYlNlcGFyYXRlZJSMD3RpbWVvdXRfc2Vjb25kc5ROjBVtYXhfaW5zZXJ0X2Jsb2NrX3NpemWUTowIc2V0dGluZ3OUfZSMAWGUSwFzjAhxdWVyeV9pZJROjBppbnNlcnRfZGVkdXBsaWNhdGlvbl90b2tlbpROdWIu",
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpCredentials": "gASVmwAAAAAAAACMLWRwb25lLnJ1bnRpbWUuY29ubmVjdG9ycy5jbGlja2hvdXNlX2h0dHBfYnVsa5SMGUNsaWNrSG91c2VIdHRwQ3JlZGVudGlhbHOUk5QpgZR9lCiMBGhvc3SUaAWMBHBvcnSUTbsfjAhkYXRhYmFzZZSMAmRilIwEdXNlcpRoCYwIcGFzc3dvcmSUjACUjAZzZWN1cmWUiXViLg==",
    "dpone.runtime.connectors.clickhouse_http_bulk:ClickHouseHttpOptions": "gASV1gAAAAAAAACMLWRwb25lLnJ1bnRpbWUuY29ubmVjdG9ycy5jbGlja2hvdXNlX2h0dHBfYnVsa5SMFUNsaWNrSG91c2VIdHRwT3B0aW9uc5STlCmBlH2UKIwMaW5wdXRfZm9ybWF0lIwMVGFiU2VwYXJhdGVklIwPdGltZW91dF9zZWNvbmRzlE0QDowKY2h1bmtfc2l6ZZRKAAAQAIwIc2V0dGluZ3OUfZSMAWGUSwFzjAhxdWVyeV9pZJROjBppbnNlcnRfZGVkdXBsaWNhdGlvbl90b2tlbpROdWIu",
    "dpone.runtime.sinks.clickhouse_validated_file_models:ClickHouseValidatedFilePolicy": "gASVpwAAAAAAAACMNGRwb25lLnJ1bnRpbWUuc2lua3MuY2xpY2tob3VzZV92YWxpZGF0ZWRfZmlsZV9tb2RlbHOUjB1DbGlja0hvdXNlVmFsaWRhdGVkRmlsZVBvbGljeZSTlCmBlF2UKIwHcGF0aGxpYpSMCVBvc2l4UGF0aJSTlIwBL5SMCXN5bnRoZXRpY5SGlFKUS2SKBQAAAAABSgAAAAFKAAAAQE0QDk0QDmViLg==",
}


@pytest.mark.parametrize(
    "value",
    [
        client.ClickHouseClientCredentials("host", 9000, "db", "user"),
        client.ClickHouseClientOptions(settings={"a": 1}),
        http.ClickHouseHttpCredentials("host", 8123, "db", "user"),
        http.ClickHouseHttpOptions(settings={"a": 1}),
        ClickHouseValidatedFilePolicy(Path("/synthetic"), 100),
        BulkTextCodec(),
    ],
)
def test_historical_class_pickle_and_instance_round_trip(value):
    cls = type(value)
    historical_global = f"c{cls.__module__}\n{cls.__qualname__}\n.".encode()
    assert pickle.loads(historical_global) is cls
    assert pickle.loads(pickle.dumps(value)) == value
    restored = pickle.loads(base64.b64decode(LEGACY_PICKLES[f"{cls.__module__}:{cls.__qualname__}"]))
    assert type(restored) is cls and restored == value
    assert replace(value) == value
    assert hasattr(value, "__dict__") is (cls is not ClickHouseValidatedFilePolicy)
    field = next(iter(asdict(value)))
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, "mutation")


@pytest.mark.parametrize("options_type", [client.ClickHouseClientOptions, http.ClickHouseHttpOptions])
def test_wire_options_merge_without_mutating_base_and_keep_subclass(options_type):
    class CustomOptions(options_type):
        pass

    base = CustomOptions(settings={"keep": 1, "override": "old"}, query_id="query", insert_deduplication_token="token")
    contract = SimpleNamespace(
        input_format="RowBinary", delimiter_profile=SimpleNamespace(clickhouse_settings={"override": "new"})
    )
    result = CustomOptions.from_bulk_wire_contract(contract, base=base)
    assert type(result) is CustomOptions
    assert asdict(result) == {**asdict(base), "input_format": "RowBinary", "settings": {"keep": 1, "override": "new"}}
    assert base.settings == {"keep": 1, "override": "old"}
    del contract.input_format
    assert CustomOptions.from_bulk_wire_contract(contract, base=base).input_format == base.input_format


def test_client_builder_order_settings_and_redaction():
    runner = client.ClickHouseClientRunner(
        client.ClickHouseClientCredentials("host", 9440, "db", "user", "synthetic", True),
        client.ClickHouseClientOptions(
            client_command='client --config "space file"',
            input_format="RowBinary",
            max_insert_block_size=7,
            settings={"first": 1, "second": False},
            query_id="qid",
            insert_deduplication_token="token",
        ),
    )
    prefix = [
        "client",
        "--config",
        "space file",
        "--host",
        "host",
        "--port",
        "9440",
        "--database",
        "db",
        "--user",
        "user",
        "--password",
        "synthetic",
        "--secure",
        "--max_insert_block_size",
        "7",
        "--first",
        "1",
        "--second",
        "False",
        "--query_id",
        "qid",
        "--insert_deduplication_token",
        "token",
    ]
    assert runner.build_insert_command("db.table", ["a", "b"]) == prefix + [
        "--query",
        "INSERT INTO db.table (`a`, `b`) FORMAT RowBinary",
    ]
    assert runner.build_query_command("") == prefix + ["--query", ""]
    assert runner.redact_command(["--password", "a", "--password", "b", "--password"]) == [
        "--password",
        "***",
        "--password",
        "***",
        "--password",
    ]


@pytest.mark.parametrize("query", [None, "", "SELECT 1", " "])
@pytest.mark.parametrize("password", [True, False])
def test_http_query_fallback_redaction_and_settings_precedence(query, password):
    runner = http.ClickHouseHttpBulkRunner(
        http.ClickHouseHttpCredentials("host", 8123, "db", "user", "synthetic&?"),
        http.ClickHouseHttpOptions(
            input_format="RowBinary", settings={"database": "override"}, query_id="q", insert_deduplication_token="t"
        ),
        HTTPConnection,
    )
    result = parse_qs(urlsplit(runner.build_insert_url("db.t", ["a"], query=query, include_password=password)).query)
    assert result == {
        "database": ["override"],
        "user": ["user"],
        "password": ["synthetic&?" if password else "***"],
        "query": [query or "INSERT INTO db.t (`a`) FORMAT RowBinary"],
        "query_id": ["q"],
        "insert_deduplication_token": ["t"],
    }


def test_public_client_subclass_dispatch_through_actual_child(tmp_path):
    script = tmp_path / "peer.py"
    script.write_text(
        "import json,sys\nprint(json.dumps({'argv':sys.argv[1:],'body':sys.stdin.buffer.read().hex() if '--public-insert' in sys.argv else ''}))\n"
    )
    command = shlex.join([sys.executable, str(script)])

    class PublicRunner(client.ClickHouseClientRunner):
        def build_insert_command(self, table, columns):
            return super().build_insert_command(table, columns) + ["--public-insert"]

        def build_query_command(self, sql):
            return super().build_query_command(sql) + ["--public-query"]

        @staticmethod
        def redact_command(command):
            return client.ClickHouseClientRunner.redact_command(command) + ["public-redaction"]

    runner = PublicRunner(
        client.ClickHouseClientCredentials("host", 9000, "db", "user"),
        client.ClickHouseClientOptions(client_command=command, timeout_seconds=2),
    )
    source = tmp_path / "source"
    source.write_bytes(b"\x00\xfftext")
    for result in (
        runner.insert_file("db.t", ["v"], str(source)),
        runner.insert_stream("db.t", ["v"], [b"\x00", b"\xfftext"]),
    ):
        observed = json.loads(result.stdout)
        assert observed["body"] == "00ff74657874"
        assert observed["argv"][-1] == "--public-insert"
        assert result.redacted_command[-1] == "public-redaction"
    assert runner.build_query_command("SELECT 1")[-1] == "--public-query"
    result = runner.execute_query("SELECT 1")
    assert "--public-query" not in json.loads(result.stdout)["argv"]


def test_public_http_subclass_dispatch_through_actual_loopback(tmp_path):
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = bytearray()
            if self.headers.get("Transfer-Encoding") == "chunked":
                while size := int(self.rfile.readline(), 16):
                    body.extend(self.rfile.read(size))
                    assert self.rfile.read(2) == b"\r\n"
                assert self.rfile.readline() == b"\r\n"
            else:
                body.extend(self.rfile.read(int(self.headers["Content-Length"])))
            observed.append((self.path, bytes(body)))
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

    class PublicRunner(http.ClickHouseHttpBulkRunner):
        def build_insert_url(self, *args, **kwargs):
            return super().build_insert_url(*args, **kwargs) + "&public_dispatch=1"

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        runner = PublicRunner(
            http.ClickHouseHttpCredentials("127.0.0.1", server.server_port, "db", "user"),
            http.ClickHouseHttpOptions(timeout_seconds=2),
            HTTPConnection,
        )
        source = tmp_path / "source"
        source.write_bytes(b"\x00\xfftext")
        assert runner.insert_file("db.t", ["v"], str(source)).response == "ok"
        assert runner.insert_stream("db.t", ["v"], [b"\x00", b"\xfftext"]).response == "ok"
        assert len(observed) == 2
        assert all(
            parse_qs(urlsplit(path).query)["public_dispatch"] == ["1"] and data == b"\x00\xfftext"
            for path, data in observed
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_reader_preserves_exact_binary_whitespace_empty_null_and_error_boundary():
    codec = BulkTextCodec()
    assert list(
        reader.iter_rows(io.BytesIO(b"00 ff\t\x1dE\t\n"), (("a", "binary"), ("b", "text"), ("c", "text")), codec)
    ) == [(b"\x00\xff", "", None)]
    with pytest.raises(reader.BulkTextFileReadError) as error:
        list(reader.iter_rows(io.BytesIO(b"\xff\n"), (("a", "text"),), codec))
    assert error.value.blocker == "utf8_invalid"
    assert isinstance(error.value.__cause__, UnicodeDecodeError)


@pytest.mark.parametrize(
    "legacy,current,names",
    [
        (
            "connectors.clickhouse_bulk",
            "connectors.clickhouse_client_request",
            ("ClickHouseClientCredentials", "ClickHouseClientOptions"),
        ),
        (
            "connectors.clickhouse_http_bulk",
            "connectors.clickhouse_http_request",
            ("ClickHouseHttpCredentials", "ClickHouseHttpOptions"),
        ),
        (
            "sinks.clickhouse_validated_file_models",
            "clickhouse_file_stage_contract",
            ("ClickHouseValidatedFilePolicy", "FileConsumptionError", "require_transport_profile"),
        ),
        (
            "support.bulk_text_file_reader",
            "connectors.bulk_text_codec",
            ("BulkTextFileReadError", "iter_wire_rows", "decode_wire_value", "iter_rows"),
        ),
        ("sinks.clickhouse_validated_file_journal", "clickhouse_file_stage_contract", ("canonical_json",)),
    ],
)
def test_canonical_implementations_keep_identical_legacy_globals_and_pickle_paths(legacy, current, names):
    old_module = importlib.import_module("dpone.runtime." + legacy)
    new_module = importlib.import_module("dpone.runtime." + current)
    for name in names:
        value = getattr(old_module, name)
        assert getattr(new_module, name) is value
        assert pickle.loads(f"c{value.__module__}\n{value.__qualname__}\n.".encode()) is value
