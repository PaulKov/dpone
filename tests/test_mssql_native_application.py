"""Production composition keeps source identity, target types and defaults closed."""

from types import SimpleNamespace

import pytest

from dpone.runtime.mssql_native_application import (
    DefaultMssqlNativeRuntimeFactory,
    _sqlclient_credentials,
    _wire_schema,
)


def test_wire_schema_uses_admitted_target_types_in_source_order() -> None:
    preplan = SimpleNamespace(
        column_mapping=(),
        target_column_types=(
            ("text_value", "nvarchar(max)"),
            ("row_key", "bigint"),
            ("__dpone__load_id", "varchar(64)"),
        ),
    )
    assert _wire_schema((("row_key", "Int64"), ("text_value", "Nullable(String)")), preplan) == (
        ("row_key", "bigint"),
        ("text_value", "nvarchar(max) nullable"),
    )


def test_wire_schema_rejects_rename_or_missing_target_type() -> None:
    with pytest.raises(ValueError, match="renamed_wire"):
        _wire_schema(
            (("source_name", "Int64"),),
            SimpleNamespace(column_mapping=(("source_name", "target_name"),), target_column_types=()),
        )
    with pytest.raises(ValueError, match="target_type_missing"):
        _wire_schema(
            (("missing", "Int64"),),
            SimpleNamespace(column_mapping=(), target_column_types=(("other", "bigint"),)),
        )


def test_sqlclient_credentials_preserve_tls_policy_without_rendering_secret() -> None:
    credentials = _sqlclient_credentials(
        SimpleNamespace(
            host="db",
            port=1433,
            database="warehouse",
            user="writer",
            password="private",
            encrypt="yes",
            trust_server_certificate="no",
        )
    )
    assert credentials.encrypt is True
    assert credentials.trust_server_certificate is False
    assert "private" not in repr(credentials)


def test_default_factory_is_lazy_until_run() -> None:
    process = SimpleNamespace()
    runtime = DefaultMssqlNativeRuntimeFactory()(process)
    assert runtime._process_config is process
