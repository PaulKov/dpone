"""Synthetic-only credential admission; no connection or secret artifacts."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials


def sample():
    return SqlClientCredentials("localhost", 1433, "synthetic", "test_user", "synthetic-password", "verified")


def test_valid_credentials_are_redacted():
    value = sample()
    assert "synthetic" not in repr(value)
    assert replace(value, password="пароль🔒", database="a" * 128)
    assert replace(value, tls_profile="disposable_test")


@pytest.mark.parametrize(
    "field,value",
    [
        ("host", "server;Encrypt=false"),
        ("host", ""),
        ("host", "a" * 256),
        ("port", True),
        ("port", 0),
        ("port", 65536),
        ("port", 1433.0),
        ("database", "😀" * 65),
        ("database", "bad\nname"),
        ("username", ""),
        ("password", ""),
        ("password", "bad\x00password"),
        ("password", "🔒" * 4097),
        ("password", "\ud800"),
        ("password", None),
        ("tls_profile", "unverified"),
        ("tls_profile", None),
    ],
)
def test_invalid_credentials_have_constant_error(field, value):
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_credentials_invalid$"):
        replace(sample(), **{field: value})


def test_password_byte_limit_and_identifier_utf16_limit():
    assert replace(sample(), password="🔒" * 4096, username="😀" * 64)
