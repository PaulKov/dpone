"""Small strict reflection helpers for opaque pregrant custody."""

from inspect import getattr_static
from typing import Any, Never

ERROR = "mssql_native.sqlclient_writer_pregrant_unknown"


def invalid() -> Never:
    raise ValueError(ERROR)


def call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    return descriptor.__get__(value, type(value))(*args, **kwargs)


def property_value(value: object, name: str) -> Any:
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        invalid()
    return descriptor.__get__(value, type(value))
