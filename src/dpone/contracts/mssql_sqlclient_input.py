"""Path-free SqlClient native descriptor; declared bindings are not file evidence.

The owning adapter admits a read-only regular descriptor, verifies its position
and fstat identity, and rechecks identity after natural EOF. This contract opens
nothing and never promotes a declared digest/count to consumption authority.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_validation import _integer, _text
from dpone.contracts.native_wire_layout import NativeWireColumnLayout
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

_LIMIT = 1 << 20
_ERROR = "mssql_native.sqlclient_input_descriptor_invalid"
_LAYOUTS = {
    "bigint": ("Int64", "bigint", 8, None, None, None),
    "float(53)": ("Float64", "float", 8, 53, None, None),
    "nvarchar(max)": ("String", "nvarchar", None, None, None, "utf-16le"),
    "datetime2(6)": ("DateTime64(6)", "datetime2", 8, None, 7, None),
}


def validate_sqlclient_column(column: NativeWireColumnLayout) -> None:
    """Admit the exact four-type physical intersection without narrowing the DTO."""
    if type(column) is not NativeWireColumnLayout or type(column.nullable) is not bool:
        raise ValueError(_ERROR)
    _text(column.name, 128)
    if len(column.name.encode("utf-16le")) > 256:
        raise ValueError(_ERROR)
    for text in (column.source_type, column.target_type, column.storage_type):
        _text(text, 256)
    _integer(column.prefix_width, 0, 8)
    for value in (column.fixed_length, column.precision, column.scale):
        if value is not None:
            _integer(value, 0, 255)
    if column.encoding is not None and type(column.encoding) is not str:
        raise ValueError(_ERROR)
    declared = column.source_type
    if column.nullable:
        if not declared.endswith(" nullable"):
            raise ValueError(_ERROR)
        declared = declared[:-9]
    expected = _LAYOUTS.get(declared)
    if expected is None:
        raise ValueError(_ERROR)
    target, storage, fixed, precision, scale, encoding = expected
    if column.nullable:
        target = f"Nullable({target})"
    prefix = 8 if declared == "nvarchar(max)" else int(column.nullable)
    if (
        column.target_type,
        column.storage_type,
        column.fixed_length,
        column.precision,
        column.scale,
        column.encoding,
        column.prefix_width,
    ) != (target, storage, fixed, precision, scale, encoding, prefix):
        raise ValueError(_ERROR)


@dataclass(frozen=True)
class SqlClientFileIdentity:
    """Original Linux fstat fields; receiving these values performs no observation."""

    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int

    def __post_init__(self) -> None:
        for value in (self.device, self.inode):
            _integer(value, 0, 2**64 - 1)
        for value in (self.size, self.mtime_ns, self.ctime_ns):
            _integer(value)


@dataclass(frozen=True)
class SqlClientInputDescriptor:
    """Immutable ordered metadata and expected complete input, bound before exec."""

    schema_version: int
    fd: int
    columns: tuple[NativeWireColumnLayout, ...]
    expected: TdsInputReceipt
    max_row_bytes: int
    file_identity: SqlClientFileIdentity

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _integer(self.fd, 3, 2**31 - 1)
        _integer(self.max_row_bytes, 1, 2**31 - 1)
        if type(self.columns) is not tuple or not 1 <= len(self.columns) <= 100:
            raise ValueError(_ERROR)
        for column in self.columns:
            validate_sqlclient_column(column)
        if len({column.name for column in self.columns}) != len(self.columns):
            raise ValueError(_ERROR)
        if type(self.expected) is not TdsInputReceipt or type(self.file_identity) is not SqlClientFileIdentity:
            raise ValueError(_ERROR)
        if self.expected.encoded_bytes != self.file_identity.size:
            raise ValueError(_ERROR)
        if (self.expected.rows == 0) != (self.expected.encoded_bytes == 0):
            raise ValueError(_ERROR)
        if self.expected.rows == 0 and self.expected.file_sha256 != sha256(b"").hexdigest():
            raise ValueError(_ERROR)


def encode_input_descriptor(record: SqlClientInputDescriptor) -> bytes:
    """Canonical body; fd stat and input bytes still require independent checks."""
    if type(record) is not SqlClientInputDescriptor:
        raise ValueError(_ERROR)
    body = canonical_json_bytes(asdict(record))
    if len(body) > _LIMIT:
        raise ValueError(_ERROR)
    return body


def decode_input_descriptor(body: bytes) -> SqlClientInputDescriptor:
    """Bound bytes/count before allocation and require every nested field."""
    try:
        if type(body) is not bytes or not 0 < len(body) <= _LIMIT:
            raise ValueError(_ERROR)
        value = record_shape(SqlClientInputDescriptor, strict_json_object(body))
        columns = value["columns"]
        if type(columns) is not list or not 1 <= len(columns) <= 100:
            raise ValueError(_ERROR)
        value["columns"] = tuple(construct_record(NativeWireColumnLayout, column) for column in columns)
        value["expected"] = construct_record(TdsInputReceipt, value["expected"])
        value["file_identity"] = construct_record(SqlClientFileIdentity, value["file_identity"])
        return SqlClientInputDescriptor(**value)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def input_descriptor_digest(record: SqlClientInputDescriptor) -> str:
    """Semantic input binding used by launch; not an evidence artifact byte hash."""
    return sha256(b"dpone.sqlclient.input.v1\0" + encode_input_descriptor(record)).hexdigest()
