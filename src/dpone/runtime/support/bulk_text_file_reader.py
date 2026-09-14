"""Compatibility aliases for the canonical BulkTextCodec wire reader."""

from dpone.runtime.connectors.bulk_text_codec import (
    BulkTextCodec as BulkTextCodec,
)
from dpone.runtime.connectors.bulk_text_codec import (
    BulkTextFileReadError as BulkTextFileReadError,
)
from dpone.runtime.connectors.bulk_text_codec import (
    decode_wire_value as decode_wire_value,
)
from dpone.runtime.connectors.bulk_text_codec import (
    is_bulk_text_type as is_bulk_text_type,
)
from dpone.runtime.connectors.bulk_text_codec import (
    iter_rows as iter_rows,
)
from dpone.runtime.connectors.bulk_text_codec import (
    iter_wire_rows as iter_wire_rows,
)
