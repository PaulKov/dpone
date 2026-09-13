"""Closed dispatcher coordinates carried by a verified connection descriptor.

Parsing establishes shape and exact canonical bytes only. The application must
obtain the property from its verified registry before treating it as authority.
The referenced API binding owns endpoint and secret resolution; neither belongs
in this public, non-secret document.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

SCHEMA = "dpone.composition-dispatcher-binding.v1"
_FIELDS = {"schema", "dispatcher_id", "connection_ref", "service_configuration_sha256"}


def require_dispatcher_connection_ref(value: object) -> str:
    """Validate a closed logical reference, never a path or URL supplied for I/O."""
    if not isinstance(value, str) or not is_valid_connection_ref(value) or ".." in value:
        raise CompositionAdmissionError("dispatcher_binding")
    return value


@dataclass(frozen=True, slots=True)
class CompositionDispatcherBinding:
    """Immutable public service identity; it contains no credentials."""

    dispatcher_id: str
    connection_ref: str
    service_configuration_sha256: str

    def __post_init__(self) -> None:
        try:
            identifier = UUID(self.dispatcher_id)
            if str(identifier) != self.dispatcher_id or identifier.int == 0:
                raise ValueError
            require_dispatcher_connection_ref(self.connection_ref)
            require_digest(self.service_configuration_sha256)
        except (ValueError, TypeError, AttributeError):
            raise CompositionAdmissionError("dispatcher_binding") from None

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "schema": SCHEMA,
                "dispatcher_id": self.dispatcher_id,
                "connection_ref": self.connection_ref,
                "service_configuration_sha256": self.service_configuration_sha256,
            }
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CompositionDispatcherBinding:
        try:
            if set(value) != _FIELDS or value["schema"] != SCHEMA:
                raise ValueError
            return cls(value["dispatcher_id"], value["connection_ref"], value["service_configuration_sha256"])
        except (ValueError, TypeError, KeyError, AttributeError):
            raise CompositionAdmissionError("dispatcher_binding") from None

    @classmethod
    def from_document(cls, document: bytes) -> CompositionDispatcherBinding:
        try:
            if type(document) is not bytes or len(document) > 4096:
                raise ValueError
            result = cls.from_mapping(strict_json_object(document))
            if result.to_bytes() != document:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError):
            raise CompositionAdmissionError("dispatcher_binding") from None
