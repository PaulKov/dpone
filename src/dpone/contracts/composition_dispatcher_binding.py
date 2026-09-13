"""Closed dispatcher coordinates carried by a verified connection descriptor.

Parsing establishes shape and exact canonical bytes only. The application must
obtain the property from its verified registry before treating it as authority.
The referenced API binding owns endpoint and secret resolution; neither belongs
in this public, non-secret document.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

SCHEMA = "dpone.composition-dispatcher-binding.v1"
POLICY_SCHEMA = "dpone.composition-dispatcher-binding.v2"
_FIELDS = {"schema", "dispatcher_id", "connection_ref", "service_configuration_sha256"}
_POLICY_FIELDS = {"schema", "dispatcher_id", "connection_ref", "service_policy_sha256"}


def require_dispatcher_connection_ref(value: object) -> str:
    """Validate a closed logical reference, never a path or URL supplied for I/O."""
    if not isinstance(value, str) or not is_valid_connection_ref(value) or ".." in value:
        raise CompositionAdmissionError("dispatcher_binding")
    return value


@dataclass(frozen=True, slots=True)
class CompositionDispatcherBinding:
    """Immutable public service identity with distinct canonical digest preimages.

    The three positional arguments retain v1 configuration identity and exact
    bytes. V2 accepts only service_policy_sha256 as a keyword; neither identity
    authorizes reinterpreting the other document's full-byte digest.
    """

    dispatcher_id: str
    connection_ref: str
    service_configuration_sha256: str | None = None
    service_policy_sha256: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        try:
            identifier = UUID(self.dispatcher_id)
            if str(identifier) != self.dispatcher_id or identifier.int == 0:
                raise ValueError
            require_dispatcher_connection_ref(self.connection_ref)
            if (self.service_configuration_sha256 is None) == (self.service_policy_sha256 is None):
                raise ValueError
            require_digest(self.identity_sha256)
        except (ValueError, TypeError, AttributeError):
            raise CompositionAdmissionError("dispatcher_binding") from None

    @property
    def identity_kind(self) -> Literal["configuration", "policy"]:
        """The selected document kind; a hash alone does not identify its preimage."""
        return "configuration" if self.service_configuration_sha256 is not None else "policy"

    @property
    def identity_sha256(self) -> str:
        """The exact digest of the selected configuration or acyclic policy original."""
        value = (
            self.service_configuration_sha256
            if self.service_configuration_sha256 is not None
            else self.service_policy_sha256
        )
        if value is None:
            raise CompositionAdmissionError("dispatcher_binding")
        return value

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "schema": SCHEMA if self.identity_kind == "configuration" else POLICY_SCHEMA,
                "dispatcher_id": self.dispatcher_id,
                "connection_ref": self.connection_ref,
                "service_" + self.identity_kind + "_sha256": self.identity_sha256,
            }
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CompositionDispatcherBinding:
        try:
            schema = value["schema"]
            if schema not in (SCHEMA, POLICY_SCHEMA) or set(value) != (_FIELDS if schema == SCHEMA else _POLICY_FIELDS):
                raise ValueError
            return cls(
                value["dispatcher_id"],
                value["connection_ref"],
                value.get("service_configuration_sha256"),
                service_policy_sha256=value.get("service_policy_sha256"),
            )
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
