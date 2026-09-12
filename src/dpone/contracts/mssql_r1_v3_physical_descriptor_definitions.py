"""Canonical definition bytes and digest binding for the physical descriptor."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_enum,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1DefinitionKindV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_schema_modules import module_definition_digest

_DOMAIN = b"dpone-r1-physical-definition-v1\0"
_TABLE_DOMAIN = b"dpone-r1-physical-table-ddl-v1\0"


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1DefinitionPayloadV1:
    definition_kind: MssqlR1DefinitionKindV1
    utf8_bytes: bytes
    definition_digest: bytes

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.definition_kind, MssqlR1DefinitionKindV1, "definition kind")
        if type(self.utf8_bytes) is not bytes or not self.utf8_bytes:
            raise MssqlR1V3ContractError("definition must be nonempty exact bytes")
        try:
            text = self.utf8_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlR1V3ContractError("definition must be valid UTF-8") from exc
        if (
            unicodedata.normalize("NFC", text) != text
            or "\r" in text
            or "\0" in text
            or not text.endswith("\n")
            or text.endswith("\n\n")
        ):
            raise MssqlR1V3ContractError("definition text is not canonical NFC/LF text")
        if self.definition_kind is MssqlR1DefinitionKindV1.TABLE_DDL:
            expected = hashlib.sha256(canonical_bytes(_TABLE_DOMAIN, (self.utf8_bytes,))).digest()
        else:
            expected = module_definition_digest(text)
        if type(self.definition_digest) is not bytes or self.definition_digest != expected:
            raise MssqlR1V3ContractError("definition digest differs from canonical definition bytes")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAIN, (self.definition_kind, self.utf8_bytes, self.definition_digest))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1DefinitionPayloadV1:
        kind, body, digest = decode_canonical_bytes(payload, _DOMAIN, field_count=3)
        return cls(expect_enum(MssqlR1DefinitionKindV1, kind, "definition kind"), body, digest)  # type: ignore[arg-type]

    @classmethod
    def create(cls, kind: MssqlR1DefinitionKindV1, text: str) -> MssqlR1DefinitionPayloadV1:
        _VALIDATE.require_exact_enum(kind, MssqlR1DefinitionKindV1, "definition kind")
        if type(text) is not str:
            raise MssqlR1V3ContractError("definition text must be text")
        body = text.encode("utf-8")
        digest = (
            hashlib.sha256(canonical_bytes(_TABLE_DOMAIN, (body,))).digest()
            if kind is MssqlR1DefinitionKindV1.TABLE_DDL
            else module_definition_digest(text)
        )
        return cls(kind, body, digest)


__all__ = ["MssqlR1DefinitionPayloadV1"]
