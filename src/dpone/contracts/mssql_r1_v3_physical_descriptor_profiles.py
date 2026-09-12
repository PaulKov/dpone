"""Exact engine and session profiles for the R1 physical descriptor."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1

_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalEngineProfileV1:
    engine_major: int
    compatibility_level: int
    contract_collation: str
    containment: str
    delayed_durability: str
    mars_enabled: bool
    pooling_enabled: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_int(self.engine_major, "engine major", minimum=16, maximum=16)
        _VALIDATE.require_int(self.compatibility_level, "compatibility level", minimum=160, maximum=160)
        for name, value in (
            ("contract collation", self.contract_collation),
            ("containment", self.containment),
            ("delayed durability", self.delayed_durability),
        ):
            _VALIDATE.require_text(value, name)
        if (
            (
                self.engine_major,
                self.compatibility_level,
                self.contract_collation,
                self.containment,
                self.delayed_durability,
            )
            != (16, 160, "Latin1_General_100_BIN2", "PARTIAL", "DISABLED")
            or self.mars_enabled is not False
            or self.pooling_enabled is not False
        ):
            raise MssqlR1V3ContractError("physical engine profile differs from the exact R1 profile")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-physical-engine-profile-v1\0",
            (
                self.engine_major,
                self.compatibility_level,
                self.contract_collation,
                self.containment,
                self.delayed_durability,
                self.mars_enabled,
                self.pooling_enabled,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalEngineProfileV1:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-physical-engine-profile-v1\0", field_count=7))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalSessionProfileV1:
    isolation_level: str
    ansi_nulls: bool
    ansi_padding: bool
    ansi_warnings: bool
    arithabort: bool
    concat_null_yields_null: bool
    quoted_identifier: bool
    numeric_roundabort: bool
    xact_abort: bool
    nocount: bool
    autocommit_enabled: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_text(self.isolation_level, "isolation level")
        values = (
            self.isolation_level,
            self.ansi_nulls,
            self.ansi_padding,
            self.ansi_warnings,
            self.arithabort,
            self.concat_null_yields_null,
            self.quoted_identifier,
            self.numeric_roundabort,
            self.xact_abort,
            self.nocount,
            self.autocommit_enabled,
        )
        for name, value in zip(
            (
                "ansi_nulls",
                "ansi_padding",
                "ansi_warnings",
                "arithabort",
                "concat_null_yields_null",
                "quoted_identifier",
                "numeric_roundabort",
                "xact_abort",
                "nocount",
                "autocommit_enabled",
            ),
            values[1:],
            strict=True,
        ):
            _VALIDATE.require_bool(value, name)
        if values != ("SERIALIZABLE", True, True, True, True, True, True, False, True, True, True):
            raise MssqlR1V3ContractError("physical session profile differs from the exact R1 profile")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-physical-session-profile-v1\0",
            (
                self.isolation_level,
                self.ansi_nulls,
                self.ansi_padding,
                self.ansi_warnings,
                self.arithabort,
                self.concat_null_yields_null,
                self.quoted_identifier,
                self.numeric_roundabort,
                self.xact_abort,
                self.nocount,
                self.autocommit_enabled,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalSessionProfileV1:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-physical-session-profile-v1\0", field_count=11))  # type: ignore[arg-type]


__all__ = ("MssqlR1PhysicalEngineProfileV1", "MssqlR1PhysicalSessionProfileV1")
