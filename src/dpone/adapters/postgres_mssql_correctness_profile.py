"""Bounded environment catalog adapter for PostgreSQL to MSSQL R1."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from dpone.contracts import ETLConfigurationError
from dpone.contracts.postgres_mssql_correctness_profile import (
    ACTIVATION_BLOCKED,
    CERTIFICATION_UNVERIFIED,
    IMPLEMENTATION_ABSENT,
    PostgresMssqlCorrectnessProfile,
    SourceMode,
)

if TYPE_CHECKING:
    from dpone.contracts.postgres_mssql_correctness_profile import PostgresMssqlCorrectnessRouteRequest


CATALOG_SCHEMA = "dpone.postgres-mssql-correctness-profiles.v1"
_MAX_PROFILES = 1_000
_MAX_REGISTRATIONS = 10_000


class PostgresMssqlCorrectnessCatalogError(ETLConfigurationError):
    """A platform catalog is malformed, ambiguous, or internally inconsistent."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class _Registration:
    source_connection_ref: str
    sink_connection_ref: str
    source_mode: SourceMode
    target_schema: str
    target_table: str
    profile_id: str

    def matches(self, request: PostgresMssqlCorrectnessRouteRequest) -> bool:
        return (
            self.source_connection_ref == request.source_connection_ref
            and self.sink_connection_ref == request.sink_connection_ref
            and self.source_mode is request.source_mode
            and self.target_schema == request.target_schema
            and self.target_table == request.target_table
        )


class MappingPostgresMssqlCorrectnessCatalog:
    """Provide immutable profiles and exact platform route registrations."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        if payload.get("schema") != CATALOG_SCHEMA:
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
        raw_profiles = _mapping(payload.get("profiles"))
        raw_registrations = payload.get("registrations")
        if not 1 <= len(raw_profiles) <= _MAX_PROFILES:
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
        if not isinstance(raw_registrations, Sequence) or isinstance(raw_registrations, str | bytes):
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
        if len(raw_registrations) > _MAX_REGISTRATIONS:
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
        self._profiles = {
            _canonical_text(profile_id): _profile(_canonical_text(profile_id), raw)
            for profile_id, raw in raw_profiles.items()
        }
        self._registrations = tuple(_registration(raw) for raw in raw_registrations)
        if len(self._registrations) != len(set(self._registrations)):
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_REGISTRATION_AMBIGUOUS")
        for registration in self._registrations:
            if registration.profile_id not in self._profiles:
                raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")

    def load(self, profile_id: str) -> PostgresMssqlCorrectnessProfile:
        """Return one exact profile; profile-authored status claims are impossible."""

        try:
            return self._profiles[profile_id]
        except KeyError:
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_REQUIRED") from None

    def select_profile_id(self, request: PostgresMssqlCorrectnessRouteRequest) -> str | None:
        """Select only an exact explicit route registration."""

        matches = tuple(item.profile_id for item in self._registrations if item.matches(request))
        if not matches:
            return None
        if len(matches) != 1:
            raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_REGISTRATION_AMBIGUOUS")
        return matches[0]


class UnverifiedPostgresMssqlCorrectnessEvidence:
    """Bind installed code while preventing local catalog claims from promoting R1."""

    def __init__(self, *, implementation_available: bool) -> None:
        self._implementation_status = "implemented" if implementation_available else IMPLEMENTATION_ABSENT

    def bind_current_evidence(
        self,
        profile: PostgresMssqlCorrectnessProfile,
    ) -> PostgresMssqlCorrectnessProfile:
        """Return an activation-blocked snapshot until trusted vendor evidence exists."""

        return replace(
            profile,
            implementation_status=self._implementation_status,
            certification_status=CERTIFICATION_UNVERIFIED,
            activation_status=ACTIVATION_BLOCKED,
        )


def _profile(profile_id: str, value: object) -> PostgresMssqlCorrectnessProfile:
    raw = _mapping(value)
    try:
        return PostgresMssqlCorrectnessProfile(
            profile_id=profile_id,
            source_connection_ref=_canonical_text(raw["source_connection_ref"]),
            sink_connection_ref=_canonical_text(raw["sink_connection_ref"]),
            allowed_source_modes=tuple(
                SourceMode(_canonical_text(item)) for item in _sequence(raw["allowed_source_modes"])
            ),
            source_major=_integer(raw["source_major"]),
            target_major=_integer(raw["target_major"]),
            topology=_canonical_text(raw["topology"]),
            object_profile=_canonical_text(raw["object_profile"]),
            key_types=tuple(_canonical_text(item) for item in _sequence(raw["key_types"])),
            receipt_contract=_canonical_text(raw["receipt_contract"]),
            hash_policy=_canonical_text(raw["hash_policy"]),
            writer_fence=_canonical_text(raw["writer_fence"]),
            session_count=_integer(raw["session_count"]),
            transaction_scope=_canonical_text(raw["transaction_scope"]),
            delayed_durability_disabled=_boolean(raw["delayed_durability_disabled"]),
            max_descendant_proof_receipts=_integer(raw["max_descendant_proof_receipts"]),
            target_binding_uuid=_canonical_text(raw["target_binding_uuid"]),
            target_contract_revision=_integer(raw["target_contract_revision"]),
            quality_policy=_canonical_text(raw["quality_policy"]),
            certification_ref=_canonical_text(raw["certification_ref"]),
            implementation_status=IMPLEMENTATION_ABSENT,
            certification_status=CERTIFICATION_UNVERIFIED,
            activation_status=ACTIVATION_BLOCKED,
        )
    except (KeyError, TypeError, ValueError):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID") from None


def _registration(value: object) -> _Registration:
    raw = _mapping(value)
    try:
        return _Registration(
            source_connection_ref=_canonical_text(raw["source_connection_ref"]),
            sink_connection_ref=_canonical_text(raw["sink_connection_ref"]),
            source_mode=SourceMode(_canonical_text(raw["source_mode"])),
            target_schema=_canonical_text(raw["target_schema"]),
            target_table=_canonical_text(raw["target_table"]),
            profile_id=_canonical_text(raw["profile_id"]),
        )
    except (KeyError, TypeError, ValueError):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID") from None


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
    return value


def _sequence(value: object) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
    return value


def _canonical_text(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
    return value


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise PostgresMssqlCorrectnessCatalogError("DPONE_POSTGRES_MSSQL_PROFILE_CATALOG_INVALID")
    return value


__all__ = [
    "CATALOG_SCHEMA",
    "MappingPostgresMssqlCorrectnessCatalog",
    "PostgresMssqlCorrectnessCatalogError",
    "UnverifiedPostgresMssqlCorrectnessEvidence",
]
