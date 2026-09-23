"""Exact deployment authority for one SQLClient preparation baseline."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, NoReturn

from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

ERROR = "mssql_native.sqlclient_preparation_qualification_invalid"
SCHEMA = "dpone.sqlclient.preparation-qualification.v1"
KEYS = frozenset(
    {
        "schema",
        "status",
        "baseline_sha256",
        "baseline_byte_count",
        "profile",
        "query_profile",
        "sql_image_digest",
        "server_build_sha256",
        "database_profile_sha256",
        "producer_source_sha256",
        "control_source_sha256",
        "first_raw_sha256",
        "repeat_raw_sha256",
        "reviewed_projection_sha256",
    }
)


def _fail() -> NoReturn:
    raise ValueError(ERROR) from None


def _text(value: object, maximum: int) -> str:
    if type(value) is not str or not value or len(value) > maximum:
        _fail()
    return value


def decode_qualification(payload: bytes) -> dict[str, Any]:
    """Decode one bounded canonical qualification receipt."""
    if type(payload) is not bytes or not 0 < len(payload) <= 16_384:
        _fail()
    value = strict_json_object(payload)
    if set(value) != KEYS or canonical_json_bytes(value) != payload:
        _fail()
    if value["schema"] != SCHEMA or value["status"] != "PASS":
        _fail()
    for name in (
        "baseline_sha256",
        "server_build_sha256",
        "database_profile_sha256",
        "producer_source_sha256",
        "control_source_sha256",
        "first_raw_sha256",
        "repeat_raw_sha256",
        "reviewed_projection_sha256",
    ):
        _hash(value[name])
    if value["first_raw_sha256"] == value["repeat_raw_sha256"]:
        _fail()
    _integer(value["baseline_byte_count"], 1, 1024 * 1024)
    _text(value["profile"], 128)
    _text(value["query_profile"], 128)
    image = _text(value["sql_image_digest"], 80)
    if not image.startswith("sha256:"):
        _fail()
    _hash(image[7:])
    return value


@dataclass(frozen=True, slots=True)
class PreparationBaselineAuthority:
    """Deployment-owned exact receipt and producer identity pins."""

    receipt_sha256: str

    def __post_init__(self) -> None:
        _hash(self.receipt_sha256)


_TOKEN = object()


class AdmittedPreparationBaseline(tuple):
    """Opaque exact bytes admitted by a deployment-owned authority."""

    __slots__ = ()

    def __new__(
        cls,
        baseline: bytes,
        qualification: bytes,
        authority: PreparationBaselineAuthority,
        *,
        _token: object,
    ) -> AdmittedPreparationBaseline:
        if _token is not _TOKEN:
            _fail()
        return tuple.__new__(cls, (baseline, qualification, authority))

    @classmethod
    def _admit(
        cls,
        baseline: bytes,
        qualification: bytes,
        authority: PreparationBaselineAuthority,
    ) -> AdmittedPreparationBaseline:
        return cls(baseline, qualification, authority, _token=_TOKEN)

    @property
    def baseline_bytes(self) -> bytes:
        return self[0]

    @property
    def qualification_bytes(self) -> bytes:
        return self[1]

    @property
    def baseline_sha256(self) -> str:
        return sha256(self.baseline_bytes).hexdigest()

    @property
    def qualification_sha256(self) -> str:
        return sha256(self.qualification_bytes).hexdigest()

    def assert_authority(self, authority: PreparationBaselineAuthority) -> None:
        if type(authority) is not PreparationBaselineAuthority or authority != self[2]:
            _fail()
        authority.__post_init__()
        receipt = decode_qualification(self.qualification_bytes)
        if (
            self.qualification_sha256 != authority.receipt_sha256
            or receipt["baseline_sha256"] != self.baseline_sha256
            or receipt["baseline_byte_count"] != len(self.baseline_bytes)
        ):
            _fail()

    def assert_admitted(self, authority: PreparationBaselineAuthority) -> None:
        """Revalidate only against deployment-supplied authority."""
        self.assert_authority(authority)

    def evidence_identity(self) -> dict[str, str]:
        return {
            "baseline_sha256": self.baseline_sha256,
            "qualification_sha256": self.qualification_sha256,
            "authority_sha256": sha256(
                canonical_json_bytes(
                    {
                        "receipt_sha256": self[2].receipt_sha256,
                    }
                )
            ).hexdigest(),
        }


__all__ = (
    "AdmittedPreparationBaseline",
    "PreparationBaselineAuthority",
    "decode_qualification",
)
