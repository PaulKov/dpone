"""Canonical preparation evidence and closed deployment-baseline parsing.

Parsing is never admission. Real baseline construction remains closed until
independent exact-build and query qualification is installed by composition.
"""

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, NoReturn

from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

PROFILE = "standalone-sql-login-preparation-v1"
QUERY_PROFILE = "sqlclient-preparation-catalog-v1"
ERROR = "mssql_native.sqlclient_preparation_invalid"
MAX_PREPARATION_BYTES = 8 * 1024 * 1024
BASELINE_KEYS = frozenset(
    (
        "schema",
        "profile",
        "query_profile",
        "server_build",
        "database_profile",
        "provenance",
        "stock_server_permissions",
        "stock_database_permissions",
        "effective_server_permissions",
        "effective_database_permissions",
        "token_rules",
    )
)
PREPARATION_KEYS = frozenset(
    (
        "schema",
        "attempt",
        "ownership",
        "parent_before",
        "directory_before",
        "observe_identity",
        "observe_registration",
        "selected_create",
        "selected_departure",
        "parent_input",
        "input_stat",
        "policy",
        "build",
        "baseline",
        "baseline_qualification",
        "inventory_open",
        "inventory_close",
        "profile_open",
        "profile_close",
        "stage_before",
        "stage_after",
        "empty",
        "operation_deadline_ns",
    )
)


def _document(payload: bytes, keys: frozenset[str], maximum: int) -> dict[str, Any]:
    if type(payload) is not bytes or not 0 < len(payload) <= maximum:
        raise ValueError(ERROR)
    value = strict_json_object(payload)
    if set(value) != keys or canonical_json_bytes(value) != payload:
        raise ValueError(ERROR)
    return value


def parse_baseline(payload: bytes) -> dict[str, Any]:
    """Parse bounded bytes; this function cannot authorize a live deployment."""
    value = _document(payload, BASELINE_KEYS, 1024 * 1024)
    if (
        value["schema"] != "dpone.sqlclient.preparation-baseline.v1"
        or value["profile"] != PROFILE
        or value["query_profile"] != QUERY_PROFILE
    ):
        raise ValueError(ERROR)
    for key in ("server_build", "database_profile", "provenance", "token_rules"):
        if type(value[key]) is not dict:
            raise ValueError(ERROR)
    expected = {
        "server_build": {"product_version", "edition", "engine_edition", "platform", "image_digest"},
        "database_profile": {"compatibility_level", "collation", "containment", "trustworthy", "database_chaining"},
        "provenance": {
            "provisioning_script_sha256",
            "observation_script_sha256",
            "canonicalizer_sha256",
            "first_raw_evidence_sha256",
            "repeat_raw_evidence_sha256",
            "reviewed_projection_sha256",
            "review_receipt_sha256",
        },
        "token_rules": {"login", "user"},
    }
    for key, fields in expected.items():
        if set(value[key]) != fields:
            raise ValueError(ERROR)
    build, database = value["server_build"], value["database_profile"]
    for name, maximum in (("product_version", 64), ("edition", 256), ("platform", 128)):
        _text(build[name], maximum)
    _integer(build["engine_edition"], 1, 100)
    if type(build["image_digest"]) is not str or not build["image_digest"].startswith("sha256:"):
        raise ValueError(ERROR)
    _hash(build["image_digest"][7:])
    _integer(database["compatibility_level"], 1, 200)
    _text(database["collation"], 128)
    for name in ("containment", "trustworthy", "database_chaining"):
        _integer(database[name], 0, 0)
    for name in ("login", "user"):
        rules = value["token_rules"][name]
        if type(rules) is not list or not 1 <= len(rules) <= 2:
            raise ValueError(ERROR)
        subjects = set()
        for rule in rules:
            if type(rule) is not dict or set(rule) != {"subject", "type", "usage"}:
                raise ValueError(ERROR)
            if (
                type(rule["subject"]) is not str
                or rule["subject"] not in ("writer", "public")
                or rule["subject"] in subjects
            ):
                raise ValueError(ERROR)
            subjects.add(rule["subject"])
            _text(rule["type"], 128)
            _text(rule["usage"], 128)
    for digest in value["provenance"].values():
        _hash(digest)
    for key in (
        "stock_server_permissions",
        "stock_database_permissions",
        "effective_server_permissions",
        "effective_database_permissions",
    ):
        if type(value[key]) is not list or len(value[key]) > 4096:
            raise ValueError(ERROR)
        if key.startswith("effective_"):
            if len(value[key]) > 128:
                raise ValueError(ERROR)
            for row in value[key]:
                if type(row) is not list or len(row) != 3:
                    raise ValueError(ERROR)
                for item in row[:2]:
                    if item is not None and (type(item) is not str or len(item) > 128):
                        raise ValueError(ERROR)
                _text(row[2], 128)
        else:
            for row in value[key]:
                _stock_row(row)
    return value


def _stock_row(row: Any) -> None:
    if type(row) is not dict or set(row) != {
        "class_id",
        "securable",
        "grantee",
        "grantor",
        "type",
        "permission",
        "state",
    }:
        raise ValueError(ERROR)
    _integer(row["class_id"], 0, 255)
    if (
        row["class_id"] not in (0, 1, 3, 4, 100, 105)
        or row["grantee"] not in ("writer", "public")
        or row["state"] != "G"
    ):
        raise ValueError(ERROR)
    _text(row["type"], 4)
    _text(row["permission"], 128)
    securable = row["securable"]
    if type(securable) is not list or len(securable) not in (5, 7):
        raise ValueError(ERROR)
    _text(securable[0], 128)
    if row["class_id"] == 105:
        if len(securable) != 7 or securable[0] != "ENDPOINT":
            raise ValueError(ERROR)
        for item in securable[1:5]:
            _text(item, 128)
        _integer(securable[5], 0, 1)
        _integer(securable[6], 1, 1)
    else:
        if len(securable) != 5:
            raise ValueError(ERROR)
        for item in securable[1:4]:
            if item is not None:
                _text(item, 128)
        if securable[4] is not None:
            _integer(securable[4], 0, 1)
    grantor = row["grantor"]
    if type(grantor) is not dict or set(grantor) != {"name", "type", "detail", "sid_binding", "sid"}:
        raise ValueError(ERROR)
    _text(grantor["name"], 128)
    _text(grantor["type"], 128)
    if type(grantor["detail"]) is int:
        _integer(grantor["detail"], 0, 1)
    else:
        _text(grantor["detail"], 128)
    if grantor["sid_binding"] == "fixed":
        sid = grantor["sid"]
        if type(sid) is not str or not 1 <= len(bytes.fromhex(sid)) <= 85 or bytes.fromhex(sid).hex() != sid:
            raise ValueError(ERROR)
    elif grantor["sid_binding"] not in ("writer", "database_owner") or grantor["sid"] is not None:
        raise ValueError(ERROR)


def admit_preparation_baseline(value: object) -> NoReturn:
    """No caller record/hash/fixture can substitute for absent control qualification.

    A later explicitly reviewed deployment producer must replace this closed
    boundary. Synthetic tests may monkeypatch trusted composition only.
    """
    raise ValueError("mssql_native.sqlclient_preparation_qualification_required")


def preparation_bytes(value: dict[str, Any]) -> bytes:
    """Canonical artifact envelope; trusted join validates original nested proofs."""
    payload = canonical_json_bytes(value)
    data = _document(payload, PREPARATION_KEYS, MAX_PREPARATION_BYTES)
    if data["schema"] != "dpone.sqlclient.preparation.v1":
        raise ValueError(ERROR)
    _integer(data["empty"], 1, 1)
    _integer(data["operation_deadline_ns"], 1, 2**63 - 1)
    if any(type(data[key]) is not dict for key in PREPARATION_KEYS - {"schema", "empty", "operation_deadline_ns"}):
        raise ValueError(ERROR)
    return payload


@dataclass(frozen=True)
class PreparationReceipt:
    """An exact artifact ACK description, never standalone Prepared authority."""

    attempt_sha256: str
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        _hash(self.attempt_sha256)
        _hash(self.payload_sha256)
        _integer(self.byte_count, 1, MAX_PREPARATION_BYTES)
        if type(self.relative_name) is not str or self.relative_name != (
            f"tds-sqlclient-preparation-{self.attempt_sha256}-{self.payload_sha256}.json"
        ):
            raise ValueError(ERROR)

    @classmethod
    def for_payload(cls, attempt: str, payload: bytes) -> "PreparationReceipt":
        if type(payload) is not bytes:
            raise ValueError(ERROR)
        digest = sha256(payload).hexdigest()
        return cls(attempt, f"tds-sqlclient-preparation-{attempt}-{digest}.json", digest, len(payload))


@dataclass(frozen=True)
class PreparationObservation:
    attempt_sha256: str
    receipt: PreparationReceipt | None = None

    def __post_init__(self) -> None:
        _hash(self.attempt_sha256)
        if self.receipt is not None:
            if type(self.receipt) is not PreparationReceipt:
                raise ValueError(ERROR)
            self.receipt.__post_init__()
            if self.receipt.attempt_sha256 != self.attempt_sha256:
                raise ValueError(ERROR)
