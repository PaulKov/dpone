"""Pure identifier, contract-membership and nullability policy for dbt merge keys."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_publish_schema_contract_common import IDENTIFIER

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtModelArtifact

DBT_UNIQUE_KEY_MISSING = "DPONE_DBT_UNIQUE_KEY_MISSING"
DBT_UNIQUE_KEY_INVALID = "DPONE_DBT_UNIQUE_KEY_INVALID"
DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED = "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED"
DBT_UNIQUE_KEY_NOT_IN_CONTRACT = "DPONE_DBT_UNIQUE_KEY_NOT_IN_CONTRACT"
DBT_UNIQUE_KEY_NULLABLE = "DPONE_DBT_UNIQUE_KEY_NULLABLE"

_IDENTIFIER_PATTERN = re.compile(str(IDENTIFIER["pattern"]))
_raw_identifier_max_length = IDENTIFIER["maxLength"]
if not isinstance(_raw_identifier_max_length, int):
    raise TypeError("dbt publish identifier maxLength must be an integer")
_IDENTIFIER_MAX_LENGTH = _raw_identifier_max_length


@dataclass(frozen=True, slots=True)
class DbtUniqueKeyPolicyIssue:
    """One content-free merge-key violation."""

    code: str
    expectation: str


@dataclass(frozen=True, slots=True)
class DbtUniqueKeyPolicyReport:
    """Normalized ordered keys and their first fail-closed violation."""

    keys: tuple[str, ...]
    issues: tuple[DbtUniqueKeyPolicyIssue, ...]

    @property
    def passed(self) -> bool:
        """Return whether the key is safe for cross-engine merge semantics."""

        return not self.issues


def evaluate_dbt_unique_key(
    value: object,
    *,
    contract_enforced: bool,
    not_null_by_column: Mapping[str, bool],
) -> DbtUniqueKeyPolicyReport:
    """Validate one raw key without SQL parsing, coercion or case normalization."""

    keys, shape_issue = _ordered_keys(value)
    if shape_issue is not None:
        return DbtUniqueKeyPolicyReport(keys, (shape_issue,))
    if any(not _is_identifier(key) for key in keys):
        return _failed(
            keys,
            DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED,
            "contain only ASCII identifiers, not SQL expressions",
        )
    folded = tuple(key.casefold() for key in keys)
    if len(folded) != len(set(folded)):
        return _failed(
            keys,
            DBT_UNIQUE_KEY_INVALID,
            "contain distinct identifiers without exact or case-fold duplicates",
        )
    if not contract_enforced or any(key not in not_null_by_column for key in keys):
        return _failed(
            keys,
            DBT_UNIQUE_KEY_NOT_IN_CONTRACT,
            "match exact columns in an enforced model contract",
        )
    if any(not not_null_by_column[key] for key in keys):
        return _failed(
            keys,
            DBT_UNIQUE_KEY_NULLABLE,
            "reference only columns with an admitted not_null constraint",
        )
    return DbtUniqueKeyPolicyReport(keys, ())


def evaluate_manifest_dbt_unique_key(
    value: object,
    *,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> DbtUniqueKeyPolicyReport:
    """Evaluate a raw manifest key against its enforced column contract."""

    contract = _mapping(config.get("contract"))
    if not contract:
        contract = _mapping(node.get("contract"))
    return evaluate_dbt_unique_key(
        value,
        contract_enforced=contract.get("enforced") is True,
        not_null_by_column=_manifest_not_null_columns(node.get("columns")),
    )


def evaluate_model_dbt_unique_key(
    value: object,
    *,
    model: DbtModelArtifact,
) -> DbtUniqueKeyPolicyReport:
    """Evaluate a planner key against the immutable manifest projection."""

    return evaluate_dbt_unique_key(
        value,
        contract_enforced=model.contract_enforced,
        not_null_by_column={
            column.name: (not column.nullable and "not_null" in column.constraints) for column in model.column_contracts
        },
    )


def _ordered_keys(
    value: object,
) -> tuple[tuple[str, ...], DbtUniqueKeyPolicyIssue | None]:
    if value is None:
        return (), _issue(
            DBT_UNIQUE_KEY_MISSING,
            "be one identifier or a non-empty ordered identifier array",
        )
    if isinstance(value, str):
        if not value:
            return (), _issue(
                DBT_UNIQUE_KEY_MISSING,
                "be one non-empty identifier",
            )
        return (value,), None
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        return (), _issue(
            DBT_UNIQUE_KEY_INVALID,
            "be one identifier or an ordered identifier array",
        )
    items = tuple(value)
    if not items:
        return (), _issue(
            DBT_UNIQUE_KEY_MISSING,
            "be a non-empty ordered identifier array",
        )
    if any(not isinstance(item, str) or not item for item in items):
        return (), _issue(
            DBT_UNIQUE_KEY_INVALID,
            "contain only non-empty string identifiers",
        )
    return items, None


def _is_identifier(value: str) -> bool:
    return len(value) <= _IDENTIFIER_MAX_LENGTH and _IDENTIFIER_PATTERN.fullmatch(value) is not None


def _manifest_not_null_columns(value: object) -> dict[str, bool]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, bool] = {}
    for name, raw_column in value.items():
        if not isinstance(name, str) or not isinstance(raw_column, Mapping):
            continue
        constraints = raw_column.get("constraints")
        result[name] = (
            isinstance(constraints, Sequence)
            and not isinstance(constraints, str | bytes)
            and any(isinstance(item, Mapping) and item.get("type") == "not_null" for item in constraints)
        )
    return result


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _failed(
    keys: tuple[str, ...],
    code: str,
    expectation: str,
) -> DbtUniqueKeyPolicyReport:
    return DbtUniqueKeyPolicyReport(keys, (_issue(code, expectation),))


def _issue(code: str, expectation: str) -> DbtUniqueKeyPolicyIssue:
    return DbtUniqueKeyPolicyIssue(code=code, expectation=expectation)


__all__ = [
    "DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED",
    "DBT_UNIQUE_KEY_INVALID",
    "DBT_UNIQUE_KEY_MISSING",
    "DBT_UNIQUE_KEY_NOT_IN_CONTRACT",
    "DBT_UNIQUE_KEY_NULLABLE",
    "DbtUniqueKeyPolicyIssue",
    "DbtUniqueKeyPolicyReport",
    "evaluate_dbt_unique_key",
    "evaluate_manifest_dbt_unique_key",
    "evaluate_model_dbt_unique_key",
]
