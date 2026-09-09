"""Immutable schema/physical DDL carried into a governed MSSQL transaction."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    MssqlTargetCatalogExpectation,
    aggregate_expectations,
)

MutationKind = Literal["schema_evolution", "physical_design"]
MutationStatementType = Literal["target_ddl", "session_policy"]


@dataclass(frozen=True, slots=True)
class MssqlTargetMutationAction:
    """One planner-authorized SQL Server DDL statement."""

    kind: MutationKind
    sql: str
    statement_type: MutationStatementType

    def __post_init__(self) -> None:
        statement = self.sql.strip()
        if not statement or "\x00" in statement:
            raise ValueError("mssql_transaction.target_mutation_sql_invalid")
        if self.statement_type not in {"target_ddl", "session_policy"}:
            raise ValueError("mssql_transaction.target_mutation_statement_type_invalid")


@dataclass(frozen=True, slots=True)
class MssqlTargetMutationPlan:
    """Exact target-bound DDL executed only under the operation fence."""

    target_identity: bytes
    target_database: str
    target_schema: str
    target_table: str
    actions: tuple[MssqlTargetMutationAction, ...] = ()
    expectations: tuple[MssqlTargetCatalogExpectation, ...] = ()
    version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.target_identity, bytes) or len(self.target_identity) != 32:
            raise ValueError("mssql_transaction.target_mutation_identity_invalid")
        if any(not str(value).strip() for value in (self.target_database, self.target_schema, self.target_table)):
            raise ValueError("mssql_transaction.target_mutation_coordinates_invalid")
        if self.version != 1:
            raise ValueError("mssql_transaction.target_mutation_version_unsupported")
        phases = [0 if action.kind == "schema_evolution" else 1 for action in self.actions]
        if phases != sorted(phases):
            raise ValueError("mssql_transaction.target_mutation_phase_order_invalid")
        kinds = [expectation.kind for expectation in self.expectations]
        if len(kinds) != len(set(kinds)):
            raise ValueError("mssql_transaction.target_catalog_expectation_duplicate")

    @classmethod
    def from_admission(cls, admission: MssqlTransactionAdmission) -> MssqlTargetMutationPlan:
        operation = admission.operation
        if operation is None:
            raise ValueError("mssql_transaction.target_mutation_operation_required")
        request = operation.attempt.request
        return cls(
            target_identity=operation.attempt.target_identity,
            target_database=request.target_database,
            target_schema=request.target_schema,
            target_table=request.target_table,
        )

    def append(
        self,
        kind: MutationKind,
        statements: tuple[str, ...] | list[str],
        *,
        expectation: MssqlTargetCatalogExpectation | None = None,
    ) -> MssqlTargetMutationPlan:
        if kind == "schema_evolution" and any(action.kind == "physical_design" for action in self.actions):
            raise ValueError("mssql_transaction.target_mutation_phase_order_invalid")
        if statements and expectation is None:
            raise ValueError("mssql_transaction.target_mutation_expectation_required")
        expected_kind = "schema_columns" if kind == "schema_evolution" else "physical_design"
        if expectation is not None and expectation.kind != expected_kind:
            raise ValueError("mssql_transaction.target_mutation_expectation_kind_invalid")
        if statements and expectation is not None and expectation.representation == "legacy_columns_v1":
            raise ValueError("mssql_transaction.exact_target_catalog_expectation_required")
        additions = tuple(
            MssqlTargetMutationAction(
                kind,
                str(statement).strip().removesuffix(";").rstrip(),
                _statement_type(self, kind, statement),
            )
            for statement in statements
        )
        expectations = self.expectations
        if expectation is not None:
            existing = next((item for item in expectations if item.kind == expectation.kind), None)
            if existing is not None and existing != expectation:
                raise ValueError("mssql_transaction.target_catalog_expectation_rebound")
            if existing is None:
                expectations = (*expectations, expectation)
        return MssqlTargetMutationPlan(
            target_identity=self.target_identity,
            target_database=self.target_database,
            target_schema=self.target_schema,
            target_table=self.target_table,
            actions=(*self.actions, *additions),
            expectations=expectations,
        )

    @property
    def digest(self) -> bytes:
        payload: dict[str, Any] = {
            "actions": [
                {
                    "kind": action.kind,
                    "sql": action.sql,
                    "statement_type": action.statement_type,
                }
                for action in self.actions
            ],
            "expectations": [
                {
                    "kind": expectation.kind,
                    "representation": expectation.representation,
                    "before_sha256": expectation.before_sha256.hex(),
                    "after_sha256": expectation.after_sha256.hex(),
                }
                for expectation in self.expectations
            ],
            "target_database": self.target_database,
            "target_identity": self.target_identity.hex(),
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "version": self.version,
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(encoded).digest()

    @property
    def expected_before_sha256(self) -> bytes:
        return aggregate_expectations(self.expectations)

    @property
    def expected_after_sha256(self) -> bytes:
        return aggregate_expectations(self.expectations, boundary="after")


def append_target_mutations(
    current: MssqlTargetMutationPlan | None,
    admission: MssqlTransactionAdmission,
    *,
    kind: MutationKind,
    statements: tuple[str, ...] | list[str],
    expectation: MssqlTargetCatalogExpectation | None = None,
) -> MssqlTargetMutationPlan:
    """Append to one exact target authority without permitting rebinding."""

    plan = current or MssqlTargetMutationPlan.from_admission(admission)
    expected = MssqlTargetMutationPlan.from_admission(admission)
    if (
        plan.target_identity != expected.target_identity
        or plan.target_database != expected.target_database
        or plan.target_schema != expected.target_schema
        or plan.target_table != expected.target_table
    ):
        raise RuntimeError("mssql_transaction.target_mutation_plan_rebound")
    return plan.append(kind, statements, expectation=expectation)


def _statement_type(
    plan: MssqlTargetMutationPlan,
    kind: MutationKind,
    raw: object,
) -> MutationStatementType:
    statement = str(raw).strip()
    if not statement or "\x00" in statement:
        raise ValueError("mssql_transaction.target_mutation_sql_invalid")
    if any(token in statement for token in ("--", "/*", "*/")):
        raise ValueError("mssql_transaction.target_mutation_comment_forbidden")
    body = statement[:-1].rstrip() if statement.endswith(";") else statement
    if ";" in body:
        raise ValueError("mssql_transaction.target_mutation_multiple_statements")
    statement_starts = re.findall(
        r"(?im)(?:\A|[\r\n])\s*(?:CREATE|ALTER|DROP|SET|USE|EXEC(?:UTE)?|BEGIN|COMMIT|ROLLBACK|"
        r"INSERT|UPDATE|DELETE|MERGE|SELECT)\b",
        body,
    )
    if len(statement_starts) != 1:
        raise ValueError("mssql_transaction.target_mutation_multiple_statements")
    if re.fullmatch(r"SET\s+LOCK_TIMEOUT\s+(?:0|[1-9]\d{0,6})", body, re.IGNORECASE):
        return "session_policy"
    target = MSSQLObjectName.from_parts(
        database=plan.target_database,
        schema=plan.target_schema,
        table=plan.target_table,
        strict=True,
    ).quoted()
    quoted_target = re.escape(target)
    if _allowed_create_table(body, quoted_target):
        return "target_ddl"
    alter_table = rf"ALTER\s+TABLE\s+{quoted_target}(?=\s|\()"
    alter_match = re.match(alter_table, body, re.IGNORECASE)
    if alter_match is not None and _allowed_alter_table_suffix(body[alter_match.end() :]):
        return "target_ddl"
    if _allowed_create_index(body, quoted_target):
        return "target_ddl"
    raise ValueError(f"mssql_transaction.target_mutation_statement_unbound:{kind}")


def _allowed_alter_table_suffix(value: str) -> bool:
    suffix = value.strip()
    identifier = r"\[(?:[^\]]|\]\])+\]"
    target_type = r"[A-Za-z][A-Za-z0-9_]*(?:\((?:max|\d+)(?:,\d+)?\))?"
    collation = r"(?:\s+COLLATE\s+[A-Za-z][A-Za-z0-9_]{0,127})?"
    return any(
        re.match(pattern, suffix, re.IGNORECASE | re.DOTALL)
        for pattern in (
            rf"ADD\s+{identifier}\s+{target_type}{collation}\s+(?:NULL|NOT\s+NULL)\s*$",
            rf"ALTER\s+COLUMN\s+{identifier}\s+{target_type}{collation}\s+(?:NULL|NOT\s+NULL)\s*$",
            rf"ADD\s+CONSTRAINT\s+{identifier}\s+PRIMARY\s+KEY\s+CLUSTERED\s+"
            rf"\({identifier}(?:\s*,\s*{identifier})*\)\s+WITH\s+"
            rf"\(DATA_COMPRESSION\s*=\s*(?:NONE|ROW|PAGE)"
            rf"(?:\s*,\s*FILLFACTOR\s*=\s*(?:[1-9]|[1-9]\d|100))?\)"
            rf"(?:\s+ON\s+{identifier})?\s*$",
            r"REBUILD\s+WITH\s+\(DATA_COMPRESSION\s*=\s*(?:NONE|ROW|PAGE)\)\s*$",
        )
    )


def _allowed_create_table(statement: str, quoted_target: str) -> bool:
    identifier = r"\[(?:[^\]]|\]\])+\]"
    column = rf"{identifier}\s+{_target_type_pattern()}(?:\s+COLLATE\s+{_collation_pattern()})?\s+(?:NULL|NOT\s+NULL)"
    placement = rf"(?:\s+ON\s+{identifier}(?:\s+TEXTIMAGE_ON\s+{identifier})?)?"
    compression = r"(?:\s+WITH\s+\(DATA_COMPRESSION\s*=\s*(?:NONE|ROW|PAGE)\))?"
    pattern = rf"CREATE\s+TABLE\s+{quoted_target}\s*\(\s*{column}(?:\s*,\s*{column})*\s*\){placement}{compression}\s*"
    return re.fullmatch(pattern, statement, re.IGNORECASE | re.DOTALL) is not None


def _allowed_create_index(statement: str, quoted_target: str) -> bool:
    identifier = r"\[(?:[^\]]|\]\])+\]"
    columns = rf"{identifier}(?:\s*,\s*{identifier})*"
    rowstore = (
        rf"CREATE\s+(?:UNIQUE\s+)?(?:(?:CLUSTERED|NONCLUSTERED)\s+)?INDEX\s+{identifier}\s+"
        rf"ON\s+{quoted_target}\s*\(\s*{columns}\s*\)"
        rf"(?:\s+INCLUDE\s*\(\s*{columns}\s*\))?"
        rf"(?:\s+WHERE\s+{identifier}\s*=\s*[01])?"
        rf"(?:\s+WITH\s+\(DATA_COMPRESSION\s*=\s*(?:NONE|ROW|PAGE)"
        rf"(?:\s*,\s*FILLFACTOR\s*=\s*(?:[1-9]|[1-9]\d|100))?\))?"
        rf"(?:\s+ON\s+{identifier})?\s*"
    )
    columnstore = (
        rf"CREATE\s+CLUSTERED\s+COLUMNSTORE\s+INDEX\s+{identifier}\s+ON\s+{quoted_target}"
        rf"(?:\s+ON\s+{identifier})?\s*"
    )
    return any(
        re.fullmatch(pattern, statement, re.IGNORECASE | re.DOTALL) is not None for pattern in (rowstore, columnstore)
    )


def _target_type_pattern() -> str:
    return r"[A-Za-z][A-Za-z0-9_]*(?:\(\s*(?:max|\d+)(?:\s*,\s*\d+)?\s*\))?"


def _collation_pattern() -> str:
    return r"[A-Za-z][A-Za-z0-9_]{0,127}"


__all__ = [
    "MssqlTargetMutationAction",
    "MssqlTargetMutationPlan",
    "append_target_mutations",
]
