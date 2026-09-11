"""Closed physical layout shared by external DDL and exact catalog inspection.

This is storage metadata, not a decoder, migration, enrollment or authority.
Execution originals retain their legacy codecs; SQL never hashes their bytes.
The eight tables belong to one protected database and transaction lock.
"""

from __future__ import annotations

import re

from dpone.adapters.composition_mssql_catalog_types import (
    COMPOSITION_COLLATION as COMPOSITION_COLLATION,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionCheck as CompositionCheck,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionColumn as CompositionColumn,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionForeignKey as CompositionForeignKey,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionKey as CompositionKey,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionTable as CompositionTable,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionTrigger as CompositionTrigger,
)

COMPOSITION_MSSQL_SCHEMA_VERSION = 2
COMPOSITION_MSSQL_LEDGER_LOCK = "dpone:composition-control:v1"
LEGACY_COMPOSITION_OBJECTS = ("activations", "activation_domains", "attempts", "attempt_domains")


def require_control_schema(value: str) -> str:
    """Validate the existing bounded identifier contract before interpolation."""
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None:
        raise ValueError("control_schema must be a simple SQL identifier of at most 128 characters")
    return value


def _digest(name: str, *, nullable: bool = False) -> CompositionColumn:
    return CompositionColumn(name, "varchar", 71, nullable)


def _uuid(name: str) -> CompositionColumn:
    return CompositionColumn(name, "uniqueidentifier", 16)


def _document(name: str) -> CompositionColumn:
    return CompositionColumn(name, "varbinary", -1)


def _key(name: str, *columns: str, primary: bool = False) -> CompositionKey:
    return CompositionKey(name, columns, primary)


def _closed(column: str, *values: str) -> str:
    # Exact length rejects SQL's otherwise equal right-padded discriminators.
    return (
        "(" + " OR ".join(f"([{column}]='{value}' AND DATALENGTH([{column}])={len(value)})" for value in values) + ")"
    )


def _bounded(column: str, maximum: int) -> str:
    return f"(DATALENGTH([{column}])>=1 AND DATALENGTH([{column}])<={maximum})"


def _family_documents(family: str, document: str) -> str:
    return (
        f"(({_closed(family, 'execution')} AND {_bounded(document, 8388608)}) OR "
        f"({_closed(family, 'qualification')} AND {_bounded(document, 1048576)}))"
    )


def _table(
    name: str,
    columns: tuple[CompositionColumn, ...],
    keys: tuple[CompositionKey, ...],
    foreign_keys: tuple[CompositionForeignKey, ...] = (),
    checks: tuple[CompositionCheck, ...] = (),
) -> CompositionTable:
    digests = tuple(
        CompositionCheck(f"ck_c_{name}_{column.name}", f"DATALENGTH([{column.name}])=71")
        for column in columns
        if column.sql_type == "varchar" and column.length == 71
    )
    return CompositionTable(name, columns, keys, foreign_keys, (*checks, *digests))


COMPOSITION_TABLES = (
    _table(
        "authority",
        (
            CompositionColumn("singleton", "tinyint", 1),
            CompositionColumn("schema_version", "int", 4),
            _uuid("service_id"),
        ),
        (_key("pk_c_authority", "singleton", primary=True),),
        checks=(
            CompositionCheck("ck_c_authority_singleton", "[singleton]=1"),
            CompositionCheck("ck_c_authority_version", f"[schema_version]={COMPOSITION_MSSQL_SCHEMA_VERSION}"),
        ),
    ),
    _table(
        "owners",
        (
            _digest("owner_key"),
            CompositionColumn("owner_kind", "varchar", 13),
            _uuid("owner_id"),
            _digest("subject_sha256"),
            _document("subject_document"),
            CompositionColumn("state", "varchar", 16),
        ),
        (
            _key("pk_c_owners", "owner_key", primary=True),
            _key("uq_c_owner_id", "owner_kind", "owner_id"),
            _key("uq_c_owner_subject", "owner_kind", "subject_sha256"),
            _key("uq_c_owner_family", "owner_key", "owner_kind", "subject_sha256"),
        ),
        checks=(
            CompositionCheck("ck_c_owner_document", _family_documents("owner_kind", "subject_document")),
            CompositionCheck(
                "ck_c_owner_state",
                (
                    f"(({_closed('owner_kind', 'execution')} AND {_closed('state', 'PREPARED', 'ACTIVE', 'RETIRING', 'RETIRED')}) OR "
                    f"({_closed('owner_kind', 'qualification')} AND {_closed('state', 'PREPARED', 'ACTIVE', 'SEALING', 'SEALED', 'TRANSFERRED', 'RETIRED')}))"
                ),
            ),
        ),
    ),
    _table(
        "domains",
        (
            _digest("guard_id"),
            CompositionColumn("connector", "varchar", 16),
            _uuid("service_id"),
            _digest("physical_subject_sha256"),
            CompositionColumn("fencing_epoch", "bigint", 8),
            _digest("owner_key", nullable=True),
        ),
        (
            _key("pk_c_domains", "guard_id", primary=True),
            _key("uq_c_domain_physical", "connector", "service_id", "physical_subject_sha256"),
        ),
        (CompositionForeignKey("fk_c_domain_owner", ("owner_key",), "owners", ("owner_key",)),),
        (
            CompositionCheck("ck_c_domain_connector", _closed("connector", "mssql", "clickhouse", "postgres")),
            CompositionCheck(
                "ck_c_domain_epoch", "([fencing_epoch]>=0 AND ([owner_key] IS NULL OR [fencing_epoch]>0))"
            ),
        ),
    ),
    _table(
        "owner_domains",
        (
            _digest("owner_key"),
            _digest("guard_id"),
            _document("claim_document"),
            CompositionColumn("fencing_epoch", "bigint", 8),
        ),
        (
            _key("pk_c_owner_domains", "owner_key", "guard_id", primary=True),
            _key("uq_c_owner_domain_epoch", "guard_id", "fencing_epoch"),
            _key("uq_c_owner_domain_association", "owner_key", "guard_id", "fencing_epoch"),
        ),
        (
            CompositionForeignKey("fk_c_owner_domain_owner", ("owner_key",), "owners", ("owner_key",)),
            CompositionForeignKey("fk_c_owner_domain_guard", ("guard_id",), "domains", ("guard_id",)),
        ),
        (
            CompositionCheck("ck_c_owner_domain_document", _bounded("claim_document", 8388608)),
            CompositionCheck("ck_c_owner_domain_epoch", "[fencing_epoch]>0"),
        ),
    ),
    _table(
        "operations",
        (
            _digest("operation_key"),
            CompositionColumn("operation_family", "varchar", 13),
            _digest("owner_key"),
            _digest("owner_subject_sha256"),
            _digest("replay_key"),
            _document("operation_document"),
            CompositionColumn("state", "varchar", 16),
            _digest("closed_gates_sha256", nullable=True),
            _digest("quiescence_sha256", nullable=True),
            _digest("outcome_evidence_sha256", nullable=True),
        ),
        (
            _key("pk_c_operations", "operation_key", primary=True),
            _key("uq_c_operation_replay", "operation_family", "replay_key"),
            _key("uq_c_operation_owner", "operation_key", "owner_key"),
            _key("uq_c_operation_family", "operation_key", "operation_family"),
        ),
        (
            CompositionForeignKey(
                "fk_c_operation_owner",
                ("owner_key", "operation_family", "owner_subject_sha256"),
                "owners",
                ("owner_key", "owner_kind", "subject_sha256"),
            ),
        ),
        (
            CompositionCheck("ck_c_operation_document", _family_documents("operation_family", "operation_document")),
            CompositionCheck(
                "ck_c_operation_state", _closed("state", "RUNNING", "SUCCEEDED", "FAILED", "COMMIT_UNKNOWN")
            ),
            CompositionCheck(
                "ck_c_operation_terminal",
                "([state]='RUNNING' OR [state]='COMMIT_UNKNOWN' OR "
                "([closed_gates_sha256] IS NOT NULL AND [quiescence_sha256] IS NOT NULL AND [outcome_evidence_sha256] IS NOT NULL))",
            ),
            CompositionCheck(
                "ck_c_operation_replay", "([operation_family]<>'execution' OR [operation_key]=[replay_key])"
            ),
        ),
    ),
    _table(
        "operation_domains",
        (
            _digest("operation_key"),
            _digest("owner_key"),
            _digest("guard_id"),
            CompositionColumn("fencing_epoch", "bigint", 8),
        ),
        (_key("pk_c_operation_domains", "operation_key", "guard_id", primary=True),),
        (
            CompositionForeignKey(
                "fk_c_operation_domain_operation",
                ("operation_key", "owner_key"),
                "operations",
                ("operation_key", "owner_key"),
            ),
            CompositionForeignKey(
                "fk_c_operation_domain_owner",
                ("owner_key", "guard_id", "fencing_epoch"),
                "owner_domains",
                ("owner_key", "guard_id", "fencing_epoch"),
            ),
        ),
        (CompositionCheck("ck_c_operation_domain_epoch", "[fencing_epoch]>0"),),
    ),
    _table(
        "issued_authorities",
        (
            _digest("operation_key"),
            CompositionColumn("connector", "varchar", 16),
            _uuid("service_id"),
            CompositionColumn("principal_id", "varchar", 128),
        ),
        (
            _key("pk_c_issued_authorities", "operation_key", "connector", "service_id", "principal_id", primary=True),
            _key("uq_c_issued_principal", "connector", "service_id", "principal_id"),
        ),
        (CompositionForeignKey("fk_c_issued_operation", ("operation_key",), "operations", ("operation_key",)),),
        (CompositionCheck("ck_c_issued_connector", _closed("connector", "mssql", "clickhouse")),),
    ),
    _table(
        "proofs",
        (
            _digest("operation_key"),
            CompositionColumn("operation_family", "varchar", 13),
            CompositionColumn("kind", "varchar", 16),
            _digest("proof_sha256"),
            _document("proof_document"),
        ),
        (_key("pk_c_proofs", "operation_key", "kind", "proof_sha256", primary=True),),
        (
            CompositionForeignKey(
                "fk_c_proof_operation",
                ("operation_key", "operation_family"),
                "operations",
                ("operation_key", "operation_family"),
            ),
        ),
        (
            CompositionCheck("ck_c_proof_family", _closed("operation_family", "execution")),
            CompositionCheck("ck_c_proof_kind", _closed("kind", "CLOSED_GATES", "QUIESCENCE", "OUTCOME")),
            CompositionCheck("ck_c_proof_document", _bounded("proof_document", 8388608)),
        ),
    ),
)
