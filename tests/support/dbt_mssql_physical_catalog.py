"""Independent synthetic one-column heap and lazy DB-API catalog test boundary."""

from dataclasses import replace
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_wire import decode_physical_plan_set
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import encode_source_executor_binding
from tests.support.dbt_mssql_physical import canonical, plan_set_document
from tests.support.dbt_mssql_physical_registration import registration_inputs

STAMP = "2024-02-29T12:00:00.1234567"
INVOCATION = "30000000-0000-0000-0000-000000000001"


def catalog_case():
    """Return independent wire tuples, source row, registration and local plan."""
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    registration = replace(registration, limits=replace(registration.limits, max_metadata_bytes=16384))
    plan = decode_physical_plan_set(canonical(plan_set_document(layout="rowstore_none"))).models[0]
    reservation = OriginalRef("generation/reservation", "sha256:" + "7" * 64)
    binding = SourceExecutorBinding(
        UUID(plan.generation_id),
        1,
        UUID(INVOCATION),
        reservation,
        registration.trusted_profile.reference,
        OriginalRef("command/build", "sha256:" + "8" * 64),
    )
    source = (
        1,
        registration.registration_id,
        physical_runtime_registration_digest(registration).encode(),
        plan.generation_id,
        INVOCATION,
        1,
        2,
        reservation.locator.encode(),
        reservation.sha256.encode(),
        encode_source_executor_binding(binding),
        registration.principals.build.model.principal_id,
        bytes.fromhex(registration.principals.build.model.sid_hex),
        registration.principals.build.control.principal_id,
        bytes.fromhex(registration.principals.build.control.sid_hex),
    )
    details = {
        "HEADER": (
            5,
            registration.model_database.database_guid,
            STAMP,
            STAMP,
            "models",
            "orders",
            "U ",
            1,
            1,
            0,
            1,
            0,
            0,
        ),
        "TABLE": (7, "models", "orders", "U ", STAMP, STAMP, False, 0, 0, False, False, False, 0, 0, None),
        "COLUMN": (
            1,
            "id",
            56,
            56,
            "sys",
            "int",
            4,
            10,
            0,
            None,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            0,
            None,
            False,
            0,
            0,
        ),
        "INDEX": (0, None, 0, "HEAP", False, False, False, False, False, False, None, 1, "PRIMARY", "FG"),
        "PARTITION": (0, 1, 123, 456, 0, "NONE", 1, "PRIMARY", "FG"),
        "COUNT": (3,),
    }
    rows = {kind: [(1, kind, 42, 1, 1) + values] for kind, values in details.items()}
    for kind, width in (("INDEX_COLUMN", 8), ("DEPENDENCY", 12), ("FORBIDDEN_PROPERTY", 3)):
        rows[kind] = [(1, kind, 42, 0, 0) + (None,) * width]
    return registration, plan, source, rows


class CatalogConnection:
    """Lazy cursor/connection with no fetchall and observable transaction events."""

    autocommit = True
    timeout = 0

    def __init__(self, source, rows, *, failure=None):
        self.source, self.rows, self.failure = source, rows, failure
        self.events = []
        self.kind = ""
        self.pending = iter(())
        self.fetched = 0
        self.headers = 0

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.events.append(("execute", sql, parameters))
        if "physical_require_source_v1" in sql:
            self.kind = "SOURCE"
            self.pending = iter([self.source])
        elif "physical_catalog_v1" in sql or "physical_catalog_v2" in sql:
            self.kind = parameters[-1]
            if self.failure == self.kind:
                raise RuntimeError("synthetic query fault")
            rows = self.rows[self.kind]
            if self.kind == "HEADER":
                self.headers += 1
                if self.failure == "drift" and self.headers == 2:
                    row = list(rows[0])
                    row[8] = "2024-02-29T12:00:00.1234568"
                    rows = [tuple(row)]
            self.pending = iter(rows)
        else:
            self.pending = iter(())
        return self

    def fetchone(self):
        self.fetched += 1
        if self.failure == "fetch":
            raise RuntimeError("synthetic fetch fault")
        if self.failure == "cancel":
            raise KeyboardInterrupt()
        return next(self.pending, None)

    def fetchall(self):
        raise AssertionError("unbounded fetch is forbidden")

    def nextset(self):
        return True if self.failure == "extra" else None

    def commit(self):
        self.events.append(("commit",))
        if self.failure == "commit":
            raise RuntimeError("synthetic uncertain commit")

    def rollback(self):
        self.events.append(("rollback",))

    def close(self):
        self.events.append(("close",))
