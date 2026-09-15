# Install and use native-original SQL bindings

Platform engineers can install an immutable binding index in the already selected
native SQL Server control database. Application developers then inject
`MssqlNativeOriginalBindings` into the [publication service](native-original-publication.md).
This provider implements real SQL transactions and independent readback. It does
not yet supply an analyst starter or automated deployment provisioning command.

## Prerequisites and installation

The platform provisioning owner supplies the existing `dbo`-owned control schema,
an **already authenticated** control-authority `OriginalRef`, a privileged fresh
connection factory and a dedicated existing database user for runtime access.
Registering a reference does not authenticate it. Automated selection of this
reference from the authenticated deployment remains pending application work;
do not invent a replacement reference or reuse an unrelated authority ledger.

```python
from dpone.adapters.native_originals_mssql_schema import MssqlNativeOriginalSchemaMigration

MssqlNativeOriginalSchemaMigration(
    connection_factory=privileged_control_connection,
    control_schema="dpone_control",
    control_authority=authenticated_control_authority,
    runtime_database_principal="dpone_native_runtime",
).apply()
```

The variables above come from platform provisioning. Both factories in this
guide must select the authenticated control database and return a fresh dedicated
connection with positive, finite connection and statement timeouts. For pyodbc,
set the connection timeout when connecting and set `connection.timeout` for
statements; an unbounded default is insufficient. Credentials remain in the
existing credential boundary and are not written to the binding index.

Installation is explicit and transactional. It creates additive versioned tables
and `native_original_bind_v1` / `native_original_resolve_v1` procedures. Exact
repeat installation succeeds. Incompatible existing column types, nullability,
primary keys, checks, ownership, triggers or procedure definitions cause an error;
the installer does not drop or repair existing data. Principal ID and SID are
registered with the exact authority. A conflicting registration cannot replace
the authority behind retained bindings.

Runtime receives procedure execution permissions, with direct table access and
schema alteration denied. Static SQL procedures use the caller's context and
the same-owner ownership chain; they verify the actual caller ID/SID and installed
authority within the transaction. They do not use `EXECUTE AS OWNER`. See the
[SQL Server ownership-chain documentation](https://learn.microsoft.com/en-us/sql/relational-databases/tutorial-ownership-chains-and-context-switching?view=sql-server-ver17).

## Runtime composition

```python
from dpone.adapters.native_originals_mssql import MssqlNativeOriginalBindings

bindings = MssqlNativeOriginalBindings(
    connection_factory=runtime_control_connection,
    control_schema="dpone_control",
    control_authority=authenticated_control_authority,
    max_binding_bytes=1048576,
)
reference = bindings.bind(complete_binding)
verified = bindings.resolve(
    reference,
    expected_subject=complete_binding.subject,
    expected_kind=complete_binding.kind,
)
```

`complete_binding` is the validated binding from the authenticated original-store
workflow. `max_binding_bytes` is an exact positive integer no larger than the
native JSON limit. Canonical bytes and all identities are validated before SQL
effects. `bind` returns an `OriginalRef` only after commit and independent resolution
through a fresh connection; `resolve` returns the full validated binding.

The SQL index uses a locator hash for lookup, then compares full binary locator,
authority, subject, kind and digest values with their lengths. The canonical
binding bytes remain authoritative. An existing locator cannot be rebound to a
different tuple. Key-range locking and a unique primary key serialize competing
creates through transaction completion; see the
[SQL Server locking guide](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide?view=sql-server-ver17).

## Failure and recovery

A missing registration, mismatched caller, incompatible schema or conflicting
tuple fails closed. Preflight errors have no SQL effect. After an execute or
commit error, the adapter attempts only independent read-only resolution of the
same expected tuple. If the transaction committed and the exact tuple is visible,
reconciliation can return the original reference. Otherwise it raises
`NativeOriginalBindingError`; it never repeats the bind automatically.

Rollback and connection cleanup are best effort and do not prove that a commit
was absent. Retain uncertain originals for investigation. Resolve-only failures
and invalid input errors propagate; no exception authorizes another source/target
dispatch or orphan deletion. A privileged administrator remains part of the trust
boundary and must protect the installed database and procedures.

## Validation scope

`tests/test_native_originals_mssql.py` covers provider faults and bounded identity
checks. `tests/test_native_originals_mssql_live.py` is opt-in with
`DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE=1`, `DPONE_NATIVE_SQL_TEST_HOST` and
`DPONE_NATIVE_SQL_TEST_PASSWORD` for an explicitly approved isolated synthetic
server. It creates uniquely named databases/logins and removes its own resources.

The live cases exercise installation, permissions, exact replay, concurrent
conflict, real rollback, lost commit acknowledgement and tampered schema or
registration. These are provider checks against local SQL Server, not an
installed-wheel analyst journey, complete S3-to-SQL-to-ClickHouse route,
production qualification or release approval. Automated authenticated application
composition, starter/preview, DEV reporting and immutable promotion remain pending.
