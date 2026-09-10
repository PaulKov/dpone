# Prepared-stage integrity and metadata projection

This developer integration guide explains how to reduce repeated preparation
work while preserving native data and publication authority. It is for dpone
maintainers and platform engineers. Start with the
[native transport guide](../mssql-native-transport.md) and the
[approved acceleration design](../feature-design-data-delivery-acceleration-v1.md).

The DDA-02 contribution supplies two internal helpers used by DDA-06's integrated
preparation path. The standalone component branch contains the helpers and
their contracts; runtime wiring and integrated evidence belong to DDA-06. No new
manifest option, CLI flag, dependency, wire format or recovery migration is
introduced. Existing native SWITCH rejection remains in force.

## Prerequisites and expected result

Use the existing admitted bounded route, completed extraction lifecycle,
contiguous verified raw receipts and invocation-owned prepared table. The caller
must hold the preparation scope, current lease and capacity authority. The
helpers neither acquire these authorities nor execute SQL or write evidence.

The integrated preparation path populates business values and authoritative
metadata in one INSERT and reads the full prepared table once to calculate both
existing digests. Its hermetic coordinator tests establish **four raw typed
readbacks plus two prepared typed readbacks**, including the independent prepared
verification before publication and **zero preparation metadata UPDATEs**.
Count/key/catalog queries remain necessary and are not typed digest
scans. These are structural acceptance criteria, not observed latency results.

See the [integrated overview](https://github.com/PaulKov/dpone/blob/49160c3982705b8576c50c0d06e740ae13991e08/docs/delivery-acceleration/index.md),
[coordinator contract tests](https://github.com/PaulKov/dpone/blob/49160c3982705b8576c50c0d06e740ae13991e08/tests/test_mssql_native_delivery_integration.py)
and [57-case scoped run](https://github.com/PaulKov/dpone/blob/49160c3982705b8576c50c0d06e740ae13991e08/test_artifacts/delivery-acceleration/dda-06/observation-wiring-reviewed.log)
at DDA-06 checkpoint `49160c3982705b8576c50c0d06e740ae13991e08`.
These preliminary hermetic results establish the scoped structure; they do not
replace final frozen integration gates or live SQL/performance certification.

## Helper contracts

`digest_prepared_rows(rows, *, business_contract, full_contract, max_row_bytes,
expected_rows)` consumes one iterator of full column mappings. Its frozen
`PreparedDigests` result contains `business_digest`, `full_digest` and `rows`.
Both digests retain the existing `mssql-native-sha256-sum-v1` envelope, canonical
native framing, order independence and duplicate multiplicity. The sums are
constant-space; only the current row and its encoded projections are retained.

The business encoder keeps the authored byte limit. The full encoder receives
the existing schema-derived finite metadata and framing allowance. Unbounded
framework placeholders must remain NULL. Prepared nullable framing can differ
from business framing, so both projections are encoded independently. A full
mapping must contain exactly the full contract's names. Values are consumed
before requesting the next row, including when a driver reuses its mapping.

`build_prepared_insert(*, target_sql, source_sql, business_schema, resolved,
lineage, quote_identifier)` returns a SQL string. `source_sql` is a trusted
derived source body: explicit SELECTs joined with `UNION ALL`, built from the
same validated receipt stage identifiers. `target_sql` is the qualified name
of the invocation-owned prepared table. Pass SQL fragments produced by those
existing ownership paths, never user-authored SQL. Identifier quoting protects
syntax; it does not establish ownership, and the helper does not parse SQL.

`business_schema` contains the **ordered resolved target names and native target
types**, excluding framework columns. Do not pass original source dialect types
or append nullable markers to types used for SQL hashing. `resolved` includes
the completed lifecycle's lineage via `resolved.with_lineage(lineage)`.
The helper emits `resolved.ordered_target_names`, uses the injected identifier
quoter for columns, and delegates metadata to
`MssqlNativeLineageProjection.expressions` and the canonical row-hash producer.
Precedence is row hash, lineage, generated placeholder, then ordinary source
value. Source-supplied framework values cannot override generated expressions.
Generated columns may be absent from the raw source.

The helper preserves generated NULLs and the extraction-start `loaded_at`
placeholder. The finalizer's target-clock UPDATE inside the publication
transaction remains mandatory. Target types, nullability and collations are
unchanged; keep existing staging DDL, including physical staging nullability.
An empty load still has a verified zero-row raw stage and uses the same SQL.

## Integration contract and original handoff recipe

The following records the call-site contract delivered to DDA-06. It is a
developer excerpt using that method's already validated local values, not a
standalone user entry point:

```python
from dpone.runtime.sinks.mssql_native_prepared_insert import build_prepared_insert

columns = ", ".join(strategy.connector.quote_identifier(name) for name, _ in schema)
source_sql = " UNION ALL ".join(
    f"SELECT {columns} FROM {receipt.stage_id}" for receipt in receipts
)
business_schema = tuple(
    (name, resolved.types[name])
    for name in resolved.ordered_target_names
    if not name.casefold().startswith("__dpone__")
)
strategy.connector.execute_query(
    build_prepared_insert(
        target_sql=strategy._staging_name(stage),
        source_sql=source_sql,
        business_schema=business_schema,
        resolved=resolved,
        lineage=lineage,
        quote_identifier=strategy.connector.quote_identifier,
    )
)
```

Preserve this surrounding sequence:

1. Complete extraction at EOF, validate contiguous receipts and current raw
   ownership/content, check capacity, reserve the journal's planned table, enter
   the preparation scope and create the owned prepared stage.
2. Execute the INSERT above. Set the receipt-derived row count, compare it with
   the completed extraction count, and attach complete consumed payload evidence.
3. Split the normalizer's existing `_finalize_direct_native` into private
   prevalidation and completion operations around its existing metadata
   projection. Prevalidation retains typed-file/deferred-evidence flags, exact
   column order/prefix, native/target type agreement, absence of a text codec and
   required-key checks. Completion retains SQL key semantics/collision checks,
   `COUNT_BIG`, exact count comparison and `finalize_native_evidence` with the
   existing `_native_evidence_columns` producer.
4. Keep the public direct BCP normalization path in its current order:
   prevalidation, metadata UPDATE, completion. The bounded preparer calls the
   same prevalidation and completion after its authoritative INSERT. Do not
   replace normalization with evidence issuance alone, or introduce any
   metadata-valid/skip-validation flag or caller-supplied validity assertion.
5. Construct `full_contract` exactly as `_stage_digest(all_columns=True)` does,
   using `stage.columns`, `stage.column_types` and target nullability. Select all
   those columns once through `get_records_iterator`, and call
   `digest_prepared_rows` with `business_contract=context.wire_contract`,
   `max_row_bytes=context.max_row_bytes` and `expected_rows=stage.row_count`.
6. Compute `expected_sum = sum(int(receipt.consumed_part_evidence["native_typed_sum"]) for receipt in receipts) % (1 << 256)`.
   Compare `business_digest` to `native_multiset_digest(stage.row_count, expected_sum)`.
   On mismatch retain
   `mssql_native.prepared_digest_mismatch`. Persist `full_digest` in the existing
   prepared resource and recovery snapshot `digest` field. Keep capacity,
   object-id and journal ordering checks.
7. Leave `reverify` independent: verify raw receipts again, capacity, prepared
   owner and object identity, and full prepared content. Retain quality before
   publication, the finalizer target-clock UPDATE, atomic target mutation and
   receipt, durable evidence before checkpoint advancement and owned cleanup.

DDA-06's integrated staged-prepare SQL fixture evaluates the derived INSERT. The
original baseline fixture recognized only the old flat INSERT and fabricated
metadata on UPDATE; that fixture alone cannot prove the new projection. Maintain
integration coverage for key failures, metadata tampering, count/evidence ordering,
completed-stage recovery and direct BCP compatibility. DDA-06 owns navigation,
shared normalizer/coordinator tests and the changelog.

## Failures, recovery and verification

The helpers preserve existing individual diagnostics for contract/type mismatch,
unexpected NULL, invalid value, byte/field overflow, unbounded metadata tamper
and prepared count mismatch. Iterator failures propagate without retry or a
partial result. With several corrupt rows, one-pass validation may report an
earlier metadata error before a later business error that the old two-pass order
would have encountered first. Neither result permits publication.

Do not retry the INSERT directly into a populated stage: it is not independently
idempotent. Use the existing journal and deterministic owned-table recovery and
cleanup path. After an unknown target commit, resolve the exact target receipt
before any replay. Retain resources when authority or commit outcome is unknown.

Run the hermetic acceptance suite from the repository root:

```bash
uv run pytest tests/test_mssql_native_integrity_readbacks.py tests/test_mssql_native_metadata_insert.py tests/test_mssql_native_metadata_insert_parity.py tests/test_mssql_native_staged_verification.py tests/test_mssql_native_lineage_authority.py -q
```

The tests use legacy digest/UPDATE parity, frozen digest/SQL examples, a poisoned
second iteration and exact encoder counts. Local SQLite execution verifies only
the portable business-only UNION ALL subset; it does not certify SQL Server
metadata evaluation. The pytest command prints test results and exits with status
0 on success; it does not save evidence in the task directory. To record the same
check, run `uv run python test_artifacts/delivery-acceleration/dda-02/run_checks.py focused`.
This evidence producer writes `focused.log` and `focused-results.json` under
`test_artifacts/delivery-acceleration/dda-02/`, including the actual exit code,
source hashes and checkout identity. It replaces that group's previous local
results; commit evidence before rerunning when it must be retained.

Live SQL Server/ClickHouse/BCP correctness is **SKIP** without an explicitly
approved disposable environment. DDA-06's integrated hermetic checks cover the
scan-count reduction, empty/duplicate rows, boundary tampering and the retained
finalizer clock UPDATE; the standalone DDA-02 suite proves helper contracts only.
Performance remains **UNVERIFIED** until DDA-05
collects eligible baseline/candidate measurements. No production-readiness or
speed claim follows from hermetic parity. Continue with the
[task execution plan](../data-delivery-acceleration-tasks.md) for integration and
certification ownership.
