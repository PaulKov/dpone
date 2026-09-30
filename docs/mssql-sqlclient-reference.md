# MSSQL SqlClient contract reference

## Manifest contract

The optimized route requires all of the following:

- ClickHouse source and MSSQL sink;
- `typed_binary` / `mssql_native` wire;
- `bounded_stream` chunking;
- explicit `import_backend: mssql_sqlclient`;
- explicit `verification_backend: target_local`;
- Linux x86-64 companion readiness before extraction.

Supported business types in the first profile are `bigint`, `float(53)`,
`nvarchar(max)`, and `datetime2(6)`, including supported nullable forms.
Unsupported schemas fail before extraction. `layout_version` defaults to 1;
version 2 must be explicit.

The sink and transaction state may reuse one logical target connection. The
source remains a distinct logical connection. Both are resolved only from the
verified runtime context; credentials do not belong in the manifest.

## Offline plan

Bounded native BCP and SqlClient plans expose:

- `state.catalog: generic_mssql_transaction_v2`;
- the target identity registry;
- target fence, load attempt, load operation, and load receipt tables;
- each table's canonical integrity trigger;
- the selected writer and verification identities;
- `live_preflight_required` until runtime checks run.

This corrects the bounded-native plan projection. Legacy non-native and XMin
plans retain their existing `state.tables` contract. Consumers that parse the
bounded-native plan must use the generic keys; no persisted state migration is
required.

## CLI contracts

| Command | Success | Blocked or failed |
|---|---|---|
| `dpone runtime mssql-sqlclient doctor --format json` | exit `0`, readiness JSON | exit `2`, readiness JSON with blocker code |
| `dpone plan … --format json` | exit `0`, offline plan | exit `2`, configuration diagnostic |
| `dpone run … --format json` | exit `0`, run report | exit `1` after execution starts; exit `2` before execution |
| `dpone ops mssql-native-recovery inspect …` | read-only inventory | non-zero closed diagnostic |
| `reconcile`, `resume`, `retire` | durable action result | non-zero; retained custody is preserved |

Example blocked-before-I/O result:

```json
{"attempts":0,"passed":false,"result":{"error_code":"DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED","extracted_rows":0,"status":"error"}}
```

Example successful transport classification:

```json
{"classification":"success","input_rows_consumed":10000,"protocol":"dpone.mssql-sqlclient.ipc.v2"}
```

Example unknown writer outcome:

```json
{"event":"UNKNOWN","observation":{"writer_outcome":"lost_ack"},"recovery_action":"reconcile"}
```

## Evidence contract

Shareable route evidence contains bounded classifications, opaque identities,
counts, digests, and phase durations. It excludes credentials, endpoints,
connection strings, table names, row values, child diagnostics, and source
samples. `SUCCEEDED` is authoritative only after independent target proof and
durable publication evidence. An unknown child result is never success.

See [certification](mssql-sqlclient-certification.md) for the separate synthetic
receipt schemas and [recovery](mssql-native-recovery.md) for custody actions.
