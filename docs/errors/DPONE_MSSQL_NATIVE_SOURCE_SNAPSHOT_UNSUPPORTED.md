# DPONE_MSSQL_NATIVE_SOURCE_SNAPSHOT_UNSUPPORTED

This troubleshooting page covers the stable `mssql_native.*` raw source
admission and recovery errors. The page identifier groups those codes; it is
not an additional runtime error code.

## What happened

An explicit `exact_raw_rows` request could not prove its source or recovery
contract. Admission fails closed; an error must never be treated as an empty
source or successful publication. A runtime failure can leave owned staging
and retained custody that require normal native recovery.

## Diagnose and recover

| Runtime code | Action |
|---|---|
| `source_snapshot_mode_invalid` | Set `mode: exact_raw_rows` and exactly one supported `replica_scope`, or omit the selector for legacy plain MergeTree. |
| `source_snapshot_route_unsupported` | Select ClickHouse to MSSQL with typed binary native wire and bounded stream chunking. |
| `raw_snapshot_requires_target_local_verification` | Set `native_transfer.execution.verification_backend: target_local`; raw mode requires durable identity v2. |
| `replacing_raw_snapshot_required` | Check the exact table engine and Atomic database. Use the explicit raw selector only for ReplacingMergeTree or its supported replicated form. |
| `replica_scope_unproven` | Use a direct native TLS connection with one host, the matching replica scope, and no reconnect or rotation. |
| `source_read_profile_unsupported` | Check trusted CA, hostname verification, bounded timeouts, supported server settings and catalog read permissions. Do not weaken TLS or row visibility settings. |
| `source_provenance_incomplete` | Inspect the supported partition expression, part count and checksum visibility. Settle the failed invocation before starting a new extraction. |
| `source_snapshot_profile_changed` | The source authority changed. Settle custody and start a new invocation with freshly acquired metadata. |

Each code in this table has the `mssql_native.` prefix. Use
`dpone plan MANIFEST --format json` for configuration diagnostics without network
access. Runtime admission supplies the actual source observation; a successful
static plan is not a route certificate.

Before EOF, use the existing observe/reconcile/retire workflow to settle owned
writers and target custody. After verified EOF, resume from the retained journal
and stages without reconnecting to the source. Do not edit journal hashes,
disable verification, or delete uncertain stages to force progress. See the
[native transport guide](../mssql-native-transport.md) for exact commands and
publication recovery semantics.
