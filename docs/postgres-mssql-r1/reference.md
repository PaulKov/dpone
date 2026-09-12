# PostgreSQL → MSSQL R1 profile reference

> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

This reference is for platform engineers configuring the bounded R1 profile.
Pipeline authors do not edit this catalog.

!!! warning "Registration does not activate R1"

    The current catalog can select and explain the profile, but executable
    provider composition is absent. Registration therefore remains
    activation-blocked and cannot turn the route into a production-ready R1
    implementation.

The catalog schema is
[`dpone.postgres-mssql-correctness-profiles.v1`](../schemas/postgres-mssql-correctness-profiles.v1.schema.json).
Set its repo-external path through
`DPONE_POSTGRES_MSSQL_CORRECTNESS_CATALOG`. The file contains no credentials.

```yaml
schema: dpone.postgres-mssql-correctness-profiles.v1
profiles:
  postgres-mssql-r1:
    source_connection_ref: postgres_orders
    sink_connection_ref: mssql_dwh
    allowed_source_modes: [batch_full_refresh, xmin_current_state]
    source_major: 16
    target_major: 2022
    topology: standalone_same_database
    object_profile: ordinary_disk_rowstore
    key_types: [int2, int4, int8, uuid]
    receipt_contract: mssql_effect_receipt_v2
    hash_policy: postgres_mssql_row_hash_v1
    writer_fence: mssql_target_head_v2
    session_count: 1
    transaction_scope: local_database
    delayed_durability_disabled: true
    max_descendant_proof_receipts: 100000
    target_binding_uuid: 3316d0bd-3d61-4a25-a14e-bf21fe038e37
    target_contract_revision: 1
    quality_policy: postgres_mssql_r1_bounded_v1
    certification_ref: postgres-mssql-r1-vendor-live
registrations:
  - source_connection_ref: postgres_orders
    sink_connection_ref: mssql_dwh
    source_mode: batch_full_refresh
    target_schema: dbo
    target_table: orders
    profile_id: postgres-mssql-r1
```

The registration key is exact: source reference, sink reference, source mode,
target schema and target table. No connector-name heuristic promotes a
compatibility route.

The catalog cannot declare itself certified. Current implementation,
certification and activation statuses come from trusted, exact-commit evidence;
local status fields are rejected by the schema and ignored by the adapter.

## Plan contract

`dpone plan --format json` and `dpone manifest validate` share one decision.
Planning never mutates source, target, state or receipts. Public output includes
the profile/capability, source mode, topology, authority contract, three status
axes, blockers and a decision digest. It omits credentials, database GUIDs,
binding UUIDs, authority table names and signed payloads.

Exit codes follow the existing CLI contract: `0` for a valid plan, `1` for a
completed blocked/failed result and `2` for invalid arguments or configuration.

Next, use the [recovery runbook](recovery.md). Return to the
[R1 overview](overview.md).
