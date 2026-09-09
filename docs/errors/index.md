# Domain-first self-service error overview

**Audience:** pipeline authors, domain owners, CI maintainers, and platform
engineers.

Domain-first commands return stable `dpone.error.v1` codes. Start with the
first error in JSON output; later errors may be consequences of the same
authoring problem.

| Exit | Meaning | First action |
| ---: | --- | --- |
| `1` | Authoring, discovery, or validation failed | Open the code-specific page and repair editable sources. |
| `2` | Request or project configuration is invalid | Check command help and `dpone.yaml`. |
| `4` | Confinement, locking, or CAS safety failed | Stop concurrent writes and inspect the rejected path or authority. |
| `5` | Unexpected internal failure | Preserve redacted output, version, and trace id for maintainers. |

## Common authoring errors

- [`CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE`](CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE.md),
  [`MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE`](MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE.md),
  [`MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE`](MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE.md), and
  [`POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE`](POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE.md)
  reject lossy target-derived single-column cursors before source or target
  I/O and point to complete route alternatives.
- [`POSTGRES_XMIN_INITIAL_CONTRACT_INVALID`](POSTGRES_XMIN_INITIAL_CONTRACT_INVALID.md)
  and
  [`POSTGRES_XMIN_INCREMENTAL_CONTRACT_INVALID`](POSTGRES_XMIN_INCREMENTAL_CONTRACT_INVALID.md)
  reject an incomplete initial/incremental handoff pair before connector I/O.
- [`POSTGRES_XMIN_EXECUTION_INVALID`](POSTGRES_XMIN_EXECUTION_INVALID.md)
  and
  [`POSTGRES_XMIN_HANDOFF_ROUTE_UNSUPPORTED`](POSTGRES_XMIN_HANDOFF_ROUTE_UNSUPPORTED.md)
  reject malformed phase authoring or a non-PostgreSQL→MSSQL route.
- [`DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED`](DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED.md):
  flat, domain-first, or mixed authority cannot be adopted implicitly.
- [`DPONE_DOMAIN_OWNERSHIP_MISSING`](DPONE_DOMAIN_OWNERSHIP_MISSING.md):
  initialize the owning domain before its pipeline.
- [`DPONE_PIPELINE_DOMAIN_MISMATCH`](DPONE_PIPELINE_DOMAIN_MISMATCH.md):
  directory/source ownership or an external recipe domain conflicts.
- [`DPONE_PIPELINE_ID_DUPLICATE`](DPONE_PIPELINE_ID_DUPLICATE.md):
  pipeline ids are unique across the project.
- [`DPONE_RECIPE_ROUTE_CONFLICT`](DPONE_RECIPE_ROUTE_CONFLICT.md):
  choose either an exact recipe or a capability route for scaffolding.
- [`DPONE_AIRFLOW_DISABLED`](DPONE_AIRFLOW_DISABLED.md):
  the pipeline is valid but intentionally excluded from Airflow.
- [`DPONE_MSSQL_ASSET_URI_INVALID`](DPONE_MSSQL_ASSET_URI_INVALID.md):
  MSSQL Airflow Asset outlets must be
  `mssql://{host}:{port}/{database}/{schema}/{table}` with deployment-owned
  `asset_authority` (AIP-60; never `connection_ref` as host).
- [`DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED`](DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED.md),
  [`DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED`](DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED.md), and
  [`DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED`](DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED.md):
  every target, staging, and state database touched by a target-atomic MSSQL
  route needs an immutable registry pin.
- [`DPONE_MSSQL_DATABASE_AUTHORITY_INVALID`](DPONE_MSSQL_DATABASE_AUTHORITY_INVALID.md):
  a database pin is open, malformed, or non-canonical.
- [`DPONE_POSTGRES_SOURCE_AUTHORITY_REQUIRED`](DPONE_POSTGRES_SOURCE_AUTHORITY_REQUIRED.md):
  a governed PostgreSQL→MSSQL route has no deployment-signed source pin.
- [`DPONE_POSTGRES_SOURCE_AUTHORITY_INVALID`](DPONE_POSTGRES_SOURCE_AUTHORITY_INVALID.md):
  the signed source document is incomplete, open, or ambiguous.
- [`DPONE_POSTGRES_SOURCE_DATABASE_AUTHORITY_MISMATCH`](DPONE_POSTGRES_SOURCE_DATABASE_AUTHORITY_MISMATCH.md):
  route/connection database coordinates do not select the signed database.
- [`DPONE_SCAFFOLD_APPLY_FAILED`](DPONE_SCAFFOLD_APPLY_FAILED.md):
  scaffold I/O failed and the returned rollback receipt may require manual
  reconciliation.
- [`DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT`](DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT.md):
  the domain catalog membership entry for a scaffolded pipeline could not be
  written safely and must be reconciled manually.

## Discovery and CI errors

- [`DPONE_DISCOVERY_PATH_INVALID`](DPONE_DISCOVERY_PATH_INVALID.md) and
  [`DPONE_LAYOUT_ROOT_INVALID`](DPONE_LAYOUT_ROOT_INVALID.md) identify unsafe
  paths, unexpected depth, or symlink escape.
- [`DPONE_WORKLOAD_INDEX_INVALID`](DPONE_WORKLOAD_INDEX_INVALID.md) means a
  baseline is unsafe, stale, forged, or outside the closed schema.
- [`DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH`](DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH.md)
  means candidate bytes changed after protected approval.
- [`DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT`](DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT.md)
  means the accepted baseline changed before compare-and-swap.
- [`DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED`](DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED.md)
  means durable replacement cleanup requires operator reconciliation.
- [`DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID`](DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID.md)
  means bootstrap/change guards were combined incorrectly.
- [`DPONE_DISCOVERY_LIMIT_EXCEEDED`](DPONE_DISCOVERY_LIMIT_EXCEEDED.md) means
  the bounded local discovery budget was exceeded.
- [`DPONE_SELECTION_STATE_CHANGED`](DPONE_SELECTION_STATE_CHANGED.md) means
  selected authority changed before the operation could commit.
- [`DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD`](DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD.md)
  means the checked source changed before preview promotion.

All fixes change editable authoring sources or CI inputs only. Do not edit
generated release/deployment artifacts, fingerprints, credentials, or Vault
configuration to make an error disappear.

[Return to the domain-first tutorial](../getting-started/domain-first-airflow.md)
