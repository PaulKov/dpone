# Nested Normalization Certification

Status: **passed**

| Check | Result |
|---|---|
| `lint` | passed |
| `spill_to_disk` | passed |
| `native_spill_formats` | passed |
| `native_fast_path_handoff` | passed |
| `reverse_readback` | passed |
| `child_identity` | passed |
| `child_reconciliation` | passed |
| `child_delete_finalizers` | passed |
| `child_snapshot_store` | passed |
| `sql_child_snapshot_store` | passed |
| `child_quality` | passed |
| `live_certification_harness` | passed |
| `child_schema_evolution_contract` | passed |
| `native_fast_path_execution_contract` | passed |
| `quarantine` | passed |
| `raw_landing` | passed |

```json
{
  "status": "passed",
  "row_count": 1000,
  "normalized_rows": 7334,
  "checks": {
    "lint": "passed",
    "spill_to_disk": "passed",
    "native_spill_formats": "passed",
    "native_fast_path_handoff": "passed",
    "reverse_readback": "passed",
    "child_identity": "passed",
    "child_reconciliation": "passed",
    "child_delete_finalizers": "passed",
    "child_snapshot_store": "passed",
    "sql_child_snapshot_store": "passed",
    "child_quality": "passed",
    "live_certification_harness": "passed",
    "child_schema_evolution_contract": "passed",
    "native_fast_path_execution_contract": "passed",
    "quarantine": "passed",
    "raw_landing": "passed"
  },
  "row_counts": {
    "orders__raw": 1000,
    "orders": 1000,
    "orders__customer": 1000,
    "order_lines": 2000,
    "orders__tags": 1334,
    "orders__quarantine": 1000
  },
  "spill_formats": {
    "orders__raw": "jsonl",
    "orders": "jsonl",
    "orders__customer": "jsonl",
    "order_lines": "jsonl",
    "orders__tags": "jsonl",
    "orders__quarantine": "jsonl"
  },
  "lint": {
    "root_table": "orders",
    "has_errors": false,
    "issues": [
      {
        "code": "nested.raw_landing_retention_unspecified",
        "severity": "info",
        "message": "Raw landing is enabled with default table/payload names.",
        "path": null,
        "recommendation": "Document retention and access policy for raw payload tables."
      }
    ]
  },
  "industrial_evidence": {
    "child_schema_evolution": {
      "status": "passed",
      "safe_changes": [
        "add_nullable_column",
        "widen_type",
        "framework_columns"
      ],
      "breaking_changes": [
        "drop",
        "rename",
        "narrowing",
        "nullable_to_not_null_without_default"
      ],
      "type_conflict_policy": "__dpone__nc__<column>",
      "scope": "root_and_generated_child_tables"
    },
    "native_fast_path_execution": {
      "mssql": "bcp",
      "postgres": "copy",
      "clickhouse": "json_each_row_or_tsv",
      "bigquery": "load_job",
      "kafka": "file_stream"
    }
  }
}
```
