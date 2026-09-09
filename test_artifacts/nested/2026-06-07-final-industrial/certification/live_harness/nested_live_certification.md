# Nested Live Certification

Status: **passed**

| Sink | Status |
|---|---|
| `mssql` | passed |
| `clickhouse` | passed |
| `bigquery` | skipped |

```json
{
  "status": "passed",
  "sinks": {
    "mssql": {
      "status": "passed",
      "details": {
        "native_route": "bcp"
      }
    },
    "clickhouse": {
      "status": "passed",
      "details": {
        "native_route": "json_each_row"
      }
    },
    "bigquery": {
      "status": "skipped",
      "reason": "local service or credentials unavailable"
    }
  },
  "required_checks": [
    "root_child_load",
    "physical_child_deletes",
    "state_commit_after_success",
    "state_not_advanced_after_failure",
    "native_fast_path",
    "child_quality"
  ]
}
```
