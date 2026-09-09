# Route bootstrap and doctor

`dpone ops connection-doctor`, `dpone ops source-discover`,
`dpone ops route-bootstrap`, and `dpone ops route-doctor` are the self-service
onboarding layer before route readiness and certification.

Use them when an operator wants to answer four questions without writing Python
or opening source and sink databases from the control plane:

1. Are the route prerequisites visible in this environment?
2. What does the source schema look like?
3. What manifest draft and next commands should I run?
4. Is the route ready to move into route readiness, certification, or live
   evidence gates?

The commands are credential-free by default. They read explicit environment,
tool, import, schema, and artifact inputs and write stable JSON/Markdown
evidence. They do not run heavy route tests, mutate sinks, advance source state,
or execute manifests.

## User workflow

1. Export source schema metadata from your database admin tool or local fixture.
2. Run `dpone ops connection-doctor` for required local tools, environment
   variables, and Python imports.
3. Run `dpone ops source-discover` against the exported schema JSON.
4. Run `dpone ops route-bootstrap` to generate a manifest draft and next-command
   plan.
5. Run `dpone ops route-doctor` with the generated artifacts.
6. Continue to `dpone ops route-readiness`, `dpone ops route-certify`, and
   release gates only after route doctor is green.

## CLI examples

Check local prerequisites for `mssql -> clickhouse`:

```bash
uv run dpone ops connection-doctor \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --tool bcp \
  --tool clickhouse-client \
  --env DPONE_MSSQL_DSN \
  --env DPONE_CLICKHOUSE_DSN \
  --python-import pyodbc \
  --output-dir test_artifacts/onboarding/mssql_to_clickhouse/connection \
  --format json
```

Normalize source schema evidence:

```bash
uv run dpone ops source-discover \
  --source mssql \
  --dataset dbo.orders \
  --schema-json exports/mssql/orders_schema.json \
  --output-dir test_artifacts/onboarding/mssql_to_clickhouse/discovery \
  --format json
```

Generate the route manifest draft:

```bash
uv run dpone ops route-bootstrap \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --dataset dbo.orders \
  --source-discovery-json test_artifacts/onboarding/mssql_to_clickhouse/discovery/source_discovery.json \
  --manifest-id orders_mssql_to_clickhouse \
  --output-dir test_artifacts/onboarding/mssql_to_clickhouse/bootstrap \
  --format json
```

Aggregate the onboarding gate:

```bash
uv run dpone ops route-doctor \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact connection_doctor=test_artifacts/onboarding/mssql_to_clickhouse/connection/connection_doctor.json \
  --artifact source_discovery=test_artifacts/onboarding/mssql_to_clickhouse/discovery/source_discovery.json \
  --artifact route_bootstrap=test_artifacts/onboarding/mssql_to_clickhouse/bootstrap/route_bootstrap.json \
  --output-dir test_artifacts/onboarding/mssql_to_clickhouse/doctor \
  --format json
```

## Artifacts

| Artifact | Command | Purpose |
| --- | --- | --- |
| `connection_doctor.json` | `dpone ops connection-doctor` | Route prerequisites, tool checks, env var checks, Python import checks, install extras, blockers, and next actions. |
| `connection_doctor.md` | `dpone ops connection-doctor` | Operator-readable prerequisite runbook. |
| `source_discovery.json` | `dpone ops source-discover` | Source tables, columns, type families, primary-key candidates, cursor candidates, type risks, blockers, and warnings. |
| `source_discovery.md` | `dpone ops source-discover` | Operator-readable schema profile. |
| `route_bootstrap.json` | `dpone ops route-bootstrap` | Route profile, generated manifest draft path, type-risk summary, and next commands. |
| `route_manifest.json` | `dpone ops route-bootstrap` | Editable manifest draft for the discovered route. |
| `route_bootstrap.md` | `dpone ops route-bootstrap` | Human-readable bootstrap plan. |
| `route_doctor.json` | `dpone ops route-doctor` | Final onboarding go/no-go report over connection, discovery, and bootstrap artifacts. |
| `route_doctor.md` | `dpone ops route-doctor` | Release-review friendly onboarding summary. |

## Source schema JSON

`source-discover` accepts a simple exported schema shape:

```json
{
  "tables": [
    {
      "schema": "dbo",
      "name": "orders",
      "row_count": 10000,
      "columns": [
        {"name": "order_id", "type": "int", "nullable": false},
        {"name": "updated_at", "type": "datetime2", "nullable": false},
        {"name": "payload", "type": "nvarchar(max)", "nullable": true}
      ]
    }
  ]
}
```

Column names ending with `id` or `_id` are primary-key candidates when they are
not nullable. `updated_at`, `created_at`, `xmin`, `lsn`, and version-like names
are cursor candidates. Unbounded text, blob, and unknown types become warnings
so operators can review physical contracts before route readiness.

## Runbook

| Symptom | Action |
| --- | --- |
| `tool.<name>.missing` | Install the executable or add it to `PATH`, then rerun `connection-doctor`. |
| `env.<name>.missing` | Set the required environment variable in the runtime or CI job. |
| `python_import.<name>.missing` | Install the correct `dpone[...]` extra or dependency bundle. |
| `source_discovery.tables_missing` | Export schema JSON with at least one table and a `columns` array. |
| `table.<name>.primary_key_candidate_missing` | Add a declared key in the manifest or update the exported schema with primary-key metadata. |
| `route_bootstrap.not_passed` | Open `route_bootstrap.json`, fix upstream discovery or route profile blockers, and regenerate the manifest draft. |
| `route_doctor` is blocked | Fix the first required artifact blocker instead of editing aggregate JSON by hand. |

## CI usage

For credential-free OSS CI, run these commands against checked-in schema
fixtures and upload `test_artifacts/onboarding/` with `if: always()`. For
vendor-live CI, add real tool and environment checks, but keep database probing
in route live certification rather than in `connection-doctor`.

## Related docs

- [Route readiness](route-readiness.md)
- [Route certify](route-certify.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Developer route bootstrap and doctor](developer-route-bootstrap-doctor.md)
