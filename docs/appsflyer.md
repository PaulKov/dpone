# AppsFlyer connector

Status: implemented as a canonical `dpone` REST provider.

The AppsFlyer integration extracts raw and aggregate reports from AppsFlyer APIs and loads them through the standard `dpone` source/sink runtime.

## Supported resources

Common resources include:

- `installs_report`
- `in_app_events_report`
- `uninstall_events_report`
- `organic_installs_report`
- `daily_report`

The connector is intentionally resource-oriented: each resource has a stable output schema, request options, and landing table recommendation.

## Authentication

Use Vault, environment variables, Airflow connections, or direct params through the standard credentials flow.

Expected secret keys:

```json
{
  "token": "...",
  "endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app"
}
```

`api_key` is still accepted as a backward-compatible alias, but `token` is the preferred key.

## Manifest example

```yaml
source:
  type: api
  api_type: appsflyer
  connection_id: appsflyer
  connection_type: vault
  resource: installs_report
  options:
    app_ids: ["com.example.app"]
    date_from: "2026-01-01"
    date_to: "2026-01-02"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__appsflyer__installs_report
  strategy:
    mode: replace
```

See [REST API](rest-api.md) for the generic provider contract and [Source -> sink matrix](source-sink-matrix.md) for target support.

## Landing pattern

Recommended table names:

- `landing__appsflyer__installs_report`
- `landing__appsflyer__in_app_events_report`
- `landing__appsflyer__uninstall_events_report`
- `landing__appsflyer__daily_report`

For multi-app loads, include `source_app_id` in every output row.

## Load strategy guidance

- Use `replace` for bounded report windows.
- Use `incremental_merge` when the resource has a stable business key.
- Use quality gates for minimum row count and source/target count comparison.

## Live testing

AppsFlyer live tests require vendor credentials and should be run manually or on a scheduled certification workflow.

Useful checks:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live -k appsflyer
```

## Related docs

- [REST API](rest-api.md)
- [Load strategies](load-strategies.md)
- [Integration tests](testing/integration-tests.md)
- [Connector certification](connector-certification.md)
