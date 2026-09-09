# SimilarWeb connector

Status: implemented for the `keywords` resource through the canonical runtime path.

This connector extracts SimilarWeb website keyword data and loads it through standard `dpone` sinks.

## Supported resource

- `keywords`

## Authentication

Recommended secret:

```json
{
  "endpoint": "https://api.similarweb.com",
  "token": "..."
}
```

`api_key` is supported as a backward-compatible alias, but `token` is preferred.

## Manifest example

```yaml
source:
  type: api
  api_type: similarweb
  connection_id: similarweb
  connection_type: vault
  resource: keywords
  options:
    domains: ["example.com"]
    snapshot_month: "2026-01"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__similarweb__keywords
  strategy:
    mode: replace
```

## Data contract

The `serp_features` field is preserved as a repeated string for BigQuery targets instead of being coerced into a JSON string.

## Validation behavior

- By default, the previous completed month is loaded.
- `snapshot_month` must be earlier than the current month.
- Duplicate `(date, domain, keyword, top_url)` rows fail the run.
- `min_keywords_count` is enforced as a hard threshold.
- Share values outside `[0, 1]` are warnings unless quality policy escalates them.

## Related docs

- [REST API](rest-api.md)
- [Quality metrics](quality-metrics.md)
- [CI/CD](ci-cd.md)
