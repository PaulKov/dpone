# Google Ads connector

The Google Ads connector extracts reporting data through the Google Ads API and loads it using standard `dpone` sinks.

## Authentication

Use the standard credentials flow with a Google Ads developer token, OAuth client credentials, and refresh token.

Typical secret shape:

```json
{
  "developer_token": "...",
  "client_id": "...",
  "client_secret": "...",
  "refresh_token": "...",
  "login_customer_id": "1234567890"
}
```

## Manifest example

```yaml
source:
  type: api
  api_type: google_ads
  connection_id: google_ads
  connection_type: vault
  resource: campaign_performance
  options:
    customer_id: "1234567890"
    date_from: "2026-01-01"
    date_to: "2026-01-02"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__google_ads__campaign_performance
  strategy:
    mode: replace
```

## Strategy guidance

- Use `replace` for bounded reporting windows.
- Use `incremental_merge` when a report has a stable composite key.
- Add freshness, row-count, and accepted-values quality gates for production runs.

## Testing

Google Ads live tests require vendor credentials and should run as manual certification jobs.

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live -k google_ads
```

## Related docs

- [REST API](rest-api.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Connector certification](connector-certification.md)
