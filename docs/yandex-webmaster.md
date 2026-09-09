# Yandex Webmaster connector

The Yandex Webmaster connector extracts host metrics and related reports through the canonical REST provider runtime.

## Authentication

Use the standard credentials flow. Recommended secret:

```json
{
  "endpoint": "https://api.webmaster.yandex.net",
  "token": "...",
  "user_id": "..."
}
```

## Manifest example

```yaml
source:
  type: api
  api_type: yandex_webmaster
  connection_id: yandex_webmaster
  connection_type: vault
  resource: host_metrics
  options:
    host_id: "https:example.com:443"
    date_from: "2026-01-01"
    date_to: "2026-01-07"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__yandex_webmaster__host_metrics
  strategy:
    mode: replace
```

## Strategy guidance

Use `replace` for bounded vendor windows. Use `incremental_merge` only when the selected resource exposes a stable key and the manifest defines it explicitly.

## Testing

Yandex Webmaster live tests require vendor credentials and should run manually or on scheduled certification workflows.

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live -k yandex_webmaster
```

## Related docs

- [REST API](rest-api.md)
- [Integration tests](testing/integration-tests.md)
- [Connector certification](connector-certification.md)
