# Mindbox connector

Mindbox support is implemented as a canonical Pull API and async export provider through `source.type: api` and `api_type: mindbox`.

## Supported resources

Typical resources include:

- `orders`
- `customers`
- `actions`
- `promo_actions`
- `points_changes`

Each resource defines its own date-window semantics and output schema.

## Authentication

Recommended Vault secret:

```json
{
  "endpoint": "https://api.mindbox.ru",
  "token": "...",
  "endpoint_id": "..."
}
```

`api_key` is supported as a backward-compatible alias, but `token` is preferred.

## Manifest example

```yaml
source:
  type: api
  api_type: mindbox
  connection_id: mindbox
  connection_type: vault
  resource: actions
  options:
    since_datetime_utc: "2026-01-01T00:00:00Z"
    till_datetime_utc: "2026-01-01T01:00:00Z"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__mindbox__actions
  strategy:
    mode: replace
```

## Window semantics

- Event-like resources use `sinceDateTimeUtc` and `tillDateTimeUtc`.
- Day-based resources use `sinceDate` and `tillDate`.
- If explicit datetime bounds are omitted, the runtime falls back to day-based windows controlled by `utc_boundary_time`.

Default `utc_boundary_time` keeps upstream-compatible behavior for business-day boundaries.

## Dependencies

Streaming JSON parsing uses `ijson` when available. Without it, the connector falls back to standard JSON loading, which is acceptable for tests but not recommended for large production exports.

## Testing

Mindbox live tests require vendor credentials and should run as manual or scheduled certification jobs.

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live -k mindbox
```
