# CBR connector

The CBR connector reads the public XML API of the Central Bank of Russia.

## Supported resource

- `xml_daily_asp` - daily exchange rates XML feed.

## Authentication

No Vault secret is required because the API is public.

## Manifest example

```yaml
source:
  type: api
  api_type: cbr
  resource: xml_daily_asp
  options:
    start_date: "2026-01-01"
    end_date: "2026-01-03"

sink:
  type: postgres
  connection_id: postgres_landing
  table:
    schema: landing
    name: landing__cbr__exchange_rates
  strategy:
    mode: replace
```

## Options

- `day` - a single day for `full_refresh`.
- `start_date`, `end_date` - explicit date range.
- `lookback_days` - reload window for incremental merge.
- `full_refresh_days` - initial window when no state exists.

## State behavior

For incremental loads, state is tracked by requested day rather than by `as_of_date` from the XML payload. This matters because weekends and holidays may return the previous business day.

## Testing

The connector can be smoke-tested without secrets:

```bash
uv run pytest tests/integration -m integration_live -k cbr
```

Keep public API smoke checks manual or scheduled so normal pull requests do not depend on vendor availability.
