# OpenExchangeRates connector

`OpenExchangeRates` is implemented as `source.type: api` with `api_type: openexchangerates`.

## Canonical resource

- `latest` - latest exchange-rate snapshot.

## Recommended landing table

```text
landing__openexchangerates__latest
```

## Options

- `base` - base currency.
- `symbols` - optional list of symbols.
- `show_alternative` - vendor passthrough option.
- `date` - optional historical date when supported by the configured endpoint.

## Secret shape

```json
{
  "app_id": "...",
  "endpoint": "https://openexchangerates.org/api"
}
```

## Manifest example

```yaml
source:
  type: api
  api_type: openexchangerates
  connection_id: openexchangerates
  connection_type: vault
  resource: latest
  options:
    base: USD
    symbols: [EUR, GBP]

sink:
  type: postgres
  connection_id: postgres_landing
  table:
    schema: landing
    name: landing__openexchangerates__latest
  strategy:
    mode: replace
```
