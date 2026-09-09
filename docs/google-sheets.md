# Google Sheets connector

The Google Sheets connector extracts worksheet data through the canonical `dpone.runtime.*` source architecture while preserving legacy authentication modes.

## Authentication modes

Supported modes:

- Service account JSON from Vault or environment variables.
- Top-level service account fields in a secret.
- Published CSV for lightweight local smoke tests.

Production runs should use service-account credentials and explicit spreadsheet identifiers.

## Manifest example

```yaml
source:
  type: api
  api_type: google_sheets
  connection_id: google_sheets
  connection_type: vault
  resource: worksheet
  options:
    spreadsheet_id: "1abc..."
    worksheet_title: Orders
    range: A:Z
    limit_rows: 100000

sink:
  type: postgres
  connection_id: postgres_landing
  table:
    schema: landing
    name: landing__google_sheets__orders
  strategy:
    mode: replace
```

## Options

- `spreadsheet_id` or `spreadsheet_url`.
- `worksheet_title`, `worksheet_index`, or `worksheet_gid`.
- `range` for bounded worksheet reads.
- `limit_rows` and `limit_columns` for smoke or development runs.
- Optional metadata columns such as source spreadsheet and worksheet identifiers.

## Schema behavior

Rows are read as worksheet strings. Headers are normalized to `snake_case`, empty rows are skipped, and the output schema is inferred from the actual header set.

## Dependencies

Install with the GCP extra when using Google authentication:

```bash
pip install "dpone[gcp]"
```

## Related docs

- [REST API](rest-api.md)
- [Load strategies](load-strategies.md)
- [Integration tests](testing/integration-tests.md)
