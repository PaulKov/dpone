# FastTrack connector

Status: implemented as a canonical REST provider.

FastTrack support uses the standard `source.type: api` runtime with provider-specific resources, authentication, and landing schema handling.

## Runtime modules

- Connector: `dpone.runtime.connectors.api.fasttrack`
- Source: `dpone.runtime.sources.api.fasttrack`
- Strategies: `dpone.runtime.strategies.api.fasttrack`
- Registration: `dpone.runtime.api_registry`

## Authentication modes

FastTrack supports two credential styles:

- Resource-specific tokens such as `dashboard_bot_key` and `flex_x_token`.
- Generic fallback token through `token` or backward-compatible `api_key`.

Recommended Vault secret:

```json
{
  "endpoint": "https://api.example.com",
  "dashboard_bot_key": "...",
  "flex_x_token": "..."
}
```

## Landing philosophy

FastTrack landing follows an ELT-safe pattern:

- Preserve provider fields.
- Do not perform semantic rename/drop in the landing layer.
- Normalize column names mechanically to warehouse-safe `snake_case`.
- Keep business normalization in downstream core models.

Temporal typing is enabled by default and can be controlled with provider options when a consumer needs raw string timestamps.

## Manifest example

```yaml
source:
  type: api
  api_type: fasttrack
  connection_id: fasttrack
  connection_type: vault
  resource: chat_sessions
  options:
    date_from: "2026-01-01"
    date_to: "2026-01-02"

sink:
  type: bigquery
  connection_id: bigquery_landing
  table:
    dataset: landing
    name: landing__fasttrack__chat_sessions
  strategy:
    mode: incremental_merge
    unique_key: [uuid_polzovatelya, data_nachala]
```

## Testing

FastTrack has two useful test layers:

- Mock integration tests with a local HTTP server.
- Manual live tests against the vendor API through Vault credentials.

```bash
uv run pytest tests/integration -k fasttrack
```

## Related docs

- [REST API](rest-api.md)
- [Integration tests](testing/integration-tests.md)
- [Connector certification](connector-certification.md)
