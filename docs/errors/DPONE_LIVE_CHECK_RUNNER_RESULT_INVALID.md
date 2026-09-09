# DPONE_LIVE_CHECK_RUNNER_RESULT_INVALID

The configured bounded live preflight runner returned no valid probe results.

Each result must name the `connection_ref`, probe name, status, runner, and
whether the probe used network, secrets, or source queries.

## Fix

Update the runner to return at least one probe with this shape:

```yaml
connection_ref: mssql_dev
probe: credential_resolution
status: passed
runner: configured
network: true
secrets: true
source_queries: false
```

Supported probe names are `credential_resolution`, `bounded_source_probe`, and
`bounded_sink_probe`. Supported statuses are `passed` and `failed`.
