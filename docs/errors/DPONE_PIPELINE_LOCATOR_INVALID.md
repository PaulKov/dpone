# DPONE_PIPELINE_LOCATOR_INVALID

**Audience:** pipeline authors.

`--from` or `--to` does not match
`connection_ref:schema.table`. Locator values are logical references and must
not contain credentials.

Example:

```bash
--from mssql_dev:dbo.orders --to clickhouse_dev:analytics.orders
```

Run `dpone init pipeline --help`, correct the locator, and repeat the original
command. The rejected request creates no files. See the
[domain-first Airflow guide](../getting-started/domain-first-airflow.md) for a
complete route example.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
