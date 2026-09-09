# DPONE_PIPELINE_KEY_INVALID

**Audience:** pipeline authors.

`--key` is not a safe connector-neutral field identifier.

Pass a non-secret identifier such as `order_id`. SQL expressions, paths,
templates, shell text, and credentials are not accepted.

Inspect the exact scaffold syntax, then rerun with a canonical field name:

```bash
dpone init pipeline --help
```

The rejected request creates no files. See the
[domain-first Airflow guide](../getting-started/domain-first-airflow.md) for a
complete route example.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
