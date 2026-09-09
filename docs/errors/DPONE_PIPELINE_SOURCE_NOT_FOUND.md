# DPONE_PIPELINE_SOURCE_NOT_FOUND

**Audience:** pipeline authors and CI maintainers.

The requested pipeline id or domain-qualified locator does not resolve to a
primary authoring source.

List the configured layout and check the exact source path. For domain-first:

```text
<layout.root>/<domain>/pipelines/<pipeline-id>/pipeline.yaml
```

Inspect project discovery:

```bash
dpone check . --format json
```

Create a missing source through the scaffold rather than a partial directory:

```bash
dpone init pipeline <pipeline-id> \
  --domain <domain> \
  --route <source>:<sink>:<strategy> \
  --from <source-ref> \
  --to <sink-ref> \
  --key <column>
dpone check <domain>/<pipeline-id>
```

If the source exists under another domain or layout, use an explicit reviewed
migration; do not duplicate it.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
