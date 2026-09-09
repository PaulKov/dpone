# DPONE_PIPELINE_DOMAIN_MISMATCH

**Audience:** pipeline authors and recipe maintainers.

Either the domain declared by a pipeline source does not match the directory
that owns it, or an external declarative recipe pins a domain that conflicts
with the requested `--domain`.

Make the source metadata match
`<layout.root>/<domain>/pipelines/<pipeline-id>/pipeline.yaml`, or move the
source through an explicit reviewed migration.

For an external recipe, inspect the recipe provenance and choose the pinned
domain or a different recipe. dpone rejects the conflict before writing any
pipeline source and never rewrites recipe authority.

Inspect the authoritative source:

```bash
dpone check <domain>/<pipeline-id> --format json
```

For an in-place correction, set `metadata.domain` to the directory domain and
rerun:

```bash
dpone check <domain>/<pipeline-id>
dpone airflow preview <pipeline-id>
```

Both commands must exit `0`. Moving a source to another domain is an
authoring migration and must update ownership and references in one reviewed
change.

[Domain-first error overview](index.md) ·
[Return to the domain-first tutorial](../getting-started/domain-first-airflow.md)
