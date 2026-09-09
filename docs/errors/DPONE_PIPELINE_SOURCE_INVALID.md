# DPONE_PIPELINE_SOURCE_INVALID

**Audience:** pipeline authors.

The discovered `pipeline.yaml` is not a valid mapping or does not satisfy its
selected authoring contract.

Validate and correct the editable source. Domain-first discovery never falls
back to a generated manifest or silently ignores an invalid pipeline.

Inspect the source-specific structured report:

```bash
dpone check <domain>/<pipeline-id> --format json
```

Repair the listed editable `pipeline.yaml`; do not edit generated manifests,
packs, or cache files. Verify canonical compilation and preview:

```bash
dpone check <domain>/<pipeline-id>
dpone airflow preview <pipeline-id>
```

Escalate with the first error code when the source conforms to the published
schema but still cannot compile.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
