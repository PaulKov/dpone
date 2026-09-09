# DPONE_PIPELINE_COMPILATION_FAILED

**Audience:** pipeline authors.

The primary pipeline source was found but could not be compiled into the
canonical manifest IR.

The structured message includes the safe inner compiler code when the
canonical authoring compiler provides one. Use that code to correct the
editable source:

```bash
dpone check <domain>/<pipeline-id> --format json
```

Inspect `errors[0].message`, then correct the primary source or one of its
declared folder dependencies. Repeating the same command without changing the
source cannot resolve the error. Do not edit generated packs or indexes.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
