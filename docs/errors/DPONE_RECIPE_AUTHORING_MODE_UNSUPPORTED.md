# DPONE_RECIPE_AUTHORING_MODE_UNSUPPORTED

External declarative recipes currently compile through the `flow` authoring
mode. The requested authoring mode cannot preserve the pinned recipe closure.

## Fix

Create the pipeline with flow authoring:

```bash
dpone init pipeline <pipeline-id> \
  --recipe <exact-recipe-ref> \
  --authoring flow \
  --airflow
```

Run `dpone init pipeline --help` to inspect the supported options. Existing
pipelines change authoring mode only through the explicit authoring migration
command; do not copy generated canonical manifests back into source files.
