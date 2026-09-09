# Variant C manifests

Variant C introduces **batch manifests** (one YAML describing many processes) and overrides/variables to reduce repetition.

See schema: [src/dpone/schema/etl-batch-manifest.schema.json](https://github.com/PaulKov/dpone/blob/master/src/dpone/schema/etl-batch-manifest.schema.json).

Key ideas:

- `layer_defaults`: root defaults for the batch
- `schemas`: one schema (dataset) can contain many `tables`
- `vars`: variables usable in naming templates via `{{ var_name }}`
