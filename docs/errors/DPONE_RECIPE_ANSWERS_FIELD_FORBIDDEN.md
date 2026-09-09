# DPONE_RECIPE_ANSWERS_FIELD_FORBIDDEN

**Audience:** pipeline authors and recipe maintainers.

The `--answers` file contains a key outside the bounded scaffold allowlist.

Use only documented non-secret authoring fields. Passwords, tokens, Vault
paths, runtime infrastructure, releases, and deployments cannot be supplied by
a pipeline answer file.

Inspect the recipe's declared parameters and remove the forbidden key:

```bash
dpone recipe show <recipe>
dpone init pipeline --help
```

The rejected request creates no files. Use logical `connection_ref` values for
connections and leave platform bindings to the environment configuration.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
