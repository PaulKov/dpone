# DPONE_RECIPE_ANSWERS_INVALID

**Audience:** pipeline authors and recipe maintainers.

The `--answers` file is missing, malformed, too large, outside the project root,
or otherwise violates the confined declarative answer contract.

Place a bounded YAML mapping under the project root. Do not use symlinks,
traversal, executable templates, or secret values.

Inspect the recipe contract and keep the answer file inside the project:

```bash
dpone recipe show <recipe>
```

Repair only declared non-secret parameters, then rerun the original scaffold:

```bash
dpone init pipeline <pipeline-id> \
  --domain <domain> \
  --recipe <recipe> \
  --answers <project-relative-answers.yaml>
dpone check <domain>/<pipeline-id>
```

The failed attempt creates no partial authoring files. Use logical
`connection_ref` values; credentials and Vault paths do not belong in answers.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
