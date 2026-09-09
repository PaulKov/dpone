# DPONE_RECIPE_ROUTE_CONFLICT

**Audience:** pipeline authors and self-service API clients.

Pipeline initialization received both `recipe` and `route`. They are two
exclusive ways to select one scaffold:

- use `--recipe` when you know the exact versioned recipe;
- use `--route` when dpone should select a beginner recipe from source, sink,
  and strategy capabilities.

Remove one selector and retry. The rejected request does not write partial
authoring files.

```bash
dpone init pipeline --help
```

[Domain-first tutorial](../getting-started/domain-first-airflow.md) ·
[Domain-first error overview](index.md)
