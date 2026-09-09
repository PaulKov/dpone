# DPONE_RECIPE_OPTION_UNSUPPORTED

The selected built-in recipe does not accept `--profile` or `--answers`.
Built-in Phase 1 recipes are immutable scaffold data and have no external
parameter contract.

## Fix

Remove `--profile` and `--answers` from the command:

```bash
dpone init pipeline <pipeline-id> --recipe <built-in-recipe> --airflow
```

For parameterized authoring, choose an exact external recipe ref from
`dpone recipe list`, inspect it with `dpone recipe show`, and provide only the
parameters allowed by that recipe.
