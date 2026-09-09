# DPONE_RECIPE_REF_INVALID

The recipe or profile ref is not exact `id@MAJOR.MINOR.PATCH` syntax. Mutable
aliases such as `latest` and version ranges are intentionally unsupported.

## Fix

```bash
dpone recipe list
dpone recipe show <exact-recipe-ref>
```

Choose one exact listed ref and rerun the original command. This is a CLI/config
usage error and exits `2`.
