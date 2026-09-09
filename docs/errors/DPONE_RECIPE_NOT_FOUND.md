# DPONE_RECIPE_NOT_FOUND

The requested built-in recipe is unknown, or the exact external recipe ref is
not present in the configured catalog.

## Fix

```bash
dpone recipe list
```

Select an exact listed ref. Platform owners should publish a new immutable
catalog entry instead of editing another version's identity.
