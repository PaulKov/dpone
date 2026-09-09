# DPONE_RECIPE_CATALOG_INVALID

The configured catalog is malformed, too large, contains duplicate kind/ref
identities, unsupported fields, or an unsafe path.

## Fix

Validate the YAML against `dpone.recipe-catalog.v1`, keep every artifact ref
exact and unique, and run:

```bash
dpone recipe validate
```

The command is read-only. It does not repair or rewrite catalog entries.
