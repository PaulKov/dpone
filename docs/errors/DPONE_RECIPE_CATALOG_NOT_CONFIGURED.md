# DPONE_RECIPE_CATALOG_NOT_CONFIGURED

An exact external recipe was requested, but the project has no usable
`authoring.recipe_catalog` configuration in `dpone.yaml`.

## Fix

Add a project-relative catalog path and at least one trusted catalog id, then run:

```bash
dpone recipe validate
dpone recipe list
```

Built-in recipes do not require an external catalog. See
[Recipes, profiles, and components](../airflow-recipes.md#project-trust-configuration).
