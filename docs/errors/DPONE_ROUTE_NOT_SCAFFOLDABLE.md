# DPONE_ROUTE_NOT_SCAFFOLDABLE

The runtime route exists, but no beginner recipe can scaffold it.

Run:

```bash
dpone recipe list --source <source> --sink <sink> --strategy <strategy>
```

The response includes the matched runtime route with
`beginner.recipe_available: false`, even when `recipes` is empty. Select a route
whose recipe has `scaffoldable: true`, or have the platform team publish an
approved declarative recipe. No partial authoring files are written.
