# DPONE_ROUTE_RECIPE_AMBIGUOUS

More than one beginner recipe claims the same route and none is marked as the
default.

Use one exact recipe explicitly:

```bash
dpone init pipeline <pipeline-id> --recipe <recipe-ref>
```

Platform owners should mark exactly one recipe as the route default. dpone
fails before filesystem writes instead of choosing nondeterministically.
