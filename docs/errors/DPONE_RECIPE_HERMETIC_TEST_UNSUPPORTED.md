# DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED

At least one process expanded by the selected external recipe uses behavior
that the credential-free starter test cannot execute safely. Examples include
`strategy: auto`, source SQL, transforms, `only_new_rows: true`, or a merge
duplicate policy other than `fail`.

No pipeline, test, fixture, or catalog file was created.

## Fix

Inspect the exact recipe contract:

```bash
dpone recipe show <recipe-id>@<version>
```

Choose a recipe whose selected process uses `full_refresh`, ordinary
`incremental_append`, or `incremental_merge` with a deterministic unique key
and `duplicate_policy: fail`. Connector SQL and other integration behavior
belong in route tests rather than a hermetic starter test.
