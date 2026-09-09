# DPONE_RECIPE_ARTIFACT_INVALID

A pinned recipe artifact failed its identity, schema, lifecycle, domain, path,
file-type, or bounded-YAML contract.

## Fix

Check that schema/id/version match the exact pin, only supported fields are
present, paths remain beneath the project root, and the artifact is a regular
non-symlink YAML file. Then run `dpone recipe pin <path>` and
`dpone recipe validate` before selecting the version again.
