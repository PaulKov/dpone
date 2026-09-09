# DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED

The source bytes changed after the migration plan was computed and before the
guarded write finished. dpone re-checks the SHA-256 under a confined directory
descriptor immediately before the atomic replace, so a concurrent edit is kept
intact and the migration does not overwrite it.

Review the concurrent change, rerun `--plan`, and apply the newly computed plan.
No stale plan is written.

See [authoring-mode migration](../airflow-authoring-migration.md).
