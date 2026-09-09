# DPONE_AUTHORING_MIGRATION_RECIPE_UNSUPPORTED

The pipeline is authored through an immutable recipe reference. The migration
tool refuses to materialize that recipe because doing so would discard its
version, digest, and update policy. Keep the recipe source or create an explicit
replacement in a reviewed change.

See [authoring-mode migration](../airflow-authoring-migration.md).
