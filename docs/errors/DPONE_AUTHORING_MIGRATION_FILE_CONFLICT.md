# DPONE_AUTHORING_MIGRATION_FILE_CONFLICT

Folder migration needs a sibling `processes.yaml`, but a different user-owned
file already exists there. Reconcile or rename that file manually, then rerun
`--plan`. dpone never overwrites or deletes conflicting content.

See [authoring-mode migration](../airflow-authoring-migration.md).
