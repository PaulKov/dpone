# DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED

**Audience:** repository maintainers and platform engineers.

Editable authoring sources already exist in another, mixed, or unsafe layout,
so dpone cannot safely create a conflicting `dpone.yaml` or select an
incomplete subset of the project.

Do not copy the same pipeline into both layouts. Plan a reviewed migration that
moves each primary source and preserves its canonical semantic fingerprint.
Automatic flat-to-domain-first migration is not available in this release.

Existing flat projects remain supported; continue using the flat layout until
the migration has been reviewed and applied atomically.

Inspect the configured policy and both possible authorities:

```bash
dpone gitops schema show dpone.project.v1
dpone check . --format json
```

After a reviewed migration leaves exactly one authority, rerun the intended
command and verify it is idempotent:

```bash
dpone init project --airflow --layout domain-first
dpone init project --airflow --layout domain-first
```

The second run must report only `no_op`. Use `--layout flat` in both commands
when flat is the intended authority.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
