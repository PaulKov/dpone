# DPONE_PROJECT_CONFIG_INVALID

**Audience:** repository maintainers and platform engineers.

`dpone.yaml` could not be read as a valid project configuration.

Correct the project schema and layout policy before initializing, checking, or
previewing pipelines. Invalid explicit layout configuration fails closed and
does not silently become flat mode.

Inspect the schema:

```bash
dpone gitops schema show dpone.project.v1
```

Repair `dpone.yaml` as bounded unique-key YAML. In particular,
`airflow.enabled` must be a JSON/YAML boolean, not the string `"false"`.
Then rerun:

```bash
dpone check . --format json
```

Exit `0` verifies the project authority. If `dpone.yaml` was lost while
authoring files remain, do not run an initializer for another layout; follow
the explicit layout migration runbook.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
