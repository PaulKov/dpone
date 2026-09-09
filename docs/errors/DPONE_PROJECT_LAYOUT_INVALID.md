# DPONE_PROJECT_LAYOUT_INVALID

**Audience:** repository maintainers.

The requested project layout is not supported.

Use:

```bash
dpone init project --layout flat
dpone init project --layout domain-first
```

The persisted schema value is `domain_first`; the CLI spelling is
`domain-first`.

Run only the command matching the intended layout in an empty repository.
Existing authoring authority requires the explicit migration described in the
[compatibility guide](../compatibility.md#project-layout-compatibility).

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
