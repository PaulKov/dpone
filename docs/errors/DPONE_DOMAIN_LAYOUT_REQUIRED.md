# DPONE_DOMAIN_LAYOUT_REQUIRED

**Audience:** domain owners and platform engineers.

`dpone init domain` was run in a flat project. Domain ownership files are an
authoring authority only when `dpone.yaml` selects `layout.mode:
domain_first`.

For a new repository, initialize domain-first layout before any flat authoring
files exist. An existing flat project's `dpone.yaml` is user-owned and is not
rewritten by `dpone init project`; create a reviewed migration plan instead.
dpone does not move files or change layout authority implicitly.

For a new empty directory, follow the
[domain-first Airflow guide](../getting-started/domain-first-airflow.md). For a
non-empty flat repository, stop here until the migration is reviewed.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
