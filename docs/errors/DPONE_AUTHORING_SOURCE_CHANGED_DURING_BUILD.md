# DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD

**Audience:** pipeline authors and Airflow platform engineers.

At least one authoring fragment changed between canonical compilation and pack
materialization. No release was published. Finish the concurrent edit, rerun
`dpone check <pipeline>`, and then rerun `dpone airflow preview <pipeline>`.

Do not edit release or pack files to bypass this integrity check.

Verify that preview succeeds and that its output names a non-runnable preview
plus `.dpone-cache/current/airflow-index.json`. A concurrent project preview is
preserved; retrying never removes unrelated DAGs.

[Domain-first error overview](index.md) ·
[Domain-first operations](../domain-first-operations.md)
