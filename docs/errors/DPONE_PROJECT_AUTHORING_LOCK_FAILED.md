# DPONE_PROJECT_AUTHORING_LOCK_FAILED

**Audience:** pipeline authors and platform engineers.

dpone could not safely hold the local project-wide authoring lock. The command
does not report successful authoring. Inspect any returned scaffold receipt
before retrying.

Confirm that the destination is on a local filesystem that supports advisory
locks. Existing project roots must be real directories. For `dpone init dbt`,
the project may be absent but its parent directory must already exist. Finish
or stop the other authoring operation, then rerun the original init command.
Do not bypass the lock by copying generated files manually.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)

[Native dbt starter](../dbt-starter-authoring.md)
