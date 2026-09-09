# DPONE_PROJECT_AUTHORING_LOCK_FAILED

**Audience:** pipeline authors and platform engineers.

dpone could not acquire the local project-wide authoring lock. The operation
stops before discovery or scaffold writes so two concurrent commands cannot
create duplicate project identities.

Confirm that the project root is a real local directory on a filesystem that
supports advisory locks. Finish or stop the other authoring operation, then
rerun the original `dpone init pipeline` command. Do not bypass the lock by
copying generated files manually.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
