# DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD

A recipe/profile/component pin observed during compilation differed from the
closure materialized into the workload pack. Preview fails closed and does not
publish a current release/deployment.

## Fix

Stop concurrent artifact edits, restore or publish the intended immutable
version, then rerun:

```bash
dpone recipe validate
dpone check pipelines/<pipeline-id>
dpone airflow preview <pipeline-id>
```

Do not retry against a changing worktree or rewrite an existing digest.
