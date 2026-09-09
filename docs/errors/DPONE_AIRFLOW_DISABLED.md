# DPONE_AIRFLOW_DISABLED

**Audience:** pipeline authors and Airflow operators.

The pipeline passed static authoring validation, but its primary source sets
`metadata.airflow: false`. dpone therefore does not create a DAG or workload
pack for it.

If no matching preview is current, the command creates no cache artifact. If a
verified current local preview advertises only this pipeline, dpone atomically
promotes an empty immutable preview projection before returning the error. This
immutable retirement prevents Airflow from continuing to show a stale DAG.
The disabled preview is still a failed operation; retirement is not success.

When current contains multiple workloads, dpone preserves it rather than
removing unrelated DAGs. Run the exact `dpone airflow preview . --exclude
id:<pipeline>` fix shown by the structured error to refresh that project view.

## Next step

If the pipeline must appear in Airflow, change the editable primary authoring
source to:

```yaml
metadata:
  airflow: true
```

Then run:

```bash
dpone check <pipeline>
dpone airflow preview <pipeline>
```

Keep `metadata.airflow: false` when the workload is intentionally runtime-only.
See the [domain-first Airflow guide](../getting-started/domain-first-airflow.md)
for the complete authoring flow.

[Domain-first error overview](index.md) ·
[Return to the domain-first tutorial](../getting-started/domain-first-airflow.md)
