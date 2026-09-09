# DPONE_SELECTION_RUN_REQUIRES_SAFE_SAMPLE

Multi-workload project execution is intentionally limited to bounded temporary
samples in selector v1. Use:

```bash
dpone run . --select 'domain:sales' --sample 1000 --target temporary
```

Production multi-workload execution remains owned by published Airflow DAGs.
