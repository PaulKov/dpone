# DPONE_PIPELINE_PROCESS_NOT_FOUND

The value passed to `--selector` did not identify exactly one process in the
selected pipeline. Use a process `name` or its exact `selector` value, then run
the bounded temporary sample again:

```bash
dpone run pipelines/orders_daily --selector load_orders \
  --sample 1000 --target temporary
```

The process selector is distinct from the project workload expression passed
through `--select`.
