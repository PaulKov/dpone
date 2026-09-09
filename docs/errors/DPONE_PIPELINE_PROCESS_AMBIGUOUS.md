# DPONE_PIPELINE_PROCESS_AMBIGUOUS

The selected pipeline contains more than one process, so a safe sample cannot
choose a process implicitly. Pass the process name or selector explicitly:

```bash
dpone run pipelines/orders_daily --selector load_orders \
  --sample 1000 --target temporary
```

Run `dpone check pipelines/orders_daily` first if the expected process is not
visible in the compiled authoring source.
