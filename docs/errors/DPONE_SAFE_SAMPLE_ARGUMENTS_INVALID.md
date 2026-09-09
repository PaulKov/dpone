# DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID

The safe-sample command combination is invalid for the beginner or live path
(missing required flags, unsupported target, or conflicting options). No network
or credential I/O is performed.

## Fix

Use the frozen five-command path from
[First Airflow DAG](../getting-started/first-airflow-dag.md). For the live
handoff, keep `--sample` and `--target temporary` and avoid adding a sixth
beginner command or ad-hoc connection overrides. The emitted safe fix preserves
an existing positive sample budget, environment, run ID, and selectors; review
the command before executing it.
