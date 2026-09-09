# DAG debugging

This document contains practical recipes for debugging dependencies.

## List edges

```bash
dpone dag list-edges root.yaml --with-refs --with-groups --with-reasons
```

## Explain a specific edge

```bash
dpone dag explain-edge root.yaml --from fileA.yaml#t1 --to fileB.yaml#t2
```

## Explain node neighborhood

```bash
dpone dag explain-node root.yaml --task fileA.yaml#t1 --direction both --output full
```

## DAG report

```bash
dpone dag report root.yaml --format md --preset ci --fail-on both
```



## JSON output envelope

All `dpone dag explain-*` and `dpone dag report --format json` commands now emit a common envelope:

- `kind` — command/view kind (`dag.explain_edge`, `dag.explain_node_e2e`, `dag.report`, …)
- `root`, `base_path`, `task_count` — shared DAG context
- `options` — normalized command options relevant to the payload
- `result` or `report` — command-specific body

This makes JSON outputs easier to diff, validate and consume from CI tooling.
