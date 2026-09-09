# ADR 0006: Authority and canonical IR

## Status

Accepted.

## Context

dpone must support classic manifests, short flow files, folder authoring, and dbt-authored publishing without creating multiple editable sources for one pipeline.

## Decision

Each pipeline has exactly one primary authoring source: `classic`, `flow`, `folder`, or `dbt`. The canonical manifest IR is generated or held in memory and is never an independently editable authoring source. For `dbt`, dpone reads resolved `node.config.meta.dpone.publish` from `manifest.json`; generated manifests and DAG specs are runtime artifacts, not editable source files. Domain catalogs with `dags:` own orchestration intent for non-dbt modes, while runtime execution consumes compiled workload catalogs and packs.

Changing authoring mode requires an explicit migration command.

## Consequences

- CI can reject duplicate primary sources.
- Generated manifests cannot drift from user-owned sources.
- Future `dpone.flow.v1` remains an authoring convenience, not a second runtime engine.
