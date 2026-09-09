# Nested Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add dlt-like nested object normalization with deterministic `__dpone__row_id`, `__dpone__parent_row_id`, `__dpone__root_row_id`, and `__dpone__list_index` support.

**Architecture:** Keep lineage identity in `dpone.runtime.lineage`; add focused normalization services in `dpone.runtime.normalization`; integrate through `ETLProcessor` as a DI collaborator. Normalized root/child tables use the existing sink contract, schema evolution, runtime contracts, and load audit lifecycle.

**Tech Stack:** Python 3.11+, dataclasses, existing `LoadPayload`/artifact contracts, manifest JSON schemas, MkDocs docs.

---

## Tasks

- [x] Add tests for service-level nested split, stable IDs, processor multi-table load, and manifest schema exposure.
- [x] Add `NestedNormalizationOptions`, `NormalizedTable`, `NormalizationResult`, and `NestedNormalizationService`.
- [x] Integrate nested normalization in `ETLProcessor` without changing the default single-table path.
- [x] Add `normalization.nested` to single and batch manifest JSON schemas.
- [x] Document nested normalization, algorithm, config, examples, and runbooks.
- [x] Link docs from MkDocs nav, documentation index, README, and load lineage guide.
- [x] Run targeted lineage/normalization contract tests.

## Acceptance criteria

- Nested dict values become child object tables.
- Nested list-of-object values become child repeated tables with stable list indexes.
- Nested list-of-scalar values become child rows using `scalar_list_value_column`.
- Child rows can be joined to parent/root rows through canonical `__dpone__*` columns.
- Source state advances only after all normalized tables have loaded successfully.
- Documentation explains production behavior and current limitations.
