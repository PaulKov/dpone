# ADR 0015: One compiler for classic, flow, and folder authoring

## Status

Accepted.

## Context

ADR 0006 establishes one editable authoring source and a generated canonical
manifest IR. Phase 1 introduced a beginner scaffold shaped as
`kind: dpone.batch.v1` plus `processes`, while the established batch compiler
accepts `defaults` plus `schemas`. The facade's shallow check accepted that file,
but the canonical manifest loader rejected it. Adding flow and folder syntax on
top of separate readiness parsing would preserve the false-success condition
and create multiple execution models.

## Decision

Classic, flow, and folder authoring use one build-plane `AuthoringCompiler`.
The compiler validates source authority, normalizes the selected mode into an
in-memory `dpone.batch.v1` mapping, delegates process compilation to the existing
batch compiler, and computes one semantic fingerprint from compiled process
semantics.

- Classic authoring is the established batch grammar and remains valid.
- Flow authoring is `dpone.flow.v1` and is never executed directly.
- Folder authoring is a bounded composition form of `dpone.flow.v1`; its root is
  the only primary source and fragment paths remain below that root.
- Generated canonical mappings are not written as editable source files.
- Airflow provider parsing never imports or invokes the authoring compiler.
- The released Phase 1 `batch + processes` shape is accepted only by a read
  compatibility adapter. New scaffolds do not emit it.
- The earlier safe-sample `schema: dpone.pipeline.v1` plus `processes` shape is
  handled by the same read adapter and is never emitted by new scaffolds.
- A source with both `processes` and `schemas`, or a kind/mode mismatch, fails
  closed before artifact creation.
- Loaded manifests expose canonical `kind: dpone.batch.v1`; `source_kind` and
  deprecation aliases retain authoring provenance without changing runtime
  dispatch semantics.

Semantic identity excludes formatting, comments, source path, timestamps, and
recipe provenance. Equivalent classic, flow, and folder sources produce the
same semantic fingerprint. Their source fingerprints remain distinct.

## Consequences

- `dpone check`, preview, GitOps build, and the ordinary manifest loader can no
  longer disagree about whether an authoring source is valid.
- Phase 2 features extend a single normalization boundary instead of adding a
  runtime or Airflow-specific DSL engine.
- Source maps and deprecation metadata are required for actionable errors.
- Folder discovery, selectors, recipes/components, step visibility, and
  hermetic tests must operate on compiler inputs/outputs, not bypass them.
- The compatibility adapter must remain for at least two minor releases and
  twelve months; removal requires explicit migration tooling and evidence.

## Alternatives rejected

- Keep `processes` as a second meaning of `dpone.batch.v1`: rejected because one
  kind would have two incompatible canonical grammars.
- Compile authoring in the Airflow provider: rejected because parsing must stay
  static and side-effect-free.
- Generate and commit a canonical batch beside flow/folder source: rejected
  because it creates two editable sources and drift.
- Introduce a new runtime IR: rejected because `dpone.batch.v1`, workload packs,
  and release/deployment contracts already own execution semantics.

## References

- [Feature design: Airflow self-service Authoring v1](../feature-design-airflow-authoring-v1.md)
- [ADR 0002: Variant C batch manifest](0002-variant-c-batch-manifest.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
