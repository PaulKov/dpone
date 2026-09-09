# ADR 0016: Content-pinned declarative recipes

## Status

Accepted.

## Context

ADR 0006 establishes one editable pipeline source and generated canonical IR.
ADR 0015 establishes one compiler for classic, flow, and folder authoring.
Phase 1 built-in recipes are package-owned scaffold data. Platform teams need to
publish governed route patterns independently, but mutable catalog lookup,
generated lockfiles, arbitrary templates, or scheduler Python would weaken
source authority, reproducibility, and Airflow parse safety.

## Decision

dpone adds a local, versioned, declarative recipe catalog for build-plane
discovery. `dpone init pipeline` resolves an exact recipe version from one
trusted configured catalog and writes a content-pinned recipe block into the
single primary flow source.

- Recipe, zero-or-one selected profile, and every ordered component are
  project-confined YAML artifacts identified by exact SemVer and byte SHA-256.
- The primary source pins the recipe and selects an exact `profile_ref`; the
  recipe is the sole profile/component path-and-digest authority through
  `profiles[]`, `default_profile_ref`, and `components[]`. Ordinary compilation
  never resolves mutable catalog state.
- Recipes use a restricted JSON Schema parameter contract. User answers are
  bounded, allowlisted, scalar, and cannot contain credentials or secrets.
- Components expand into process mappings using whole-value `$param` and
  `$context` placeholders. String interpolation, Jinja, Python, shell, dynamic
  imports, URLs, and executable hooks are forbidden.
- Flow recipe sources contain exactly one of `recipe` or `processes`. Classic
  and folder sources remain explicit in v1.
- The injected resolver returns expanded process mappings only. The existing
  `AuthoringCompiler` remains the sole owner that delegates those mappings to
  the canonical batch compiler.
- Recipe domain, primary-source metadata domain, and generated domain-catalog
  identity must be equal. Recipe domain is the single originating authority.
- Recipe parsing, parameter merging, and component expansion are build-time
  only. Packs materialize a fingerprinted canonical runtime manifest; runtime
  executes canonical IR and never parses recipe/profile/component YAML.
- Source fingerprint includes the exact pinned closure; semantic fingerprint
  includes only canonical process semantics.
- Compact packs and safe-sample verification retain the complete closure in
  canonical `(kind, path, sha256)` order and fail closed on drift.
- Airflow provider parsing consumes only materialized index/spec/pack artifacts
  and never imports, discovers, fetches, or executes recipe logic.
- Built-in unversioned recipe aliases remain compatible. There is no `latest`,
  version range, implicit fallback, or automatic upgrade.

Signed/remote catalogs remain Phase 4. A future materializer may place verified
bundles under the same local contract without changing compiler semantics.

## Consequences

- A pipeline source remains readable and deterministic without a separate
  generated lockfile or live catalog service.
- Adding unrelated catalog entries does not change existing pipeline source or
  release identity.
- Referenced recipe artifacts must be retained as long as source/releases need
  rebuild or reproducible evidence.
- Platform teams receive reusable profiles/components without gaining an
  arbitrary execution surface.
- Source files are slightly more verbose because pins are explicit.
- External recipes are flow-only in v1; broader composition requires a new
  approved contract rather than silent grammar expansion.

## Alternatives rejected

- Resolve `latest` or a version range at build/runtime: non-reproducible.
- Store a generated recipe lockfile beside the primary source: adds ceremony and
  drift between two files.
- Expand recipes only during scaffolding: provenance and dependency closure
  cannot be verified later.
- Run Python/Jinja recipe plugins: violates static, data-only trust boundaries.
- Resolve recipes in the Airflow provider: violates parse-side-effect budgets.
- Build a generic multi-registry plugin framework: speculative and outside v1.

## References

- [Feature design: content-pinned declarative recipe catalog v1](../feature-design-airflow-recipe-catalog-v1.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0015: One authoring compiler](0015-single-authoring-compiler.md)
