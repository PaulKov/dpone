# Content-pinned declarative recipe catalog v1 validation report

- Date: 2026-07-16
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `054e100302be20fa7b6cc3ced4d68c6be5c3a03f`
- Pre-implementation branch head: `8e9c9b5ed382681d5761437502b77db8bf556fa3`
- Specification: `docs/feature-design-airflow-recipe-catalog-v1.md`
- ADR: `docs/adr/0016-content-pinned-declarative-recipes.md`
- Task contract: `test_artifacts/agent-policy/airflow-recipe-catalog-v1.yml`
- Toolchain: `uv 0.9.18`, Python `3.11.14`, Darwin arm64

## Result

**IMPLEMENTATION GO. Ready for merge review.**

The Phase 2 recipe slice implements one local trusted catalog of exact recipe,
profile, and component pins. Compilation is bounded, data-only, deterministic,
and delegates to the existing canonical manifest IR. The Airflow provider and
runtime execute generated spec/pack/canonical-manifest artifacts; they do not
import, discover, or execute recipe code.

Digest drift fails before pack publication and before safe-sample data access.
Generated runtime manifests preserve relative SQL semantics, and no-root pack
builds are explicitly non-runnable instead of inventing a digest. Existing
built-in recipes and classic/flow/folder authoring remain compatible.

Live connector certification is `N/A`: this capability is confined to local
authoring/build-plane resolution and does not add a source, sink, strategy,
credential resolver, or Airflow runtime route.

## Contract evidence

| Contract | Status | Evidence |
| --- | --- | --- |
| Exact `kind/path/sha256` recipe closure | PASS | recipe compiler, pack, and safe-sample tests |
| One flow primary source and canonical IR | PASS | semantic-equivalence and runtime materialization tests |
| No Python/Jinja/shell/network/secret lookup | PASS | schema negatives and side-effect spies |
| Bounded scalar answers and override policy | PASS | parameter, lock, sensitive-key/value, and expansion tests |
| Descriptor-confined paths and symlink rejection | PASS | catalog/source/pin traversal and symlink tests |
| CLI validator/compiler parity | PASS | invalid profile/context/schema regression tests |
| SQL content participates in source identity | PASS | SQL byte-mutation fingerprint test |
| Folder semantic identity remains formatting-stable | PASS | folder compatibility regression matrix |
| Pack executes generated canonical IR only | PASS | compact-runtime command/archive tests |
| No-root pack cannot become runnable | PASS | `unmaterialized_manifest` blocker test |
| Preview release identity is deterministic | PASS | repeated preview and benchmark release fingerprints |
| Built-in/classic/flow/folder compatibility | PASS | focused matrix and full non-live suite |
| Airflow parse path excludes recipe/runtime integrations | PASS | static import/network spy and provider wheel smoke |

## Measurable differentiation

Producer:

```bash
uv run python tools/airflow_recipe_catalog_benchmark.py
```

Artifact: `test_artifacts/airflow-recipe-catalog-v1/benchmark.json`.

| Metric | Target | Observed | Status |
| --- | ---: | ---: | --- |
| Beginner golden-path commands | <=5 | 5 | PASS |
| Recipe compile p95, 100 sources / 3 components | <=50 ms | 10.924 ms | PASS |
| Independent source fingerprints equal | 100% | 100/100 | PASS |
| Independent semantic fingerprints equal | 100% | 100/100 | PASS |
| Independent release fingerprints equal | 100% | 100/100 | PASS |
| Affected builds blocked after dependency mutation | 100% | 100/100 | PASS |
| False-success builds after mutation | 0 | 0 | PASS |
| Network calls during recipe resolution | 0 | 0 | PASS |
| Forbidden runtime integration imports | 0 | 0 | PASS |

The benchmark exercises compile and preview identity for all 100 sources. Pack
and safe-sample drift behavior is covered by executable focused contract tests;
those stages are not misreported as 100 live executions.

## Executed checks

| Status | Check | Observed result |
| --- | --- | --- |
| PASS | Focused authoring/recipe/folder/pack/safe-sample/interval tests | All passed |
| PASS | `uv run pytest -m "not integration_live" -n auto --dist loadfile` | 4,695 passed, 473 skipped in 227.85 s |
| PASS | `uv run ruff check .` | No findings |
| PASS | `uv run ruff format --check .` | 3,130 files formatted |
| PASS | `uv run mypy --config-file mypy.ini` | 517 source files, no issues |
| PASS | Architecture fitness hard gates | clustering `0.179470875732`; cross-layer `0.299597713318` |
| PASS | Import rules | No violations |
| PASS | Layer metrics | 56 layers, 4,723 edges, no issues |
| PASS | Module-size gate | No hard failures or new warning module |
| PASS | Documentation links | 473 Markdown files, 1,783 local links |
| PASS | Generated references | 2/2 synchronized |
| PASS | Compatibility registry | 19 entries synchronized |
| PASS | Documentation language contracts | 5 passed |
| PASS | `uv run mkdocs build --strict` | Strict build completed |
| PASS | Root/native/provider builds | Six wheel/sdist artifacts built |
| PASS | Twine metadata | Six artifacts passed |
| PASS | Wheel content smoke | Four recipe schemas in core; provider excludes recipe/runtime code |
| PASS | Task contract validator | 0 errors, 0 warnings |
| N/A | Live MSSQL/ClickHouse/Vault/Airflow route | No runtime/connector capability changed |

Transient package output used for wheel inspection:
`/tmp/dpone-recipe-catalog-final-dist.1KXRX9/`. Durable evidence is this report, the
benchmark JSON, schemas, and executable tests.

## Public contract and compatibility

New compatible surfaces are `dpone.recipe-catalog.v1`, `dpone.recipe.v1`,
`dpone.profile.v1`, `dpone.component.v1`, the pinned `recipe` flow block,
`dpone recipe list/show/pin/validate`, and optional `init pipeline --profile`
and `--answers`. External refs require exact SemVer plus byte SHA-256.

Built-in recipe names remain compatibility aliases. Classic and folder grammar
does not gain an external recipe block in v1. Runtime/pack compatibility is
additive: packs carry ordered source dependencies and execute a generated
canonical manifest. A deprecated recipe/profile/component is readable and
fingerprinted; it emits deterministic migration metadata without mutating the
immutable artifact.

## Documentation and CJM impact

The beginner First DAG keeps the same five commands and built-in recipe default.
Platform users receive a separate recipe publishing/discovery guide, exact CLI
reference, configuration/schema reference, compatibility guidance, ADR, error
catalog, and safe recovery instructions. No beginner needs to understand pack,
KPO, Vault paths, release/deployment identity, or recipe digests to use the
built-in path.

## Review evidence

A fresh-context architecture/security review first found one P1 archive TOCTOU
and one P2 recipe-domain schema mismatch. The archive now verifies the exact
bytes it embeds against every declared dependency digest and returns a blocked,
pod-less pack on drift; recipe-mode domain validation now matches runtime while
leaving explicit flow compatibility unchanged. The reviewer reran all 39 recipe
tests, reported no remaining P0/P1/P2 findings, and returned `APPROVE`. The
integrator then reran the complete non-live and quality gates listed above.

## Remaining risk and follow-up

- Human usability testing with at least five new users is roadmap evidence and
  is not replaced by hermetic tests.
- Remote/signed recipe catalogs and trusted Python plugins remain explicit
  Phase 4/non-goals; v1 accepts only local materialized declarative YAML.
- Selector UX, hermetic test/DLQ/step-visibility contracts, Phase 3 Assets and
  evidence, and Phase 4 standardization remain later roadmap slices.
- Route production certification remains independent of recipe contract
  validation.
