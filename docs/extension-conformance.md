# Extension conformance

Extension conformance gives maintainers a common, machine-readable definition
of "this extension passed its required contract checks." It is release evidence,
not runtime authorization and not automatic production certification.

## Closed profiles

| Profile | Required checks | Live proof required |
|---|---|---|
| `airflow_provider` | public contract, provider discovery, exact Airflow matrix, package smoke, parse budget | No |
| `recipe_catalog` | signed bundle verification, catalog validation, scaffold fixture, semantic fingerprint | No |
| `credential_resolver` | contract, recovery, secret redaction, runtime isolation, live | Yes |
| `route` | route contract, reconciliation, recovery, state/evidence ordering, live | Yes |

Profiles and check names are closed in v1. A request cannot load a Python entry
point, add a custom check, or waive a required check.

## Prepare evidence

Each generic producer writes an exact receipt:

```yaml
schema: dpone.extension-check-receipt.v1
check: provider_discovery
subject_digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
status: PASS
producer: provider-wheel-smoke-v1
command: uv run pytest tests/test_airflow_provider_distribution.py -q
```

The `bundle_verification` check uses
`dpone.catalog-bundle-verification.v1` instead. Every request pins the exact
evidence bytes with SHA-256.

## Evaluate a profile

```yaml
schema: dpone.extension-conformance-request.v1
profile: recipe_catalog
subject:
  id: data-platform-recipes
  version: 1.2.0
  digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
evidence:
  - check: bundle_verification
    artifact_ref: .dpone/catalog-verification/receipt.json
    sha256: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    expected_schema: dpone.catalog-bundle-verification.v1
  - check: catalog_validation
    artifact_ref: test_artifacts/recipe/catalog-validation.json
    sha256: sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    expected_schema: dpone.extension-check-receipt.v1
```

Run:

```bash
dpone supply-chain extension-conformance \
  --request extension-conformance-request.yaml \
  --output-dir test_artifacts/extension-conformance/my-extension \
  --format json
```

The output directory contains `extension-conformance.json` and a human-readable
`extension-conformance.md`. Publication is create-only and idempotent for exact
bytes.

## Status algorithm

1. The selected built-in profile supplies the complete required checklist.
2. Unknown and duplicate check names reject the request.
3. Each available evidence file must stay inside the project root and match its
   declared digest and schema.
4. Evidence must name the same subject digest. Generic evidence must also name
   the expected check and include producer/command metadata.
5. An explicit failure, malformed receipt, digest mismatch, or subject mismatch
   yields `FAIL`.
6. Missing, skipped, unavailable, `N/A`, or unapproved live proof yields
   `UNVERIFIED`.
7. `FAIL` dominates `UNVERIFIED`. Only all-required-`PASS` yields `PASS`.

In particular, a route or credential resolver without approved live evidence
cannot receive `PASS`. A skipped live test is not a pass.

## Interpretation

- `PASS`: this exact subject conforms to the selected dpone extension contract;
- `FAIL`: current exact evidence proves a contract violation;
- `UNVERIFIED`: required proof is absent or unavailable.

Route certification still follows the route-level source × sink × strategy ×
transport × schema-evolution × runtime-mode process. Credential resolver
production support still requires approved runtime-side live evidence. The
conformance report records those facts; it does not replace them.

## Safe recovery

For `FAIL`, fix the producer or extension and generate new evidence. For
`UNVERIFIED`, run the documented producer in an approved environment. Never
edit a receipt or report by hand to manufacture `PASS`.
