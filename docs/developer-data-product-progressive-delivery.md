# Developer Guide: Data Product Progressive Delivery

The progressive delivery pack follows the existing data-product architecture:
readiness modules are pure and provider-neutral, service facades do file IO,
and command modules only parse arguments and render payloads.

## Taxonomy

| Component | Responsibility |
| --- | --- |
| `ProgressiveDeliveryOptions` | Immutable manifest config for rings, shadow validation, rollback and profile policy. |
| `RolloutPlanBuilder` | Builds product, pack and bundle-bound rollout plans. |
| `ShadowValidationEvaluator` | Compares baseline and candidate runtime evidence. |
| `RingGateEvaluator` | Checks ring order, required gates, shadow status and profile rules. |
| `RolloutPromotionService` | Emits deterministic promote or hold receipts. |
| `RolloutRenderer` | Renders operator-friendly Markdown rollout reports. |
| `DataProductRolloutFacade` | Thin CLI facade for JSON/YAML file loading and output parity. |

Generic readiness code must not import ClickHouse, DB clients, Airflow, SCM
SDKs, catalog SDKs, notification SDKs or object-store SDKs. Target read-only
shadow probes can be added later behind a new service-layer port.

## Algorithms

Plan:

1. Load `sink.options.data_product.progressive_delivery`.
2. Emit a deterministic disabled plan when the feature is off.
3. Bind product id, owner, tier, pack id, bundle id and evidence refs.
4. Normalize rings by `order`.
5. Block duplicate ring ids, non-increasing order, missing product id, blocked
   bundle evidence and regulated owner gaps.
6. Compute `rollout_plan_id` from the normalized payload.

Shadow validation:

1. Read the rollout plan and candidate/baseline runtime artifacts.
2. Compare row count, null-key count, duplicate-key count and typed hash when
   those metrics are present.
3. Treat strict typed-hash mismatches as blockers and warning mode mismatches
   as warnings.
4. Emit `shadow_validation_id`.

Ring gate:

1. Read the plan, selected ring, evidence index, shadow validation and previous
   promotions.
2. Verify previous rings are promoted for strict profiles.
3. Verify required gates exist, match pack/bundle ids when present and have an
   allowed status.
4. Require shadow validation for configured rings.
5. Apply profile semantics and emit `ring_gate_id`.

Promotion:

1. Promote only from an `allowed` or `warning` ring gate.
2. Require authority evidence when rollback policy enables it.
3. Emit `promoted` or `held`; V1 never shifts traffic or mutates runtime state.
4. Render Markdown reports from promotion receipts.

## Integration Rules

- Add bundle artifact kinds, status checks and summary ids together.
- Add evidence registry stages, refs and blockers together.
- Keep JSON schemas in `docs/schemas/data-product/` in sync with artifact
  payloads.
- Keep command output parity: console payload and `--output` file must match.
- Keep every production module under 400 SLOC.
- Add tests before implementation changes for new profile or artifact behavior.

## Current Extension Points

- `RolloutEvidenceIndex` behavior is intentionally implemented in the facade as
  local evidence-dir loading. If evidence sources grow, extract it into a small
  provider-neutral readiness helper.
- Shadow validation currently uses local runtime artifacts only. Add target
  probes behind a service-layer protocol, never in readiness.
- Rollback currently emits recommendations. Execution should continue to use
  migration rollback/remediation paths with authority evidence.
