# Developer Guide: Data Product Cost Governance

The cost governance pack follows the existing data-product architecture:
generic readiness modules are pure and provider-neutral, service facades do
file IO only, command modules only parse arguments and render payloads.

## Taxonomy

| Component | Responsibility |
| --- | --- |
| `CostGovernanceOptions` | Immutable manifest config for budgets, capacity, rates, ownership and profile policy. |
| `CostGovernancePlanner` | Builds product/bundle-bound cost plans and planned capacity risk. |
| `CostBudgetEvaluator` | Applies manifest rates to runtime/target metrics and emits budget/capacity findings. |
| `CostForecastEvaluator` | Compares latest evaluation with historical cost artifacts. |
| `CostGovernanceGate` | Profile-aware `allowed | warning | blocked` release decision. |
| `ClickHouseCostProbe` | Optional read-only metadata adapter behind the service layer. |
| `DataProductCostFacade` | Thin CLI facade for JSON/YAML file loading and target-probe dispatch. |

Generic modules must not import ClickHouse, DB clients, cloud billing SDKs,
Airflow, SCM, catalog or notification SDKs. Target-specific code lives behind
the `TargetCostProbe` adapter boundary.

## Algorithms

Plan:

1. Load `sink.options.data_product.cost_governance`.
2. Emit a deterministic disabled plan when the feature is off.
3. Bind product id, owner, tier, cost center, pack id and bundle id.
4. Derive planned capacity hints such as full refresh and strategy from the bundle.
5. Block missing owner/cost center and unbounded full refresh according to policy.
6. Compute `cost_plan_id` from the normalized payload.

Evaluate:

1. Read the plan plus optional runtime artifact, registry records and target metrics.
2. Compute storage, staging, query and runtime costs from manifest rates.
3. Compare per-run, projected monthly and release-delta cost to declared budgets.
4. Compare table growth, staging GB, query duration and concurrency to capacity limits.
5. Emit deterministic blockers, warnings and `cost_evaluation_id`.

Gate:

1. Start from evaluation blockers/warnings.
2. Apply profile semantics: advisory converts blockers to warnings; strict profiles block.
3. Regulated mode requires owner/cost-center evidence.
4. Emit `cost_gate_id` for bundle and registry evidence.

Forecast/report:

1. Compare latest evaluation to historical cost evaluations.
2. Flag spikes as warnings.
3. Render Markdown for PR/MR and release-closeout review.

## Extension Rules

- Add a new target probe in `services/` and keep readiness imports generic.
- Return redacted metrics only; never serialize connection secrets or SQL text.
- Keep every new production module below 400 SLOC.
- Add unit, CLI and integration tests before implementation changes.
- Extend bundle artifact catalog, registry refs and docs schemas together.
