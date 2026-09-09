# Developer Guide: Data Product Trust Center

The Trust Center pack follows the data-product readiness architecture:
provider-neutral readiness modules are pure, service facades do file IO, and
command modules only parse arguments and render artifacts.

## Taxonomy

| Component | Responsibility |
| --- | --- |
| `TrustCenterOptions` | Immutable manifest config for domains, freshness, score and profile policy. |
| `TrustEvidenceLakeIndexer` | Normalizes manifests, registry records and local evidence artifacts into evidence refs. |
| `TrustQueryEngine` | Filters evidence refs by product, owner, domain, kind and status. |
| `TrustSnapshotBuilder` | Builds domain status, completeness and trust score for one product. |
| `TrustGate` | Profile-aware `allowed | warning | blocked` release decision. |
| `TrustReportRenderer` | Renders deterministic Markdown and JSON-friendly reports. |
| `TrustExportRenderer` | Renders local interop payloads without network writes. |
| `DataProductTrustFacade` | Thin CLI facade for JSON/YAML/glob file loading. |

## Algorithms

Index:

1. Expand manifest and evidence-dir inputs deterministically.
2. Load `sink.options.data_product.trust_center`.
3. Normalize every artifact into product, owner, domain, kind, status, ids,
   blockers and warnings.
4. Deduplicate refs by product, artifact kind and evidence id.
5. Emit `evidence_lake_index_id`.

Snapshot:

1. Select refs for one product id.
2. Build the required domain matrix.
3. Mark domains `healthy`, `degraded`, `blocked` or `missing`.
4. Compute weighted trust score.
5. Emit owner-routed blockers and `trust_snapshot_id`.

Gate:

1. Start from snapshot blockers and warnings.
2. Apply profile semantics.
3. In strict profiles, block missing required domains and score threshold
   failures.
4. Emit `trust_gate_id` for bundle and registry evidence.

## Extension Rules

- Add new artifact-kind-to-domain mappings in
  `data_product_trust_support.py`.
- Keep readiness modules free of DB clients, provider SDKs, SCM SDKs and
  notification SDKs.
- Add a schema, contract test, CLI test and bundle/registry integration test
  when introducing a new public artifact.
- Keep every production module below 400 SLOC and avoid growing existing dense
  data-product modules.
