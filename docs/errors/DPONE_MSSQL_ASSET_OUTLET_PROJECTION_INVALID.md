# DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID

## Meaning

The closed `dpone.mssql-asset-outlet-projection.v1` document failed structural
validation, or build-time binding/registry resolution could not produce a
canonical AIP-60 URI.

## Typical causes

1. Logical `connection_ref` is missing from `binding_set.bindings`.
2. Bound registry ref has no `asset_authority` in the connection registry.
3. Projection entries have an incomplete `asset_ref`, non-canonical URI, or
   unknown fields (`additionalProperties: false`).
4. Airflow `Asset()` / `Dataset()` rejected a projected URI (`ValueError`) —
   projected outlets are fail-closed (legacy authoring still uses the parse
   shield).

## Remediation

1. Bind every logical MSSQL alias used by outlets:
   `bindings.<logical>.connection_ref: <registry_ref>`.
2. Ensure the bound registry entry declares `connection.asset_authority`.
3. Rebuild the deployment projection; do not patch URIs by hand.
4. Confirm the URI is closed-canonical via the shared codec (explicit port,
   3/4 path segments, lowercase host/instance).
