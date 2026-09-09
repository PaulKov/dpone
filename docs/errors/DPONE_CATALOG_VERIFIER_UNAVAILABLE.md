# DPONE_CATALOG_VERIFIER_UNAVAILABLE

The certified cosign verifier is missing, timed out, returned an unsupported
version document, or lies outside the certified version range. The decision is
`unverified`, never `verified` or `invalid`.

## Fix

Restore a certified cosign version in the build/materializer environment and
retry verification with the same immutable bundle, signature, policy, and
trusted root. Do not invoke verification during Airflow DAG parsing or inside a
connector runtime.
