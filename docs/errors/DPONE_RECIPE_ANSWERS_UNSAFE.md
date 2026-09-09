# DPONE_RECIPE_ANSWERS_UNSAFE

The answers file is outside the project, malformed/unbounded, or contains a
credential-like key/value. The diagnostic redacts values and exits `4`.

## Fix

Keep the file beneath the project root and include only declared scalar values.
Use logical `connection_ref` aliases instead of passwords, tokens, Vault paths,
environment secret expressions, or private keys. Credentials belong in the
platform registry/resolver, not recipe answers.
