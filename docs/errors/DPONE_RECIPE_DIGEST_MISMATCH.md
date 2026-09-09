# DPONE_RECIPE_DIGEST_MISMATCH

The SHA-256 of a recipe, profile, or component differs from its exact pin. dpone
stops before YAML parsing, compilation, credentials, or data access and exits
`4` on the self-service path.

## Fix

Do not update the digest merely to silence the error. Restore the reviewed
immutable bytes or publish a new semantic version, regenerate its pin, and run:

```bash
dpone recipe pin <artifact-path>
dpone recipe validate
```
