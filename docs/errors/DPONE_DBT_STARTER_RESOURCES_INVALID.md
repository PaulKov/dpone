# DPONE_DBT_STARTER_RESOURCES_INVALID

**Audience:** analytics authors and platform maintainers diagnosing dbt starter
installation failures.

The installed dbt starter resources are incomplete or invalid. The resource
inventory must contain exactly 22 files; templates must contain the expected
scalar placeholders. Missing or extra resources, invalid UTF-8, unsafe resource
paths, and unexpected or missing placeholders reject starter preparation.

This is a configuration failure (exit `2`) detected before scaffold apply.
The supplied policy snapshot is a separate project output; it is not one of the
22 installed resources. Diagnostic messages must not contain policy bytes,
model contents, rendered templates, or file diffs.

Ask the platform maintainer to restore the complete approved dpone distribution
in the environment that runs the starter, then retry the same request. Do not
repair installed resources by copying files from a source checkout or fabricating
package pins and lock files. A successful retry proves resource preparation;
it does not qualify the dbt runtime or certify a publishing route.

This code is reserved for the native starter service under implementation.
It does not indicate that the complete public starter workflow is available.

[Self-service error overview](index.md) ·
[dbt error reference](../dbt-self-service-errors.md)
