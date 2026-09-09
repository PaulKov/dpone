# DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_REQUIRED

`credential-runtime.yaml` must declare the environment it belongs to.

This file is part of the environment-specific deployment identity. Without an
explicit `environment`, dpone cannot safely prove that the Vault auth role,
namespace, and runtime credential settings belong to the same environment as
the `binding-set` and connection registry.

## Fix

Add the environment field to the runtime file:

```yaml
schema: dpone.credential-runtime.v1
environment: prod

vault:
  address: https://vault.internal
  namespace: data-platform
  auth:
    method: kubernetes
    role: dpone-runtime-prod
```

Do not add Vault tokens, Kubernetes JWTs, AppRole `secret_id` values,
`password`, `client_secret`, `private_key`, `access_key`, `vault_token`, or
other secret material to this file. It should contain only non-secret runtime
references.
