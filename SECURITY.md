# Security Policy

## Reporting vulnerabilities

Please report security issues privately through GitHub Security Advisories for `PaulKov/dpone`.

Do not open public issues that include exploit details, tokens, service-account JSON, Vault paths with live credentials, or other sensitive data.

## Secrets policy

Never commit or paste:

- GitHub tokens
- PyPI tokens
- Vault tokens, AppRole secrets, JWTs, or Kubernetes auth material
- API keys for data providers
- Google service-account JSON
- Database passwords or connection strings with credentials

If a secret is exposed in chat, logs, issues, commits, or CI output, revoke it before publishing or pushing public history.

## Supported versions

Security fixes are currently provided for the latest released `0.x` version.
