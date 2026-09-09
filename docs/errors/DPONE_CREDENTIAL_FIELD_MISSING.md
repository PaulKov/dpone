# DPONE_CREDENTIAL_FIELD_MISSING

A declared credential field mapping resolved to a missing or empty value. dpone
does not name the field or print the backend payload because either may reveal
sensitive configuration.

## Fix

Ask the platform owner to compare the restricted registry field mapping with the
backend secret keys, repair the value, and retry the workload. Do not paste the
secret or backend response into logs or tickets.
