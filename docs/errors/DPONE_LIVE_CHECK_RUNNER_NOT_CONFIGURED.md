# DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED

`dpone check --live` completed static authoring and connection configuration
checks, but no bounded live preflight runner was configured.

The command stays fail-closed in this state. It does not call Vault,
Kubernetes, source databases, sinks, Airflow Variables, or Airflow Connections.

## Fix

For local authoring, run the static checks first:

```bash
dpone check pipelines/orders_daily
dpone check pipelines/orders_daily --connections --environment dev
```

For a real live preflight, configure a platform/runtime runner that implements
the `AirflowLivePreflightRunner` contract and executes only bounded probes.
The runner must return `dpone.live-preflight.v1` probe results and must not
serialize secret values into errors, logs, evidence, or XCom.
