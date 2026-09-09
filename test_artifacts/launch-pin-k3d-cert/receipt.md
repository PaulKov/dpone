# Launch-pin K3d CAS/RBAC certification receipt

- **Status:** PASS
- **Certified at:** 2026-08-11T19:22:37.558369+00:00
- **Commit SHA:** `18570c11788088744e9eab2d33ab4fcfadf32224`
- **Cluster:** `dpone-lp-cert`
- **Namespace:** `airflow`
- **Kubernetes:** `v1.33.6+k3s1`

## Scenarios

- **worker_configmap_cas:** PASS — ACTIVE pin uid=9e478e60-22d9-45d1-a43b-213852836aa7 (30 ms)
- **worker_pod_delete:** PASS — deleted pod dpone-lp-cert-pod-b487067d (created by platform, deleted by worker SA) (12 ms)
- **runtime_configmap_get_only:** PASS — runtime get ok; create denied as expected (15 ms)
- **multi_worker_cas_single_winner:** PASS — single ACTIVE winner try=2 (38 ms)
- **runtime_reads_active_barrier:** PASS — runtime read ACTIVE per-try dpone-lp-d81a7058ff5978637a4e911ccbd5399b9fe52f84 (29 ms)

## Limitations

- Single-node k3d cluster with two logical worker clients (not separate Airflow worker pods).
- Does not exercise full Airflow KPO/barrier init container or outcome_gate task graph.
- Gate/cleanup workers reuse worker RBAC profile (same verbs as documented worker SA).
