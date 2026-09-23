from dpone.adapters.mssql_sqlclient_native_retirement_effects import (
    CallbackSqlClientNativeRetirementEffects,
)


def test_effect_adapter_delegates_each_explicit_capability_without_interpretation() -> None:
    values = [object() for _ in range(7)]
    adapter = CallbackSqlClientNativeRetirementEffects(
        prove_containment=lambda request: values[0],
        observe_incarnation=lambda request: values[1],
        drop=lambda request, reservation, intent: values[2],
        reconcile=lambda request, reservation, intent, prior: values[3],
        settle=lambda request, reservation, drop, incarnation: values[4],
        observe_absence=lambda request: values[5],
        observe_capacity=lambda request: values[6],
    )
    marker = object()
    assert adapter.prove_containment(marker) is values[0]
    assert adapter.observe_exact_incarnation(marker) is values[1]
    assert adapter.drop_exact(marker, marker, marker) is values[2]
    assert adapter.reconcile_drop(marker, marker, marker, None) is values[3]
    assert adapter.settle_drop(marker, marker, marker, "digest") is values[4]
    assert adapter.observe_absence(marker) is values[5]
    assert adapter.observe_capacity(marker) is values[6]
