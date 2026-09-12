"""Internal owner relocation supplements, outside frozen behavioral case IDs."""


def test_binding_core_has_one_canonical_model_owner() -> None:
    import importlib

    core = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
    for name in (
        "MssqlR1StageBufferSymbolV2",
        "MssqlR1StageEnvelopeSourceV2",
        "MssqlR1RegisteredTargetRefV2",
        "MssqlR1BusinessColumnMappingV2",
        "MssqlR1BindingTargetMappingV2",
        "MssqlR1StageScanProjectionV2",
        "MssqlR1StageScanInvocationV2",
        "MssqlR1ModuleStageBufferPlanV2",
        "MssqlR1InstantiatedBindingModuleV2",
    ):
        assert getattr(core, name).__module__ == core.__name__
