import pickle


def _legacy_global(module: str, name: str) -> object:
    return pickle.loads(f"c{module}\n{name}\n.".encode())


def test_schema_migration_status_facade_preserves_identity() -> None:
    from dpone.readiness import schema_migration_bundle_status as facade
    from dpone.readiness import schema_migration_bundle_status_policy as leaf

    assert facade.STATUS_CHECKS is leaf.STATUS_CHECKS
    assert facade.status_blockers is leaf.status_blockers


def test_func_command_facade_preserves_identity_and_legacy_pickle_global() -> None:
    from dpone.commands import func_command as facade
    from dpone.commands import func_command_impl as leaf

    for name in ("FuncCommand", "CommandGroup"):
        value = getattr(leaf, name)
        assert getattr(facade, name) is value
        assert _legacy_global("dpone.commands.func_command", name) is value


def test_certification_artifacts_facade_preserves_identity_and_legacy_pickle_global() -> None:
    from dpone.ops import certification_artifacts as facade
    from dpone.ops import certification_artifacts_impl as leaf

    for name in ("CertificationEvidenceItem", "CertificationArtifactReader"):
        value = getattr(leaf, name)
        assert getattr(facade, name) is value
        assert _legacy_global("dpone.ops.certification_artifacts", name) is value
    assert facade.artifact_payload_passed is leaf.artifact_payload_passed
