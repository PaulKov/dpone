"""Public capability contracts do not depend on concrete artifact implementations."""

import subprocess
import sys


def test_artifact_contracts_import_without_loading_runtime_or_workflow_reader():
    probe = """
import sys
from dpone.contracts.dbt_artifact_publication import DbtArtifactOutputConflict, DbtArtifactPublicationError
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
assert "dpone.runtime.immutable_local_tree" not in sys.modules
assert "dpone.services.dbt_release_workflow_reader" not in sys.modules
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactOutputConflict as old_conflict
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactPublicationError as old_publication
from dpone.services.dbt_release_workflow_reader import ConfinedReleaseFileReader as old_reader
assert old_conflict is DbtArtifactOutputConflict
assert old_publication is DbtArtifactPublicationError
assert old_reader is ConfinedReleaseFileReader
"""
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_pure_producer_imports_do_not_load_services_or_optional_frameworks():
    probe = """
import sys
from dpone.contracts.dbt_project_artifacts import DbtProjectArtifacts, DbtReleaseInputs
from dpone.contracts.dbt_release import build_dbt_release_metadata
from dpone.contracts.dbt_workspace_release import assemble_dbt_workspace_release
for name in sys.modules:
    assert not name.startswith(("dpone.services.", "dpone.readiness.", "dpone.runtime.",
                                "airflow", "dbt.", "dpone_airflow_pack")), name
"""
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
