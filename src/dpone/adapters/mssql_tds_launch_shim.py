"""Fixed isolated Python entrypoint: source-load guards before optional imports.

The command is independent of cwd, PYTHONPATH, site hooks and framework bytecode.
An admitted interpreter supplies its standard library. Dependency directories are
explicit, immutable composition inputs; SDK binary admission remains separate.
"""

SOURCE_SHIM = r"""
import argparse
import os
import runpy
import sys

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--package-root', required=True)
parser.add_argument('--dependency-path', action='append', default=[])
args, worker_args = parser.parse_known_args()
root = args.package_root
if not os.path.isabs(root) or any(not os.path.isabs(p) for p in args.dependency_path):
    raise SystemExit(1)
parent = int(worker_args[worker_args.index('--parent') + 1])
limit = int(worker_args[worker_args.index('--address-space') + 1])
guard_path = os.path.join(root, 'dpone', 'adapters', 'mssql_tds_worker_guard.py')
guard = runpy.run_path(guard_path, run_name='__dpone_guard__')
guard['install_worker_guard'](expected_parent_pid=parent, max_address_space_bytes=limit)
sys.path[:0] = [root, *args.dependency_path]
bootstrap = os.path.join(root, 'dpone', 'app', 'mssql_tds_worker_bootstrap.py')
sys.argv = [bootstrap, *worker_args]
runpy.run_path(bootstrap, run_name='__main__')
"""

# Both entrypoints are fixed by trusted composition. Keep the bulk program
# byte-for-byte unchanged; callers cannot supply an executable module or path.
COORDINATOR_SOURCE_SHIM = SOURCE_SHIM.replace("'mssql_tds_worker_bootstrap.py'", "'mssql_tds_coordinator_bootstrap.py'")

# The read-only departure helper cannot dispatch the CREATE coordinator.
DEPARTURE_SOURCE_SHIM = SOURCE_SHIM.replace(
    "'mssql_tds_worker_bootstrap.py'", "'mssql_sqlclient_departure_bootstrap.py'"
)

# Permission SQL runs in its own fixed role and cannot dispatch the bulk worker.
PERMISSION_GRANT_SOURCE_SHIM = SOURCE_SHIM.replace(
    "'mssql_tds_worker_bootstrap.py'", "'mssql_sqlclient_permission_grant_bootstrap.py'"
)
