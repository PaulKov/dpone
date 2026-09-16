"""Isolated same/two-database SQL-auth fixtures with owned-object cleanup."""

import os
import secrets
import sys
from contextlib import contextmanager
from hashlib import sha256
from uuid import UUID, uuid4

from dpone.adapters.dbt_mssql_physical_registration_schema import MssqlPhysicalRegistrationSchemaMigration
from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader
from dpone.adapters.dbt_mssql_physical_source_schema import MssqlPhysicalSourceSchemaProvisioner
from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    DatabaseRoleMapping,
    DedicatedObserver,
    RegisteredPrincipals,
)
from dpone.contracts.dbt_workspace_observation import catalog_observation_fingerprint
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.runtime.dbt_workspace_mssql_queries import HEADER_SQL
from tests.support.dbt_mssql_physical_source_authority import ADMISSION, LOCAL, ROOT, SCHEMA, FixtureAuthority

CERTIFICATE = "source_bridge_certificate"
CERTIFICATE_USER = "source_bridge_certificate_user"


def database_pin(connection):
    row = connection.execute(
        "SELECT DB_NAME(),DB_ID(),CONVERT(char(19),d.create_date,126)+'.'+"
        "RIGHT('0000000'+CONVERT(varchar(7),DATEPART(NANOSECOND,CONVERT(datetime2(7),d.create_date))/100),7),"
        "CONVERT(char(36),r.database_guid) FROM sys.databases d "
        "JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.database_id=DB_ID()"
    ).fetchone()
    return MssqlDatabaseAuthorityPin(row[0], row[1], row[2], UUID(row[3]))


class SourceFixture:
    """Connections are independently authenticated; never emulate positive login."""

    def __init__(self, pyodbc, *, layout="two_database"):
        self.pyodbc = pyodbc
        assert layout in {"same_database", "two_database"}
        self.layout = layout
        self.databases = {name: "source_" + name + "_" + uuid4().hex for name in ("model", "control")}
        if layout == "same_database":
            self.databases["control"] = self.databases["model"]
        self.credentials = {
            role: ("source_" + role + "_" + uuid4().hex, "Aa!9" + secrets.token_hex(24))
            for role in ("metadata", "build", "observer")
        }
        self.created_databases = []
        self.created_logins = []
        self.authority = FixtureAuthority()

    def connect(self, namespace="model", role=None, *, autocommit=False):
        user, secret = self.credentials[role] if role else ("sa", os.environ["DPONE_NATIVE_SQL_TEST_PASSWORD"])
        database = self.databases.get(namespace, namespace)
        connection = self.pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.environ['DPONE_NATIVE_SQL_TEST_HOST']};"
            f"DATABASE={database};UID={user};PWD={secret};Encrypt=no;TrustServerCertificate=yes",
            timeout=5,
            autocommit=autocommit,
        )
        connection.timeout = 15
        return connection

    @contextmanager
    def connection(self, namespace="model", role=None, *, autocommit=False):
        connection = self.connect(namespace, role, autocommit=autocommit)
        try:
            yield connection
        finally:
            if not autocommit:
                connection.rollback()
            connection.close()

    def install(self):
        with self.connection("master", autocommit=True) as admin:
            for database in dict.fromkeys(self.databases.values()):
                admin.execute(f"CREATE DATABASE [{database}]")
                self.created_databases.append(database)
            for login, secret in self.credentials.values():
                admin.execute(f"CREATE LOGIN [{login}] WITH PASSWORD='{secret}', CHECK_POLICY=OFF")
                self.created_logins.append(login)
        principals, pins, services = {}, {}, {}
        for namespace, schema in (("model", LOCAL), ("control", SCHEMA)):
            with self.connection(namespace, autocommit=True) as admin:
                admin.execute(f"CREATE SCHEMA [{schema}] AUTHORIZATION dbo")
                new_database = namespace == "model" or self.layout == "two_database"
                if namespace == "control" and new_database:
                    admin.execute("CREATE USER namespace_offset WITHOUT LOGIN")
                principals[namespace] = {}
                for role, (login, _) in self.credentials.items():
                    if new_database:
                        admin.execute(f"CREATE USER [{login}] FOR LOGIN [{login}]")
                    row = admin.execute(
                        "SELECT principal_id,sid FROM sys.database_principals WHERE name=?", login
                    ).fetchone()
                    principals[namespace][role] = DatabasePrincipal(row[0], bytes(row[1]).hex())
                pins[namespace] = database_pin(admin)
                cursor = admin.execute(HEADER_SQL, "[]")
                header = dict(zip((column[0] for column in cursor.description), cursor.fetchone(), strict=True))
                facts = {key: header[key] for key in ("server_name", "machine_name", "physical_name", "instance_name")}
                self.authority.retain(namespace + "-server-facts", facts)
                services[namespace] = {
                    "schema": "dpone.mssql-service-authority.v1",
                    "server_facts_sha256": catalog_observation_fingerprint(facts),
                    "engine_version": header["engine_version"],
                    "server_collation": header["server_collation"],
                }
                if new_database:
                    admin.execute("CREATE MASTER KEY ENCRYPTION BY PASSWORD='" + "Aa!9" + secrets.token_hex(24) + "'")
        mappings = {
            role: DatabaseRoleMapping(principals["control"][role], principals["model"][role])
            for role in self.credentials
        }
        assert all(
            (mapping.model.principal_id == mapping.control.principal_id) == (self.layout == "same_database")
            for mapping in mappings.values()
        )
        assert services["model"] == services["control"]
        self.registration = self.authority.registration(
            pins["model"],
            pins["control"],
            RegisteredPrincipals(mappings["metadata"], mappings["build"], DedicatedObserver(mappings["observer"])),
            services["model"],
        )
        self.request, self.executor, self.provider, self.reserved = self.authority.admit(
            self.registration,
            lambda: self.connect("control"),
            lambda: self.connect("control", "metadata"),
            self.credentials["metadata"][0],
        )
        # Preserve native metadata grants, deny direct native table access also to BUILD/OBSERVER.
        with self.connection("control", autocommit=True) as admin:
            tables = admin.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID(?)", SCHEMA).fetchall()
            for role in ("build", "observer"):
                login = self.credentials[role][0]
                for row in tables:
                    admin.execute(
                        f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON OBJECT::[{SCHEMA}].[{row[0]}] TO [{login}]"
                    )
                admin.execute(f"DENY ALTER ON SCHEMA::[{SCHEMA}] TO [{login}]")
        MssqlPhysicalRegistrationSchemaMigration(
            connection_factory=self.connect,
            local_schema=LOCAL,
            runtime_principals=tuple(mapping.model for mapping in mappings.values()),
        ).apply()
        secret = "Aa!9" + secrets.token_hex(24)
        with self.connection(autocommit=True) as admin:
            admin.execute(f"CREATE CERTIFICATE [{CERTIFICATE}] WITH SUBJECT='Isolated source bridge'")
            row = admin.execute(
                f"SELECT CERTENCODED(CERT_ID('{CERTIFICATE}')), CERTPRIVATEKEY(CERT_ID('{CERTIFICATE}'), ?)", secret
            ).fetchone()
            public, private = bytes(row[0]), bytes(row[1])
        if self.layout == "two_database":
            with self.connection("control", autocommit=True) as admin:
                admin.execute(
                    f"CREATE CERTIFICATE [{CERTIFICATE}] FROM BINARY=0x{public.hex()} "
                    f"WITH PRIVATE KEY (BINARY=0x{private.hex()}, DECRYPTION BY PASSWORD='{secret}')"
                )
        self.provisioner = MssqlPhysicalSourceSchemaProvisioner(
            connection_factory=self.connect,
            admission_sql=ADMISSION.read_bytes(),
            certificate_name=CERTIFICATE,
            certificate_public_bytes=public,
            certificate_user=CERTIFICATE_USER,
        )
        assert self.provisioner.apply(self.registration) == self.registration

    def read(self, role="metadata", *, generation=None, invocation=None):
        return MssqlPhysicalSourceReader(
            connection_factory=lambda: self.connect("model", role),
            registration=self.registration,
        ).read(generation or str(self.executor.generation_id), invocation or str(self.executor.invocation_id))

    def snapshot(self):
        """Copy exact native rows, not CHECKSUM collisions or mere counts."""
        with self.connection("control") as admin:
            names = sorted(
                row[0] for row in admin.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID(?)", SCHEMA)
            )
            return {
                name: tuple(tuple(row) for row in admin.execute(f"SELECT * FROM [{SCHEMA}].[{name}]")) for name in names
            }

    def evidence(self):
        """Observe installed bytes/signatures and engine identity without secrets."""
        result = {"layout": self.layout}
        for namespace in ("model", "control"):
            with self.connection(namespace) as admin:
                result[namespace] = {
                    "engine": admin.execute("SELECT @@VERSION").fetchone()[0],
                    "driver": admin.getinfo(self.pyodbc.SQL_DRIVER_VER),
                    "database_flags": tuple(
                        admin.execute(
                            "SELECT is_trustworthy_on,is_db_chaining_on FROM sys.databases WHERE database_id=DB_ID()"
                        ).fetchone()
                    ),
                    "modules": [
                        {
                            "name": row[0],
                            "definition_utf16_sha256": sha256(row[1].encode("utf-16-le")).hexdigest(),
                            "signature": row[2],
                            "thumbprint": bytes(row[3]).hex(),
                        }
                        for row in admin.execute(
                            "SELECT p.name,m.definition,c.crypt_type,c.thumbprint FROM sys.procedures p "
                            "JOIN sys.sql_modules m ON p.object_id=m.object_id JOIN sys.crypt_properties c ON c.major_id=p.object_id "
                            "WHERE p.name IN ('physical_require_source_v1','physical_control_require_source_v1')"
                        )
                    ],
                }
        result["retained_originals"] = {name: self.authority.get(name).sha256 for name in self.authority.references}
        result["source_files_sha256"] = {
            path: sha256((ROOT / path).read_bytes()).hexdigest()
            for path in (
                "packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql",
                "packages/dbt-dpone/control/sqlserver/physical-v1/schema.sql",
                "src/dpone/adapters/dbt_mssql_physical_source_queries.py",
                "src/dpone/adapters/dbt_mssql_physical_source.py",
                "src/dpone/adapters/dbt_mssql_physical_source_schema.py",
                "src/dpone/adapters/native_generation_mssql_owner.py",
                "src/dpone/adapters/native_generation_mssql_json.py",
                "tests/support/dbt_mssql_physical_source_authority.py",
                "tests/support/dbt_mssql_physical_source_fixture.py",
                "tests/test_dbt_mssql_physical_source_live.py",
            )
        }
        return result

    def cleanup(self):
        primary, errors = sys.exception(), []
        with self.connection("master", autocommit=True) as admin:
            statements = [
                statement
                for name in self.created_databases
                for statement in (
                    f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                    f"DROP DATABASE [{name}]",
                )
            ]
            statements.extend(f"DROP LOGIN [{name}]" for name in self.created_logins)
            for statement in statements:
                try:
                    admin.execute(statement)
                except Exception as error:
                    errors.append(error)
        if errors:
            if primary:
                primary.add_note("Source fixture cleanup failed; remove its owned isolated container")
            else:
                raise ExceptionGroup("Source fixture cleanup failed", errors)
