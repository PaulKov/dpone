"""SQL rendering regressions, not live owner certification.

Golden digests were captured by a standalone Python process from git archive
31c0dbd8846c7e131b15e6ef90fe0fea2c2b4f86 before extracting the producer.
"""

from hashlib import sha256

import pytest

from dpone.adapters import native_generation_mssql_owner as owner
from dpone.adapters import native_generation_mssql_queries as queries


@pytest.mark.parametrize(
    ("schema", "operation", "extended", "completed", "digest"),
    [
        ("native_control", "reserve", False, False, "46149c852abd9c12941beabe727a3041320b70bea6fa4853a3d257075fcbaa0c"),
        ("native_control", "bind", False, False, "21a79b69dfe845ebc09409eecf120a599f2a369f21907774022631fc57d96b3a"),
        ("native_control", "read", False, False, "ff0d2e6d13578d6d38291d9b0193ab9ae284dfe47050645a643777d2a4e30b7d"),
        ("native_control", "close", False, False, "0eaa71cce6811c9a063429f2fa9f626480f100137cb94dc793334768df650b45"),
        (
            "native_control",
            "closure_read",
            False,
            False,
            "49d1342b09a514ac5f496404d6e217a2eddefcba51389e008a84693ec5286c4a",
        ),
        ("native_control", "reserve", True, False, "28bb5bb934be79d11d6ba4932ae95b749c5e442f1fb1b4cdf2b789df8954c64b"),
        ("native_control", "bind", True, False, "d381e09cbbb1a37c8571369f648c10d3815794f5e6defe6e2b584b48b3026dbc"),
        ("native_control", "read", True, False, "8654f747e09719d957962909994f5587aa8c088c0a37698991a970ec93c8e137"),
        ("native_control", "close", True, False, "0eaa71cce6811c9a063429f2fa9f626480f100137cb94dc793334768df650b45"),
        (
            "native_control",
            "closure_read",
            True,
            False,
            "49d1342b09a514ac5f496404d6e217a2eddefcba51389e008a84693ec5286c4a",
        ),
        ("native_control", "reserve", True, True, "5e822831375a9bc0afff69f64477bb6a819af206b6eb4fcae0a40997e84d271d"),
        ("native_control", "bind", True, True, "f94577a38876bae6bf60caa335c2021ca9cb5a94a8f85aeefebb488f154c28e4"),
        ("native_control", "read", True, True, "00ab75e5ee1ef36f7462a899db4b34e53ccef3cc1c1e8add86efeb7d0645f32d"),
        ("native_control", "close", True, True, "0eaa71cce6811c9a063429f2fa9f626480f100137cb94dc793334768df650b45"),
        (
            "native_control",
            "closure_read",
            True,
            True,
            "49d1342b09a514ac5f496404d6e217a2eddefcba51389e008a84693ec5286c4a",
        ),
        ("native_control", "complete", True, True, "a5e7807235beecc1744bc70141a74723ad1b72016c709a89e6ce2d76589f4966"),
        (
            "native_control",
            "completion_read",
            True,
            True,
            "0ea24e7f4502862fa5294f1e88d70c97c1df345a5e712e463f39ee2be7a8154c",
        ),
        ("native_control", "freeze", True, True, "e8d6bf3c98abadc60cff3d85cfd07c3d8abacd0c3a351f2b719b013155024dae"),
        (
            "native_control",
            "freeze_read",
            True,
            True,
            "526b1182cdff874e23dd9ba4311becce2ba85a989d6abb4789507e771e4bbaf4",
        ),
        (
            "native_control",
            "freeze_inspect",
            True,
            True,
            "5eec849098f696de4a4871568b48d607e560de8b871799a5640c9c40d83d89da",
        ),
        ("Control_42", "reserve", False, False, "e9056a7115a35a91ebdcacdeb34b05a77fa3db1a11e14a8d69f38b1e56c2f471"),
        ("Control_42", "bind", False, False, "12b61a4ee6ea64967ae243a2487d3075c66489fd914881f308eb566d9e924817"),
        ("Control_42", "read", False, False, "35c95db197360d010e44bffe589fa7ca77dbbf48c534b6b8bcbc3a39ed226b8d"),
        ("Control_42", "close", False, False, "9c4098c9d7de70c6e61f4869cb13f53c1f0f0f5a544ce77e4e6fe692f69df945"),
        (
            "Control_42",
            "closure_read",
            False,
            False,
            "ad4674afcbea7aae8a6d4690c2a47f1dbe4581dd3b46599994ecd49a687c86af",
        ),
        ("Control_42", "reserve", True, False, "c28d41e5e7d5b8d713adbfd3086f89e2e2888c54536fbc283a178bdebc8e02c4"),
        ("Control_42", "bind", True, False, "d7f61204329101d2599486a6f3470ab969dde80ee99350ba5848a490e5bfedbe"),
        ("Control_42", "read", True, False, "b839a9a08d8322c1728dc2116e7137d7328c59b3281113ec05a1505a4be314c6"),
        ("Control_42", "close", True, False, "9c4098c9d7de70c6e61f4869cb13f53c1f0f0f5a544ce77e4e6fe692f69df945"),
        ("Control_42", "closure_read", True, False, "ad4674afcbea7aae8a6d4690c2a47f1dbe4581dd3b46599994ecd49a687c86af"),
        ("Control_42", "reserve", True, True, "d6e56ca252aa2ed4baa500c2a6550e42bde8b77feb2059d1e5b1b5a33f289480"),
        ("Control_42", "bind", True, True, "460f151b6cc27a9bd9999b987667db1caf7615c3926f490126ce0ae988f3aad5"),
        ("Control_42", "read", True, True, "7dbcbdbbc63012a844995ba186c6c293c3793b00e1b5a9ec9bdfc636d9d5d1dc"),
        ("Control_42", "close", True, True, "9c4098c9d7de70c6e61f4869cb13f53c1f0f0f5a544ce77e4e6fe692f69df945"),
        ("Control_42", "closure_read", True, True, "ad4674afcbea7aae8a6d4690c2a47f1dbe4581dd3b46599994ecd49a687c86af"),
        ("Control_42", "complete", True, True, "19cda9c1a68c54bb495683c56fc6180472c48cc06b486685a8bfdfb078a56fdb"),
        (
            "Control_42",
            "completion_read",
            True,
            True,
            "0bd8180683f39ac6261ff4fad1d923be862bc1f030d6a9295c675afabbf71edd",
        ),
        ("Control_42", "freeze", True, True, "4b68a1a9ac8e1acc6a213193fa05c1c52c5927250ebf234073acee52d8fd9a6c"),
        ("Control_42", "freeze_read", True, True, "bbf502bc7ea62932b5f89c9ac59b72e0dba1ab57c9b0ba9728d3cf06b1355c99"),
        (
            "Control_42",
            "freeze_inspect",
            True,
            True,
            "e066211721f1da2b7bb70be3d8cb1cf6e4e85681a63343098cafe39bab6d0f0a",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "reserve",
            False,
            False,
            "853b7270f1094fedda7f9368b9db59353e0bc68fc341b6ed7ede595da07e47b6",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "bind",
            False,
            False,
            "735dadc933d89cc023a705967c408769d3967a3cd25925dff39ca125f67bd816",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "read",
            False,
            False,
            "4f001585e23ddeee8d9a63fc6ccc4250758d660d840d49525535a56b3aeed4fb",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "close",
            False,
            False,
            "8dc2ffe2b1889ec867a8dc30b203ddc1c026bc4ebad2f73d92cce63fd3e495b8",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "closure_read",
            False,
            False,
            "fe2eadc9bb65eafff1aeb4a46206454ee7e8fd8b2cd4fffc02eadc325ea7f648",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "reserve",
            True,
            False,
            "1277e68d675e3cce3e5077bd2c222047262eb29e87e876dead87a956303ec8bc",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "bind",
            True,
            False,
            "32fe27cf62ae274ea473ed1dedda857e8004b2c96828f1fdf28dce9b68e8612f",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "read",
            True,
            False,
            "e49242552d7d3c0b0a9046d2b2b537677181ac6b15721952d3d624c5aba4aae7",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "close",
            True,
            False,
            "8dc2ffe2b1889ec867a8dc30b203ddc1c026bc4ebad2f73d92cce63fd3e495b8",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "closure_read",
            True,
            False,
            "fe2eadc9bb65eafff1aeb4a46206454ee7e8fd8b2cd4fffc02eadc325ea7f648",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "reserve",
            True,
            True,
            "861eeb58c90dca70928f23c1ceab73c99ca447d662de32f66cb5b0e62b57822f",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "bind",
            True,
            True,
            "7edc44423ab473ab32a355dc346459c43d4ee70b376679c8a1476e17a4ac1d15",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "read",
            True,
            True,
            "16ebeb13ef1c53d49f792907463d867273d351bb38e531f9ce00c777172df6c5",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "close",
            True,
            True,
            "8dc2ffe2b1889ec867a8dc30b203ddc1c026bc4ebad2f73d92cce63fd3e495b8",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "closure_read",
            True,
            True,
            "fe2eadc9bb65eafff1aeb4a46206454ee7e8fd8b2cd4fffc02eadc325ea7f648",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "complete",
            True,
            True,
            "bf1f46cad3a29d2d0bdc15f637a8badc43996df377b1ff3f0c0f0c09b6e8ce7a",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "completion_read",
            True,
            True,
            "0d4308235bb39ac0d700ee8a9900aacecec5f1ca748de408905c5960a9077b11",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "freeze",
            True,
            True,
            "2aace52f53cc5b09797d0aeb3886339fbf23c5cff70d93fb477560c9b08d353a",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "freeze_read",
            True,
            True,
            "9b0c6e41a015a73d2427f5036b129999e7327f5c608d7d6ce71617f961919851",
        ),
        (
            "ctl_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "freeze_inspect",
            True,
            True,
            "b9b2211c91409b7a095e9af2c5c86b05b714bc107b2d4625db9f71e0ce91ae3e",
        ),
    ],
)
def test_all_historical_procedure_bytes(
    schema: str, operation: str, extended: bool, completed: bool, digest: str
) -> None:
    sql = queries.generation_procedure(schema, operation, extended=extended, completed=completed)
    assert sha256(sql.encode("utf-8")).hexdigest() == digest


def test_compatibility_imports_share_the_owner_producer() -> None:
    assert queries._physical_owner is owner.physical_owner
    assert queries._attempt_identity is owner._attempt_identity


def test_owner_retains_full_request_identity_and_exact_lease_joins() -> None:
    sql = owner.physical_owner("native_control")
    for required in (
        "dpone.dbt-workspace-attempt-request.v1",
        "HASHBYTES('SHA2_256',",
        "DPONE_NATIVE_GENERATION_ATTEMPT_HASH_INVALID",
        "DATALENGTH(@activation_text)<>72",
        "DATALENGTH(@attempt_text)<>142",
        "t.activation_id=a.activation_id",
        "ag.attempt_id=t.attempt_id",
        "g.activation_id=a.activation_id AND g.guard_id=ag.guard_id AND g.fencing_epoch=ag.fencing_epoch",
        "p.resource_id=g.guard_id AND p.fencing_epoch=g.fencing_epoch",
        "a.state=N'ACTIVE' AND t.attempt_id=@attempt AND t.state=N'RUNNING'",
        "CONVERT(varbinary(max),t.request_sha256)=",
        "CONVERT(varbinary(max),t.workflow_id)=CONVERT(varbinary(max),JSON_VALUE(@json,'$.workspace_attempt.workflow_id'))",
        "CONVERT(varbinary(max),g.guard_id)=CONVERT(varbinary(max),@guard) AND g.fencing_epoch=@epoch",
        "p.owner_id=N'dbt-workspace:'+LOWER(CONVERT(nvarchar(36),@activation))",
        "p.workflow_id=LOWER(CONVERT(nvarchar(36),@activation)) AND p.operation_id IS NULL AND p.status=N'HELD'",
        "a.environment=JSON_VALUE(@json,'$.subject.authority.environment')",
        "a.release_id=JSON_VALUE(@json,'$.subject.authority.release_id')",
        "a.deployment_id=JSON_VALUE(@json,'$.subject.authority.deployment_id')",
        "DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED",
    ):
        assert required in sql


def test_owner_retains_exact_subject_membership_and_single_guard_footprint() -> None:
    sql = owner.physical_owner("native_control")
    for required in (
        "WHERE a.subject>=b.subject",
        "LEFT JOIN [native_control].[dbt_workspace_activation_write_subjects] w WITH (UPDLOCK,HOLDLOCK)",
        "w.activation_id=@activation AND CONVERT(varbinary(max),w.write_subject_sha256)=",
        "CONVERT(varbinary(max),w.guard_id)=CONVERT(varbinary(max),@guard)",
        "WHERE j.type<>1 OR w.guard_id IS NULL",
        "WHERE attempt_id=@attempt AND CONVERT(varbinary(max),guard_id)<>CONVERT(varbinary(max),@guard)",
        "DPONE_NATIVE_GENERATION_WRITE_FOOTPRINT_INVALID",
    ):
        assert required in sql


def test_reservation_retains_utf16_guard_hash_identity() -> None:
    sql = queries.generation_procedure("native_control", "reserve", completed=True)
    assert "DECLARE @guard nvarchar(512)=JSON_VALUE(@json,'$.guard.guard_id');" in sql
    assert "guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))" in sql
    assert "guard_id=CONVERT(varbinary(max),@guard) AND DATALENGTH(guard_id)=DATALENGTH(@guard)" in sql
