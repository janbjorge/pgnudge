"""Doctor unit tests against the scripted fake walsender; no PostgreSQL.

One handler plays all three probe connections (plain / logical / physical)
by branching on the ``replication`` startup parameter.
"""

import asyncio
from collections.abc import Callable, Coroutine

from wire import (
    auth_request,
    backend_key,
    data_row,
    error_response,
    read_frame,
    read_startup,
    ready_for_query,
    refused_port,
    scripted_server,
)

from pgnudge.doctor import Check, Diagnosis, _explain, diagnose
from pgnudge.proto import PgServerError

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Coroutine[None, None, None]]

_NO_ROW = object()  # sentinel: answer a query with ReadyForQuery and no DataRow


def _handshake(pid: int = 4242) -> bytes:
    return auth_request(0) + backend_key(pid) + ready_for_query()


def _row_reply(value: object) -> bytes:
    if value is _NO_ROW:
        return ready_for_query()
    assert isinstance(value, bytes) or value is None
    return data_row(value) + ready_for_query()


def scripted_doctor(
    *,
    version: object = b"170002",
    wal_level: object = b"logical",
    privileged: object = b"t",
    platform: tuple[bytes | None, bytes | None, bytes | None] | None = (b"f", b"f", b"f"),
    logical_error: tuple[str, str] | None = None,
    physical: str = "ok",  # "ok" | "empty" | "error"
) -> Handler:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        params = await read_startup(reader)
        writer.write(_handshake())
        await writer.drain()
        mode = params.get("replication")
        if mode == "false":
            for value in (version, wal_level, privileged):
                await read_frame(reader)
                writer.write(_row_reply(value))
                await writer.drain()
            await read_frame(reader)  # platform fingerprint: one row, three booleans
            row = ready_for_query() if platform is None else data_row(*platform) + ready_for_query()
            writer.write(row)
            await writer.drain()
        elif mode == "database":
            await read_frame(reader)
            if logical_error is not None:
                writer.write(error_response(logical_error[1], logical_error[0]) + ready_for_query())
            else:
                writer.write(ready_for_query())
            await writer.drain()
        elif mode == "true":
            await read_frame(reader)
            if physical == "ok":
                writer.write(data_row(b"sys", b"1", b"0/0", None) + ready_for_query())
            elif physical == "empty":
                writer.write(data_row() + ready_for_query())
            else:
                writer.write(error_response("permission denied", "42501") + ready_for_query())
            await writer.drain()
        await reader.read()

    return handler


async def _diagnose(host: str, port: int, plugin: str = "wal2json") -> Diagnosis:
    return await diagnose(host=host, port=port, user="u", database="d", plugin=plugin, connect_timeout=2.0)


def _check(diag: Diagnosis, name: str) -> Check:
    return next(c for c in diag.checks if c.name == name)


# -- recommendation branches --------------------------------------------------


async def test_recommends_walfeed_when_logical_probe_succeeds() -> None:
    async with scripted_server(scripted_doctor()) as (host, port):
        diag = await _diagnose(host, port)
    assert diag.recommended == "WalFeed"
    assert all(c.ok for c in diag.checks)


async def test_recommends_rawfeed_when_only_physical_works() -> None:
    handler = scripted_doctor(wal_level=b"replica", logical_error=("55000", "logical decoding requires wal_level"))
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert diag.recommended == "RawFeed"
    walfeed = _check(diag, "WalFeed (logical decoding)")
    assert not walfeed.ok
    assert "wal_level=replica" in walfeed.detail  # the actionable hint


async def test_no_transport_when_both_probes_fail() -> None:
    handler = scripted_doctor(logical_error=("55000", "nope"), physical="error")
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert diag.recommended is None


# -- individual checks --------------------------------------------------------


async def test_connect_failure_yields_single_check() -> None:
    diag = await _diagnose("127.0.0.1", refused_port())
    assert diag.recommended is None
    assert len(diag.checks) == 1
    assert diag.checks[0].name == "connect"
    assert not diag.checks[0].ok


async def test_old_server_version_fails_check() -> None:
    async with scripted_server(scripted_doctor(version=b"150000")) as (host, port):
        diag = await _diagnose(host, port)
    assert not _check(diag, "server version").ok
    assert diag.recommended == "WalFeed"  # version check is independent of the probes


async def test_missing_version_row_fails_gracefully() -> None:
    async with scripted_server(scripted_doctor(version=_NO_ROW)) as (host, port):
        diag = await _diagnose(host, port)
    version = _check(diag, "server version")
    assert not version.ok
    assert "?" in version.detail


async def test_missing_replication_privilege_fails_check() -> None:
    async with scripted_server(scripted_doctor(privileged=b"f")) as (host, port):
        diag = await _diagnose(host, port)
    assert not _check(diag, "REPLICATION role").ok


async def test_physical_probe_empty_row_fails() -> None:
    handler = scripted_doctor(logical_error=("55000", "nope"), physical="empty")
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    physical = _check(diag, "RawFeed (physical WAL)")
    assert not physical.ok
    assert "no row" in physical.detail


async def test_missing_plugin_gets_a_hint() -> None:
    handler = scripted_doctor(logical_error=("58P01", "could not open extension control file"), physical="ok")
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    walfeed = _check(diag, "WalFeed (logical decoding)")
    assert "output plugin not installed" in walfeed.detail
    assert walfeed.fix is not None and "test_decoding" in walfeed.fix  # install wal2json or fall back
    assert diag.recommended == "RawFeed"


# -- test_decoding fallback ---------------------------------------------------


async def test_walfeed_falls_back_to_test_decoding_when_wal2json_missing() -> None:
    """A missing wal2json (58P01) retries with test_decoding; logical still works."""
    db_calls = 0

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal db_calls
        params = await read_startup(reader)
        writer.write(_handshake())
        await writer.drain()
        mode = params.get("replication")
        if mode == "false":
            for value in (b"170002", b"logical", b"t"):
                await read_frame(reader)
                writer.write(data_row(value) + ready_for_query())
                await writer.drain()
            await read_frame(reader)
            writer.write(data_row(b"f", b"f", b"f") + ready_for_query())
            await writer.drain()
        elif mode == "database":
            _mtype, body = await read_frame(reader)
            db_calls += 1
            if b"wal2json" in body:
                writer.write(error_response("could not open extension control file", "58P01") + ready_for_query())
            else:
                writer.write(ready_for_query())
            await writer.drain()
        elif mode == "true":
            await read_frame(reader)
            writer.write(data_row(b"sys", b"1", b"0/0", None) + ready_for_query())
            await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)  # default plugin wal2json
    walfeed = _check(diag, "WalFeed (logical decoding)")
    assert walfeed.ok
    assert "test_decoding fallback" in walfeed.detail
    assert diag.recommended == "WalFeed"
    assert db_calls == 2  # wal2json probe, then the test_decoding fallback


# -- platform-aware fixes -----------------------------------------------------


async def test_detects_rds_and_tailors_fixes() -> None:
    handler = scripted_doctor(
        platform=(b"t", b"f", b"f"),
        privileged=b"f",
        wal_level=b"replica",
        logical_error=("55000", "logical decoding requires wal_level >= logical"),
        physical="error",
    )
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert _check(diag, "platform").detail.endswith("Amazon RDS / Aurora")
    role_fix = _check(diag, "REPLICATION role").fix
    assert role_fix is not None and "rds_replication" in role_fix
    walfeed_fix = _check(diag, "WalFeed (logical decoding)").fix
    assert walfeed_fix is not None and "rds.logical_replication" in walfeed_fix
    physical_fix = _check(diag, "RawFeed (physical WAL)").fix
    assert physical_fix is not None and "use WalFeed" in physical_fix


async def test_self_managed_fixes_use_alter_system() -> None:
    handler = scripted_doctor(
        privileged=b"f",
        wal_level=b"replica",
        logical_error=("55000", "logical decoding requires wal_level >= logical"),
        physical="error",
    )
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert _check(diag, "platform").detail.endswith("self-managed / undetected")
    role_fix = _check(diag, "REPLICATION role").fix
    assert role_fix is not None and "ALTER ROLE" in role_fix
    walfeed_fix = _check(diag, "WalFeed (logical decoding)").fix
    assert walfeed_fix is not None and "ALTER SYSTEM SET wal_level" in walfeed_fix
    physical_fix = _check(diag, "RawFeed (physical WAL)").fix
    assert physical_fix is not None and "pg_hba.conf" in physical_fix


async def test_detects_azure_and_tailors_fixes() -> None:
    handler = scripted_doctor(
        platform=(b"f", b"t", b"f"),
        privileged=b"f",
        wal_level=b"replica",
        logical_error=("55000", "logical decoding requires wal_level >= logical"),
        physical="error",
    )
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert _check(diag, "platform").detail.endswith("Azure Flexible Server")
    role_fix = _check(diag, "REPLICATION role").fix
    assert role_fix is not None and "azure_pg_admin" in role_fix
    walfeed_fix = _check(diag, "WalFeed (logical decoding)").fix
    assert walfeed_fix is not None and "az postgres flexible-server" in walfeed_fix
    physical_fix = _check(diag, "RawFeed (physical WAL)").fix
    assert physical_fix is not None and "managed platforms usually block" in physical_fix


async def test_detects_gcp_and_tailors_fixes() -> None:
    handler = scripted_doctor(
        platform=(b"f", b"f", b"t"),
        wal_level=b"replica",
        logical_error=("55000", "logical decoding requires wal_level >= logical"),
        physical="error",
    )
    async with scripted_server(handler) as (host, port):
        diag = await _diagnose(host, port)
    assert _check(diag, "platform").detail.endswith("Google Cloud SQL")
    walfeed_fix = _check(diag, "WalFeed (logical decoding)").fix
    assert walfeed_fix is not None and "cloudsql.logical_decoding" in walfeed_fix
    physical_fix = _check(diag, "RawFeed (physical WAL)").fix
    assert physical_fix is not None and "managed platforms usually block" in physical_fix


async def test_platform_detection_tolerates_missing_row() -> None:
    """A server that returns no fingerprint row is treated as self-managed."""
    async with scripted_server(scripted_doctor(platform=None)) as (host, port):
        diag = await _diagnose(host, port)
    assert _check(diag, "platform").detail.endswith("self-managed / undetected")


async def test_plugin_argument_is_probed() -> None:
    seen: dict[str, bytes] = {}

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        params = await read_startup(reader)
        writer.write(_handshake())
        await writer.drain()
        mode = params.get("replication")
        if mode == "false":
            for _ in range(3):
                await read_frame(reader)
                writer.write(data_row(b"170002") + ready_for_query())
                await writer.drain()
            await read_frame(reader)  # platform fingerprint
            writer.write(data_row(b"f", b"f", b"f") + ready_for_query())
            await writer.drain()
        elif mode == "database":
            _mtype, body = await read_frame(reader)
            seen["slot"] = body
            writer.write(ready_for_query())
            await writer.drain()
        elif mode == "true":
            await read_frame(reader)
            writer.write(data_row(b"sys", b"1", b"0/0", None) + ready_for_query())
            await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        await _diagnose(host, port, plugin="test_decoding")
    assert b"TEMPORARY LOGICAL test_decoding" in seen["slot"]


# -- _explain -----------------------------------------------------------------


def test_explain_maps_plugin_error_code() -> None:
    err = PgServerError({"C": "58P01", "M": "could not open"})
    assert "output plugin not installed" in _explain(err)


def test_explain_uses_code_and_message_for_other_server_errors() -> None:
    assert _explain(PgServerError({"C": "42501", "M": "denied"})) == "42501: denied"


def test_explain_falls_back_to_str_then_type_name() -> None:
    assert _explain(ValueError("boom")) == "boom"
    assert _explain(ValueError()) == "ValueError"  # empty str -> class name
