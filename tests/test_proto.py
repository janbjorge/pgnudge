"""WalsenderConnection unit tests against a scripted in-process server.

No PostgreSQL: a plain asyncio TCP server plays the walsender side of the
wire protocol, one scripted handler per test.
"""

import asyncio
import ssl
import struct
from typing import cast

import pytest
from wire import (
    auth_request,
    backend_key,
    command_complete,
    copy_both_response,
    error_response,
    keepalive,
    msg,
    notice_response,
    read_frame,
    read_startup,
    ready_for_query,
    scripted_server,
    xlog_data,
)

from pgnudge import proto
from pgnudge.proto import Keepalive, PgServerError, WalsenderConnection, XLogData


async def connect(
    host: str,
    port: int,
    *,
    password: str | None = None,
    use_ssl: bool = False,
    timeout: float = 2.0,
) -> WalsenderConnection:
    return await WalsenderConnection.connect(
        host=host,
        port=port,
        user="alice",
        database="db",
        password=password,
        ssl=use_ssl,
        connect_timeout=timeout,
    )


def trust_handshake(pid: int = 4242) -> bytes:
    return auth_request(0) + backend_key(pid) + ready_for_query()


# -- connect & startup ----------------------------------------------------------


async def test_startup_requests_replication_database_mode() -> None:
    seen: dict[str, str] = {}

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        seen.update(await read_startup(reader))
        writer.write(trust_handshake(pid=7))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        conn.abort()
    assert seen["replication"] == "database"
    assert seen["user"] == "alice"
    assert seen["database"] == "db"
    assert seen["application_name"] == "pgnudge"
    assert conn.backend_pid == 7


async def test_connect_raises_when_server_refuses_ssl() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readexactly(8)  # SSLRequest
        writer.write(b"N")
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(ConnectionError, match="refused SSL"):
            await connect(host, port, use_ssl=True)


async def test_connect_times_out_on_silent_server() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        await reader.read()  # never answer

    async with scripted_server(handler) as (host, port):
        with pytest.raises(TimeoutError):
            await connect(host, port, timeout=0.2)


def test_default_ssl_context_is_verify_full_shaped() -> None:
    ctx = proto._default_ssl_context()
    assert ctx.verify_mode is ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


# -- authentication ---------------------------------------------------------------


async def test_cleartext_password_auth_sends_password() -> None:
    got: list[bytes] = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(3))  # CleartextPassword
        await writer.drain()
        mtype, body = await read_frame(reader)
        got.append(mtype + body)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port, password="hunter2")
        conn.abort()
    assert got == [b"p" + b"hunter2\x00"]


async def test_cleartext_request_without_password_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(3))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="none was given"):
            await connect(host, port)


async def test_unsupported_auth_code_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(5, b"\x01\x02\x03\x04"))  # MD5, deliberately absent
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="code 5"):
            await connect(host, port, password="pw")


async def test_sasl_with_only_plus_mechanisms_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(10, b"SCRAM-SHA-256-PLUS\x00\x00"))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="unsupported SASL"):
            await connect(host, port, password="pw")


async def test_notice_during_auth_is_skipped() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(notice_response() + trust_handshake(pid=11))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        conn.abort()
    assert conn.backend_pid == 11


async def test_auth_error_response_raises_with_fields() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(error_response("password authentication failed", code="28P01"))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="28P01") as exc:
            await connect(host, port)
    assert exc.value.fields["C"] == "28P01"


async def test_post_auth_error_before_ready_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(0) + error_response("too many connections", code="53300"))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="53300"):
            await connect(host, port)


# -- simple query -----------------------------------------------------------------


async def test_simple_query_drains_to_ready_then_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)  # the Q message
        writer.write(error_response("relation missing", code="42P01") + notice_response() + ready_for_query())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        with pytest.raises(PgServerError, match="42P01"):
            await conn.simple_query("SELECT 1")
        conn.abort()


async def test_simple_query_ignores_result_rows() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)
        writer.write(
            msg(b"T", b"\x00\x01")  # RowDescription (content irrelevant, skipped)
            + msg(b"D", b"\x00\x01")  # DataRow
            + command_complete("SELECT 1")
            + ready_for_query()
        )
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.simple_query("SELECT 1")  # rows skipped, returns cleanly
        conn.abort()


# -- replication stream -----------------------------------------------------------


async def test_start_replication_skips_chatter_until_copy_both() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)
        writer.write(notice_response() + copy_both_response())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.start_replication('START_REPLICATION SLOT "s" LOGICAL 0/0')
        conn.abort()


async def test_start_replication_error_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)
        writer.write(error_response("replication slot does not exist", code="42704"))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        with pytest.raises(PgServerError, match="42704"):
            await conn.start_replication('START_REPLICATION SLOT "nope" LOGICAL 0/0')
        conn.abort()


async def test_read_stream_parses_xlog_keepalive_and_stream_end() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(
            trust_handshake()
            + msg(b"d", b"z-unknown-subtype")  # skipped
            + notice_response()  # non-CopyData chatter, also skipped
            + xlog_data(42, b"table public.picks: INSERT")
            + keepalive(99, reply=True)
            + command_complete("COPY 0")
            + ready_for_query()
        )
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        assert await conn.read_stream() == XLogData(end_lsn=42, payload=b"table public.picks: INSERT")
        assert await conn.read_stream() == Keepalive(end_lsn=99, reply_requested=True)
        with pytest.raises(ConnectionResetError, match="stream ended"):
            await conn.read_stream()
        conn.abort()


async def test_read_stream_raises_on_server_error() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake() + error_response("terminating connection", code="57P01"))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        with pytest.raises(PgServerError, match="57P01"):
            await conn.read_stream()
        conn.abort()


async def test_send_standby_status_acknowledges_lsn_three_ways() -> None:
    got: list[bytes] = []
    received = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        mtype, body = await read_frame(reader)
        got.append(mtype + body)
        received.set()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.send_standby_status(0x1_0000_002A)
        await asyncio.wait_for(received.wait(), 2.0)
        conn.abort()

    frame = got[0]
    assert frame[:2] == b"dr"
    written, flushed, applied = struct.unpack("!QQQ", frame[2:26])
    assert written == flushed == applied == 0x1_0000_002A
    assert frame[34:35] == b"\x00"  # no reply requested


async def test_abort_is_idempotent() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        conn.abort()
        conn.abort()  # second abort: no-op, never raises


async def test_abort_tolerates_non_write_transport() -> None:
    class ReadOnlyTransport(asyncio.ReadTransport):
        def is_closing(self) -> bool:
            return True  # keeps StreamWriter.__del__ quiet at GC

    writer = asyncio.StreamWriter(
        cast(asyncio.Transport, ReadOnlyTransport()),
        asyncio.Protocol(),
        None,
        asyncio.get_running_loop(),
    )
    conn = WalsenderConnection(asyncio.StreamReader(), writer)
    conn.abort()  # nothing abortable on a read-only transport; must not raise
