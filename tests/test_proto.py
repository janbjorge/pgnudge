"""WalsenderConnection unit tests against a scripted in-process server.

No PostgreSQL: a plain asyncio TCP server plays the walsender side of the
wire protocol, one scripted handler per test.
"""

import asyncio
import ssl
from typing import cast

import pytest
from wire import (
    auth_request,
    backend_key,
    command_complete,
    copy_both_response,
    data_row,
    error_response,
    keepalive,
    msg,
    notice_response,
    read_frame,
    read_standby_status,
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


async def test_startup_requests_physical_mode_when_asked() -> None:
    seen: dict[str, str] = {}

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        seen.update(await read_startup(reader))
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await WalsenderConnection.connect(
            host=host, port=port, user="alice", database="db", replication="true", connect_timeout=2.0
        )
        conn.abort()
    assert seen["replication"] == "true"


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


async def test_connect_times_out_when_server_never_answers_ssl_request() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readexactly(8)  # SSLRequest
        await reader.read()  # never answer

    async with scripted_server(handler) as (host, port):
        with pytest.raises(TimeoutError):
            await connect(host, port, use_ssl=True, timeout=0.2)


def test_parse_error_tolerates_truncated_field() -> None:
    assert proto._parse_error(b"Mboom") == {"M": "boom"}  # final NUL terminator missing


def test_default_ssl_context_is_verify_full_shaped() -> None:
    ctx = proto._default_ssl_context()
    assert ctx.verify_mode is ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


# -- authentication ---------------------------------------------------------------


async def tls_flagged_connection(host: str, port: int) -> WalsenderConnection:
    # White-box: the refusal keys off the tls flag, not the encryption
    # itself, so flip the flag on a plain socket instead of teaching the
    # scripted server TLS.
    reader, writer = await asyncio.open_connection(host, port)
    return WalsenderConnection(reader, writer, tls=True)


async def test_cleartext_password_without_tls_is_refused() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(3))  # CleartextPassword
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match="refusing cleartext"):
            await connect(host, port, password="hunter2")


async def test_cleartext_password_auth_sends_password_over_tls() -> None:
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
        conn = await tls_flagged_connection(host, port)
        await conn._startup(user="alice", database="db", password="hunter2", application_name="pgnudge")
        conn.abort()
    assert got == [b"p" + b"hunter2\x00"]


async def test_md5_password_auth_sends_hashed_token() -> None:
    import hashlib

    got: list[bytes] = []
    salt = b"\x01\x02\x03\x04"

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(5, salt))  # MD5Password
        await writer.drain()
        mtype, body = await read_frame(reader)
        got.append(mtype + body)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port, password="hunter2")
        conn.abort()

    inner = hashlib.md5(b"hunter2" + b"alice").hexdigest()
    expected = ("md5" + hashlib.md5(inner.encode() + salt).hexdigest()).encode() + b"\x00"
    assert got == [b"p" + expected]


async def test_cleartext_request_without_password_raises() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(3))
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await tls_flagged_connection(host, port)
        with pytest.raises(PgServerError, match="none was given"):
            await conn._startup(user="alice", database="db", password=None, application_name="pgnudge")
        conn.abort()


@pytest.mark.parametrize(
    ("auth", "match"),
    [
        pytest.param(auth_request(2, b""), "code 2", id="kerberos-deliberately-absent"),
        pytest.param(auth_request(10, b"SCRAM-SHA-256-PLUS\x00\x00"), "unsupported SASL", id="only-plus-mechanisms"),
        pytest.param(auth_request(11, b"r=nope"), "SASLContinue before SASL", id="continue-with-no-exchange"),
        pytest.param(auth_request(12, b"v=nope"), "SASLFinal before SASL", id="final-with-no-exchange"),
    ],
)
async def test_unacceptable_auth_request_raises(auth: bytes, match: str) -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth)
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        with pytest.raises(PgServerError, match=match):
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
            + data_row(b"1")  # DataRow, parsed then discarded
            + command_complete("SELECT 1")
            + ready_for_query()
        )
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.simple_query("SELECT 1")  # rows skipped, returns cleanly
        conn.abort()


async def test_simple_query_rows_returns_text_values_and_nulls() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)
        writer.write(
            msg(b"T", b"\x00\x03")  # RowDescription, content skipped
            + data_row(b"16384", b"public.picks", None)
            + data_row(b"16400", None, b"x")
            + command_complete("SELECT 2")
            + ready_for_query()
        )
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        rows = await conn.simple_query_rows("SELECT relfilenode, name, note FROM t")
        conn.abort()
    assert rows == [("16384", "public.picks", None), ("16400", None, "x")]


async def test_simple_query_rows_raises_after_ready_on_error() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await read_frame(reader)
        writer.write(error_response("permission denied", code="42501") + ready_for_query())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        with pytest.raises(PgServerError, match="42501"):
            await conn.simple_query_rows("SELECT 1")
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
        assert await conn.read_stream() == XLogData(
            start_lsn=42, end_lsn=42, payload=b"table public.picks: INSERT"
        )
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
    got: list[tuple[bytes, bytes]] = []
    received = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        got.append(await read_frame(reader))
        received.set()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.send_standby_status(0x1_0000_002A)
        await asyncio.wait_for(received.wait(), 2.0)
        conn.abort()

    mtype, body = got[0]
    assert mtype == b"d"
    written, flushed, applied, reply = read_standby_status(body)
    assert written == flushed == applied == 0x1_0000_002A
    assert reply is False  # no reply requested


async def test_send_standby_status_can_request_a_reply() -> None:
    got: list[tuple[bytes, bytes]] = []
    received = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        got.append(await read_frame(reader))
        received.set()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.send_standby_status(42, reply=True)
        await asyncio.wait_for(received.wait(), 2.0)
        conn.abort()

    assert read_standby_status(got[0][1]) == (42, 42, 42, True)


async def test_send_standby_status_serializes_concurrent_senders() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        await conn.send_lock.acquire()
        send = asyncio.create_task(conn.send_standby_status(1))
        await asyncio.sleep(0.05)
        assert not send.done()  # a concurrent sender holds the lock; nothing on the wire yet
        conn.send_lock.release()
        await asyncio.wait_for(send, 2.0)
        conn.abort()


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


async def test_async_context_manager_aborts_on_exit() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        async with await connect(host, port) as conn:
            assert conn.backend_pid == 4242
        assert conn._writer.transport.is_closing()  # __aexit__ hard-closed the socket


async def test_async_context_manager_aborts_on_error() -> None:
    """An exception inside the block still hard-closes and propagates (no suppression)."""

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(trust_handshake())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (host, port):
        conn = await connect(host, port)
        with pytest.raises(RuntimeError, match="boom"):
            async with conn:
                raise RuntimeError("boom")
        assert conn._writer.transport.is_closing()


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
