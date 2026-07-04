"""WalFeed unit tests: payload parsing, command assembly, and a full
lifecycle against a scripted in-process walsender — no PostgreSQL.
"""

import asyncio
import logging
import struct

import pytest
from wire import (
    auth_request,
    backend_key,
    command_complete,
    copy_both_response,
    error_response,
    keepalive,
    read_frame,
    read_startup,
    ready_for_query,
    refused_port,
    scripted_server,
    xlog_data,
)

from pgnudge import Batch, Resync, WalFeed
from pgnudge.proto import WalsenderConnection


def wal_feed(port: int, *, plugin: str = "wal2json", status_interval: float = 10.0) -> WalFeed:
    return WalFeed(
        host="127.0.0.1",
        port=port,
        user="alice",
        database="db",
        plugin=plugin,
        status_interval=status_interval,
        connect_timeout=1.0,
        debounce=0.05,
        backoff=(0.01, 0.05),
    )


# -- payload parsing --------------------------------------------------------------


def test_parse_wal2json_v2_emits_schema_table_for_dml_and_truncate() -> None:
    for action in ("I", "U", "D", "T"):
        payload = f'{{"action":"{action}","schema":"public","table":"picks"}}'.encode()
        assert WalFeed._parse_wal2json_v2(payload) == ["public.picks"]


def test_parse_wal2json_v2_ignores_non_table_actions_and_junk() -> None:
    assert WalFeed._parse_wal2json_v2(b'{"action":"B"}') == []  # begin
    assert WalFeed._parse_wal2json_v2(b'{"action":"M","prefix":"p","content":"c"}') == []  # message
    assert WalFeed._parse_wal2json_v2(b"[1, 2]") == []  # not an object
    assert WalFeed._parse_wal2json_v2(b'"just a string"') == []
    assert WalFeed._parse_wal2json_v2(b"not json at all") == []


def test_parse_wal2json_v2_tolerates_missing_names() -> None:
    assert WalFeed._parse_wal2json_v2(b'{"action":"I"}') == ["?.?"]


def test_parse_test_decoding_matches_dml_and_truncate() -> None:
    assert WalFeed._parse_test_decoding(b"table public.picks: INSERT: id[integer]:1") == ["public.picks"]
    assert WalFeed._parse_test_decoding(b"table public.picks: UPDATE: id[integer]:1") == ["public.picks"]
    assert WalFeed._parse_test_decoding(b"table public.picks: DELETE: id[integer]:1") == ["public.picks"]
    assert WalFeed._parse_test_decoding(b"table public.picks: TRUNCATE: (no-flags)") == ["public.picks"]
    assert WalFeed._parse_test_decoding(b"table public.a, public.b: TRUNCATE: restart_seqs cascade") == [
        "public.a",
        "public.b",
    ]
    assert WalFeed._parse_test_decoding(b"BEGIN 777") == []
    assert WalFeed._parse_test_decoding(b"COMMIT 777") == []


# -- command assembly -------------------------------------------------------------


def test_unsupported_plugin_is_rejected() -> None:
    with pytest.raises(ValueError, match="pgoutput"):
        WalFeed(host="h", port=5432, user="u", database="d", plugin="pgoutput")


def test_plugin_options_wal2json_quotes_and_filters_tables() -> None:
    feed = WalFeed(host="h", port=5432, user="u", database="d", tables=["public.picks", "s.o'brien"])
    opts = feed._plugin_options()
    assert "\"format-version\" '2'" in opts
    assert "\"include-transaction\" 'false'" in opts
    assert "\"add-tables\" 'public.picks,s.o''brien'" in opts  # SQL-quoted, apostrophe doubled


def test_plugin_options_wal2json_without_tables_has_no_filter() -> None:
    feed = WalFeed(host="h", port=5432, user="u", database="d")
    assert "add-tables" not in feed._plugin_options()


def test_plugin_options_test_decoding_skips_empty_xacts() -> None:
    feed = WalFeed(host="h", port=5432, user="u", database="d", plugin="test_decoding")
    assert feed._plugin_options() == "\"skip-empty-xacts\" '1'"


# -- teardown & feedback ----------------------------------------------------------


async def test_extra_close_aborts_and_clears_connection() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await writer.drain()
        await reader.read()

    def attach(feed: WalFeed, conn: WalsenderConnection) -> None:
        # in a helper so mypy's attribute narrowing doesn't outlive the call
        feed._conn = conn
        feed.slot_name = "pgnudge_x"

    async with scripted_server(handler) as (host, port):
        conn = await WalsenderConnection.connect(host=host, port=port, user="u", database="d")
        feed = wal_feed(port)
        attach(feed, conn)
        await feed._extra_close()
        assert feed._conn is None
        assert feed.slot_name is None


class RecordingConn(WalsenderConnection):
    """Stub connection: records statuses/aborts, fails the nth send."""

    def __init__(self, *, fail_after: int | None = None) -> None:
        self.backend_pid = None
        self.fail_after = fail_after
        self.sent: list[tuple[int, bool]] = []
        self.aborted = False

    async def send_standby_status(self, lsn: int, *, reply: bool = False) -> None:
        self.sent.append((lsn, reply))
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise ConnectionResetError

    def abort(self) -> None:
        self.aborted = True


async def test_feedback_loop_sends_status_until_send_fails() -> None:
    conn = RecordingConn(fail_after=2)
    feed = wal_feed(5432, status_interval=0.01)
    feed._last_lsn = 77
    await asyncio.wait_for(feed._feedback_loop(conn), 2.0)  # returns on send failure
    assert conn.sent == [(77, False), (77, False)]
    assert not conn.aborted


# -- supervisor -------------------------------------------------------------------


async def test_supervisor_keeps_retrying_when_server_unreachable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="pgnudge.wal")
    feed = wal_feed(refused_port())
    async with feed:
        await asyncio.sleep(0.1)  # several connect -> backoff rounds
        assert feed.connection_pid is None
        assert feed.slot_name is None
    assert any("failed" in r.message and r.levelno == logging.WARNING for r in caplog.records)
    assert any("reconnect attempt" in r.message and r.levelno == logging.DEBUG for r in caplog.records)


async def test_supervisor_retries_when_slot_creation_fails() -> None:
    attempts = 0

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal attempts
        attempts += 1
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await writer.drain()
        await read_frame(reader)  # CREATE_REPLICATION_SLOT
        writer.write(error_response("all replication slots are in use", code="53400") + ready_for_query())
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (_, port):
        async with wal_feed(port) as feed:
            await asyncio.sleep(0.15)
            assert feed.slot_name is None  # never went live
    assert attempts >= 2  # fresh connection per retry


async def test_supervisor_exits_cleanly_when_closing_during_stream_error() -> None:
    proceed = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await read_frame(reader)
        writer.write(command_complete("CREATE_REPLICATION_SLOT") + ready_for_query())
        await read_frame(reader)
        writer.write(copy_both_response())
        await writer.drain()
        await proceed.wait()
        writer.write(command_complete("COPY 0") + ready_for_query())  # end the stream
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (_, port):
        feed = wal_feed(port)
        supervisor = asyncio.create_task(feed._supervisor())
        assert await asyncio.wait_for(feed._service.next_item(), 2.0) == Resync("connected")
        feed._service.closing = True  # as aclose would, but without cancelling
        proceed.set()  # stream ends now; the supervisor must return, not reconnect
        await asyncio.wait_for(supervisor, 2.0)
        assert feed.slot_name is None


class FakeWalsender:
    """Scripted walsender: trust auth, slot ritual, one insert plus a
    reply-requested keepalive, then a stream end that forces a reconnect;
    the second session streams a different table and idles."""

    def __init__(self) -> None:
        self.connections = 0
        self.commands: list[str] = []
        self.statuses: list[int] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        nth = self.connections
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1000 + nth) + ready_for_query())
        _, create = await read_frame(reader)
        self.commands.append(create.rstrip(b"\x00").decode())
        writer.write(command_complete("CREATE_REPLICATION_SLOT") + ready_for_query())
        _, start = await read_frame(reader)
        self.commands.append(start.rstrip(b"\x00").decode())
        writer.write(copy_both_response())
        await writer.drain()
        if nth == 1:
            writer.write(xlog_data(10, b'{"action":"I","schema":"public","table":"picks"}'))
            writer.write(keepalive(20, reply=True))
            await writer.drain()
            _, status = await read_frame(reader)  # the standby-status reply
            self.statuses.append(int(struct.unpack("!Q", status[1:9])[0]))
            await asyncio.sleep(0.3)  # let the debounce window close before the stream ends
            writer.write(command_complete("COPY 0") + ready_for_query())
            await writer.drain()
        else:
            writer.write(xlog_data(30, b'{"action":"U","schema":"public","table":"stations"}'))
            await writer.drain()
        await reader.read()  # hold the session until the client aborts


async def test_walfeed_lifecycle_against_scripted_walsender(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="pgnudge.wal")
    server = FakeWalsender()
    async with scripted_server(server.handle) as (_, port):
        async with wal_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            assert feed.connection_pid == 1001
            first_slot = feed.slot_name
            assert first_slot is not None and first_slot.startswith("pgnudge_")

            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.picks",)

            # server ended the stream -> fresh slot, resync, new pid
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("reconnected")
            assert feed.connection_pid == 1002
            assert feed.slot_name != first_slot

            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)

    assert server.connections == 2
    assert server.statuses == [20]  # keepalive reply acked max(xlog 10, keepalive 20)
    assert sum("streaming from slot" in r.message for r in caplog.records) == 2
    assert any("stream error" in r.message and r.levelno == logging.WARNING for r in caplog.records)
    create, start = server.commands[0], server.commands[1]
    assert "CREATE_REPLICATION_SLOT" in create and "TEMPORARY" in create
    assert "SNAPSHOT 'nothing'" in create  # from-connect-only, law 5
    assert "START_REPLICATION" in start and "LOGICAL 0/0" in start
    assert "\"format-version\" '2'" in start
