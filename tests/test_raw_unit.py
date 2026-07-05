"""RawFeed unit tests: resolver behavior, supervisor lifecycle, and stream
framing against a scripted in-process physical walsender; no PostgreSQL.

WAL payload bytes come from test_xlog's synthetic ``WalStream`` writer, so
the whole path from wire frame to Batch is exercised byte-exactly.
"""

import asyncio
import logging
import struct
import time

import pytest
from test_xlog import BLCKSZ, WalStream, commit, heap_insert, start_lsn
from wire import (
    auth_request,
    backend_key,
    command_complete,
    data_row,
    keepalive,
    msg,
    read_frame,
    read_startup,
    ready_for_query,
    refused_port,
    scripted_server,
    xlog_data,
)

from pgnudge import Batch, RawFeed, Resync
from pgnudge.proto import WalsenderConnection, format_lsn
from pgnudge.raw import RelResolver
from pgnudge.xlog import RelChange

PICKS = (1663, 5, 16384)
STATIONS = (1663, 5, 16400)
OTHER_DB = (1663, 6, 16384)


def raw_feed(port: int, *, tables: list[str] | None = None) -> RawFeed:
    return RawFeed(
        host="127.0.0.1",
        port=port,
        user="alice",
        database="db",
        tables=tables,
        status_interval=5.0,
        liveness_timeout=None,
        connect_timeout=1.0,
        debounce=0.05,
        backoff=(0.01, 0.05),
    )


def copy_both_response() -> bytes:
    return msg(b"W", b"\x00\x00\x00")


def identify_system(timeline: int, lsn: int) -> bytes:
    row = data_row(b"7000", str(timeline).encode(), format_lsn(lsn).encode(), None)
    return msg(b"T", b"\x00\x04") + row + command_complete("IDENTIFY_SYSTEM") + ready_for_query()


def catalog_rows(query: str) -> bytes:
    known = {16384: ("public", "picks"), 16400: ("public", "stations")}
    rows = b""
    for node, (schema, table) in known.items():
        if str(node) in query:
            rows += data_row(str(node).encode(), schema.encode(), table.encode())
    return msg(b"T", b"\x00\x03") + rows + command_complete("SELECT") + ready_for_query()


async def serve_catalog(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, insert_lsn_row: bytes = data_row(b"0/0")
) -> None:
    while True:
        _, body = await read_frame(reader)
        query = body.rstrip(b"\x00").decode()
        if "pg_database" in query:
            writer.write(msg(b"T", b"\x00\x01") + data_row(b"5") + command_complete("SELECT") + ready_for_query())
        elif "pg_current_wal_insert_lsn" in query:
            writer.write(msg(b"T", b"\x00\x01") + insert_lsn_row + command_complete("SELECT") + ready_for_query())
        else:
            writer.write(catalog_rows(query))
        await writer.drain()


class FakePhysicalWalsender:
    """Scripted walsender pair: physical stream sessions plus catalog sessions.

    ``first`` selects the first stream session's behavior; a second session
    always streams ``wal2`` and idles.
    """

    def __init__(self, *, wal1: WalStream, wal2: WalStream, first: str = "ok") -> None:
        self.wal1 = wal1
        self.wal2 = wal2
        self.first = first
        self.stream_sessions = 0
        self.catalog_sessions = 0
        self.commands: list[str] = []
        self.statuses: list[int] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        params = await read_startup(reader)
        if params["replication"] == "database":
            self.catalog_sessions += 1
            if self.first == "no-catalog" and self.catalog_sessions == 1:
                return  # connection dies during startup
            writer.write(auth_request(0) + backend_key(9999) + ready_for_query())
            await writer.drain()
            broken = self.first == "no-insert-lsn" and self.catalog_sessions == 1
            await serve_catalog(reader, writer, insert_lsn_row=b"" if broken else data_row(b"0/0"))
            return
        self.stream_sessions += 1
        nth = self.stream_sessions
        writer.write(auth_request(0) + backend_key(1000 + nth) + ready_for_query())
        await writer.drain()
        _, identify = await read_frame(reader)
        self.commands.append(identify.rstrip(b"\x00").decode())
        wal = self.wal1 if nth == 1 else self.wal2
        if nth == 1 and self.first == "no-identify":
            writer.write(command_complete("IDENTIFY_SYSTEM") + ready_for_query())
            await writer.drain()
            await reader.read()
            return
        writer.write(identify_system(1, wal.start))
        await writer.drain()
        _, start = await read_frame(reader)
        self.commands.append(start.rstrip(b"\x00").decode())
        writer.write(copy_both_response())
        if nth == 2:
            writer.write(keepalive(wal.start, reply=False))  # no ack expected
        data_lsn = wal.start + BLCKSZ if nth == 1 and self.first == "desync" else wal.start
        writer.write(xlog_data(data_lsn, bytes(wal.out)))
        await writer.drain()
        if nth == 1 and self.first == "ok":
            writer.write(keepalive(wal.pos + 0x1000, reply=True))
            await writer.drain()
            _, status = await read_frame(reader)
            self.statuses.append(int(struct.unpack("!Q", status[1:9])[0]))
            await asyncio.sleep(0.3)  # let the debounce window close before the stream ends
            writer.write(command_complete("COPY 0") + ready_for_query())
            await writer.drain()
        await reader.read()  # hold the session until the client aborts


def picks_then_stations() -> tuple[WalStream, WalStream]:
    wal1 = WalStream(start_lsn())
    wal1.add(heap_insert(xid=1, rel=PICKS))
    wal1.add(commit(xid=1))
    wal2 = WalStream(start_lsn() + 64 * BLCKSZ)
    wal2.add(heap_insert(xid=2, rel=STATIONS))
    wal2.add(commit(xid=2))
    return wal1, wal2


# -- constructor validation ----------------------------------------------------------


def test_nonpositive_status_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="status_interval"):
        RawFeed(host="h", port=5432, user="u", database="d", status_interval=0.0)


def test_liveness_timeout_not_exceeding_status_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="liveness_timeout"):
        RawFeed(host="h", port=5432, user="u", database="d", status_interval=10.0, liveness_timeout=10.0)


def test_empty_tables_list_is_rejected() -> None:
    with pytest.raises(ValueError, match="tables"):
        RawFeed(host="h", port=5432, user="u", database="d", tables=[])


# -- resolver ------------------------------------------------------------------------


class FakeCatalogBackend:
    """Catalog-only scripted backend: canned pg_class responses, query log."""

    def __init__(self, *, db_rows: list[bytes] | None = None, class_responses: list[bytes] | None = None) -> None:
        self.db_rows = db_rows if db_rows is not None else [data_row(b"5")]
        self.class_responses = class_responses if class_responses is not None else []
        self.class_queries: list[str] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await writer.drain()
        while True:
            _, body = await read_frame(reader)
            query = body.rstrip(b"\x00").decode()
            if "pg_database" in query:
                rows = b"".join(self.db_rows)
            else:
                self.class_queries.append(query)
                rows = self.class_responses.pop(0) if self.class_responses else b""
            writer.write(msg(b"T", b"\x00\x03") + rows + command_complete("SELECT") + ready_for_query())
            await writer.drain()


async def connected_resolver(host: str, port: int) -> RelResolver:
    conn = await WalsenderConnection.connect(host=host, port=port, user="u", database="d")
    return RelResolver(conn=conn)


async def test_resolver_caches_names_and_system_schema_drops() -> None:
    backend = FakeCatalogBackend(
        class_responses=[data_row(b"13000", b"pg_catalog", b"pg_class") + data_row(b"16384", b"public", b"picks")]
    )
    async with scripted_server(backend.handle) as (host, port):
        resolver = await connected_resolver(host, port)
        await resolver.prime()
        assert resolver.db_oid == 5
        assert await resolver.resolve({13000, 16384}) == {16384: "public.picks"}
        assert await resolver.resolve({13000, 16384}) == {16384: "public.picks"}  # served from cache
        resolver.conn.abort()
    assert len(backend.class_queries) == 1


async def test_resolver_retries_unresolved_relfilenodes() -> None:
    backend = FakeCatalogBackend(class_responses=[b"", data_row(b"16400", b"public", b"stations")])
    async with scripted_server(backend.handle) as (host, port):
        resolver = await connected_resolver(host, port)
        assert await resolver.resolve({16400}) == {}  # not visible yet; must not cache
        assert await resolver.resolve({16400}) == {16400: "public.stations"}
        resolver.conn.abort()
    assert len(backend.class_queries) == 2


async def test_resolver_skips_rows_with_nulls() -> None:
    backend = FakeCatalogBackend(class_responses=[data_row(b"16384", None, b"picks")])
    async with scripted_server(backend.handle) as (host, port):
        resolver = await connected_resolver(host, port)
        assert await resolver.resolve({16384}) == {}
        resolver.conn.abort()


async def test_resolver_prime_without_db_row_raises() -> None:
    backend = FakeCatalogBackend(db_rows=[])
    async with scripted_server(backend.handle) as (host, port):
        resolver = await connected_resolver(host, port)
        with pytest.raises(ConnectionError, match="database oid"):
            await resolver.prime()
        resolver.conn.abort()


# -- supervisor lifecycle --------------------------------------------------------------


async def test_supervisor_keeps_retrying_when_server_unreachable(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="pgnudge.raw")
    async with raw_feed(refused_port()) as feed:
        await asyncio.sleep(0.1)  # several connect -> backoff rounds
        assert feed.connection_pid is None
    assert any("failed" in r.message and r.levelno == logging.WARNING for r in caplog.records)
    assert any("reconnect attempt" in r.message and r.levelno == logging.DEBUG for r in caplog.records)


async def test_rawfeed_lifecycle_against_scripted_walsender(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="pgnudge.raw")
    wal1, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2)
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            assert feed.connection_pid == 1001

            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.picks",)

            # server ended the stream -> reconnect at the new flush position
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("reconnected")
            assert feed.connection_pid == 1002

            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)

    assert server.stream_sessions == 2
    assert server.statuses == [wal1.pos + 0x1000]  # keepalive end acked
    assert server.commands[0] == "IDENTIFY_SYSTEM"
    assert server.commands[1] == f"START_REPLICATION PHYSICAL {format_lsn(wal1.start)} TIMELINE 1"
    assert sum("streaming physical WAL" in r.message for r in caplog.records) == 2
    assert any("stream error" in r.message and r.levelno == logging.WARNING for r in caplog.records)


async def test_uncommitted_and_foreign_db_changes_never_nudge() -> None:
    wal1 = WalStream(start_lsn())
    wal1.add(heap_insert(xid=1, rel=OTHER_DB))  # other database
    wal1.add(commit(xid=1))
    wal1.add(heap_insert(xid=2, rel=STATIONS))  # never commits
    wal1.add(heap_insert(xid=3, rel=PICKS))
    wal1.add(commit(xid=3))
    _, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2)
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.picks",)


async def test_tables_filter_applies_client_side() -> None:
    wal1 = WalStream(start_lsn())
    wal1.add(heap_insert(xid=1, rel=PICKS))
    wal1.add(commit(xid=1))
    wal1.add(heap_insert(xid=2, rel=STATIONS))
    wal1.add(commit(xid=2))
    _, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2)
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port, tables=["public.stations"]) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)


async def test_stream_position_desync_forces_reconnect(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="pgnudge.raw")
    wal1, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2, first="desync")
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("reconnected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)
    assert any("does not follow" in r.message for r in caplog.records)


async def test_empty_identify_system_forces_reconnect(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="pgnudge.raw")
    wal1, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2, first="no-identify")
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)
    assert any("IDENTIFY_SYSTEM returned no position" in r.message for r in caplog.records)


async def test_catalog_connection_failure_retries_with_fresh_pair(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="pgnudge.raw")
    wal1, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2, first="no-catalog")
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            # first pair dies before going live, so the first Resync is "connected"
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)
    assert server.catalog_sessions == 2
    assert any("stream error" in r.message for r in caplog.records)


async def test_extra_close_aborts_and_clears_both_connections() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await writer.drain()
        await reader.read()

    def attach(feed: RawFeed, stream: WalsenderConnection, catalog: WalsenderConnection) -> None:
        # in a helper so mypy's attribute narrowing doesn't outlive the call
        feed.stream_conn = stream
        feed.catalog_conn = catalog

    async with scripted_server(handler) as (host, port):
        stream = await WalsenderConnection.connect(host=host, port=port, user="u", database="d")
        catalog = await WalsenderConnection.connect(host=host, port=port, user="u", database="d")
        feed = raw_feed(port)
        attach(feed, stream, catalog)
        await feed._extra_close()
        assert feed.stream_conn is None
        assert feed.catalog_conn is None


async def test_push_committed_without_local_changes_skips_resolution() -> None:
    feed = raw_feed(5432)
    resolver = RelResolver(conn=RecordingConn())
    resolver.db_oid = 5
    foreign = RelChange(xid=1, db_oid=6, relfilenode=1, kind="insert")
    await feed.push_committed([], resolver)
    await feed.push_committed([foreign], resolver)  # no resolve call: conn would explode


async def test_missing_insert_lsn_forces_reconnect(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="pgnudge.raw")
    wal1, wal2 = picks_then_stations()
    server = FakePhysicalWalsender(wal1=wal1, wal2=wal2, first="no-insert-lsn")
    async with scripted_server(server.handle) as (_, port):
        async with raw_feed(port) as feed:
            assert await asyncio.wait_for(anext(feed), 2.0) == Resync("connected")
            batch = await asyncio.wait_for(anext(feed), 2.0)
            assert isinstance(batch, Batch)
            assert batch.payloads() == ("public.stations",)
    assert any("WAL insert position" in r.message for r in caplog.records)


async def test_supervisor_exits_cleanly_when_closing_during_stream_error() -> None:
    proceed = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        params = await read_startup(reader)
        writer.write(auth_request(0) + backend_key(1) + ready_for_query())
        await writer.drain()
        if params["replication"] == "database":
            await serve_catalog(reader, writer)
            return
        await read_frame(reader)
        writer.write(identify_system(1, start_lsn()))
        await writer.drain()
        await read_frame(reader)
        writer.write(copy_both_response())
        await writer.drain()
        await proceed.wait()
        writer.write(command_complete("COPY 0") + ready_for_query())  # end the stream
        await writer.drain()
        await reader.read()

    async with scripted_server(handler) as (_, port):
        feed = raw_feed(port)
        supervisor = asyncio.create_task(feed._supervisor())
        assert await asyncio.wait_for(feed._service.next_item(), 2.0) == Resync("connected")
        feed._service.closing = True  # as aclose would, but without cancelling
        proceed.set()  # stream ends now; the supervisor must return, not reconnect
        await asyncio.wait_for(supervisor, 2.0)
        await feed._extra_close()


# -- feedback ----------------------------------------------------------------------


class RecordingConn(WalsenderConnection):
    """Stub connection: records statuses, fails the nth send."""

    def __init__(self, *, fail_after: int | None = None) -> None:
        self.backend_pid = None
        self.fail_after = fail_after
        self.sent: list[tuple[int, bool]] = []

    async def send_standby_status(self, lsn: int, *, reply: bool = False) -> None:
        self.sent.append((lsn, reply))
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise ConnectionResetError


async def test_feedback_loop_reports_last_received_lsn() -> None:
    conn = RecordingConn(fail_after=2)
    feed = raw_feed(5432)
    feed.status_interval = 0.01
    feed.last_lsn = 4242
    feed.last_inbound = time.monotonic()
    await asyncio.wait_for(feed._feedback_loop(conn), 2.0)  # returns on send failure
    assert conn.sent == [(4242, False)] * 2  # liveness off -> no probe requested
