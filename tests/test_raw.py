"""RawFeed contract tests against a real PostgreSQL.

The proofs this transport stands on: zero server objects even while
streaming, commit-gated nudges, noise rejection (vacuum/checkpoint), and
the pg_waldump oracle that diffs our decoder against PostgreSQL's own
over a live WAL range.
"""

import asyncio
import os
import re
import uuid
from typing import TypeVar

import asyncpg
import pytest
from conftest import PgParams, allow_replication_connections
from testcontainers.postgres import PostgresContainer

from pgnudge import Batch, RawFeed, Resync
from pgnudge.proto import WalsenderConnection, XLogData, format_lsn, parse_lsn
from pgnudge.xlog import RelChange, XLogWalker

# live tests are exempt from the 2s budget: the first one pays for the
# session-scoped container pull + start
pytestmark = pytest.mark.timeout(300)

# module-level by necessity: a generic function needs a TypeVar on 3.11
T = TypeVar("T", bound=Resync | Batch)


def raw_feed(pg: PgParams, *, tables: list[str] | None = None) -> RawFeed:
    return RawFeed(
        host=pg.host,
        port=pg.port,
        user=pg.user,
        password=pg.password,
        database=pg.database,
        tables=tables,
        debounce=0.15,
    )


async def expect(feed: RawFeed, kind: type[T], timeout: float = 8.0) -> T:
    item = await asyncio.wait_for(anext(feed), timeout)
    assert isinstance(item, kind), f"expected {kind.__name__}, got {item!r}"
    return item


async def expect_quiet(feed: RawFeed, seconds: float) -> None:
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(feed), seconds)


async def slots(admin: asyncpg.Connection) -> list[asyncpg.Record]:
    return list(await admin.fetch("SELECT slot_name FROM pg_replication_slots"))


async def create_tables(admin: asyncpg.Connection) -> None:
    await admin.execute("""
        CREATE TABLE stations (id int PRIMARY KEY, name text NOT NULL, paused bool NOT NULL DEFAULT false);
        CREATE TABLE picks (id serial PRIMARY KEY, station_id int NOT NULL, status text NOT NULL DEFAULT 'pending');
        INSERT INTO stations VALUES (1, 'st-1', false), (2, 'st-2', false);
    """)


async def test_connect_streams_with_zero_server_objects(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    assert await slots(admin) == []
    async with raw_feed(pg) as feed:
        item = await expect(feed, Resync)
        assert item.reason == "connected"
        assert await slots(admin) == []  # stronger than the temp-slot proof: nothing at all
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)
        assert await slots(admin) == []
    assert await slots(admin) == []


async def test_no_backfill_of_preconnect_writes(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)  # writes BEFORE the feed connects
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        await expect_quiet(feed, 0.7)


async def test_update_delete_and_copy_all_nudge(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)

        await admin.execute("UPDATE stations SET paused = true WHERE id = 1")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.stations",)

        await admin.execute("DELETE FROM stations WHERE id = 2")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.stations",)

        await admin.copy_records_to_table(  # COPY -> heap2 multi-insert
            "picks", records=[(100 + i, 1, "pending") for i in range(20)], columns=["id", "station_id", "status"]
        )
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)


async def test_nudges_are_held_until_commit(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        txn = admin.transaction()
        await txn.start()
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        await expect_quiet(feed, 0.7)  # written to WAL, not committed: no nudge yet
        await txn.commit()
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)


async def test_rolled_back_writes_never_nudge(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        txn = admin.transaction()
        await txn.start()
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        await txn.rollback()
        await expect_quiet(feed, 0.7)
        # and the pipeline still works afterwards
        await admin.execute("INSERT INTO picks (station_id) VALUES (2)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)


async def test_vacuum_and_checkpoint_stay_silent(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    await admin.execute("DELETE FROM picks")  # give vacuum something to do, before connect
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        await admin.execute("VACUUM picks")
        await admin.execute("CHECKPOINT")
        await expect_quiet(feed, 0.7)


async def test_truncate_is_a_documented_gap(pg: PgParams, admin: asyncpg.Connection) -> None:
    # Physical WAL has no reliable TRUNCATE signature at wal_level=replica;
    # the README documents the gap. This test is the living reminder: if it
    # ever starts nudging, the docs must change with it.
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        await admin.execute("TRUNCATE picks")
        await expect_quiet(feed, 0.7)


async def test_vacuum_full_remap_still_resolves_next_insert(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)

        await admin.execute("VACUUM FULL picks")  # new relfilenode for picks
        await admin.execute("INSERT INTO picks (station_id) VALUES (2)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)


async def test_other_databases_never_nudge(
    pg: PgParams, admin: asyncpg.Connection, postgres: tuple[str, bool, PostgresContainer | None]
) -> None:
    await create_tables(admin)
    base_dsn, _tls, _container = postgres
    other = await asyncpg.connect(base_dsn)
    table = f"noise_{uuid.uuid4().hex[:8]}"
    try:
        async with raw_feed(pg) as feed:
            await expect(feed, Resync)
            await other.execute(f"CREATE TABLE {table} (id int); INSERT INTO {table} VALUES (1), (2)")
            await expect_quiet(feed, 0.7)
            await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
            batch = await expect(feed, Batch)
            assert batch.payloads() == ("public.picks",)
    finally:
        await other.execute(f"DROP TABLE IF EXISTS {table}")
        await other.close()


async def test_reconnect_after_backend_terminate(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with raw_feed(pg) as feed:
        await expect(feed, Resync)
        old_pid = feed.connection_pid
        assert old_pid is not None

        await admin.execute("SELECT pg_terminate_backend($1)", old_pid)
        item = await expect(feed, Resync, timeout=10.0)
        assert item.reason == "reconnected"
        assert feed.connection_pid != old_pid

        await admin.execute("INSERT INTO picks (station_id) VALUES (2)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)
    assert await slots(admin) == []


# -- the oracle ----------------------------------------------------------------------


async def capture_changes(pg: PgParams, start: int, end: int) -> list[tuple[int, int, str]]:
    """Stream physical WAL over [page_floor(start), end) through XLogWalker."""
    conn = await WalsenderConnection.connect(
        host=pg.host, port=pg.port, user=pg.user, database=pg.database,
        password=pg.password, replication="true",
    )
    try:
        rows = await conn.simple_query_rows("IDENTIFY_SYSTEM")
        assert rows and rows[0][1] is not None
        floor = XLogWalker.page_floor(start)
        await conn.start_replication(
            f"START_REPLICATION PHYSICAL {format_lsn(floor)} TIMELINE {int(rows[0][1])}"
        )
        walker = XLogWalker(start_lsn=floor)
        changes: list[RelChange] = []
        while walker.pos < end:
            msg = await conn.read_stream()
            if isinstance(msg, XLogData):
                changes += [e for e in walker.feed(msg.payload) if isinstance(e, RelChange)]
    finally:
        conn.abort()
    return [(c.db_oid, c.relfilenode, c.kind) for c in changes]


def waldump_changes(container: PostgresContainer, start: int, end: int, db_oid: int) -> list[tuple[int, str]]:
    """The same WAL range through PostgreSQL's own decoder."""
    kinds = {
        ("Heap", "INSERT"): "insert",
        ("Heap", "DELETE"): "delete",
        ("Heap", "UPDATE"): "update",
        ("Heap", "HOT_UPDATE"): "hot_update",
        ("Heap2", "MULTI_INSERT"): "multi_insert",
    }
    command = (
        f"pg_waldump --start {format_lsn(XLogWalker.page_floor(start))} --end {format_lsn(end)}"
        " -p /var/lib/postgresql/data/pg_wal"
    )
    code, output = container.get_wrapped_container().exec_run(["bash", "-c", command], user="postgres")
    assert int(code) == 0, output.decode(errors="replace")
    line_re = re.compile(r"rmgr: (Heap2?)\s.*?desc: (\w+).*?blkref #0: rel \d+/(\d+)/(\d+)")
    out: list[tuple[int, str]] = []
    for line in output.decode(errors="replace").splitlines():
        m = line_re.search(line)
        if m and int(m.group(3)) == db_oid:
            kind = kinds.get((m.group(1), m.group(2)))
            if kind:
                out.append((int(m.group(4)), kind))
    return out


async def test_decoder_matches_pg_waldump_oracle(
    pg: PgParams, admin: asyncpg.Connection, postgres: tuple[str, bool, PostgresContainer | None]
) -> None:
    _dsn, _tls, container = postgres
    if container is None:
        pytest.skip("no container to run pg_waldump in (EXTERNAL_POSTGRES_DSN)")
    await create_tables(admin)
    db_oid: int = await admin.fetchval("SELECT oid FROM pg_database WHERE datname = current_database()")

    # the container runs synchronous_commit=off; the capture below streams
    # [start, end) which must be flushed, so opt this session back in
    await admin.execute("SET synchronous_commit = on")
    start = parse_lsn(await admin.fetchval("SELECT pg_current_wal_insert_lsn()::text"))
    await admin.execute("INSERT INTO picks (station_id) SELECT 1 FROM generate_series(1, 30)")
    await admin.execute("UPDATE picks SET status = 'done' WHERE id % 3 = 0")
    await admin.execute("DELETE FROM picks WHERE id % 5 = 0")
    await admin.copy_records_to_table(
        "picks", records=[(500 + i, 2, "pending") for i in range(25)], columns=["id", "station_id", "status"]
    )
    await admin.execute("UPDATE stations SET paused = NOT paused")
    end = parse_lsn(await admin.fetchval("SELECT pg_current_wal_insert_lsn()::text"))

    # both sides filter identically: this database, heap change records only
    ours = [(node, kind) for db, node, kind in await capture_changes(pg, start, end) if db == db_oid]
    theirs = waldump_changes(container, start, end, db_oid)
    assert theirs, "oracle produced no rows; the workload or range capture is broken"
    assert ours == theirs


async def test_works_at_stock_wal_level_replica() -> None:
    if os.environ.get("EXTERNAL_POSTGRES_DSN"):
        pytest.skip("dedicated container test; external server in use")
    # No wal_level flag: stock postgres default is replica. This is the
    # transport's reason to exist; prove it on an untouched server.
    container = PostgresContainer("postgres:17", username="test", password="test", dbname="test", driver=None)
    container.with_command("-c fsync=off -c synchronous_commit=off")
    with container as running:
        allow_replication_connections(running)
        host = running.get_container_host_ip()
        port = int(running.get_exposed_port(5432))
        admin = await asyncpg.connect(host=host, port=port, user="test", password="test", database="test")
        try:
            assert await admin.fetchval("SHOW wal_level") == "replica"
            await create_tables(admin)
            feed = RawFeed(host=host, port=port, user="test", password="test", database="test", debounce=0.15)
            async with feed:
                await expect(feed, Resync)
                await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
                batch = await expect(feed, Batch)
                assert batch.payloads() == ("public.picks",)
        finally:
            await admin.close()
