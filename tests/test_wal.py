"""WalFeed contract tests against a real PostgreSQL, incl. the zero-footprint proofs."""

import asyncio
import ssl

import asyncpg
import pytest
from conftest import PgParams, create_tables, expect, expect_quiet

from pgnudge import Batch, Resync, WalFeed
from pgnudge.doctor import diagnose

# live tests are exempt from the 2s budget: the first one pays for the
# session-scoped container pull + start
pytestmark = pytest.mark.timeout(300)


def wal_feed(
    pg: PgParams,
    *,
    tables: list[str] | None = None,
    ssl_ctx: ssl.SSLContext | None = None,
) -> WalFeed:
    return WalFeed(
        host=pg.host,
        port=pg.port,
        user=pg.user,
        password=pg.password,
        database=pg.database,
        plugin=pg.plugin,
        tables=tables,
        ssl=ssl_ctx if ssl_ctx is not None else False,
        debounce=0.15,
    )


async def slots(admin: asyncpg.Connection) -> list[asyncpg.Record]:
    return list(await admin.fetch("SELECT slot_name, temporary, active FROM pg_replication_slots"))


async def test_connect_emits_resync_then_only_a_temp_slot(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    assert await slots(admin) == []
    async with wal_feed(pg) as feed:
        item = await expect(feed, Resync)
        assert item.reason == "connected"
        rows = await slots(admin)
        assert len(rows) == 1
        assert rows[0]["temporary"] is True
        assert rows[0]["slot_name"] == feed.slot_name


async def test_no_backfill_of_preconnect_writes(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)  # writes BEFORE the feed connects
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        await expect_quiet(feed, 0.7)  # none of that history may be delivered


async def test_txn_coalesces_client_side(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        async with admin.transaction():
            await admin.executemany(
                "INSERT INTO picks (station_id, status) VALUES ($1, 'pending')", [(1,)] * 50
            )
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)
        assert batch.events[0].count == 50


async def test_two_table_burst_coalesces(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        await admin.execute("UPDATE stations SET paused = true WHERE id = 1")
        seen: set[str] = set()
        for _ in range(2):
            batch = await expect(feed, Batch)
            seen |= set(batch.payloads())
            if seen == {"public.picks", "public.stations"}:
                break
        assert seen == {"public.picks", "public.stations"}


async def test_truncate_emits_wakeup(pg: PgParams, admin: asyncpg.Connection) -> None:
    await create_tables(admin)
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        await admin.execute("TRUNCATE picks, stations")
        seen: set[str] = set()
        for _ in range(2):
            batch = await expect(feed, Batch)
            seen |= set(batch.payloads())
            if seen == {"public.picks", "public.stations"}:
                break
        assert seen == {"public.picks", "public.stations"}


async def test_reconnect_gets_fresh_slot_old_one_auto_dropped(
    pg: PgParams, admin: asyncpg.Connection
) -> None:
    await create_tables(admin)
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        old_slot, old_pid = feed.slot_name, feed.connection_pid
        assert old_slot is not None and old_pid is not None

        await admin.execute("SELECT pg_terminate_backend($1)", old_pid)
        item = await expect(feed, Resync, timeout=10.0)
        assert item.reason == "reconnected"

        await asyncio.sleep(0.2)
        names = [r["slot_name"] for r in await slots(admin)]
        assert feed.slot_name != old_slot
        assert old_slot not in names
        assert feed.slot_name in names

        await admin.execute("INSERT INTO picks (station_id) VALUES (2)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)


async def test_hard_abort_leaves_no_slots(pg: PgParams, admin: asyncpg.Connection) -> None:
    # THE proof: aclose() hard-aborts the socket (no protocol goodbye, no
    # DROP command) and the server still cleans up everything.
    await create_tables(admin)
    async with wal_feed(pg) as feed:
        await expect(feed, Resync)
        assert len(await slots(admin)) == 1
    await asyncio.sleep(0.5)
    assert await slots(admin) == []


async def test_doctor_recommends_walfeed_and_leaves_no_slot(pg: PgParams, admin: asyncpg.Connection) -> None:
    # doctor's WalFeed probe creates a TEMPORARY slot; it must vanish with
    # the probe connection, same zero-footprint guarantee as the product.
    assert await slots(admin) == []
    diag = await diagnose(
        host=pg.host,
        port=pg.port,
        user=pg.user,
        password=pg.password,
        database=pg.database,
        plugin=pg.plugin,
    )
    assert diag.recommended == "WalFeed"
    assert all(check.ok for check in diag.checks), [c for c in diag.checks if not c.ok]
    await asyncio.sleep(0.3)
    assert await slots(admin) == []


async def test_tls_scram_over_encrypted_stream(pg: PgParams, admin: asyncpg.Connection) -> None:
    if not pg.tls_available:
        pytest.skip("server has no TLS configured")
    await create_tables(admin)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE  # self-signed dev cert
    async with wal_feed(pg, ssl_ctx=ctx) as feed:
        await expect(feed, Resync)
        await admin.execute("INSERT INTO picks (station_id) VALUES (1)")
        batch = await expect(feed, Batch)
        assert batch.payloads() == ("public.picks",)
    await asyncio.sleep(0.5)
    assert await slots(admin) == []
