"""RawFeed: slot-less physical replication, decoded client-side.

Works at ``wal_level=replica`` — any stock PostgreSQL — with nothing on
the server at any point, not even a temporary slot. The stream starts at
the server's current flush position (``IDENTIFY_SYSTEM``); from-connect-
only comes for free because nothing retains history for us. Nudges are
commit-gated for parity with logical decoding. TRUNCATE is not detected
on this transport. Server needs a role with the REPLICATION attribute.
Mechanism: docs/physical-wal.md.
"""

import asyncio
import contextlib
import logging
import ssl as ssl_module
import time
from dataclasses import dataclass, field
from typing import ClassVar

from pgnudge.engine import BaseFeed
from pgnudge.proto import StatusFeedback, WalsenderConnection, XLogData, format_lsn, parse_lsn
from pgnudge.xlog import CommitGate, RelChange, WalSyncError, XLogWalker

__all__ = ["RawFeed", "RelResolver"]


@dataclass(slots=True, kw_only=True)
class RelResolver:
    """relfilenode -> ``schema.table`` through a catalog connection, cached.

    Lookups happen only for unseen relfilenodes (event-driven, never
    periodic). System-schema relations cache as drops; an unresolvable
    relfilenode is dropped uncached so the next occurrence retries, which
    covers catalog visibility lagging the WAL by one commit.
    """

    conn: WalsenderConnection
    db_oid: int = field(init=False, default=0)
    names: dict[int, str] = field(init=False, default_factory=dict)

    SYSTEM_SCHEMAS: ClassVar[frozenset[str]] = frozenset({"pg_catalog", "pg_toast", "information_schema"})

    async def prime(self) -> None:
        rows = await self.conn.simple_query_rows("SELECT oid FROM pg_database WHERE datname = current_database()")
        if not rows or rows[0][0] is None:
            raise ConnectionError("could not determine the current database oid")
        self.db_oid = int(rows[0][0])

    async def resolve(self, relfilenodes: set[int]) -> dict[int, str]:
        unseen = sorted(node for node in relfilenodes if node not in self.names)
        if unseen:
            nodes = ",".join(str(node) for node in unseen)
            rows = await self.conn.simple_query_rows(
                "SELECT c.relfilenode, n.nspname, c.relname FROM pg_class c"
                " JOIN pg_namespace n ON n.oid = c.relnamespace"
                f" WHERE c.relfilenode IN ({nodes})"
            )
            for row in rows:
                filenode, nspname, relname = row[0], row[1], row[2]
                if filenode is None or nspname is None or relname is None:
                    continue
                name = "" if nspname in self.SYSTEM_SCHEMAS else f"{nspname}.{relname}"
                self.names[int(filenode)] = name  # "" caches the drop
        return {node: found for node in relfilenodes if (found := self.names.get(node))}


class RawFeed(BaseFeed):
    """Async-iterable ``Resync | Batch`` feed from slot-less physical replication.

    Payloads are ``schema.table``; ``tables`` filters client-side. Same
    contract and knobs as ``WalFeed`` minus ``plugin``; see the class and
    README for the transport trade-offs (cluster-wide WAL bandwidth,
    commit-gated nudges, no TRUNCATE detection).
    """

    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge.raw")

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 5432,
        user: str,
        database: str,
        password: str | None = None,
        ssl: bool | ssl_module.SSLContext = False,
        tables: list[str] | None = None,
        application_name: str = "pgnudge",
        status_interval: float = 10.0,
        liveness_timeout: float | None = 30.0,
        connect_timeout: float = 10.0,
        debounce: float = 0.05,
        max_batch_wait: float | None = None,
        failsafe: float | None = None,
        backoff: tuple[float, float] = (0.1, 5.0),
        raw_queue_size: int = 8192,
    ) -> None:
        super().__init__(
            debounce=debounce,
            max_batch_wait=max_batch_wait,
            failsafe=failsafe,
            backoff=backoff,
            raw_queue_size=raw_queue_size,
        )
        if status_interval <= 0:
            raise ValueError("status_interval must be positive")
        if liveness_timeout is not None and liveness_timeout <= status_interval:
            raise ValueError("liveness_timeout must exceed status_interval")
        if tables is not None and not tables:
            raise ValueError("tables must be None or a non-empty list")
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.ssl = ssl
        self.tables = list(tables) if tables is not None else None
        self.application_name = application_name
        self.status_interval = status_interval
        self.liveness_timeout = liveness_timeout
        self.connect_timeout = connect_timeout

        self.stream_conn: WalsenderConnection | None = None
        self.catalog_conn: WalsenderConnection | None = None
        self.last_lsn = 0
        self.last_inbound = time.monotonic()

    # -- teardown ---------------------------------------------------------------

    async def _extra_close(self) -> None:
        # Hard-close on purpose: there is nothing server-side to say goodbye to.
        for conn in (self.stream_conn, self.catalog_conn):
            if conn is not None:
                conn.abort()
        self.stream_conn = None
        self.catalog_conn = None

    # -- supervisor ---------------------------------------------------------------

    async def connect_once(self, replication: str) -> WalsenderConnection:
        return await WalsenderConnection.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
            ssl=self.ssl,
            application_name=self.application_name,
            connect_timeout=self.connect_timeout,
            replication=replication,
        )

    async def _supervisor(self) -> None:
        attempt = 0
        first = True
        while not self._closing:
            try:
                stream = await self.connect_once("true")
            except Exception as exc:
                self.log.warning("connect to %s:%d failed: %s", self.host, self.port, exc)
                attempt += 1
                delay = self._backoff_delay(attempt)
                self.log.debug("reconnect attempt %d in %.2fs", attempt, delay)
                await asyncio.sleep(delay)
                continue

            self.stream_conn = stream
            self.connection_pid = stream.backend_pid
            feedback: asyncio.Task[None] | None = None
            try:
                catalog = await self.connect_once("database")
                self.catalog_conn = catalog
                resolver = RelResolver(conn=catalog)
                await resolver.prime()

                rows = await stream.simple_query_rows("IDENTIFY_SYSTEM")
                if not rows or rows[0][1] is None or rows[0][2] is None:
                    raise ConnectionError("IDENTIFY_SYSTEM returned no position")
                timeline = int(rows[0][1])
                flush_lsn = parse_lsn(rows[0][2])
                # page-align down; the first page's rem_len resynchronizes the walker
                start = XLogWalker.page_floor(flush_lsn)
                await stream.start_replication(
                    f"START_REPLICATION PHYSICAL {format_lsn(start)} TIMELINE {timeline}"
                )
                attempt = 0
                self._emit_resync("connected" if first else "reconnected")
                self.log.info(
                    "streaming physical WAL from %s timeline %d (backend pid %s)",
                    format_lsn(start),
                    timeline,
                    stream.backend_pid,
                )
                first = False

                walker = XLogWalker(start_lsn=start)
                gate = CommitGate()
                self.last_lsn = flush_lsn
                self.last_inbound = time.monotonic()
                feedback = asyncio.create_task(self._feedback_loop(stream))
                while True:
                    msg = await stream.read_stream()
                    self.last_inbound = time.monotonic()
                    self.last_lsn = max(self.last_lsn, msg.end_lsn)
                    if isinstance(msg, XLogData):
                        expected = walker.pos + len(walker.buf)
                        if msg.start_lsn != expected:
                            raise WalSyncError(
                                f"stream position {format_lsn(msg.start_lsn)}"
                                f" does not follow {format_lsn(expected)}"
                            )
                        await self.push_committed(gate.push(walker.feed(msg.payload)), resolver)
                    elif msg.reply_requested:
                        await stream.send_standby_status(self.last_lsn)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # fall through to reconnect from the new flush position
                self.log.warning("stream error, reconnecting: %s", exc)
            finally:
                if feedback is not None:
                    feedback.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await feedback
                self.connection_pid = None
                for conn in (self.stream_conn, self.catalog_conn):
                    if conn is not None:
                        conn.abort()
                self.stream_conn = None
                self.catalog_conn = None

            if not self._closing:
                attempt += 1
                delay = self._backoff_delay(attempt)
                self.log.debug("reconnect attempt %d in %.2fs", attempt, delay)
                await asyncio.sleep(delay)

    async def push_committed(self, committed: list[RelChange], resolver: RelResolver) -> None:
        mine = [change for change in committed if change.db_oid == resolver.db_oid]
        if not mine:
            return
        names = await resolver.resolve({change.relfilenode for change in mine})
        for change in mine:
            name = names.get(change.relfilenode)
            if name and (self.tables is None or name in self.tables):
                self._push_raw(name)

    async def _feedback_loop(self, conn: WalsenderConnection) -> None:
        await StatusFeedback(
            conn=conn,
            interval=self.status_interval,
            liveness=self.liveness_timeout,
            lsn=lambda: self.last_lsn,
            idle=lambda: time.monotonic() - self.last_inbound,
            log=self.log,
        ).run()
