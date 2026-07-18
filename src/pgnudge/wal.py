"""WalFeed: logical decoding from a TEMPORARY replication slot.

The slot auto-drops when the session ends, cleanly or not; nothing pgnudge
creates outlives the connection. From-connect-only: a fresh slot per
(re)connect, no history, no backfill. Semantics and the gap-free handshake
argument: README. Server needs ``wal_level=logical``, a REPLICATION role,
and an output plugin (wal2json or test_decoding).
"""

import asyncio
import json
import logging
import os
import re
import secrets
import ssl as ssl_module
import time
from typing import ClassVar

from pgnudge.engine import BaseFeed, trace_frame
from pgnudge.errors import ConfigError
from pgnudge.proto import WalsenderConnection, XLogData

__all__ = ["WalFeed"]


def _quote_value(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


class WalFeed(BaseFeed):
    """Async-iterable ``Resync | Batch`` feed from a temporary logical slot.

    Payloads are ``schema.table``. ``tables`` filters server-side and is
    wal2json-only: pairing it with ``plugin="test_decoding"`` raises
    ``ConfigError`` rather than silently ignoring the filter. Entries feed
    wal2json's comma-separated ``add-tables`` option, so a name containing a
    comma is rejected (it would split and never match); ``*`` is a valid
    wal2json wildcard and is passed through. ``ssl`` takes True or an
    ``ssl.SSLContext``; ``status_interval`` must stay under the server's
    ``wal_sender_timeout`` (default 60 s). ``liveness_timeout`` (must exceed
    ``status_interval``; None disables) bounds how long the feed tolerates a
    silent server before reconnecting.
    """

    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge.wal")

    _TEST_DECODING_RE: ClassVar[re.Pattern[bytes]] = re.compile(
        rb"^table (.+?): (?:INSERT|UPDATE|DELETE|TRUNCATE)"
    )

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
        plugin: str = "wal2json",
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
            status_interval=status_interval,
            liveness_timeout=liveness_timeout,
            connect_timeout=connect_timeout,
            tables=tables,
            debounce=debounce,
            max_batch_wait=max_batch_wait,
            failsafe=failsafe,
            backoff=backoff,
            raw_queue_size=raw_queue_size,
        )
        if plugin not in ("wal2json", "test_decoding"):
            raise ConfigError(f"unsupported plugin {plugin!r}")
        if tables is not None:
            # test_decoding has no table filter, so a tables= would be silently
            # ignored - an unfiltered feed with no signal. Reject it.
            if plugin == "test_decoding":
                raise ConfigError(
                    "tables filtering requires the wal2json plugin; "
                    "test_decoding streams every table"
                )
            # A comma in a name splits inside wal2json's own comma-separated
            # add-tables parsing, so that table would never match: a missed
            # wakeup with no bracketing Resync. Reject it up front. ('*' is a
            # legitimate wal2json wildcard and is kept.)
            for name in tables:
                if "," in name:
                    raise ConfigError(
                        f"table name {name!r} contains a comma, which breaks wal2json's "
                        "comma-separated add-tables option parsing"
                    )
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.ssl = ssl
        self.tables = list(tables) if tables is not None else None
        self.plugin = plugin
        self.application_name = application_name

        self.conn: WalsenderConnection | None = None
        self.last_lsn = 0
        self.slot_name: str | None = None

    # -- payload parsing ----------------------------------------------------------

    @staticmethod
    def _parse_wal2json_v2(payload: bytes) -> list[str]:
        """One format-version-2 message to affected tables; non-change actions parse to []."""
        try:
            obj: object = json.loads(payload)
        except ValueError:
            return []
        if isinstance(obj, dict) and obj.get("action") in ("I", "U", "D", "T"):
            return [f"{obj.get('schema', '?')}.{obj.get('table', '?')}"]
        return []

    @classmethod
    def _parse_test_decoding(cls, payload: bytes) -> list[str]:
        """One test_decoding text line to affected tables; non-DML lines parse to []."""
        # Match on bytes and decode only the captured table list: a wide row
        # line's column dump never needs decoding. TRUNCATE lists every
        # affected table on one line, ", "-joined.
        m = cls._TEST_DECODING_RE.match(payload)
        return m.group(1).decode("utf-8", "replace").split(", ") if m else []

    # -- teardown ---------------------------------------------------------------

    async def _extra_close(self) -> None:
        # Hard-close on purpose, no DROP: crash and clean exit must exercise
        # the same server-side cleanup path.
        if self.conn is not None:
            self.conn.abort()
            self.conn = None
        self.slot_name = None

    # -- replication command assembly --------------------------------------------

    def _plugin_options(self) -> str:
        if self.plugin == "wal2json":
            opts = [('"format-version"', "2"), ('"include-transaction"', "false")]
            if self.tables:
                opts.append(('"add-tables"', ",".join(self.tables)))
            return ", ".join(f"{name} {_quote_value(value)}" for name, value in opts)
        return "\"skip-empty-xacts\" '1'"

    # -- supervisor ---------------------------------------------------------------

    async def _supervisor(self) -> None:
        """Connect, create a fresh TEMPORARY slot, stream until error, back off, repeat."""
        parse = self._parse_wal2json_v2 if self.plugin == "wal2json" else self._parse_test_decoding
        attempt = 0
        first = True
        while not self._closing:
            try:
                conn = await WalsenderConnection.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    database=self.database,
                    password=self.password,
                    ssl=self.ssl,
                    application_name=self.application_name,
                    connect_timeout=self.connect_timeout,
                )
            except Exception as exc:
                self.log.warning("connect to %s:%d failed: %s", self.host, self.port, exc)
                self._record_connect_failure(exc)
                attempt += 1
                await self._reconnect_pause(attempt)
                continue

            # async with drives abort() on both the normal and the error path
            # (WalsenderConnection.__aexit__); the finally only resets our state.
            async with conn:
                self.conn = conn
                self.connection_pid = conn.backend_pid
                slot = f"pgnudge_{os.getpid()}_{secrets.token_hex(3)}"
                live = False  # set once the stream goes live; setup deaths count as connect failures
                try:
                    # SNAPSHOT 'nothing': from-connect-only, the Resync refetch is the backfill
                    create_slot = (
                        f'CREATE_REPLICATION_SLOT "{slot}" TEMPORARY LOGICAL '
                        f"{self.plugin} (SNAPSHOT 'nothing')"
                    )
                    await conn.simple_query(create_slot)
                    await conn.start_replication(
                        f'START_REPLICATION SLOT "{slot}" LOGICAL 0/0 ({self._plugin_options()})'
                    )
                    self.slot_name = slot
                    attempt = 0
                    live = True
                    self._record_connect_success()
                    self._emit_resync("connected" if first else "reconnected")
                    self.log.info("streaming from slot %s (backend pid %s)", slot, conn.backend_pid)
                    first = False

                    self.last_lsn = 0
                    self.last_inbound = time.monotonic()
                    async with self._feedback_running(conn):
                        while True:
                            msg = await conn.read_stream()
                            self.last_inbound = time.monotonic()
                            if isinstance(msg, XLogData):
                                self.last_lsn = max(self.last_lsn, msg.end_lsn)
                                names = parse(msg.payload)
                                trace_frame(self.log, msg, "-> %s", names)
                                for table in names:
                                    self._push_raw(table)
                            else:  # Keepalive; read_stream returns nothing else
                                self.last_lsn = max(self.last_lsn, msg.end_lsn)
                                if msg.reply_requested:
                                    await conn.send_standby_status(self.last_lsn)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # fall through to reconnect with a fresh slot
                    self.log.warning("stream error on slot %s, reconnecting: %s", slot, exc)
                    if not live:
                        self._record_connect_failure(exc)
                finally:
                    self.connection_pid = None
                    self.slot_name = None
                    self.conn = None

            if not self._closing:
                attempt += 1
                await self._reconnect_pause(attempt)

    def _current_lsn(self) -> int:
        return self.last_lsn
