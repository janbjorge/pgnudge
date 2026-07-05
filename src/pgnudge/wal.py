"""WalFeed: logical decoding from a TEMPORARY replication slot.

The slot auto-drops when the session ends, cleanly or not; nothing pgnudge
creates outlives the connection. From-connect-only: a fresh slot per
(re)connect, no history, no backfill. Semantics and the gap-free handshake
argument: README. Server needs ``wal_level=logical``, a REPLICATION role,
and an output plugin (wal2json or test_decoding).
"""

import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import ssl as ssl_module
import time
from typing import ClassVar

from pgnudge.engine import BaseFeed
from pgnudge.proto import StatusFeedback, WalsenderConnection, XLogData

__all__ = ["WalFeed"]


def _quote_value(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


class WalFeed(BaseFeed):
    """Async-iterable ``Resync | Batch`` feed from a temporary logical slot.

    Payloads are ``schema.table``. ``tables`` filters server-side (wal2json
    only); ``ssl`` takes True or an ``ssl.SSLContext``; ``status_interval``
    must stay under the server's ``wal_sender_timeout`` (default 60 s).
    ``liveness_timeout`` (must exceed ``status_interval``; None disables)
    bounds how long the feed tolerates a silent server before reconnecting.
    """

    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge.wal")

    _TEST_DECODING_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"^table (.+?): (?:INSERT|UPDATE|DELETE|TRUNCATE)"
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
            debounce=debounce,
            max_batch_wait=max_batch_wait,
            failsafe=failsafe,
            backoff=backoff,
            raw_queue_size=raw_queue_size,
        )
        if plugin not in ("wal2json", "test_decoding"):
            raise ValueError(f"unsupported plugin {plugin!r}")
        if status_interval <= 0:
            raise ValueError("status_interval must be positive")
        if liveness_timeout is not None and liveness_timeout <= status_interval:
            raise ValueError("liveness_timeout must exceed status_interval")
        if tables is not None and not tables:
            raise ValueError("tables must be None or a non-empty list")
        self._host = host
        self._port = port
        self._user = user
        self._database = database
        self._password = password
        self._ssl = ssl
        self._tables = list(tables) if tables is not None else None
        self._plugin = plugin
        self._application_name = application_name
        self._status_interval = status_interval
        self.liveness_timeout = liveness_timeout
        self._connect_timeout = connect_timeout

        self._conn: WalsenderConnection | None = None
        self._last_lsn = 0
        self.last_inbound = time.monotonic()
        self.slot_name: str | None = None

    # -- payload parsing ----------------------------------------------------------

    @staticmethod
    def _parse_wal2json_v2(payload: bytes) -> list[str]:
        try:
            obj: object = json.loads(payload)
        except ValueError:
            return []
        if isinstance(obj, dict) and obj.get("action") in ("I", "U", "D", "T"):
            return [f"{obj.get('schema', '?')}.{obj.get('table', '?')}"]
        return []

    @classmethod
    def _parse_test_decoding(cls, payload: bytes) -> list[str]:
        # TRUNCATE lists every affected table on one line, ", "-joined
        m = cls._TEST_DECODING_RE.match(payload.decode("utf-8", "replace"))
        return m.group(1).split(", ") if m else []

    # -- teardown ---------------------------------------------------------------

    async def _extra_close(self) -> None:
        # Hard-close on purpose, no DROP: crash and clean exit must exercise
        # the same server-side cleanup path.
        if self._conn is not None:
            self._conn.abort()
            self._conn = None
        self.slot_name = None

    # -- replication command assembly --------------------------------------------

    def _plugin_options(self) -> str:
        if self._plugin == "wal2json":
            opts = [('"format-version"', "2"), ('"include-transaction"', "false")]
            if self._tables:
                opts.append(('"add-tables"', ",".join(self._tables)))
            return ", ".join(f"{name} {_quote_value(value)}" for name, value in opts)
        return '"skip-empty-xacts" \'1\''

    # -- supervisor ---------------------------------------------------------------

    async def _supervisor(self) -> None:
        parse = self._parse_wal2json_v2 if self._plugin == "wal2json" else self._parse_test_decoding
        attempt = 0
        first = True
        while not self._closing:
            try:
                conn = await WalsenderConnection.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    database=self._database,
                    password=self._password,
                    ssl=self._ssl,
                    application_name=self._application_name,
                    connect_timeout=self._connect_timeout,
                )
            except Exception as exc:
                self.log.warning("connect to %s:%d failed: %s", self._host, self._port, exc)
                attempt += 1
                delay = self._backoff_delay(attempt)
                self.log.debug("reconnect attempt %d in %.2fs", attempt, delay)
                await asyncio.sleep(delay)
                continue

            self._conn = conn
            self.connection_pid = conn.backend_pid
            slot = f"pgnudge_{os.getpid()}_{secrets.token_hex(3)}"
            feedback: asyncio.Task[None] | None = None
            try:
                await conn.simple_query(
                    # SNAPSHOT 'nothing': from-connect-only, the Resync refetch is the backfill
                    f'CREATE_REPLICATION_SLOT "{slot}" TEMPORARY LOGICAL {self._plugin} (SNAPSHOT \'nothing\')'
                )
                await conn.start_replication(
                    f'START_REPLICATION SLOT "{slot}" LOGICAL 0/0 ({self._plugin_options()})'
                )
                self.slot_name = slot
                attempt = 0
                self._emit_resync("connected" if first else "reconnected")
                self.log.info("streaming from slot %s (backend pid %s)", slot, conn.backend_pid)
                first = False

                self._last_lsn = 0
                self.last_inbound = time.monotonic()
                feedback = asyncio.create_task(self._feedback_loop(conn))
                while True:
                    msg = await conn.read_stream()
                    self.last_inbound = time.monotonic()
                    if isinstance(msg, XLogData):
                        self._last_lsn = max(self._last_lsn, msg.end_lsn)
                        for table in parse(msg.payload):
                            self._push_raw(table)
                    else:  # Keepalive; read_stream returns nothing else
                        self._last_lsn = max(self._last_lsn, msg.end_lsn)
                        if msg.reply_requested:
                            await conn.send_standby_status(self._last_lsn)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # fall through to reconnect with a fresh slot
                self.log.warning("stream error on slot %s, reconnecting: %s", slot, exc)
            finally:
                if feedback is not None:
                    feedback.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await feedback
                self.connection_pid = None
                self.slot_name = None
                self._conn = None
                conn.abort()

            if not self._closing:
                attempt += 1
                delay = self._backoff_delay(attempt)
                self.log.debug("reconnect attempt %d in %.2fs", attempt, delay)
                await asyncio.sleep(delay)

    async def _feedback_loop(self, conn: WalsenderConnection) -> None:
        await StatusFeedback(
            conn=conn,
            interval=self._status_interval,
            liveness=self.liveness_timeout,
            lsn=lambda: self._last_lsn,
            idle=lambda: time.monotonic() - self.last_inbound,
            log=self.log,
        ).run()
