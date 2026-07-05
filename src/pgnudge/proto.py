"""Minimal walsender-mode protocol client, stdlib asyncio + scramp.

Startup with ``replication=database`` (logical) or ``replication=true``
(physical), optional TLS, trust/cleartext/SCRAM auth, simple query,
CopyBoth streaming. See PostgreSQL docs: "Streaming Replication
Protocol", "Message Formats".
"""

import asyncio
import contextlib
import logging
import ssl as ssl_module
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar, Self

from scramp import ScramClient

__all__ = [
    "PgServerError",
    "XLogData",
    "Keepalive",
    "StatusFeedback",
    "WalsenderConnection",
    "format_lsn",
    "parse_lsn",
]


def parse_lsn(text: str) -> int:
    """``X/Y`` hex notation to an integer WAL position."""
    high, low = text.split("/")
    return int(high, 16) << 32 | int(low, 16)


def format_lsn(lsn: int) -> str:
    """Integer WAL position to ``X/Y`` hex notation."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


class PgServerError(Exception):
    """ErrorResponse from the server, with the field map preserved."""

    def __init__(self, fields: dict[str, str]) -> None:
        self.fields = fields
        super().__init__(f"{fields.get('S', 'ERROR')} {fields.get('C', '?????')}: {fields.get('M', 'unknown')}")

    @classmethod
    def from_wire(cls, body: bytes) -> Self:
        return cls(_parse_error(body))

    @classmethod
    def from_message(cls, message: str) -> Self:
        return cls({"M": message})


@dataclass(frozen=True, slots=True)
class XLogData:
    start_lsn: int  # WAL position of payload[0]; physical decoding needs it
    end_lsn: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class Keepalive:
    end_lsn: int
    reply_requested: bool


@dataclass(frozen=True, slots=True)
class BackendMessage:
    """One backend protocol frame: type byte and body."""

    mtype: bytes
    body: bytes


def _parse_error(body: bytes) -> dict[str, str]:
    """ErrorResponse body (field-code byte, NUL-terminated text, ... NUL) to a field map."""
    fields: dict[str, str] = {}
    i = 0
    while i < len(body) and body[i : i + 1] != b"\x00":
        code = chr(body[i])
        j = body.find(b"\x00", i + 1)
        if j < 0:
            fields[code] = body[i + 1 :].decode("utf-8", "replace")
            break
        fields[code] = body[i + 1 : j].decode("utf-8", "replace")
        i = j + 1
    return fields


def _default_ssl_context() -> ssl_module.SSLContext:
    ctx = ssl_module.create_default_context()
    # Azure/managed endpoints commonly need verify-full with the platform CA
    # bundle, which create_default_context gives you. For self-signed dev
    # servers pass your own context with CERT_NONE.
    return ctx


class WalsenderConnection:
    """One logical-replication walsender session."""

    _PROTOCOL_V3: ClassVar[int] = 196608
    _SSL_REQUEST: ClassVar[int] = 80877103
    _PG_EPOCH_UNIX: ClassVar[int] = 946_684_800  # 2000-01-01 00:00:00 UTC

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, tls: bool = False
    ) -> None:
        self._reader = reader
        self._writer = writer
        self.tls = tls
        self.backend_pid: int | None = None
        self.send_lock = asyncio.Lock()

    # -- connection & auth ----------------------------------------------------

    @classmethod
    async def connect(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        password: str | None = None,
        ssl: bool | ssl_module.SSLContext = False,
        application_name: str = "pgnudge",
        connect_timeout: float = 10.0,
        replication: str = "database",
    ) -> Self:
        """Open a socket, optionally upgrade to TLS, and authenticate.

        The SSLRequest exchange precedes the startup packet: one request,
        one ``S`` byte back, then ``start_tls`` on the same transport.
        Every phase is bounded by ``connect_timeout``, and any failure
        closes the socket before re-raising.
        """
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), connect_timeout)
        if ssl:
            try:
                async with asyncio.timeout(connect_timeout):
                    writer.write(struct.pack("!ii", 8, cls._SSL_REQUEST))
                    await writer.drain()
                    if await reader.readexactly(1) != b"S":
                        raise ConnectionError("server refused SSL")
                    ctx = ssl if isinstance(ssl, ssl_module.SSLContext) else _default_ssl_context()
                    await writer.start_tls(ctx, server_hostname=host)
            except BaseException:
                writer.close()
                raise
        conn = cls(reader, writer, tls=bool(ssl))
        try:
            await asyncio.wait_for(
                conn._startup(
                    user=user,
                    database=database,
                    password=password,
                    application_name=application_name,
                    replication=replication,
                ),
                connect_timeout,
            )
        except BaseException:
            conn.abort()
            raise
        return conn

    async def _startup(
        self,
        *,
        user: str,
        database: str,
        password: str | None,
        application_name: str,
        replication: str = "database",
    ) -> None:
        """Send the startup packet and drive authentication to ReadyForQuery.

        Speaks trust, cleartext (refused unless the connection is TLS), and
        SCRAM-SHA-256; ``-PLUS`` mechanisms are filtered out because channel
        binding is not implemented. After AuthenticationOk, drains
        ParameterStatus and BackendKeyData until ReadyForQuery.
        """
        params = {
            "user": user,
            "database": database,
            "replication": replication,
            "application_name": application_name,
            "client_encoding": "UTF8",
        }
        body = b"".join(k.encode() + b"\x00" + v.encode() + b"\x00" for k, v in params.items()) + b"\x00"
        self._writer.write(struct.pack("!ii", 8 + len(body), self._PROTOCOL_V3) + body)
        await self._writer.drain()

        scram: ScramClient | None = None
        while True:
            msg = await self._read_message()
            if msg.mtype == b"R":
                (code,) = struct.unpack("!i", msg.body[:4])
                if code == 0:  # AuthenticationOk
                    break
                if code == 3:  # CleartextPassword
                    if not self.tls:
                        raise PgServerError.from_message(
                            "refusing cleartext password on an unencrypted connection; "
                            "enable ssl= or use SCRAM-SHA-256"
                        )
                    if password is None:
                        raise PgServerError.from_message("server requested a password but none was given")
                    self._write_message(b"p", password.encode() + b"\x00")
                    await self._writer.drain()
                elif code == 10:  # SASL
                    mechanisms = [m.decode() for m in msg.body[4:].split(b"\x00") if m]
                    plain = [m for m in mechanisms if not m.endswith("-PLUS")]
                    if not plain or password is None:
                        raise PgServerError.from_message(f"unsupported SASL mechanisms {mechanisms} or missing password")
                    scram = ScramClient(plain, user, password)
                    first = scram.get_client_first().encode()
                    self._write_message(b"p", scram.mechanism_name.encode() + b"\x00" + struct.pack("!i", len(first)) + first)
                    await self._writer.drain()
                elif code == 11:  # SASLContinue
                    if scram is None:
                        raise PgServerError.from_message("server sent SASLContinue before SASL")
                    scram.set_server_first(msg.body[4:].decode())
                    self._write_message(b"p", scram.get_client_final().encode())
                    await self._writer.drain()
                elif code == 12:  # SASLFinal
                    if scram is None:
                        raise PgServerError.from_message("server sent SASLFinal before SASL")
                    scram.set_server_final(msg.body[4:].decode())
                else:
                    raise PgServerError.from_message(f"unsupported authentication request (code {code}); pgnudge speaks trust, cleartext and SCRAM-SHA-256")
            elif msg.mtype == b"E":
                raise PgServerError.from_wire(msg.body)
            else:  # NoticeResponse etc.
                continue

        while True:  # post-auth: BackendKeyData / ReadyForQuery ('S' ParameterStatus skipped)
            msg = await self._read_message()
            if msg.mtype == b"K":
                self.backend_pid = struct.unpack("!i", msg.body[:4])[0]
            elif msg.mtype == b"Z":
                return
            elif msg.mtype == b"E":
                raise PgServerError.from_wire(msg.body)

    # -- framing ---------------------------------------------------------------

    async def _read_message(self) -> BackendMessage:
        header = await self._reader.readexactly(5)
        (length,) = struct.unpack("!i", header[1:5])
        body = await self._reader.readexactly(length - 4)
        return BackendMessage(mtype=header[:1], body=body)

    def _write_message(self, mtype: bytes, body: bytes) -> None:
        self._writer.write(mtype + struct.pack("!i", 4 + len(body)) + body)

    # -- simple query (the only subprotocol walsender mode speaks) --------------

    async def simple_query(self, sql: str) -> None:
        """Run a command and drain to ReadyForQuery; result rows are ignored."""
        await self.simple_query_rows(sql)

    async def simple_query_rows(self, sql: str) -> list[tuple[str | None, ...]]:
        """Run a query and return its DataRow values as text; None for SQL NULL."""
        self._write_message(b"Q", sql.encode() + b"\x00")
        await self._writer.drain()
        rows: list[tuple[str | None, ...]] = []
        error: PgServerError | None = None
        while True:
            msg = await self._read_message()
            if msg.mtype == b"D":
                (ncols,) = struct.unpack("!H", msg.body[:2])
                values: list[str | None] = []
                offset = 2
                for _ in range(ncols):
                    (length,) = struct.unpack("!i", msg.body[offset : offset + 4])
                    offset += 4
                    if length < 0:
                        values.append(None)
                    else:
                        values.append(msg.body[offset : offset + length].decode("utf-8", "replace"))
                        offset += length
                rows.append(tuple(values))
            elif msg.mtype == b"E":
                error = PgServerError.from_wire(msg.body)
            elif msg.mtype == b"Z":
                if error is not None:
                    raise error
                return rows
            # 'T' RowDescription, 'C' CommandComplete, 'N' Notice: skipped

    # -- CopyBoth streaming ------------------------------------------------------

    async def start_replication(self, command: str) -> None:
        """Send START_REPLICATION and consume up to CopyBothResponse."""
        self._write_message(b"Q", command.encode() + b"\x00")
        await self._writer.drain()
        while True:
            msg = await self._read_message()
            if msg.mtype == b"W":
                return
            if msg.mtype == b"E":
                raise PgServerError.from_wire(msg.body)

    async def read_stream(self) -> XLogData | Keepalive:
        """Read the next replication message. Raises on stream end or error."""
        while True:
            msg = await self._read_message()
            if msg.mtype == b"d":
                kind = msg.body[:1]
                if kind == b"w":
                    start, end, _ts = struct.unpack("!QQQ", msg.body[1:25])
                    return XLogData(start_lsn=start, end_lsn=end, payload=msg.body[25:])
                if kind == b"k":
                    end, _ts, reply = struct.unpack("!QQB", msg.body[1:18])
                    return Keepalive(end_lsn=end, reply_requested=bool(reply))
                continue  # unknown CopyData subtype
            if msg.mtype == b"E":
                raise PgServerError.from_wire(msg.body)
            if msg.mtype in (b"c", b"C", b"Z"):
                raise ConnectionResetError("replication stream ended")

    async def send_standby_status(self, lsn: int, *, reply: bool = False) -> None:
        """Acknowledge everything up to ``lsn``; ``reply`` asks the server to answer with a keepalive."""
        ts = int((time.time() - self._PG_EPOCH_UNIX) * 1_000_000)
        # serialized: concurrent drain() on a paused transport trips the
        # single-waiter assert in asyncio's FlowControlMixin
        async with self.send_lock:
            self._write_message(b"d", b"r" + struct.pack("!QQQQB", lsn, lsn, lsn, ts, int(reply)))
            await self._writer.drain()

    # -- teardown ----------------------------------------------------------------

    def abort(self) -> None:
        """Hard-close the socket with no protocol goodbye; slot cleanup must survive crashes."""
        with contextlib.suppress(Exception):
            transport = self._writer.transport
            if isinstance(transport, asyncio.WriteTransport):
                transport.abort()


@dataclass(slots=True, kw_only=True)
class StatusFeedback:
    """Periodic standby-status sender with an optional liveness probe.

    With ``liveness`` set, every status requests a keepalive back, so a
    healthy connection has inbound traffic every ``interval`` and silence
    beyond ``liveness`` means the link or walsender is dead; the abort
    breaks the supervisor's blocked read and forces a reconnect.
    ``liveness`` is snapshotted at construction: a runtime mutation of the
    feed's public attribute must not half-apply.
    """

    conn: WalsenderConnection
    interval: float
    liveness: float | None
    lsn: Callable[[], int]
    idle: Callable[[], float]
    log: logging.Logger

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            idle = self.idle()
            if self.liveness is not None and idle > self.liveness:
                self.log.warning("no server traffic for %.1fs; aborting connection", idle)
                self.conn.abort()
                return
            try:
                await self.conn.send_standby_status(self.lsn(), reply=self.liveness is not None)
            except Exception as exc:
                self.log.debug("standby status send failed: %s", exc)
                return
