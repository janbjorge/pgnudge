"""PostgreSQL wire-protocol helpers for the scripted fake-walsender tests.

Backend frame builders, frontend frame readers, and a tiny asyncio server
harness so protocol paths can be unit-tested without PostgreSQL.
"""

import asyncio
import contextlib
import socket
import struct
from collections.abc import AsyncIterator, Callable, Coroutine

__all__ = [
    "msg",
    "auth_request",
    "backend_key",
    "ready_for_query",
    "command_complete",
    "data_row",
    "error_response",
    "notice_response",
    "copy_both_response",
    "xlog_data",
    "keepalive",
    "read_startup",
    "read_frame",
    "read_standby_status",
    "scripted_server",
    "refused_port",
]


def msg(mtype: bytes, body: bytes) -> bytes:
    """One typed backend message: type byte + int32 length + body."""
    return mtype + struct.pack("!i", 4 + len(body)) + body


def auth_request(code: int, extra: bytes = b"") -> bytes:
    return msg(b"R", struct.pack("!i", code) + extra)


def backend_key(pid: int) -> bytes:
    return msg(b"K", struct.pack("!ii", pid, 0))


def ready_for_query() -> bytes:
    return msg(b"Z", b"I")


def command_complete(tag: str) -> bytes:
    return msg(b"C", tag.encode() + b"\x00")


def data_row(*values: bytes | None) -> bytes:
    body = struct.pack("!H", len(values))
    for value in values:
        if value is None:
            body += struct.pack("!i", -1)
        else:
            body += struct.pack("!i", len(value)) + value
    return msg(b"D", body)


def error_response(message: str = "boom", code: str = "XX000") -> bytes:
    return msg(b"E", f"SERROR\x00C{code}\x00M{message}\x00".encode() + b"\x00")


def notice_response(message: str = "fyi") -> bytes:
    return msg(b"N", f"SNOTICE\x00C00000\x00M{message}\x00".encode() + b"\x00")


def copy_both_response() -> bytes:
    return msg(b"W", b"\x00\x00\x00")


def xlog_data(lsn: int, payload: bytes) -> bytes:
    return msg(b"d", b"w" + struct.pack("!QQQ", lsn, lsn, 0) + payload)


def keepalive(lsn: int, *, reply: bool) -> bytes:
    return msg(b"d", b"k" + struct.pack("!QQB", lsn, 0, int(reply)))


async def read_startup(reader: asyncio.StreamReader) -> dict[str, str]:
    """Consume the client's StartupMessage; return its parameter map."""
    (length,) = struct.unpack("!i", await reader.readexactly(4))
    body = await reader.readexactly(int(length) - 4)
    parts = body[4:].split(b"\x00")  # body[:4] is the protocol version
    return {parts[i].decode(): parts[i + 1].decode() for i in range(0, len(parts) - 2, 2)}


async def read_frame(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """Read one typed frontend message: (type byte, body)."""
    header = await reader.readexactly(5)
    (length,) = struct.unpack("!i", header[1:5])
    return header[:1], await reader.readexactly(int(length) - 4)


def read_standby_status(body: bytes) -> tuple[int, int, int, bool]:
    """Fields of a StandbyStatusUpdate CopyData body:
    (written, flushed, applied, reply_requested)."""
    assert body[:1] == b"r"
    written, flushed, applied = struct.unpack("!QQQ", body[1:25])
    return written, flushed, applied, body[33:34] == b"\x01"


@contextlib.asynccontextmanager
async def scripted_server(
    handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Coroutine[None, None, None]],
) -> AsyncIterator[tuple[str, int]]:
    """Serve ``handler`` on an ephemeral localhost port for one test.

    Handler exceptions are suppressed: the client hard-aborts sockets on
    purpose, so reads on the server side routinely end in resets.
    """

    async def guarded(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(Exception):
            await handler(reader, writer)
        with contextlib.suppress(Exception):
            writer.close()

    server = await asyncio.start_server(guarded, "127.0.0.1", 0)
    port: int = server.sockets[0].getsockname()[1]
    try:
        yield "127.0.0.1", port
    finally:
        server.close()
        await server.wait_closed()


def refused_port() -> int:
    """A localhost port with nothing listening (bound, then released)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port
