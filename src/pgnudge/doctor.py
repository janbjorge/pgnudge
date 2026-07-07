"""Preflight: probe a server and recommend a transport.

Runs the real handshakes rather than guessing. The WalFeed probe creates a
TEMPORARY logical slot and drops it by disconnecting, so ``doctor`` leaves
nothing behind, exactly like the product. Each probe failure becomes a
diagnostic line; nothing here raises to the caller.
"""

import functools
import os
import secrets
import ssl as ssl_module
from dataclasses import dataclass

from pgnudge.proto import PgServerError, WalsenderConnection

__all__ = ["Check", "Diagnosis", "diagnose"]

_MIN_VERSION_NUM = 160000  # PostgreSQL 16.0


@dataclass(frozen=True, slots=True)
class Check:
    """One readiness check: a name, pass/fail, and a human-readable detail."""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class Diagnosis:
    """The full report and the recommended transport, if any."""

    checks: tuple[Check, ...]
    recommended: str | None  # "WalFeed" | "RawFeed" | None


def _explain(exc: Exception) -> str:
    """A short cause string, enriched from a server error's fields when present."""
    if isinstance(exc, PgServerError):
        code = exc.fields.get("C", "?????")
        message = exc.fields.get("M", "unknown")
        if code == "58P01":  # undefined_file: output plugin not installed
            return f"{message} (output plugin not installed on the server)"
        return f"{code}: {message}"
    return str(exc) or type(exc).__name__


async def _scalar(conn: WalsenderConnection, sql: str) -> str | None:
    rows = await conn.simple_query_rows(sql)
    if not rows or not rows[0]:
        return None
    return rows[0][0]


async def _connect(
    *,
    host: str,
    port: int,
    user: str,
    database: str,
    password: str | None,
    ssl: bool | ssl_module.SSLContext,
    connect_timeout: float,
    replication: str,
) -> WalsenderConnection:
    return await WalsenderConnection.connect(
        host=host,
        port=port,
        user=user,
        database=database,
        password=password,
        ssl=ssl,
        application_name="pgnudge-doctor",
        connect_timeout=connect_timeout,
        replication=replication,
    )


async def diagnose(
    *,
    host: str,
    port: int,
    user: str,
    database: str,
    password: str | None = None,
    ssl: bool | ssl_module.SSLContext = False,
    plugin: str = "wal2json",
    connect_timeout: float = 10.0,
) -> Diagnosis:
    """Probe ``host:port`` and return readiness checks plus a recommendation."""
    checks: list[Check] = []
    connect = functools.partial(
        _connect,
        host=host,
        port=port,
        user=user,
        database=database,
        password=password,
        ssl=ssl,
        connect_timeout=connect_timeout,
    )

    # -- basics: a plain connection needs no REPLICATION attribute, so a
    # missing grant still yields a clean version/wal_level report --
    try:
        basic = await connect(replication="false")
    except Exception as exc:
        checks.append(Check("connect", False, f"cannot connect to {host}:{port}: {_explain(exc)}"))
        return Diagnosis(tuple(checks), None)

    wal_level: str | None = None
    try:
        checks.append(Check("connect", True, f"connected to {host}:{port} (backend pid {basic.backend_pid})"))
        version = await _scalar(basic, "SHOW server_version_num")
        version_num = int(version) if version and version.isdigit() else 0
        if version_num >= _MIN_VERSION_NUM:
            checks.append(Check("server version", True, f"PostgreSQL server_version_num={version_num}"))
        else:
            checks.append(
                Check("server version", False, f"server_version_num={version or '?'}; pgnudge needs PostgreSQL 16+")
            )
        wal_level = await _scalar(basic, "SHOW wal_level")
        privileged = await _scalar(basic, "SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user")
        if privileged == "t":
            checks.append(Check("REPLICATION role", True, f"role {user!r} has REPLICATION (or is superuser)"))
        else:
            checks.append(
                Check("REPLICATION role", False, f"role {user!r} lacks REPLICATION; grant it or use a dedicated role")
            )
    finally:
        basic.abort()

    # -- WalFeed probe: a TEMPORARY logical slot proves wal_level=logical,
    # the plugin, and REPLICATION all at once; it dies with this connection --
    logical_ok = False
    try:
        conn = await connect(replication="database")
        try:
            slot = f"pgnudge_doctor_{os.getpid()}_{secrets.token_hex(3)}"
            await conn.simple_query(
                f'CREATE_REPLICATION_SLOT "{slot}" TEMPORARY LOGICAL {plugin} (SNAPSHOT \'nothing\')'
            )
            logical_ok = True
            checks.append(Check("WalFeed (logical decoding)", True, f"temporary {plugin} slot created (and dropped)"))
        finally:
            conn.abort()
    except Exception as exc:
        hint = f" (wal_level={wal_level})" if wal_level and wal_level != "logical" else ""
        checks.append(Check("WalFeed (logical decoding)", False, f"{_explain(exc)}{hint}"))

    # -- RawFeed probe: physical streaming proves REPLICATION and a pg_hba
    # replication entry; no server object is created --
    physical_ok = False
    try:
        conn = await connect(replication="true")
        try:
            rows = await conn.simple_query_rows("IDENTIFY_SYSTEM")
            if rows and rows[0]:
                physical_ok = True
                checks.append(Check("RawFeed (physical WAL)", True, "IDENTIFY_SYSTEM ok; physical streaming permitted"))
            else:
                checks.append(Check("RawFeed (physical WAL)", False, "IDENTIFY_SYSTEM returned no row"))
        finally:
            conn.abort()
    except Exception as exc:
        checks.append(Check("RawFeed (physical WAL)", False, _explain(exc)))

    recommended = "WalFeed" if logical_ok else "RawFeed" if physical_ok else None
    return Diagnosis(tuple(checks), recommended)
