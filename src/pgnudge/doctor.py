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
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Protocol, TypeAlias

from pgnudge.proto import PgServerError, WalsenderConnection

__all__ = ["Check", "Diagnosis", "diagnose"]

_MIN_VERSION_NUM = 160000  # PostgreSQL 16.0
_FALLBACK_PLUGIN = "test_decoding"  # zero-install; WalFeed parses it too

# A detected managed platform, or None for self-managed / undetected. The label
# shapes remediation text: the SQL that fixes wal_level on a self-hosted box is
# a parameter-group toggle on RDS and a portal/CLI setting on Azure.
Platform: TypeAlias = str  # "rds" | "azure" | "gcp"


class Connect(Protocol):
    """The ``connect`` partial each probe calls, varying only ``replication``."""

    def __call__(self, *, replication: str) -> Coroutine[None, None, WalsenderConnection]: ...


@dataclass(frozen=True, slots=True)
class Check:
    """One readiness check: a name, pass/fail, detail, and an optional fix.

    ``fix`` is a copy-paste remediation shown under a failed check; it is None
    when the check passed or when nothing actionable applies (e.g. an old
    server version, which no SQL can fix).
    """

    name: str
    ok: bool
    detail: str
    fix: str | None = None


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


async def _detect_platform(conn: WalsenderConnection) -> Platform | None:
    """Best-effort managed-platform fingerprint from GUCs and vendor roles.

    All probes are missing-safe (``current_setting(..., true)`` and role
    existence), so an unknown platform simply returns None.
    """
    rows = await conn.simple_query_rows(
        "SELECT current_setting('rds.extensions', true) IS NOT NULL, "
        "EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'azure_pg_admin'), "
        "EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cloudsqlsuperuser')"
    )
    if not rows or len(rows[0]) < 3:
        return None
    is_rds, is_azure, is_gcp = (value == "t" for value in rows[0][:3])
    if is_rds:
        return "rds"
    if is_azure:
        return "azure"
    if is_gcp:
        return "gcp"
    return None


def _platform_label(platform: Platform | None) -> str:
    return {"rds": "Amazon RDS / Aurora", "azure": "Azure Flexible Server", "gcp": "Google Cloud SQL"}.get(
        platform or "", "self-managed / undetected"
    )


def _wal_level_fix(platform: Platform | None) -> str:
    if platform == "rds":
        return "set rds.logical_replication=1 in the DB parameter group, then reboot the instance"
    if platform == "azure":
        return (
            "set server parameter wal_level=logical "
            "(az postgres flexible-server parameter set --name wal_level --value logical), then restart"
        )
    if platform == "gcp":
        return "set the cloudsql.logical_decoding flag to on, then restart the instance"
    return "ALTER SYSTEM SET wal_level = 'logical';  -- then restart PostgreSQL"


def _replication_role_fix(platform: Platform | None, user: str) -> str:
    if platform == "rds":
        return f'GRANT rds_replication TO "{user}";'
    if platform == "azure":
        return f'ALTER ROLE "{user}" WITH REPLICATION;  -- run as azure_pg_admin'
    return f'ALTER ROLE "{user}" WITH REPLICATION;  -- superuser required'


def _physical_fix(platform: Platform | None) -> str:
    if platform == "rds":
        return "RDS / Aurora blocks external physical streaming; use WalFeed (logical) instead"
    if platform in ("azure", "gcp"):
        return "managed platforms usually block external physical streaming; use WalFeed (logical) instead"
    return (
        'add "host replication <user> <client-ip>/32 <method>" to pg_hba.conf (then reload), '
        "and grant the role REPLICATION"
    )


def _wal2json_fix() -> str:
    return (
        "install wal2json on the server (e.g. apt-get install postgresql-<ver>-wal2json), "
        f"or run pgnudge with --plugin {_FALLBACK_PLUGIN}"
    )


async def _probe_logical(connect: Connect, plugin: str) -> Exception | None:
    """Create and drop a TEMPORARY logical slot; return the failure, or None."""
    try:
        async with await connect(replication="database") as conn:
            slot = f"pgnudge_doctor_{os.getpid()}_{secrets.token_hex(3)}"
            await conn.simple_query(
                f'CREATE_REPLICATION_SLOT "{slot}" TEMPORARY LOGICAL {plugin} (SNAPSHOT \'nothing\')'
            )
        return None
    except Exception as exc:
        return exc


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


def _version_check(version: str | None) -> Check:
    version_num = int(version) if version and version.isdigit() else 0
    if version_num >= _MIN_VERSION_NUM:
        return Check("server version", True, f"PostgreSQL server_version_num={version_num}")
    return Check(
        "server version",
        False,
        f"server_version_num={version or '?'}; pgnudge needs PostgreSQL 16+",
        fix="upgrade the server to PostgreSQL 16 or newer",
    )


def _role_check(privileged: str | None, platform: Platform | None, user: str) -> Check:
    if privileged == "t":
        return Check("REPLICATION role", True, f"role {user!r} has REPLICATION (or is superuser)")
    return Check(
        "REPLICATION role",
        False,
        f"role {user!r} lacks REPLICATION; grant it or use a dedicated role",
        fix=_replication_role_fix(platform, user),
    )


async def _walfeed_check(
    connect: Connect, plugin: str, wal_level: str | None, platform: Platform | None, user: str
) -> Check:
    """Probe WalFeed readiness with a throwaway TEMPORARY logical slot.

    A slot exercises wal_level, the plugin, and the REPLICATION grant at once,
    and dies with the connection. When ``plugin`` is missing (58P01) we retry
    with the built-in ``test_decoding`` so we can tell "logical decoding is
    blocked" apart from "logical works, wal2json just is not installed".
    """
    exc = await _probe_logical(connect, plugin)
    if exc is None:
        return Check("WalFeed (logical decoding)", True, f"temporary {plugin} slot created (and dropped)")

    plugin_missing = isinstance(exc, PgServerError) and exc.fields.get("C") == "58P01"
    if (
        plugin_missing
        and plugin != _FALLBACK_PLUGIN
        and await _probe_logical(connect, _FALLBACK_PLUGIN) is None
    ):
        return Check(
            "WalFeed (logical decoding)",
            True,
            f"{plugin} not installed, but logical decoding works via the {_FALLBACK_PLUGIN} fallback",
        )

    detail = _explain(exc)
    if plugin_missing:
        return Check("WalFeed (logical decoding)", False, detail, fix=_wal2json_fix())
    # A wrong wal_level and a missing REPLICATION grant both surface here (the
    # grant fails first, as 42501), so when wal_level is wrong name both fixes;
    # otherwise the remaining cause is the grant.
    if wal_level and wal_level != "logical":
        detail = f"{detail} (wal_level={wal_level})"
        fix = (
            f"{_wal_level_fix(platform)}; the role also needs REPLICATION "
            f"({_replication_role_fix(platform, user)})"
        )
        return Check("WalFeed (logical decoding)", False, detail, fix=fix)
    return Check("WalFeed (logical decoding)", False, detail, fix=_replication_role_fix(platform, user))


async def _physical_check(connect: Connect, platform: Platform | None) -> Check:
    """Probe RawFeed readiness with IDENTIFY_SYSTEM; creates no server object."""
    try:
        async with await connect(replication="true") as conn:
            rows = await conn.simple_query_rows("IDENTIFY_SYSTEM")
    except Exception as exc:
        return Check("RawFeed (physical WAL)", False, _explain(exc), fix=_physical_fix(platform))
    if rows and rows[0]:
        return Check("RawFeed (physical WAL)", True, "IDENTIFY_SYSTEM ok; physical streaming permitted")
    return Check(
        "RawFeed (physical WAL)", False, "IDENTIFY_SYSTEM returned no row", fix=_physical_fix(platform)
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

    # A plain connection needs no REPLICATION attribute, so a missing grant
    # still yields a clean version / wal_level / platform report.
    try:
        basic = await connect(replication="false")
    except Exception as exc:
        detail = f"cannot connect to {host}:{port}: {_explain(exc)}"
        return Diagnosis((Check("connect", False, detail),), None)

    async with basic:
        backend_pid = basic.backend_pid
        version = await _scalar(basic, "SHOW server_version_num")
        wal_level = await _scalar(basic, "SHOW wal_level")
        privileged = await _scalar(
            basic, "SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user"
        )
        platform = await _detect_platform(basic)

    checks = [
        Check("connect", True, f"connected to {host}:{port} (backend pid {backend_pid})"),
        Check("platform", True, f"detected {_platform_label(platform)}"),
        _version_check(version),
        _role_check(privileged, platform, user),
        await _walfeed_check(connect, plugin, wal_level, platform, user),
        await _physical_check(connect, platform),
    ]
    walfeed, physical = checks[-2], checks[-1]
    recommended = "WalFeed" if walfeed.ok else "RawFeed" if physical.ok else None
    return Diagnosis(tuple(checks), recommended)
