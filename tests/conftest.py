"""Session-scoped PostgreSQL via testcontainers; one scratch database per test.

Env knobs: EXTERNAL_POSTGRES_DSN (skip the container), POSTGRES_IMAGE
(default postgres:17), PGNUDGE_PLUGIN (test_decoding default | wal2json —
needs an image that ships it; no public one does for PG 16, build your own:
postgres:16 + apt postgresql-16-wal2json, see ci.yml), PGNUDGE_TLS=1
(external server has TLS).
"""

import os
import uuid
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass
from urllib.parse import urlparse

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer


@dataclass(frozen=True, slots=True)
class PgParams:
    """Connection facts for one scratch database, WalFeed-shaped."""

    host: str
    port: int
    user: str
    password: str | None
    database: str
    plugin: str
    tls_available: bool

    @property
    def dsn(self) -> str:
        auth = f"{self.user}:{self.password}@" if self.password else f"{self.user}@"
        return f"postgresql://{auth}{self.host}:{self.port}/{self.database}"


def _enable_tls(container: PostgresContainer) -> bool:
    """Self-signed cert + ssl=on inside the container. Best effort."""
    script = (
        "cd /var/lib/postgresql/data"
        " && openssl req -new -x509 -days 1 -nodes -subj '/CN=localhost'"
        " -out server.crt -keyout server.key 2>/dev/null"
        " && chmod 600 server.key"
        " && psql -U test -d test -c 'ALTER SYSTEM SET ssl=on'"
        " && psql -U test -d test -c 'SELECT pg_reload_conf()'"
    )
    code, _ = container.get_wrapped_container().exec_run(["bash", "-c", script], user="postgres")
    return int(code) == 0


@pytest.fixture(scope="session")
def postgres() -> Iterator[tuple[str, bool]]:
    """Yields (dsn, tls_available) for the session's server."""
    if external := os.environ.get("EXTERNAL_POSTGRES_DSN"):
        yield external, os.environ.get("PGNUDGE_TLS") == "1"
        return

    image = os.environ.get("POSTGRES_IMAGE", "postgres:17")
    container = PostgresContainer(image, username="test", password="test", dbname="test", driver=None)
    container.with_command("-c wal_level=logical -c fsync=off -c synchronous_commit=off -c full_page_writes=off")
    with container as running:
        yield running.get_connection_url(), _enable_tls(running)


@pytest.fixture
async def pg(postgres: tuple[str, bool]) -> AsyncGenerator[PgParams]:
    base_dsn, tls_available = postgres
    parsed = urlparse(base_dsn)
    scratch = f"pgnudge_test_{uuid.uuid4().hex[:12]}"

    admin = await asyncpg.connect(base_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{scratch}"')
    finally:
        await admin.close()

    yield PgParams(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 5432,
        user=parsed.username or "postgres",
        password=parsed.password,
        database=scratch,
        plugin=os.environ.get("PGNUDGE_PLUGIN", "test_decoding"),
        tls_available=tls_available,
    )

    admin = await asyncpg.connect(base_dsn)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)')
    finally:
        await admin.close()


@pytest.fixture
async def admin(pg: PgParams) -> AsyncGenerator[asyncpg.Connection]:
    """Plain asyncpg connection into the test's scratch database."""
    conn = await asyncpg.connect(pg.dsn)
    try:
        yield conn
    finally:
        await conn.close()
