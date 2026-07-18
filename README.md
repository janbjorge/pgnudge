# pgnudge

**Your database moved. Your app already knows.**

[![CI](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgnudge)](https://pypi.org/project/pgnudge/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://pypi.org/project/pgnudge/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

pgnudge is a tiny async library that tells you which tables just changed in
PostgreSQL, so a live read model can re-render the instant the data moves. It
carries no row data by design: you already know how to load your data, pgnudge
just tells you *when*, and *what to reload*. And it leaves nothing behind on
the server: no triggers or functions to install, no persistent slots or
cleanup jobs to manage.

```python
from pgnudge import Batch, Resync, WalFeed

async with WalFeed(
    host="db.example.com", user="wal_user", password=..., database="app", ssl=True,
    tables=["public.orders", "public.stations"],   # filtered in the output plugin
    debounce=0.05,
) as feed:
    async for item in feed:
        match item:
            case Resync():           # connected / reconnected / overflow / failsafe
                await reload_everything()
            case Batch(events=evs):  # coalesced wakeups: which tables moved
                await reload(tables={e.payload for e in evs})
```

There is no step 1. Nothing to install in the database, nothing to migrate.
Close the connection and the server forgets pgnudge ever existed.

```bash
pip install pgnudge
```

Python >= 3.11, PostgreSQL >= 16. One runtime dependency:
[scramp](https://github.com/tlocke/scramp) (pure-Python SCRAM auth). There is
no database driver: pgnudge speaks the PostgreSQL replication protocol itself.

## Why pgnudge

- The only server object is a *temporary* replication slot, dropped
  automatically however the session ends. `RawFeed` needs no slot at all.
- A hand-rolled walsender client (TLS, SCRAM-SHA-256, CopyBoth) stands in for
  a database driver, so `pip install pgnudge` pulls in scramp and nothing else.
- `WalFeed` (logical decoding) and `RawFeed` (physical WAL, decoded
  client-side) yield the same `Resync | Batch` stream; the choice is one
  constructor.
- Wakeups coalesce: a 500-row transaction on one table is one `Event`,
  `count=500`, one wakeup, one refetch, debounced client-side.
- At-least-once wakeups from the point of connect, with every gap bracketed by
  a `Resync`. Handle `Resync` and nothing can make your view wrong; there are
  no cursors to persist and no exactly-once delivery to get wrong.
- `pgnudge doctor` connects, fingerprints the platform, and tells you which
  feed to use, with a copy-paste fix under every failed check.

## Should you use pgnudge?

| Reach for pgnudge when...                                   | Look elsewhere when...                                          |
|-------------------------------------------------------------|----------------------------------------------------------------|
| A dashboard / cache / read model must re-render on change   | You need the changed **rows** (before/after images) -> that's CDC ([Debezium]) |
| You can refetch from the DB - it's the source of truth      | Every message must be processed exactly once -> use a queue ([pgqueuer]) |
| You want **nothing installed** in the database              | You need history / backfill of changes that happened while disconnected |
| Missing changes while disconnected is fine (you'll refetch) | You need cross-datacenter durable replication -> use logical replication |

pgqueuer moves work; pgnudge moves *wakefulness*.

[Debezium]: https://debezium.io/
[pgqueuer]: https://github.com/janbjorge/pgqueuer

## The contract

A feed yields exactly two item types:

- **`Resync(reason)`**: reload everything. Emitted on every connect and
  reconnect, on internal queue overflow, and (optionally) on a failsafe
  interval.
- **`Batch(events)`**: one debounce window's worth of wakeups, deduplicated, in
  arrival order. Each `Event` carries `payload` (`schema.table`, the stable v1
  payload contract), `first_seen`, `count`.

Delivery is at-least-once wakeups, from the point of connect only. Events
are hints to refetch, never facts to apply. There is no history and no backfill,
by design and by mechanism: the slot is created fresh at every (re)connect with
`SNAPSHOT 'nothing'`, and can only decode forward. On reconnect a feed *resyncs*
rather than resumes. There is no replay, no exactly-once delivery, and no row
images to apply: refetching is idempotent and you have a database right there. The gap-free-handshake argument
is in [docs/temporary-slots.md](docs/temporary-slots.md).

Coalescing: per-row changes within the debounce window collapse client-side
into one `Event` with a `count`. `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE`
all nudge on `WalFeed` (`RawFeed` covers all but `TRUNCATE`). Neither transport
carries other DDL, so pair migrations with a refetch if your view depends on
them.

## The zero-footprint guarantee

`WalFeed` creates nothing on the server that outlives the connection.

```
your app ──── async for item in feed ────▶ Resync | Batch
  ▲
  │  walsender protocol, no driver (TLS, SCRAM-SHA-256, CopyBoth)
  │
PostgreSQL ── TEMPORARY replication slot ── logical decoding
              └── dropped by the server the instant the session ends,
                  cleanly or not
```

The temporary replication slot is the only PostgreSQL primitive that gives a
change feed with connection-scoped lifetime: the server is contractually obliged
to drop it the moment the session ends, whether by clean close, crash, `kill -9`,
or `pg_terminate_backend`. The test suite ends by hard-aborting the socket with
no protocol goodbye and asserting `pg_replication_slots` is empty. What is
required is one-time server configuration (settings, not objects):
`wal_level = logical`, a `REPLICATION` role, and an output plugin.

## Two transports, one contract

You make exactly one decision: which feed class, driven by the server's
`wal_level`. If you get to choose, choose `WalFeed`. Neither is more "correct";
they are the same contract over two different server capabilities.

|                   | `WalFeed` (logical decoding)       | `RawFeed` (physical WAL)                 |
|-------------------|------------------------------------|------------------------------------------|
| `wal_level`       | `logical` (usually needs a restart)| `replica`, the stock default             |
| Output plugin     | wal2json or test_decoding          | none; WAL is decoded client-side         |
| Server objects    | one TEMPORARY slot while connected | none at any point, not even a slot       |
| `TRUNCATE` nudges | yes                                | no (documented gap)                      |
| Stream scope      | one database, filtered server-side | whole cluster, filtered client-side      |

`WalFeed` is the fuller transport: `TRUNCATE` nudges too, the server filters
tables for you, and only your database's WAL is decoded. It costs a one-time
restart on most servers plus an output plugin (`wal2json`, preinstalled on most
managed platforms, or `test_decoding`, built into PostgreSQL). Mechanics in
[docs/temporary-slots.md](docs/temporary-slots.md).

```python
from pgnudge import RawFeed

feed = RawFeed(
    host="db.example.com", user="wal_user", password=..., database="app",
    tables=["public.orders", "public.stations"],  # client-side filter
)
```

`RawFeed` needs no server change: it decodes physical WAL client-side, slot-less,
at stock `wal_level = replica`. Nudges are commit-gated (rollbacks never nudge, a
refetch never races an open transaction). The trade: the server streams the
whole cluster's WAL, `TRUNCATE` is not detected, it opens a second plain
connection for catalog lookups, and it needs a `pg_hba.conf` `replication` entry.
Treat it as a self-hosted transport; managed platforms are not known to
expose external physical streaming (untested). Mechanics in
[docs/physical-wal.md](docs/physical-wal.md); byte layouts in
[docs/parsing-reference.md](docs/parsing-reference.md).

Either transport needs a role with the `REPLICATION` attribute and a **direct**
connection (replication traffic cannot go through a pooler like PgBouncer).

## doctor

```bash
pgnudge doctor --host ... --user ... --database ...
```

It connects, checks `wal_level`, the `REPLICATION` grant, and the output plugin,
then tells you which feed to use, printing a copy-paste fix under any failed
check (tuned to the detected platform: RDS parameter groups, `az` commands, or
plain `ALTER SYSTEM`). If `wal2json` is absent it retries with the built-in
`test_decoding` to distinguish "logical decoding is blocked" from "the plugin
just is not installed". The WalFeed check creates a temporary slot and drops it,
so `doctor` leaves nothing behind.

## Managed platforms

Each documented platform (RDS, Aurora, Cloud SQL, Azure, Supabase, Neon) has a
`WalFeed` path: flip `wal_level = logical`, grant a REPLICATION-capable role.
`RawFeed` is **untested** on managed services (it needs external physical
streaming that vendors are not known to expose); the one confirmed data point is
**Azure Flexible Server, which blocks it**. Full matrix, per-vendor caveats, and
vendor doc links: [docs/managed-platforms.md](docs/managed-platforms.md).
pgnudge has not been integration-tested against any of them; let `pgnudge doctor`
confirm the live handshake.

## Ops notes

- `status_interval` (default 10 s) must stay under `wal_sender_timeout`
  (default 60 s). `liveness_timeout` (default 30 s, `None` disables): inbound
  silence past it means a dead link, so the feed aborts and reconnects instead
  of blocking forever.
- While connected, each `WalFeed` holds one slot and one WAL sender against
  `max_replication_slots` / `max_wal_senders`; a disconnected feed holds
  nothing and retains no WAL. A `REPLICATION` role sees every table regardless
  of SELECT grants; scope with `tables=` and treat it as privileged.
- `ssl=True` uses platform CA verification; pass an `ssl.SSLContext` for
  custom trust. pgnudge refuses to send any password (cleartext or MD5) on an
  unencrypted connection. Prefer SCRAM-SHA-256; MD5 is deprecated in
  PostgreSQL 18. CLI TLS keys off `PGSSLMODE`
  (`require`/`verify-ca`/`verify-full` all map to verify-full; `prefer` and
  below stay plaintext, with no silent upgrade).
- Every exception inherits `PgnudgeError`; `ConfigError` also inherits
  `ValueError`. Stream/connection failures are internal lifecycle (the
  supervisor backs off and reconnects with a `Resync`), never surfaced on the
  iterator. An *uncaught* internal bug is logged once at ERROR and re-raised
  from `async for`, so a defect fails loudly rather than hanging.
- For many consumers, run one `WalFeed` in a small bridge daemon that
  republishes via `pg_notify`; consumers attach with plain `LISTEN` (any
  driver, zero objects). `NOTIFY` can't track data changes without triggers,
  and triggers are persistent catalog objects pgnudge refuses by premise; the
  bridge keeps LISTEN's ergonomics with logical decoding's zero footprint.
- A restart reconnects every feed at once and every `Resync` handler refetches
  at once. Reconnect timing is jittered; add jitter to your refetch too, or
  fan out through the bridge daemon.

## Testing

The suite spins up real PostgreSQL via testcontainers (nothing to install beyond
Docker) and proves the claims live: no backfill of pre-connect writes,
client-side coalescing, reconnect gets a fresh slot with the old one auto-dropped,
TLS + SCRAM over an encrypted stream, and the flagship proof: hard socket abort
with no protocol goodbye leaves `pg_replication_slots` empty. `RawFeed` gets its
own proofs (zero slots while streaming, commit gating, cross-database isolation),
and its decoder is checked against `pg_waldump` as an oracle on every PostgreSQL
major in CI.

```bash
uv sync && uv run pytest
```

## Non-goals

- **Not a queue.** There is no durability, no competing consumers, and no
  retries. Use a job queue ([pgqueuer]) if a message must be processed;
  pgnudge is its broadcast-shaped sibling.
- **Not CDC.** There are no row images, no before/after, and no replay:
  refetch instead.
- **Not a driver.** The protocol client implements exactly what a
  logical-decoding consumer needs: startup, auth, simple query, CopyBoth.

Contributor docs and the roadmap live in [AGENTS.md](AGENTS.md). MIT licensed.
