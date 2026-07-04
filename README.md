# pgnudge

**Push-only change nudges from PostgreSQL — nothing left behind on the server.**

[![CI](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgnudge)](https://pypi.org/project/pgnudge/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://pypi.org/project/pgnudge/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Your database moves; your app wakes up. pgnudge tells you *that* something
changed and *which tables* — you already know how to load the data. Built
for live read models: dashboards, cache invalidation, anything that
renders a query and wants to re-render the instant the database moves.

```
pip install pgnudge
```

Python ≥ 3.11, PostgreSQL ≥ 16. One dependency:
[scramp](https://github.com/tlocke/scramp) (pure-Python SCRAM auth). No
database driver — pgnudge speaks the PostgreSQL replication protocol
itself.

## Sixty-second tour

```python
from pgnudge import Batch, Resync, WalFeed

async with WalFeed(
    host="db.example.com", user="wal_user", password=...,
    database="app", ssl=True,
    tables=["public.orders", "public.stations"],   # server-side filter
    debounce=0.05,
) as feed:
    async for item in feed:
        match item:
            case Resync():           # connected / reconnected / overflow / failsafe
                await reload_everything()
            case Batch(events=evs):  # coalesced wakeups: which tables moved
                await reload(tables={e.payload for e in evs})
```

There is no step 1. Nothing to install in the database, nothing to migrate,
nothing to revert. Close the connection and the server forgets pgnudge ever
existed.

## The guarantee

`WalFeed` creates **nothing on the server that outlives the connection.**

```
your app ──── async for item in feed ────▶ Resync | Batch
  ▲
  │  walsender protocol (TLS, SCRAM-SHA-256, CopyBoth) — no driver
  │
PostgreSQL ── TEMPORARY replication slot ── logical decoding
              └── dropped by the server the instant the session ends,
                  cleanly or not
```

The temporary replication slot is the only primitive in PostgreSQL that
gives you a change feed with connection-scoped lifetime: the server is
contractually obliged to drop it the moment the session ends — cleanly, by
crash, by `kill -9`, by yanked cable, by `pg_terminate_backend`. No
triggers, no functions, no catalog objects, no persistent slots, no cleanup
jobs. The test suite ends by hard-aborting the socket with no protocol
goodbye and asserting `pg_replication_slots` is empty.

What *is* required is PostgreSQL 16+ and one-time server **configuration**
(settings, not objects — nothing accumulates): `wal_level = logical`, a
role with `REPLICATION`, and an output plugin — `wal2json` (default;
preinstalled on Azure Flexible Server, RDS, and most managed platforms) or
`test_decoding` (ships inside PostgreSQL itself).

The full mechanics — logical decoding, temporary-slot semantics, the
gap-free handshake argument, and **when not to use pgnudge** — are in
[docs/temporary-slots.md](docs/temporary-slots.md).

## The contract

A feed yields exactly two item types:

- **`Resync(reason)`** — reload everything. Emitted on every connect and
  reconnect, on internal queue overflow, and (optionally) on a failsafe
  interval. Handle `Resync` correctly and nothing can make your view wrong.
- **`Batch(events)`** — one debounce window's worth of wakeups,
  deduplicated, in arrival order. Each `Event` carries `payload`
  (`schema.table` — the stable v1 payload contract), `first_seen`, `count`.

**Delivery is at-least-once wakeups, from the point of connect only.**
Events are hints to refetch, never facts to apply. There is no history and
no backfill, by design and by mechanism: the slot is created fresh at every
(re)connect with `SNAPSHOT 'nothing'`, and a logical slot can only decode
forward from its creation point. The handshake is gap-free — `Resync` is
emitted only after the stream is live, so the refetch it triggers observes
a state at or after the slot's start point, and every later commit produces
a nudge; anything landing in between is simply covered twice, which
at-least-once absorbs. On reconnect `WalFeed` resyncs rather than resumes.
No replay, no exactly-once, no row images, *on purpose*: refetching is
idempotent and you have a database right there. (One nuance: slot creation
waits for write transactions in flight at connect time, so a long-running
write delays connect — it never causes history to be delivered.)

**Coalescing:** per-row changes within the debounce window collapse
client-side into one `Event` with a `count` — a 500-row transaction on one
table is one `Event`, `count=500`, one wakeup, one refetch.

`INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` all nudge. Logical decoding
does not carry other DDL, so schema changes don't — pair migrations with a
refetch if your view depends on them.

## Why not LISTEN/NOTIFY?

`NOTIFY` doesn't fire itself: making it track data changes means triggers,
and triggers are persistent catalog objects — schema footprint, migration
reviews, cleanup jobs, drift. pgnudge's whole premise is refusing that
trade. Logical decoding gets the same wakeups straight from the WAL with
zero objects. (LISTEN is still great on the *consuming* side — see Fan-out.)

## Fan-out

One `WalFeed` per process is the normal shape. For many consumers, run one
`WalFeed` in a small bridge daemon that republishes to a NOTIFY channel via
`pg_notify`, and let consumers attach with plain LISTEN (any driver —
LISTEN is session state, zero objects). One REPLICATION grant total, one
decoding pass total, and still zero persistent server objects: the bridge's
temp slot dies with the bridge.

## Ops notes

- `status_interval` (default 10 s) must stay under the server's
  `wal_sender_timeout` (default 60 s); the feed also answers
  reply-requested keepalives immediately.
- `liveness_timeout` (default 30 s, must exceed `status_interval` —
  enforced at construction, `None` disables): each status report asks the
  server to answer with a
  keepalive, so a healthy connection always has inbound traffic — silence
  longer than the timeout means a dead link (NAT drop, yanked VPN, hung
  walsender) and the feed aborts and reconnects instead of blocking
  forever.
- While connected, each `WalFeed` holds one replication slot and one WAL
  sender against `max_replication_slots` / `max_wal_senders`. Disconnected
  feeds hold nothing — that's the point — which also means an idle feed
  never retains WAL.
- Managed platforms: enabling `wal_level=logical` typically requires a
  restart (once); grant `REPLICATION` to a dedicated role rather than
  widening an app role — logical decoding sees the whole database's stream.
- Thundering herd: a database restart reconnects every feed at once, and
  every consumer's `Resync` handler refetches at once. Reconnect timing is
  already jittered, but the refetch is your code — add jitter there when
  many consumers share a database, or fan out through the bridge daemon so
  a single process refetches per change.
- TLS: `ssl=True` uses platform CA verification; pass an `ssl.SSLContext`
  for custom trust. SCRAM-SHA-256 is supported everywhere; cleartext auth
  only over TLS — pgnudge refuses to send a password on an unencrypted
  connection.
- Logging: the `pgnudge.wal` logger (stdlib `logging`, no handlers
  configured by the library) reports connect failures and stream errors at
  WARNING, successful (re)connects at INFO, and backoff timing at DEBUG —
  a feed that reconnects in a loop is visible, not silent.

## Tested how

The suite spins up real PostgreSQL via testcontainers (nothing to install
beyond Docker) and proves the claims live: no backfill of pre-connect
writes, client-side coalescing (50-row txn → one `Event`, `count=50`),
reconnect gets a fresh slot with the old one auto-dropped, TLS + SCRAM over
an encrypted stream, and the flagship — hard socket abort with no protocol
goodbye leaves `pg_replication_slots` empty.

```bash
uv sync && uv run pytest
```

## Non-goals

- **Not a queue.** No durability, no competing consumers, no retries. If a
  message must be processed, use a job queue
  (e.g. [pgqueuer](https://github.com/janbjorge/pgqueuer)) — pgnudge is its
  broadcast-shaped sibling: pgqueuer moves *work*, pgnudge moves
  *wakefulness*.
- **Not CDC.** No row images, no before/after, no replay. Refetch.
- **Not a driver.** The protocol client implements exactly what a
  logical-decoding consumer needs: startup, auth, simple query, CopyBoth.

## Roadmap

- Native `pgoutput` parsing would drop the wal2json server-plugin
  requirement — but pgoutput only decodes through a *publication*, and a
  publication is a persistent catalog object, in direct tension with the
  nothing-outlives-the-connection guarantee. Conditional at best: viable
  only if a pre-existing, application-owned publication counts as
  configuration rather than footprint.
- Opt-in `schema.table:pk` payloads for sharper client-side routing.
- The bridge daemon as a first-class artifact — same feed contract, one
  slot fanned out over NOTIFY; a native (Zig) implementation is the
  intended long-term core.

MIT licensed.
