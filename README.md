# pgwake

**Push-only change nudges from PostgreSQL — nothing left behind on the server.**

Emitters nudge, consumers refetch. pgwake never carries application data —
it tells you *that* something changed; you already know how to load it. Built
for live read models: TUIs, dashboards, cache invalidation, anything that
renders a query and wants to re-render the instant the database moves.

```
pip install pgwake
```

Requires Python ≥ 3.13. One dependency: scramp (pure-Python SCRAM auth).
No database driver, no psycopg anywhere.

## The guarantee

`WalFeed` creates **nothing on the server that outlives the connection**. It speaks the walsender protocol itself and
streams logical decoding from a **TEMPORARY replication slot**, which
PostgreSQL is contractually obliged to drop the moment the session ends —
cleanly, by crash, by `kill -9`, by yanked cable, by `pg_terminate_backend`.
No triggers, no functions, no catalog objects, no persistent slots, no
cleanup jobs. The live test suite ends by hard-aborting the socket with no
protocol goodbye and asserting `pg_replication_slots` is empty.

What *is* required is one-time server **configuration** (not objects, no
cleanup, nothing accumulates): `wal_level = logical`, a role with
`REPLICATION`, and an output plugin — `wal2json` (default; preinstalled on
Azure Flexible Server, RDS, and most managed platforms) or `test_decoding`
(ships inside PostgreSQL itself).

## Sixty-second tour

```python
from pgwake import Batch, Resync, WalFeed

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
nothing to revert.

## The contract

A feed yields exactly two item types:

- **`Resync(reason)`** — reload everything. Emitted on every connect and
  reconnect, on internal queue overflow, and (optionally) on a failsafe
  interval. Handle `Resync` correctly and nothing can make your view wrong.
- **`Batch(events)`** — one debounce window's worth of wakeups,
  deduplicated, in arrival order. Each `Event` carries `channel`, `payload`
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
client-side into one `Event` with a `count` (a 500-row transaction on one
table = one Event, count=500).

## Fan-out

One `WalFeed` per process is the normal shape. For many consumers, run one
`WalFeed` in a small bridge daemon that republishes to a NOTIFY channel via
`pg_notify`, and let consumers attach with plain LISTEN (any driver — LISTEN
is session state, zero objects) — no REPLICATION grant per consumer, one
decoding pass total, and still zero persistent server objects (the bridge's
temp slot dies with the bridge).

## Ops notes

- `status_interval` (default 10 s) must stay under the server's
  `wal_sender_timeout` (default 60 s); the feed also answers
  reply-requested keepalives immediately.
- While connected, each `WalFeed` holds one replication slot and one WAL
  sender against `max_replication_slots` / `max_wal_senders`. Disconnected
  feeds hold nothing — that's the point — which also means an idle feed
  never retains WAL.
- Managed platforms: enabling `wal_level=logical` typically requires a
  restart (once); grant `REPLICATION` to a dedicated role rather than
  widening an app role — logical decoding sees the whole database's stream.
- TLS: `ssl=True` uses platform CA verification; pass an `ssl.SSLContext`
  for custom trust. SCRAM-SHA-256 and cleartext auth are supported.

## Non-goals

- **Not a queue.** No durability, no competing consumers, no retries. If a
  message must be processed, use a job queue
  (e.g. [pgqueuer](https://github.com/janbjorge/pgqueuer)) — pgwake is its
  broadcast-shaped sibling: pgqueuer moves *work*, pgwake moves
  *wakefulness*.
- **Not CDC.** No row images, no before/after, no replay. Refetch.
- **Not a driver.** The protocol client implements exactly what a
  logical-decoding consumer needs: startup, auth, simple query, CopyBoth.

## Roadmap

- Native `pgoutput` parsing (drops the wal2json server-plugin requirement;
  publications permitting).
- Opt-in `schema.table:pk` payloads for sharper client-side routing.
- The bridge daemon as a first-class artifact — same feed contract, one
  slot fanned out over NOTIFY; a native (Zig) implementation is the
  intended long-term core.

## Lineage

Designed out of a real problem — a warehouse-monitoring TUI refetching its
world every two seconds — via a research arc through sentinel polling,
triggers (rejected: persistent), and the discovery that the temporary
replication slot is the *only* connection-scoped change-feed primitive
PostgreSQL has. So pgwake learned to speak walsender.

MIT licensed.
