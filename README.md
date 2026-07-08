# pgnudge

**Your database moved. Your app already knows.**

[![CI](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgnudge/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgnudge)](https://pypi.org/project/pgnudge/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://pypi.org/project/pgnudge/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

pgnudge is a tiny async library that tells you **which tables just changed** in
PostgreSQL, so a live read model can re-render the instant the data moves. It
leaves **nothing behind on the server** - no triggers, no functions, no
persistent slots, no cleanup jobs. Close the connection and PostgreSQL forgets
pgnudge ever existed.

It carries no row data, by design. You already know how to load your data;
pgnudge just tells you *when*, and *what to reload*.

```python
from pgnudge import Batch, Resync, WalFeed

async with WalFeed(host="db", user="wal_user", password=..., database="app",
                   tables=["public.orders", "public.stations"]) as feed:
    async for item in feed:
        match item:
            case Resync():           # (re)connected: reload everything
                await reload_everything()
            case Batch(events=evs):  # coalesced wakeups: which tables moved
                await reload(tables={e.payload for e in evs})
```

```bash
pip install pgnudge
```

Python >= 3.11, PostgreSQL >= 16. One runtime dependency:
[scramp](https://github.com/tlocke/scramp) (pure-Python SCRAM auth). **No
database driver** - pgnudge speaks the PostgreSQL replication protocol itself.

---

## Why pgnudge

- **Zero server footprint.** The only server object is a *temporary*
  replication slot, dropped automatically the instant the session ends -
  clean close, crash, `kill -9`, or `pg_terminate_backend`. `RawFeed` needs no
  slot at all. Nothing to install, migrate, review, or clean up.
- **Driver-free, one dependency.** A hand-rolled walsender client (TLS,
  SCRAM-SHA-256, CopyBoth) instead of a database driver. `pip install pgnudge`
  pulls in scramp and nothing else.
- **Two transports, one contract.** `WalFeed` (logical decoding) if you can set
  `wal_level = logical`; `RawFeed` (physical WAL, decoded client-side) on a
  stock `wal_level = replica`. Same `Resync | Batch` stream either way - the
  choice is one constructor and touches nothing else.
- **Coalesced wakeups.** A 500-row transaction on one table is one `Event`,
  `count=500`, one wakeup, one refetch. Debounced client-side.
- **Correct by construction.** At-least-once wakeups from the point of connect;
  every gap is bracketed by a `Resync`. Handle `Resync` and nothing can make
  your view wrong. No cursors to persist, no exactly-once to get wrong.
- **async-first and typed.** `async for item in feed`. Strict mypy, 100%
  line+branch coverage, claims proven against real PostgreSQL in CI.
- **Preflight `doctor`.** One command connects, fingerprints the platform
  (RDS/Aurora, Azure, Cloud SQL, or self-managed), and tells you which feed to
  use - with a copy-paste fix under every failed check, tailored to that
  platform. Leaves nothing behind.

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

## Get started

You make exactly one decision: which feed class. It is driven by a single
server setting, `wal_level`.

- **You can set `wal_level = logical`** -> use `WalFeed`. The fuller transport:
  `TRUNCATE` nudges too, the server filters tables for you, and only your
  database's WAL is decoded. Costs a one-time restart on most servers, plus an
  output plugin (`wal2json`, preinstalled on most managed platforms, or
  `test_decoding`, built into PostgreSQL).
- **You are stuck at the stock `wal_level = replica`** -> use `RawFeed`. No
  server change at all: it decodes physical WAL client-side. The trade: no
  `TRUNCATE` detection, and the server streams the whole cluster's WAL for
  pgnudge to filter locally. `RawFeed` is best treated as a
  **self-hosted / VM-Postgres** transport: it needs external physical WAL
  streaming, which managed platforms are not known to expose (untested; see
  [Managed platforms](#managed-platforms)). On a managed service, flip
  `wal_level = logical` and use `WalFeed`.

If you get to choose, choose `WalFeed`. Neither is more "correct"; they are the
same contract over two different server capabilities.

Either way you need a role with the `REPLICATION` attribute and a **direct**
connection (replication traffic cannot go through a pooler like PgBouncer).
Then:

1. `pip install pgnudge`
2. `pgnudge doctor --host ... --user ... --database ...` - it connects, checks
   `wal_level`, the `REPLICATION` grant, and the output plugin, then tells you
   which feed to use, printing a copy-paste fix under any failed check (tuned
   to the detected platform: RDS parameter groups, `az` commands, or plain
   `ALTER SYSTEM`). If `wal2json` is absent it retries with the built-in
   `test_decoding` so it can distinguish "logical decoding is blocked" from
   "logical works, the plugin just is not installed". The WalFeed check
   creates a temporary slot and drops it, so `doctor` leaves nothing behind.
3. point that feed at the database (the tour below)
4. handle the two items: `Resync` -> reload everything, `Batch` -> reload the
   named tables

That is the whole setup. Nothing is installed in the database and nothing is
left behind when the connection closes.

## Sixty-second tour

```python
from pgnudge import Batch, Resync, WalFeed

async with WalFeed(
    host="db.example.com", user="wal_user", password=...,
    database="app", ssl=True,
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

## The contract

A feed yields exactly two item types:

- **`Resync(reason)`**: reload everything. Emitted on every connect and
  reconnect, on internal queue overflow, and (optionally) on a failsafe
  interval. Handle `Resync` correctly and nothing can make your view wrong.
- **`Batch(events)`**: one debounce window's worth of wakeups, deduplicated, in
  arrival order. Each `Event` carries `payload` (`schema.table`, the stable v1
  payload contract), `first_seen`, `count`.

**Delivery is at-least-once wakeups, from the point of connect only.** Events
are hints to refetch, never facts to apply. There is no history and no backfill,
by design and by mechanism: the slot is created fresh at every (re)connect with
`SNAPSHOT 'nothing'`, and a logical slot can only decode forward from its
creation point. The handshake is gap-free. `Resync` is emitted only after the
stream is live, so the refetch it triggers observes a state at or after the
slot's start point, and every later commit produces a nudge; anything landing in
between is covered twice, which at-least-once absorbs. On reconnect `WalFeed`
resyncs rather than resumes. No replay, no exactly-once, no row images:
refetching is idempotent and you have a database right there. (One nuance: slot
creation waits for write transactions in flight at connect time, so a
long-running write delays connect, but it never causes history to be delivered.)

**Coalescing:** per-row changes within the debounce window collapse client-side
into one `Event` with a `count`. A 500-row transaction on one table is one
`Event`, `count=500`, one wakeup, one refetch.

`INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` all nudge on `WalFeed` (`RawFeed`
covers all but `TRUNCATE`). Neither transport carries other DDL, so schema
changes don't nudge; pair migrations with a refetch if your view depends on
them.

## The zero-footprint guarantee

`WalFeed` creates **nothing on the server that outlives the connection.**

```
your app ──── async for item in feed ────▶ Resync | Batch
  ▲
  │  walsender protocol, no driver (TLS, SCRAM-SHA-256, CopyBoth)
  │
PostgreSQL ── TEMPORARY replication slot ── logical decoding
              └── dropped by the server the instant the session ends,
                  cleanly or not
```

The temporary replication slot is the only primitive in PostgreSQL that gives
you a change feed with connection-scoped lifetime: the server is contractually
obliged to drop it the moment the session ends, whether by clean close, crash,
`kill -9`, or `pg_terminate_backend`. No triggers, no functions, no persistent
slots, no cleanup jobs. The test suite ends by hard-aborting the socket with no
protocol goodbye and asserting `pg_replication_slots` is empty.

What is required is PostgreSQL 16+ and one-time server **configuration**
(settings, not objects): `wal_level = logical`, a role with `REPLICATION`, and
an output plugin. That plugin is `wal2json` (default; preinstalled on Azure
Flexible Server, RDS, and most managed platforms) or `test_decoding` (ships
inside PostgreSQL itself).

The full mechanics (logical decoding, temporary-slot semantics, the gap-free
handshake argument, and when not to use pgnudge) are in
[docs/temporary-slots.md](docs/temporary-slots.md).

## Two transports, one contract

Both feeds yield the same `Resync | Batch` stream; pick by what your server
allows.

|                   | `WalFeed` (logical decoding)       | `RawFeed` (physical WAL)                 |
|-------------------|------------------------------------|------------------------------------------|
| `wal_level`       | `logical` (usually needs a restart)| `replica`, the stock default             |
| Output plugin     | wal2json or test_decoding          | none; WAL is decoded client-side         |
| Server objects    | one TEMPORARY slot while connected | none at any point, not even a slot       |
| `TRUNCATE` nudges | yes                                | no (documented gap)                      |
| Stream scope      | one database, filtered server-side | whole cluster, filtered client-side      |

`RawFeed` exists for **self-hosted** servers where `wal_level=logical` is not on
the table: change-averse ops, no restart window, or a policy against logical
decoding. It needs external physical WAL streaming, which managed platforms are
not known to expose (untested; see [Managed platforms](#managed-platforms)), so
on a managed service you use `WalFeed`. It streams raw physical
WAL **slot-less** and parses record headers client-side, just enough to answer
*which relation changed*, never row contents. Nudges are commit-gated: a change
is delivered only after its transaction's commit record, so rollbacks never
nudge and a refetch never races an open transaction. The start position is the
server's current WAL insert point, so from-connect-only holds exactly as it does
for `WalFeed`.

```python
from pgnudge import RawFeed

feed = RawFeed(
    host="db.example.com", user="wal_user", password=..., database="app",
    tables=["public.orders", "public.stations"],  # client-side filter
)
```

Costs, stated honestly: the server sends the whole cluster's WAL to the client
(every database, index churn, vacuum traffic); pgnudge filters client-side, but
the bandwidth is paid. `TRUNCATE` is not detected at `wal_level=replica` (the
WAL carries no reliable signature for it; the next write to the table nudges
normally). `RawFeed` also opens a second, plain connection for catalog lookups
(relfilenode to table name), and `pg_hba.conf` needs a `replication` entry for
the role, because physical replication matches the `replication`
pseudo-database, not `all`. Mechanics in [docs/physical-wal.md](docs/physical-wal.md);
the byte layouts and parser structures behind both transports are in
[docs/parsing.md](docs/parsing.md).

## Managed platforms

Short version: each platform below documents a `WalFeed` path - flip
`wal_level = logical` and grant a REPLICATION-capable role. Whether `RawFeed`
works (it needs external `START_REPLICATION PHYSICAL` to a non-managed standby)
is **untested** on most of them, and mostly undocumented; pgnudge makes no claim
either way. The one confirmed data point is **Azure Flexible Server, which
blocks it** (see below). If you confirm it works, or that a platform blocks it,
open an issue and this table gets updated.

The `WalFeed` column reflects each vendor's own documentation (linked below).
pgnudge has **not** been integration-tested against any of these services;
verify against your plan and region, and let `pgnudge doctor` confirm the live
handshake.

| Platform                | `WalFeed` (documented) | `RawFeed` | Enable `wal_level = logical`                                  |
|-------------------------|------------------------|-----------|--------------------------------------------------------------|
| AWS RDS PostgreSQL      | yes                    | untested  | `rds.logical_replication=1`, grant `rds_replication`         |
| AWS Aurora PostgreSQL   | yes                    | untested  | `rds.logical_replication=1` (cluster parameter group)        |
| Google Cloud SQL        | yes                    | untested  | flag `cloudsql.logical_decoding=on`, user `WITH REPLICATION` |
| Azure Flexible Server   | yes                    | **no**    | `wal_level=logical`, `ALTER ROLE ... WITH REPLICATION`       |
| Supabase                | yes\*                  | untested  | role `WITH REPLICATION`; **direct** connection only          |
| Neon                    | yes\*                  | untested  | enabling logical repl flips `wal_level` project-wide         |

`\*` Supabase and Neon require a **direct** connection, not their pooler
(Supavisor / PgBouncer) - the same rule pgnudge already states for any pooler.
The `RawFeed` column is left **untested**: these vendors document logical
decoding as the external replication path and do not document an external
physical-streaming endpoint, but we have neither confirmed nor ruled one out.
Reports welcome.

Caveats worth a pre-flight `pgnudge doctor`:
- **RDS / Aurora:** `rds_replication` grants logical-slot access but does not
  carry the raw `REPLICATION` role attribute; confirm the temporary-slot
  `START_REPLICATION` path.
- **Azure Flexible Server:** external `START_REPLICATION PHYSICAL` is blocked
  (`28000: no pg_hba.conf entry for replication connection`), confirmed live
  via `pgnudge doctor`. `RawFeed` is unavailable; use `WalFeed`. Enabling
  `wal_level=logical` needs a server restart, and the login role needs the
  `REPLICATION` attribute (grant it as `azure_pg_admin`).
- **Neon:** enabling logical replication changes `wal_level` for the whole
  project and cannot be undone.
- **Output plugin:** `wal2json` is common but not universal; `test_decoding`
  ships with core PostgreSQL and is the zero-install fallback.

Sources (vendor docs): [RDS logical replication][rds-lr], [RDS/Aurora to
self-managed][rds-selfmanaged], [Aurora logical replication][aurora-lr],
[Cloud SQL logical replication][gcp-lr], [Cloud SQL external server][gcp-ext],
[Azure logical][azure-lr], [Supabase external replication][supa-lr],
[Neon logical replication][neon-lr], [Neon connection pooling][neon-pool].

[rds-lr]: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html
[rds-selfmanaged]: https://aws.amazon.com/blogs/database/using-logical-replication-to-replicate-managed-amazon-rds-for-postgresql-and-amazon-aurora-to-self-managed-postgresql/
[aurora-lr]: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.Configure.html
[gcp-lr]: https://docs.cloud.google.com/sql/docs/postgres/replication/configure-logical-replication
[gcp-ext]: https://docs.cloud.google.com/sql/docs/postgres/replication/external-server
[azure-lr]: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical
[supa-lr]: https://supabase.com/docs/guides/database/postgres/setup-replication-external
[neon-lr]: https://neon.com/docs/guides/logical-replication-neon
[neon-pool]: https://neon.com/docs/connect/connection-pooling

## Why not LISTEN/NOTIFY?

`NOTIFY` doesn't fire itself: making it track data changes means triggers, and
triggers are persistent catalog objects. Schema footprint, migration reviews,
cleanup jobs, drift. pgnudge's whole premise is refusing that trade. Logical
decoding gets the same wakeups straight from the WAL with zero objects. (LISTEN
is still great on the *consuming* side; see Fan-out.)

## Fan-out

One `WalFeed` per process is the normal shape. For many consumers, run one
`WalFeed` in a small bridge daemon that republishes to a NOTIFY channel via
`pg_notify`, and let consumers attach with plain LISTEN (any driver; LISTEN is
session state, zero objects). One REPLICATION grant total, one decoding pass
total, and still zero persistent server objects: the bridge's temp slot dies
with the bridge.

## Ops notes

- `status_interval` (default 10 s) must stay under the server's
  `wal_sender_timeout` (default 60 s); the feed also answers reply-requested
  keepalives immediately.
- `liveness_timeout` (default 30 s, must exceed `status_interval`, enforced at
  construction; `None` disables): each status report asks the server to answer
  with a keepalive, so a healthy connection always has inbound traffic. Silence
  longer than the timeout means a dead link (NAT drop, yanked VPN, hung
  walsender) and the feed aborts and reconnects instead of blocking forever.
- While connected, each `WalFeed` holds one replication slot and one WAL sender
  against `max_replication_slots` / `max_wal_senders`. Disconnected feeds hold
  nothing (that's the point), which also means an idle feed never retains WAL.
- Managed platforms: enabling `wal_level=logical` typically requires a restart
  (once); grant `REPLICATION` to a dedicated role rather than widening an app
  role, since logical decoding sees the whole database's stream. On managed
  services, plan on that restart: whether `RawFeed` can serve as a no-restart
  alternative there is untested (external physical streaming is not known to be
  exposed; see [Managed platforms](#managed-platforms)).
- A role with `REPLICATION` sees every table's changes through either transport
  regardless of its SELECT grants; table grants do not scope a change feed.
  Scope with `tables=` and treat the role as privileged.
- Physical replication (`RawFeed`) needs a `pg_hba.conf` entry for the
  `replication` pseudo-database (`host replication <role> ...`); the usual
  `host all` rules do not match it. This is a self-hosted concern; whether
  managed platforms expose external physical streaming at all is untested (see
  [Managed platforms](#managed-platforms)).
- Thundering herd: a database restart reconnects every feed at once, and every
  consumer's `Resync` handler refetches at once. Reconnect timing is already
  jittered, but the refetch is your code. Add jitter there when many consumers
  share a database, or fan out through the bridge daemon so a single process
  refetches per change.
- TLS: `ssl=True` uses platform CA verification; pass an `ssl.SSLContext` for
  custom trust. SCRAM-SHA-256 is supported everywhere; cleartext auth only over
  TLS. pgnudge refuses to send a password on an unencrypted connection.
- Logging: the `pgnudge.wal` logger (stdlib `logging`, no handlers configured by
  the library) reports connect failures and stream errors at WARNING, successful
  (re)connects at INFO, and backoff timing at DEBUG. A feed that reconnects in a
  loop is visible, not silent.
- Errors: every exception pgnudge raises inherits `PgnudgeError`, so
  `except PgnudgeError` catches them all. `ConfigError` (bad constructor
  argument) also inherits `ValueError`, so an existing `except ValueError` keeps
  working. Stream and connection failures are internal lifecycle: the supervisor
  catches them, backs off, and reconnects with a `Resync`; they do not surface
  on the iterator.

## Tested how

The suite spins up real PostgreSQL via testcontainers (nothing to install beyond
Docker) and proves the claims live: no backfill of pre-connect writes,
client-side coalescing (50-row txn -> one `Event`, `count=50`), reconnect gets a
fresh slot with the old one auto-dropped, TLS + SCRAM over an encrypted stream,
and the flagship proof: hard socket abort with no protocol goodbye leaves
`pg_replication_slots` empty.

`RawFeed` gets its own proofs: `pg_replication_slots` stays empty *while
streaming*, an open transaction never nudges until COMMIT and a rollback never
nudges at all, VACUUM and CHECKPOINT stay silent, writes in other databases stay
silent, and an end-to-end run on an untouched `wal_level=replica` container. The
decoder itself is checked against an oracle: the same live WAL range through our
client-side walker and through `pg_waldump` must produce the identical change
sequence, on every PostgreSQL major in CI.

```bash
uv sync && uv run pytest
```

## Non-goals

- **Not a queue.** No durability, no competing consumers, no retries. If a
  message must be processed, use a job queue (e.g.
  [pgqueuer](https://github.com/janbjorge/pgqueuer)). pgnudge is its
  broadcast-shaped sibling: pgqueuer moves work, pgnudge moves wakefulness.
- **Not CDC.** No row images, no before/after, no replay. Refetch.
- **Not a driver.** The protocol client implements exactly what a
  logical-decoding consumer needs: startup, auth, simple query, CopyBoth.

## Roadmap

- Native `pgoutput` parsing would drop the wal2json server-plugin requirement,
  but pgoutput only decodes through a *publication*, and a publication is a
  persistent catalog object, in direct tension with the
  nothing-outlives-the-connection guarantee. Conditional at best: viable only if
  a pre-existing, application-owned publication counts as configuration rather
  than footprint.
- Opt-in `schema.table:pk` payloads for sharper client-side routing.
- The bridge daemon as a first-class artifact: same feed contract, one slot
  fanned out over NOTIFY; a native (Zig) implementation is the intended
  long-term core.

MIT licensed.
