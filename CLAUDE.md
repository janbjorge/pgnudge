# CLAUDE.md — project handoff

This file is the primary context for working on this codebase. Read it fully
before changing anything. The design history lives in rekal memory (owner
removed `docs/` from the public repo 2026-07-03) — search it before
re-opening any decision marked settled here.

## What this is

A Python framework for **push-only change nudges from PostgreSQL with zero
server footprint**. Consumers get an async iterator of two item types —
`Resync` (reload everything) and `Batch` (coalesced wakeups saying *which
tables moved*) — and refetch their own data. The framework never carries
application data.

Named `pgwake` (v1.0.0): *pgqueuer moves work, pgwake moves wakefulness.*

Owner style rules (2026-07-03): no underscore-prefixed module or class
names, no module-level globals (constants live as class attributes; test
config lives inside functions). Python-mandated protocol dunders
(`__init__.py`, `__aenter__`, `__version__`, …) are exempt. Plain-class
instance attributes and helper functions keep the single-underscore
convention, but dataclass fields are NEVER underscore-prefixed
(owner, 2026-07-03) — internal state uses `field(init=False)` with a
plain name instead.
Python ≥ 3.13, modern syntax where it fits (PEP 695 `type` aliases and
generics, `Self`, `collections.abc` imports, no `from __future__`).
Strictest mypy: `strict` + `disallow_any_explicit` — no `Any` anywhere.
Docstrings are PEP 257, terse; design prose lives in README/docs, not code.

## The five laws (non-negotiable, owner-imposed)

These were each fought for across a long design process. Do not trade any of
them away, even for apparently good reasons. If a task seems to require
violating one, stop and ask the owner.

1. **Push only.** No polling of any kind — no `pg_logical_slot_get_changes`,
   no periodic SELECTs, no sentinel queries. The only timers permitted:
   reconnect backoff, standby-status feedback (a protocol keepalive on an
   open stream, not a data poll), and the *user-opt-in* `failsafe` knob
   (default off).
2. **Native Postgres only.** No external CDC systems, brokers, or sidecars
   beyond an optional bridge daemon that is itself just this library.
3. **Zero server persistence.** Nothing created on the server may outlive
   the connection. The only primitive in all of PostgreSQL that satisfies
   this for a change feed is the **temporary replication slot** ("not saved
   to disk, automatically dropped on error or when the session has
   finished"). Triggers were explicitly vetoed by the owner: persistent
   catalog objects fail this law by definition. `ddl.py` (a trigger
   generator) was the boxed exception; the owner had it deleted 2026-07-03.
   Do not reintroduce a trigger path.
4. **Driver-free.** The replication transport is a hand-rolled walsender
   client (`proto.py`, stdlib + scramp), because no Python library outside
   psycopg2 speaks the replication protocol (asyncpg issue #91 has been
   open since 2017) and psycopg2 is vetoed. Runtime dependency: scramp
   only. asyncpg exists solely in the dev group for the live test's admin
   connection.
5. **From-connect-only.** No history, no backfill, no replay. Slots are
   created fresh at every (re)connect with `SNAPSHOT 'nothing'`; reconnect
   *resyncs*, never resumes. Missing changes while disconnected is a
   requirement, not a bug — the `Resync` item brackets every gap and the
   consumer's refetch is the source of truth.

## Things you will be tempted to do — don't

- **Add a graceful `DROP_REPLICATION_SLOT` or protocol goodbye on close.**
  `WalFeed._extra_close()` hard-aborts the socket *on purpose*: crash and
  clean exit must exercise the identical server-side cleanup path, and the
  test suite depends on proving that. Cleanup is the server's job.
- **Persist the slot / store an LSN cursor to "not miss events".** Violates
  laws 3 and 5. Resync-plus-refetch *is* the reliability model.
- **Wrap protocol reads in `asyncio.wait_for`.** Cancelling a read between
  frame header and body desyncs the stream. Feedback runs on its own timer
  task precisely so reads can block freely. Keep it that way.
- **Put row data in payloads by default.** The v1 payload contract is
  `schema.table`, and coalescing depends on payload identity. A `:pk`
  payload is a planned *opt-in* (see Backlog).
- **Switch to pgoutput to drop the wal2json dependency.** Careful: pgoutput
  requires `publication_names`, and a **publication is a persistent catalog
  object** — it likely violates law 3. This roadmap item is conditional at
  best; raise it with the owner before starting (a pre-existing
  app-owned publication *might* be acceptable as "config", might not).
- **Reintroduce a trigger-based emitter.** `ddl.py` existed once and was
  deleted on owner instruction (2026-07-03). Persistent catalog objects
  violate law 3; fan-out goes through the bridge daemon.

## Architecture map

```
src/pgwake/
  proto.py    WalsenderConnection: the protocol client (~260 lines, stdlib
              asyncio + scramp only). Startup with replication=database,
              optional TLS via StreamWriter.start_tls, auth = trust /
              cleartext / SCRAM-SHA-256 (-PLUS mechanisms filtered out; no
              channel binding), simple_query (walsender mode speaks ONLY the
              simple query subprotocol), start_replication -> CopyBoth,
              read_stream() -> XLogData | Keepalive, send_standby_status(),
              abort() = deliberate hard close.
  core.py     The contract only: Event/Batch/Resync dataclasses + FeedItem.
  engine.py   The machinery, one dataclass per concern, pure stdlib:
              Intake (bounded wakeup buffer, payload_filter, overflow flag),
              Coalescer (dedup buffer, count per (channel, payload)),
              Debouncer (rolling window, hard max_batch_wait cap; overflow
              -> Resync("overflow")), Backoff (jittered exponential),
              FeedService (wires intake -> debouncer -> out queue, owns
              tasks/failsafe/shutdown), BaseFeed (thin async-iterator
              facade; subclass hooks _supervisor/_extra_close). Unit-tested
              without PostgreSQL in tests/test_engine.py.
  wal.py      WalFeed(BaseFeed): the transport. Supervisor creates a fresh
              TEMPORARY slot per (re)connect (name: pgwake_<pid>_<hex>),
              SNAPSHOT 'nothing', starts replication at 0/0, emits Resync
              only once the stream is live (this ordering is the gap-free
              handshake argument — see README), parses wal2json
              format-version 2 (default) or test_decoding (zero-install
              fallback), feedback task every status_interval (default 10s,
              must stay under wal_sender_timeout, default 60s) plus
              immediate reply on keepalive reply-requested.
tests/
  conftest.py  session-scoped PostgreSQL via testcontainers (pgqueuer
               pattern), scratch database per test, best-effort TLS enable
               (self-signed cert) inside the container. Env knobs:
               EXTERNAL_POSTGRES_DSN, POSTGRES_IMAGE, PGWAKE_PLUGIN,
               PGWAKE_TLS.
  test_engine.py  unit tests for the engine classes (Intake, Coalescer,
               Debouncer, Backoff, FeedService, BaseFeed via a FakeFeed) —
               pure asyncio, no PostgreSQL.
  test_wal.py  7 tests incl. the two proofs: no backfill of pre-connect
               writes, and hard abort -> pg_replication_slots EMPTY.
```

There was a second transport, `ChangeFeed` (NOTIFY consumer on asyncpg).
The owner had it removed 2026-07-03 along with the trigger module: pgwake
is WalFeed-only. Fan-out is a bridge daemon republishing via `pg_notify`;
consumers LISTEN with whatever driver they already have. Do not reintroduce
a NOTIFY consumer class.

The consumer contract, coalescing semantics, and ops notes are in
`README.md` — treat it as the spec. The full decision history (why not
sentinel polling, why not triggers, why not Go/Rust/psycopg2, the Zig
track) lived in `docs/design-history.md` and `docs/research-notifications.md`,
removed from the repo 2026-07-03 (owner wanted no origin-story/internal
context public). Settled decisions are summarized in the five laws above;
ask the owner before re-litigating any of them.

## Dev environment

uv-based; the test suite owns its own PostgreSQL via testcontainers —
nothing to install beyond Docker:
```bash
uv sync                              # .venv + uv.lock, editable + dev group
uv run pytest                        # spins postgres:17, runs the proofs
uv run ruff check . && uv run mypy   # both must stay clean
uv build
```
Against an external server instead:
`EXTERNAL_POSTGRES_DSN=postgresql://... uv run pytest` (role needs
CREATEDB + REPLICATION; add `PGWAKE_TLS=1` if it has TLS). Default plugin
is `test_decoding` (zero-install); `PGWAKE_PLUGIN=wal2json` needs an image
that ships it (`POSTGRES_IMAGE=debezium/postgres:16`).

Debugging trick from the PG docs: a replication connection can be tested
with nothing but psql —
`psql "dbname=x replication=database user=y" -c "IDENTIFY_SYSTEM;"`

## Proven vs. untested (be honest in docs and commits)

Proven live (PG 16 with wal2json, PG 17 with test_decoding via the pytest
suite): SCRAM-SHA-256 auth; TLS handshake + SCRAM over TLS (self-signed
cert, CERT_NONE context); temporary slot is the only server object while
connected; no backfill; client-side coalescing (50-row txn -> one Event,
count=50); kill -> fresh slot, old auto-dropped; hard abort ->
`pg_replication_slots` empty; wheel installs and passes from site-packages
(pre-1.0 layout — re-verify from wheel before release).

Untested / absent: `ssl=True` verify-full against a real managed endpoint
(Azure Flexible Server is the owner's target — replication bypasses the
built-in PgBouncer there, use direct 5432); MD5 auth (deliberately absent);
pgoutput (see the publications warning above); behavior under a
long-running write transaction at connect (slot creation waits — connect
latency, never history); PG 14/15 (in the CI matrix, not yet run); the CI
workflow itself.

## Backlog, priority order

0. **Rename — DONE** (`pgnudge` → `pgwake`, 2026-07-03, owner's choice).
   Runners-up preserved in case of a future collision, PyPI-verified free
   at the time: `waltail`, `pgripple`, `pgnudge`, `pgwisp`, `pgghost`,
   `pgfollow`, `pgblip`, `pgwhisper`, `pgpulse`, `pgbeacon`, `pgfeed`
   (`pgtail`, `pgflux`, `pglive` were taken). The PyPI name `pgwake` is
   **not yet registered** — claiming it with the first release is the real
   Task 0 remainder.
1. ~~pytest-ify~~ — DONE 2026-07-03: testcontainers session fixture,
   scratch DB per test, EXTERNAL_POSTGRES_DSN escape hatch.
2. ~~CI~~ — DONE 2026-07-03: `.github/workflows/ci.yml`, Python 3.13/3.14 ×
   PG 14–17 (test_decoding) + one wal2json job on debezium/postgres:16.
   Repo pushed to github.com/janbjorge/pgwake 2026-07-03 — check Actions
   for the first real runs.
3. **Bridge daemon** as a first-class example or subpackage: one WalFeed ->
   `pg_notify` on a channel -> plain LISTEN consumers. Zero persistent
   objects end to end (the bridge's slot dies with the bridge).
4. **Opt-in payload v2** (`schema.table:pk`) — wal2json carries pk columns;
   keep v1 the default, document the coalescing trade (dedup granularity
   changes).
5. Azure end-to-end validation (verify-full TLS, direct 5432, wal2json
   preinstalled) — coordinate with owner, needs real infra.
6. ~~Typing/lint hardening~~ — DONE 2026-07-03: mypy --strict and ruff
   configured in pyproject, both clean. Keep them clean. Docs polish remains.

## Release checklist

Bump `__version__` and `pyproject.toml` together; `uv build`; run the full
suite from the installed wheel (not the source tree) against a live server;
tag; `uv publish`. Never release with the live suite skipped — the
zero-footprint proof (`test_hard_abort_leaves_no_slots`) is the product.

## Owner context

Python/asyncio shop; the owner maintains pgqueuer (job queue on
LISTEN/NOTIFY) — this library is its broadcast-shaped sibling: *pgqueuer
moves work, this moves wakefulness*. Target deployment: Azure Database for
PostgreSQL Flexible Server, PG 16+, schema `versaai`, read-only consumers.
A native Zig implementation of the walsender core is a someday-ambition
("the Zig track") — design decisions here should not foreclose it: the
feed contract and payload contract are the stable API, transports are
pluggable.
