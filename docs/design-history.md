# Push-native change feed: design

**Status:** design, follow-up to "Notification-based refresh: connection-scoped change feeds, researched" (rev 2). Constraint applied: **native Postgres, push only.** Baseline PostgreSQL 16+. Rev 2: transport reworked **psycopg-free**. Rev 3: **Zig track** added. Rev 4: v1 pivoted to trigger-emitted NOTIFY (superseded). Rev 5: framework extracted as `pgwake`. Rev 6: **trigger emitter vetoed — nothing may persist server-side.** The constraint set {push, native, no-psycopg, nothing-persistent} forced the walsender client to be written: pgwake 0.2.0 ships `WalFeed` — a pure-asyncio walsender protocol client (TLS, SCRAM, CopyBoth, ~260 lines) streaming a **TEMPORARY slot**. Live-verified on PG 16.14: SCRAM auth, temp-slot-only server state, kill→fresh-slot reconnect, and hard-abort → `pg_replication_slots` empty. The trigger generator remains only as an explicitly-labeled persistent option outside the guarantee; rev-4's recommendation is retracted. Rev 6.1: **from-connect-only semantics made structural** — slots created with `SNAPSHOT 'nothing'` (no backfill pattern), an explicit no-history assertion added to the live suite, and the gap-free handshake argument (stream live before `Resync`, overlap absorbed by at-least-once) documented. Rev 6.2: package renamed **pgnudge → pgwake** (owner decision; runners-up preserved in CLAUDE.md); v0.3.0 rebuilt and fully re-verified live under the new name.

**Assumption (marked):** the TUI and the spin-off library are Python/asyncio — inferred from the pgqueuer work in our earlier threads, not stated in this one. If that's wrong, the protocol design below is language-agnostic and only the driver section changes.

---

## What the constraint eliminates, and what survives

Out: the sentinel (stays in the codebase as the already-shipped legacy path, but it's not the future), Mode A slot-polling from the research doc, anything stats/fingerprint shaped, and every external CDC framework (Debezium, Sequin, Supabase Realtime, etc. — non-native by definition). Two architectures survive, and they're the same machinery at different radii:

1. **Single-process:** the TUI holds a temporary logical slot over the streaming replication protocol. Changes arrive within milliseconds of commit, pushed down an open socket.
2. **Two-tier:** one small decoder daemon holds the slot and re-broadcasts through `NOTIFY`; TUIs `LISTEN`. Both hops are push. Worth stating plainly because some client APIs make LISTEN *look* like polling: at the protocol level notifications are asynchronous messages on the socket, and the good Python APIs surface them that way — `asyncpg.Connection.add_listener()` fires a callback, psycopg 3's `conn.notifies()` is an async iterator. No query loop anywhere. (This is the same machinery pgqueuer already rides.)

One honesty checkpoint on "native" before going further, because it decides the decoder plugin. **wal2json** is not in-core — it's a third-party plugin that Azure ships preinstalled as a supported option. **pgoutput** is the in-core plugin, but it demands a publication: a persistent catalog object requiring `CREATE` on the database and table ownership to populate, plus binary protocol parsing on the client. My read of "native" is "nothing installed, nothing bolted on" — wal2json satisfies that on Azure (it's already there) and costs zero DDL, so it's the recommendation. If "native" means "in-core only," the pgoutput path is viable: a publication *without* row filters avoids the replica-identity trap from the research doc entirely (the picks tables have PKs, so `REPLICA IDENTITY DEFAULT` suffices), and the price is one migration-ish `CREATE PUBLICATION` plus a binary parser (`pypgoutput` exists, or ~300 lines by hand). Your call; everything below works with either, I'll write it as wal2json.

---

## Protocol anatomy — what the framework wraps

This is the full wire-level contract; it's small.

**Connect** with `replication=database dbname=versaai user=pio_cdc ...`, direct to 5432 (the replication protocol doesn't traverse the built-in PgBouncer). Since PG10, a logical walsender connection in database mode also accepts plain SQL, which turns out to matter below.

**Create the slot:**

```
CREATE_REPLICATION_SLOT pio_tui_<uniq> TEMPORARY LOGICAL wal2json
```

Returns `slot_name, consistent_point, snapshot_name, output_plugin`. Per the [streaming replication protocol docs](https://www.postgresql.org/docs/16/protocol-replication.html): temporary slots are not saved to disk and are automatically dropped on error or when the session has finished — the exact "registration that dies with the connection" semantics this whole exercise is about, now on the true-push interface.

**Start streaming:**

```
START_REPLICATION SLOT pio_tui_<uniq> LOGICAL 0/0
    ('format-version' '2',
     'add-tables' 'versaai.versaai_picks,versaai.versaai_sessions,versaai.stations',
     'actions' 'insert,update,delete',
     'include-transaction' 'false')
```

The server replies `CopyBothResponse` and starts pushing. Streaming begins at the greater of the requested LSN and the slot's `confirmed_flush_lsn` ([docs](https://www.postgresql.org/docs/16/protocol-replication.html)) — with `0/0` on a freshly created temp slot that's the consistent point, i.e. "from now," which is precisely the contract we want.

**Inside the CopyBoth stream**, three message types matter:

- `XLogData` (`'w'`): carries one chunk of plugin output — with wal2json v2, one JSON object per tuple.
- Primary keepalive (`'k'`): server heartbeat with a reply-requested flag.
- Standby status update (`'r'`, client → server): the feedback message carrying written/flushed/applied LSNs. Send it on a deadline (~10 s) and immediately whenever the keepalive requests a reply; go quiet past `wal_sender_timeout` (default 60 s) and the walsender terminates the connection. This timer is protocol keepalive on an open socket, not database polling — no query is issued, ever.

**Acknowledgement semantics get to be trivially lazy**, and this is worth savoring: because the consumer's action is "refetch the window" (idempotent) and the slot is temporary (every reconnect is from-now by construction), the delivery contract is *at-least-once wakeups*. Report flushed = last-processed LSN and move on. All the classic CDC pain — exactly-once bookkeeping, LSN checkpointing across restarts, the known wal2json/psycopg2 flush-position quirks — is out of scope by design, because we never resume; we resync.

**Lifecycle state machine** (the whole thing):

```
CONNECT → CREATE TEMP SLOT → emit Resync → STREAM ──(any error)──▶ BACKOFF(jitter) → CONNECT
                                              │
                              decode → predicate → debounce → emit Batch
```

Every distinct failure from the research doc — network drop, server restart, HA failover, standby-slot invalidation if running against the read replica — collapses into the same single edge: back to CONNECT, and the consumer's `Resync` handler (full window fetch) makes it whole. Ordering on connect stays slot-before-fetch so nothing falls in the gap; anything that lands in both the fetch and the stream is absorbed by idempotence.

---

## The spin-off: a small framework, pgqueuer's sibling

The shape that keeps this a *framework* rather than app code: **the library never touches application data.** It owns connection, slot, stream, decode, predicate, debounce — and emits exactly two event types. What to do about them belongs to the consumer.

```python
feed = ChangeFeed(
    dsn="postgresql://pio_cdc@host:5432/versaai",
    tables=["versaai.versaai_picks", "versaai.versaai_sessions", "versaai.stations"],
    actions={"insert", "update", "delete"},
    predicate=lambda c: c.table != "versaai_picks"
                        or c.new["station_id"] in watched_stations,
    debounce=0.05,           # coalesce a session's burst of pick updates
    plugin="wal2json",       # or "pgoutput" + publication name
)

async for event in feed:
    match event:
        case Resync(reason=r):   await reload_window()   # connect, reconnect, slot lost
        case Batch(changes=cs):  await reload_window()   # or patch in-memory state from cs
```

Internals as composable stages — `Stream → Decode → Predicate → Debounce → Queue` — where `Change` is `(schema, table, action, new: dict | None, key: dict | None, lsn, commit_ts)`. The replica-identity rule from the research doc becomes an API doc line: `new` is the full row for insert/update, `key` is identity columns only for update/delete, so predicates are written over `new` + action + table (which your phase-transition insight makes sufficient for picks). This exact surface reuses unchanged for any live read model — dashboards, cache invalidation, another team's TUI — which is what earns it a repo of its own. (Naming note: `pgwatch` is taken by the monitoring tool; something in the `pgfeed`/`pglive` family is free last I looked.)

**Two deployment shapes, one core.** Embedded: the TUI instantiates `ChangeFeed` directly. Bridge: a daemon instantiates the same `ChangeFeed`, and its consumer loop is ~five lines — serialize `(table, action, key)` to JSON, `SELECT pg_notify('pio_events', payload)` on a companion plain connection (payloads capped ~8 KB, so keys not rows). Consumers then use a thin `Listener` class over asyncpg `add_listener` — push, zero privileges beyond CONNECT, and functionally the code pgqueuer already contains. The bridge is where the REPLICATION grant concentrates when TUI count grows; the framework shouldn't care which shape it's running in.

### Transport: the psycopg-free options (landscape verified)

First, the map of what *doesn't* exist, so nobody relitigates it later. asyncpg has never grown replication support — [issue #91](https://github.com/MagicStack/asyncpg/issues/91) requesting it has been open since March 2017. psycopg 3 lacks it too; the workarounds in the wild are literally raw-socket bridges bolted onto it. The pure-Python odds and ends (replisome, assorted demo repos) are dead, psycopg2-based underneath, or require custom server-side plugins — doubly disqualified on Azure. pg8000 and psqlpy don't expose the walsender protocol either, to my knowledge (verify psqlpy at build time; it's young and moving). So "a Python driver that isn't psycopg and speaks replication" is an **empty set** — which reframes the question from *which driver* to *where the protocol should live*. Four real answers, ranked:

**1. Go micro-bridge + asyncpg listeners — pragmatic, ships this week.** Promote the two-tier shape from fleet-scale option to day-one architecture, purely for dependency taste. The REPLICATION-speaking piece becomes ~200 lines of Go on [pglogrepl](https://github.com/jackc/pglogrepl) — the reference-quality logical replication library, built on pgx/pgconn — doing temp slot → decode → coarse table/action filter → `pg_notify('pio_events', payload)`. Include a small configurable column projection in the payload (`table, action, key, station_id, status`) so row-level predicates stay in Python on the NOTIFY side. Python then never touches the replication protocol: the TUI and the framework's consumer half are pure asyncpg, `add_listener` push callbacks — the machinery pgqueuer already runs in production. Costs: a second language and a second deployable. The bridge's own temp slot + resync-broadcast-on-restart preserves the connection-scoped semantics; and per option 2, the Go binary is disposable by design.

**2. Own the protocol in the framework — the spin-off with teeth.** Implement the walsender client directly on asyncio inside the new library, zero database driver at all. This is smaller than it sounds, and the original psycopg2 design discussion spells out why ([psycopg2 #351](https://github.com/psycopg/psycopg2/issues/351)): once `replication=database` is in the startup packet, replication commands like `CREATE_REPLICATION_SLOT` are ordinary protocol messages — the *only* special machinery is CopyBoth streaming. Concrete scope: startup packet with the `replication` parameter; TLS via stdlib `ssl` (the SSLRequest dance — mandatory against Azure); SCRAM-SHA-256 via `scramp` (the tiny standalone pure-Python SCRAM library pg8000 uses — verify current state); then length-prefixed frames: CopyData in (`'w'` XLogData, `'k'` keepalive), CopyData out (`'r'` status update). Roughly 600–1200 lines plus tests. The payoff follows from the empty-set finding: a dependency-light asyncio logical-replication client is a genuinely vacant niche in the Python ecosystem, which makes this the version of the library actually worth publishing. And it's inherently push — `loop.add_reader` on the socket, the only timer being the ~10 s feedback heartbeat, which is protocol keepalive, not a query. Once this core exists, option 1's Go binary gets deleted and "bridge" becomes just a deployment mode of the same package.

**3. Rust core, Python surface — the ambitious cousin of 2.** Build the engine on the rust-postgres ecosystem's replication support (tokio-postgres copy-both; Supabase's `etl`, née `pg_replicate`, as prior art) and expose `ChangeFeed` through PyO3/maturin, asyncpg-style. Best performance story and a real moat; the most work; justified only if the OSS ambition outweighs the TUI deadline. Verify current crate state at build time — the Rust side of this has been moving fast.

**4. `pg_recvlogical` subprocess — native tooling, zero protocol code.** Spawn `pg_recvlogical --start -o format-version=2 ... -f -` and async-read its stdout. Honest downsides: it creates **persistent slots only** (no temporary flag), so the connection-scoped guarantee is lost and lifecycle discipline returns — paired `--drop-slot` on exit, `max_slot_wal_keep_size` mandatory, a janitor for crash orphans; plus it needs the postgres client tools installed and a supervised child process. Acceptable inside the bridge daemon; wrong shape for per-laptop TUIs.

**The Zig track — options 1 and 3 collapsed into one project.** If you've been hunting for a Zig project, this is close to an ideal one, and here's the honest case. The scope shape is right: a fully specified, versioned binary protocol (no reverse engineering), a milestone ladder where every rung is independently testable against a Docker Postgres, ~1–2k lines total, and a real production consumer waiting at the end (your own warehouse). The niche is verifiably vacant: no native Zig walsender client exists — the only Zig CDC experiment findable ([dwyl/learn-zig #35](https://github.com/dwyl/learn-zig/issues/35)) wraps **libpq** for the stream and pushes to NATS, i.e. Zig-as-glue, not a native client — so this would plausibly be *the* Zig logical-replication library. And the problem plays to the language: length-prefixed frame codecs with comptime-generated encoders, explicit allocators on a streaming hot path, a cross-compiled static binary for the bridge deploy, and a natural C-ABI export later, which turns option 3 into "Zig core, Python surface" instead of Rust. The protocol docs even shrink the scope for you: [walsender mode only speaks the simple query protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — no extended protocol, no parse/bind/describe — and logical walsender mode additionally accepts normal SQL, so the entire client is startup + auth + simple query + CopyBoth.

Three implementation strategies, descending purity, ascending pragmatism:

- **Z1 — pure Zig.** Hand-roll startup packet, SCRAM-SHA-256 (std.crypto has the primitives; plain SCRAM without channel binding is fine over TLS), and TLS. TLS is the honest risk: `std.crypto.tls` exists but note that [pg.zig chose OpenSSL for its TLS support](https://github.com/karlseguin/pg.zig) — which is telling. Mitigation: test std TLS against the actual Azure endpoint in week one, or link OpenSSL the way pg.zig does.
- **Z2 — stand on pg.zig.** [karlseguin/pg.zig](https://github.com/karlseguin/pg.zig) is an active native driver with auth, TLS, pooling, LISTEN/NOTIFY, and a full protocol-message layer already written (there's also an async fork on zio). It has no replication mode — so either lift its SCRAM/TLS code into your walsender client, or add `replication=database` + CopyBoth *to pg.zig* and upstream it. The second is the highest-leverage version of this project: your library becomes a feature of the ecosystem's main driver. Either way pg.zig serves as-is for the bridge's second connection (the `pg_notify` fan-out and any plain SQL).
- **Z3 — libpq via `@cImport`.** What the prior experiment did: libpq handles connect/TLS/SCRAM flawlessly (it *is* the reference client), Zig owns CopyBoth framing, feedback, wal2json parsing, and notify. Least glory, most production-grade on day one, and the natural fallback if Z1/Z2 stall.

Milestone ladder (each rung ships something testable):

1. **M0, zero code:** `psql "dbname=versaai replication=database" -c "IDENTIFY_SYSTEM;"` — the docs' own trick for testing replication connections; proves role, pg_hba, and TLS reachability before a line of Zig exists.
2. **M1:** TCP + startup packet with the `replication` parameter + auth → `ReadyForQuery`.
3. **M2:** simple `Query` round-trip — parse RowDescription/DataRow/CommandComplete (needed for `IDENTIFY_SYSTEM` and the slot-create result).
4. **M3:** `CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL wal2json` → read back `consistent_point`.
5. **M4:** `START_REPLICATION` → `CopyBothResponse` → decode `'w'`/`'k'` frames, print JSON to stdout.
6. **M5:** feedback writer — `'r'` frames on a ~10 s deadline and on reply-requested; survive `wal_sender_timeout` for an hour.
7. **M6:** reconnect/backoff/resync state machine; the `kill -9` test (slot gone from `pg_replication_slots`).
8. **M7:** `pg_notify` fan-out on a second connection (pg.zig as-is) — bridge complete, TUI listens via asyncpg.
9. **M8, stretch:** pgoutput binary parser (peak Zig), C-ABI export for a Python surface, upstream replication mode to pg.zig.

Honest cons, stated once: Zig is pre-1.0 and the churn tax is real — pg.zig's history includes literal ["accommodate Zig changes" commits](https://github.com/karlseguin/pg.zig/commit/4ddae09948cb1563b394cd724b95de14cc88fc12) — and std has no async story, though this is genuinely a two-thread problem (blocking reader + feedback timer), or the zio fork exists. The structural mitigation: **design the NOTIFY payload contract first.** The bridge's language then becomes swappable — if the Zig build stalls or a maintainer-bus-factor conversation happens, the Go/pglogrepl bridge drops in behind the same contract with zero changes anywhere else.

**Recommendation (rev 4):** for v1, none of the walsender tracks above — see the next section. They all remain live as the **v2 emitter swap**, with Zig as the preferred core when that itch gets scratched.

---

## The for-now path (v1): trigger-emitted NOTIFY, asyncpg listeners

Constraint algebra first. Push-only + native + psycopg-free + "lean on the existing Python ecosystem" has exactly one first-class citizen: **LISTEN/NOTIFY over asyncpg** — mature, evented, socket-push, zero psycopg anywhere. But NOTIFY needs an emitter, every WAL-based emitter routes through the walsender protocol, and Python-without-psycopg2 cannot speak walsender (the empty set, above). Which leaves the emitter the original research rejected on day one: **a trigger.**

That rejection deserves honest re-litigation, because the ground moved under it. Option 1 was rejected for "needing a migration or an app change outside the TUI" — but the slot path we then blessed needs a server parameter change, a **restart**, and a very-highly-privileged role, all equally outside the TUI. A three-trigger migration is strictly the smaller ask. Choosing it also **deletes the entire slot-era ops ledger**: no `wal_level` flip, no restart, no REPLICATION grant, no slot/WAL-sender budget, no `max_slot_wal_keep_size` guardrail, no per-consumer decode CPU. The TUI keeps running as plain `pio_reader` (LISTEN requires no table privileges) plus one dedicated listen connection. And — marked as recollection, verify against the repo — this is the emitter pattern pgqueuer itself runs in production for job wakeups, which would make it the most battle-tested piece of this entire design rather than the least.

The emitter, with the design rules that make a trigger on pio-api's tables defensible:

```sql
CREATE OR REPLACE FUNCTION versaai.pio_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('pio_events', TG_TABLE_NAME);
  RETURN NULL;  -- AFTER trigger: return value ignored
END $$;

CREATE TRIGGER pio_notify_picks
  AFTER INSERT OR UPDATE OR DELETE ON versaai.versaai_picks
  FOR EACH ROW EXECUTE FUNCTION versaai.pio_notify();
-- likewise versaai_sessions; stations can add
--   WHEN (OLD.paused IS DISTINCT FROM NEW.paused)
```

1. **Payload minimalism is the whole trick.** Postgres folds identical notifications within a transaction, so with payload = table name, a session commit touching 50 picks collapses **server-side into a single wakeup** — coalescing for free, before the Python debounce even runs. Resist rich payloads in v1; the consumer refetches anyway. If sharper routing is ever wanted, `table:station_id` is the next notch (dedup then per station), with a `left(..., 7900)` guard against the ~8 KB payload cap.
2. **Write-path safety by construction.** The function body is one `pg_notify` call with no payload assembly — there is nothing in it that can throw. That, more than anything, is what makes coupling to pio-api's write path acceptable.
3. **Volume valve, if ever needed:** switch to `AFTER ... FOR EACH STATEMENT` (optionally with transition tables) for one emit per statement instead of per row. Not needed day one; the txn-dedup already does most of this.
4. **Migration mechanics:** `CREATE TRIGGER` takes a brief SHARE ROW EXCLUSIVE lock (research doc, Part 1) — deploy in a quiet window. Triggers have no `IF NOT EXISTS`, so the migration is `DROP TRIGGER IF EXISTS` + `CREATE` in one transaction. Ownership rules put this in pio-api's migration pipeline: the ask the original doc dodged, now accepted with eyes open as the smallest ask on the table.
5. **Lossiness is a feature-shaped constraint.** NOTIFY has no replay — miss the window and the event is gone — which this architecture already absorbs: Resync on every (re)connect, events as wakeups only, the fetch as source of truth. Keep the 30 s failsafe for one release. Watch `pg_notification_queue_usage()` out of politeness; a live listener keeps it at zero.
6. **Listener mechanics:** one dedicated asyncpg connection, `add_listener('pio_events', cb)` feeding the ChangeFeed queue; the predicate runs on (payload + TUI state); reconnect → Resync. LISTEN is session state — direct 5432 or session pooling only, never transaction-mode pooling.

The `ChangeFeed` interface is untouched — same `Resync`/`Batch` events; the emitter becomes a config detail. That is the payoff of the payload-contract-first rule: **the channel is the stable API, emitters are pluggable.** v1: the trigger. v2: the Zig (or Go) walsender bridge publishes to the same channel, the trigger migration is reverted, and no consumer changes a line. The trigger isn't a betrayal of the WAL vision — it's the WAL vision's placeholder that ships this week from parts that already exist.

Framework v1 scope adjusts accordingly: the asyncpg listener client (debounce + resync semantics) plus a **migration generator** — tables and coarse predicate in, idempotent trigger SQL out. Small, publishable, and a natural sibling to pgqueuer.

---

## Ops constants — v1 vs v2

**v1 (trigger emitter): the slot-era ledger vanishes.** No `wal_level` change, no restart, no REPLICATION role, no slot/WAL-sender budget, no `max_slot_wal_keep_size`, no decode CPU. What remains: the trigger migration itself (brief SHARE ROW EXCLUSIVE at deploy time), LISTEN connections on direct 5432 or session pooling, and `pg_notification_queue_usage()` on a dashboard somewhere.

**v2 (WAL emitter swap):** the full ledger from the research doc returns — `wal_level = logical` + restart, dedicated `pio_cdc` role with REPLICATION, slot budget against `max_replication_slots`/`max_wal_senders` (HA reserves four), the keep-size guardrail, and the read-replica placement option — but confined entirely to the one bridge daemon; TUIs never see any of it.

## Spike plan — now vs later

**Now (v1, roughly a day of work total):**

1. Scratch server (30 min): apply the trigger migration, `LISTEN pio_events;` in a psql session, run a fake pick cycle, and watch the wakeups — including the per-transaction coalescing doing its thing on a multi-pick commit.
2. asyncpg listener spike (1 h): `add_listener` → ChangeFeed queue → debounce → print. This is the whole v1 transport.
3. Wire into the TUI behind a flag next to the sentinel; measure change-to-screen latency (expect ≈ commit latency + debounce, i.e. tens of milliseconds).
4. Land the real migration in pio-api's pipeline; deploy in a quiet window; retire the sentinel one release later.

**Later (v2, the emitter swap — unchanged from rev 2/3):** the timeboxed fork stands — Go/pglogrepl in an afternoon, pure-asyncio Python in a day, or the Zig track over a weekend (M0–M4 as go/no-go) — publishing to the **same channel**; the trigger migration reverts at cutover and no consumer changes.

## Sources

- [PostgreSQL streaming replication protocol (16)](https://www.postgresql.org/docs/16/protocol-replication.html) — CREATE_REPLICATION_SLOT TEMPORARY semantics, START_REPLICATION LOGICAL, CopyBoth message flow, snapshot options
- [asyncpg issue #91](https://github.com/MagicStack/asyncpg/issues/91) — replication-protocol support requested 2017, still open (the empty-set evidence)
- [pglogrepl](https://github.com/jackc/pglogrepl) — Go logical replication library on pgx/pgconn, with runnable demo programs
- [pg.zig](https://github.com/karlseguin/pg.zig) — native Zig PostgreSQL driver: auth, TLS via OpenSSL, pooling, LISTEN/NOTIFY, full protocol-message layer; no replication mode (the upstream-contribution target)
- [dwyl/learn-zig #35](https://github.com/dwyl/learn-zig/issues/35) — the only findable Zig CDC experiment: libpq-based bridge to NATS with standby-status-update acks; evidence the native-Zig niche is vacant and the bridge architecture works
- [psycopg2 issue #351](https://github.com/psycopg/psycopg2/issues/351) — the design discussion establishing that with `replication=database` in the startup, only CopyBoth needs special client machinery
- [wal2json README](https://github.com/eulerto/wal2json) — format-version 2, add-tables, actions, streaming vs SQL consumption
- Reference for v1 semantics: [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) (transactional delivery, same-transaction dedup of identical notifications, ~8 KB payload cap, queue-usage function) and [CREATE TRIGGER](https://www.postgresql.org/docs/current/sql-createtrigger.html) (WHEN clauses, statement-level triggers with transition tables)
- Companion doc: *Notification-based refresh: connection-scoped change feeds, researched* (rev 2) — option analysis, Azure enablement, cost ledger, read-replica placement
