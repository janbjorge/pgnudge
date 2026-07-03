# Notification-based refresh: connection-scoped change feeds, researched

**Status:** research / design input (rev 2 — baseline moved to PostgreSQL 16+). Follow-up to "Refresh strategy: options considered" (sentinel polling chosen). This doc answers two questions: (1) can a trigger be bound to a connection, and (2) if not, what does the `CREATE_REPLICATION_SLOT` path actually look like end-to-end — privileges, Azure setup, predicate design, failure modes.

**Assumptions (marked as assumptions):** Azure Database for PostgreSQL Flexible Server, **PostgreSQL 16 or newer** (per decision — this unlocks standby decoding; 17 additionally brings failover slots), modest write volume on `versaai_picks`, small number of concurrent TUI users. TUI language not assumed; client options for all common ones are listed.

---

## Part 1 — Can a trigger be bound to a connection?

Short answer: **no**, and every emulation turns out to be worse than it looks. Postgres has no `CREATE TEMPORARY TRIGGER`. Triggers are persistent catalog objects, full stop. The tricks people reach for:

**The uncommitted-DDL trick.** `BEGIN; CREATE TRIGGER ...;` and just never commit — the trigger would exist only inside your transaction and vanish on disconnect. This fails twice. First, other sessions can't see your uncommitted trigger, so pio-api's writes would never fire it. Second, and fatally, `CREATE TRIGGER` acquires a **SHARE ROW EXCLUSIVE** lock on the table, which conflicts with ROW EXCLUSIVE — the lock taken by INSERT/UPDATE/DELETE — and locks are held until end of transaction ([explicit locking docs](https://www.postgresql.org/docs/current/explicit-locking.html)). Holding the transaction open means **blocking every write to `versaai_picks` for as long as the TUI runs**. This isn't a workaround, it's an outage.

**Create-on-connect / drop-on-exit with a janitor.** The honest emulation: a semi-privileged role gets `GRANT TRIGGER ON versaai_picks` (the TRIGGER privilege is grantable per-table to non-owners), the TUI creates a uniquely named trigger at startup (embed `pg_backend_pid()` + `backend_start` in the name), drops it on clean exit, and a `pg_cron` janitor drops triggers whose owning backend no longer appears in `pg_stat_activity`. It works, but: the trigger *function* is a persistent object anyway, so "no schema changes" is already broken; every create/drop takes that same SHARE ROW EXCLUSIVE lock (a brief write stall on every TUI start/stop); a crashed TUI leaves an orphan trigger firing NOTIFYs to nobody until the janitor runs; and the role is no longer read-only. Possible, ugly, litters on crash. Not recommended.

**Everything else.** Event triggers fire on DDL only. Rules are persistent (and deprecated in spirit). `session_replication_role` *disables* triggers for a writer session — the inverse of what we need. No extension provides temp triggers.

The reframe that makes this stop hurting: **the emitter already exists — it's the WAL.** Every committed change to `versaai_picks` is already written to the write-ahead log. We don't need to bolt an emitter onto the write path (trigger) because logical decoding lets a reader turn the WAL itself into an event stream. And the connection-scoped primitive Postgres actually ships for this is exactly the thing from Option 2: the **temporary replication slot**, which "will be dropped at the end of the current session or if an error occurs" — the notification registration that dies with the connection, by design.

---

## Part 2 — The temporary-slot approach, properly

### Two ways to consume a slot

There are two interfaces to the same machinery, and the cheaper one is easy to overlook:

**Mode A — SQL interface (poll the slot).** On a *normal* connection:

```sql
SELECT pg_create_logical_replication_slot(
    'pio_tui_' || pg_backend_pid(), 'wal2json', temporary => true);
```

then every 0.2 s:

```sql
SELECT lsn, xid, data
FROM pg_logical_slot_get_changes(
    'pio_tui_<pid>', NULL, NULL,
    'format-version', '2',
    'add-tables', 'versaai.versaai_picks,versaai.versaai_sessions,versaai.stations',
    'actions', 'insert,update,delete');
```

No replication-protocol client, no keepalive handling — a plain query loop, drop-in where the sentinel query sits today. `get_changes` consumes (advances the slot); `peek_changes` is the non-consuming variant for debugging. Yes, it's still polling — but the poll now returns the *actual changes*, so there are zero false-positive refetches, the predicate gets real data to chew on, and the sentinel's one blind spot (late updates to old-session picks) disappears, because the WAL sees everything. This is "sentinel++": same cadence, strictly more information.

**Mode B — streaming protocol (true push).** Open a replication connection (`replication=database` in the conninfo), issue `CREATE_REPLICATION_SLOT pio_tui_x TEMPORARY LOGICAL wal2json`, then `START_REPLICATION` with the same plugin options. Changes arrive within milliseconds of commit. The client must answer server keepalives with standby status updates (feedback), or the walsender kills the connection after `wal_sender_timeout` (default 60 s) — every serious client library handles this. This is the endgame if sub-poll latency ever matters; for a human-facing TUI, 0.2 s from Mode A is usually already below perception.

Either way, both slot creation and the `pg_logical_slot_*` functions are restricted to superusers and roles with the **REPLICATION** attribute ([replication management functions](https://www.postgresql.org/docs/current/functions-admin.html)) — that's the privilege ask, covered in Part 3.

### Plugin choice: wal2json, deliberately

Azure Flexible Server ships **pgoutput**, **test_decoding**, and **wal2json** (plus the pglogical extension). Recommendation: **wal2json, format-version 2**.

- **wal2json** needs no publication, no DDL of any kind. `format-version '2'` emits one JSON object per tuple. `add-tables` restricts output to exactly our three tables server-side; `actions` restricts to insert/update/delete; `include-transaction 'false'` drops begin/commit noise if unwanted ([wal2json README](https://github.com/eulerto/wal2json)).
- **test_decoding** is text-formatted, fine for a psql smoke test, annoying to parse.
- **pgoutput** is the tempting wrong answer. It requires a **publication**, which is a persistent object needing `CREATE` on the database, and adding a table to it requires table *ownership* ([logical replication security](https://www.postgresql.org/docs/current/logical-replication-security.html)) — the self-contained story dies immediately. It does buy server-side row filters, i.e. the predicate pushed into the server: `CREATE PUBLICATION p FOR TABLE versaai_picks WHERE (...)`. But there's a sharp edge: **if a publication publishes UPDATE or DELETE, the row-filter WHERE clause may only reference replica-identity columns** ([row filters](https://www.postgresql.org/docs/current/logical-replication-row-filter.html)). `status` and `station_id` are almost certainly not in the PK, so filtering on them would force `REPLICA IDENTITY FULL` on `versaai_picks` — an ALTER TABLE on the production write path plus WAL amplification on every update. Exactly the blast radius this tool is supposed to avoid. So: table-level filtering server-side (wal2json `add-tables`), row-level predicate client-side.

### The predicate function

This is the piece you asked for, and client-side is where it gets full expressiveness. Shape:

```
Change { table, action(I/U/D), new: Row?, old_identity: Row? }
interesting(change, tui_state) -> bool
```

One nuance decides how you write these: **what the old row contains depends on REPLICA IDENTITY.** With the default (PK), an UPDATE carries the full *new* row but only the PK of the old row; a DELETE carries only the PK; a table with *no* PK and no replica identity emits nothing usable for UPDATE/DELETE at all — wal2json warns and skips ([wal2json README](https://github.com/eulerto/wal2json)). So a predicate like "did `status` change?" can't be answered from the event alone without `REPLICA IDENTITY FULL` (don't — see above).

Three clean ways around it, in order of preference:

1. **Write predicates over the new row.** Your own key insight from the sentinel doc carries over: picks only change via phase transitions, so *any* UPDATE to a pick is interesting by construction. `interesting := (table = picks AND new.station_id ∈ watched) OR (table = sessions) OR (table = stations AND new.id ∈ watched)`. No old row needed.
2. **Diff against TUI state.** The TUI already holds the current window in memory; for rows in view it knows the old `status`/`paused` and can compute transitions itself (e.g. only redraw the station panel when `paused` actually flipped).
3. **Replica identity full** — listed for completeness, rejected for write amplification and requiring DDL on pio-api's table.

Pipeline: **decode → predicate → debounce → refetch.** Coalesce interesting events for ~50–100 ms (a session completing fires dozens of pick updates in one commit), then run the *existing* full window fetch. Keeping the fetch as the source of truth is what makes this easy: the stream is only a wakeup signal, so double delivery is harmless, ordering doesn't matter, and there's zero LSN bookkeeping beyond what the slot does itself. Bootstrap order on connect: create the slot *first*, then run the initial full fetch — anything that commits in between shows up in both, which the refetch model absorbs for free. Later, if you want it, the same events can drive incremental patching of the in-memory window instead of a refetch — but that's an optimization, not a requirement.

---

## Part 3 — What it costs (the honest ledger)

This path does *not* fully preserve "no infrastructure change." It swaps a schema/app change for a **one-time config + role change**, invisible to pio-api's code and reversible:

1. `wal_level = logical` — server parameter, **requires a restart** ([Azure logical replication docs](https://learn.microsoft.com/en-us/azure/postgresql/configure-maintain/concepts-logical)). Azure also recommends `max_worker_processes ≥ 16`; defaults for `max_replication_slots` / `max_wal_senders` (10 each) are plenty.
2. `ALTER ROLE <role> WITH REPLICATION;` — run by the Azure admin, exactly as the Azure doc shows.

Two honest caveats on the role. First, the CREATE ROLE docs call REPLICATION "a very highly privileged" attribute — logical decoding reads the change stream of **every table in the database**, regardless of per-table SELECT grants. If the `versaai` DB contains anything `pio_reader` isn't supposed to see, don't widen `pio_reader`; mint a dedicated `pio_cdc` role and treat it as read-everything-in-this-DB. Azure's own guidance is to keep the replication user separate from regular accounts. Second, `wal_level=logical` makes WAL somewhat more verbose cluster-wide; that's the tax everyone pays for CDC.

Runtime failure modes, all of which the temp slot's lifecycle makes benign:

- **Connection drops / TUI crashes / server restarts / HA failover** → the slot evaporates server-side (temp slots aren't crash-safe or failover-preserved — on Azure, slots aren't preserved across HA failover on ≤PG16 at all, and PG17's native `sync_replication_slots` only applies to persistent slots). TUI response is uniform: reconnect, recreate slot, full refresh. This is your 30 s fallback generalized to "on slot loss" — and with the blind spot gone, the periodic 30 s failsafe can eventually be retired (keep it one release as belt-and-braces).
- **Live-but-stalled consumer** is the only real WAL-retention risk (slot alive, nobody reading). Mitigations: consume promptly (the 0.2 s loop does), have the admin set `max_slot_wal_keep_size` as a hard cap so a stuck slot gets invalidated instead of filling the disk, and note Azure's own backstop: at ~95 % storage the server goes read-only, and Azure will auto-drop *unused* slots near the threshold ([Azure docs](https://learn.microsoft.com/en-us/azure/postgresql/configure-maintain/concepts-logical)). Monitor `pg_replication_slots` (`active`, `restart_lsn`, `wal_status`, and on 16+ `conflicting` for standby-slot invalidation), plus `pg_stat_replication_slots` for decode statistics.
- **Slot creation latency**: building the initial consistent point waits for in-flight transactions to finish, so startup can pause behind a long-running write transaction. Rare in this workload; worth knowing.
- **Decode CPU** scales with total DB write volume, per slot — N TUIs = N walsenders each decoding everything (then filtering). Fine for a handful of users; see Part 4 for the fleet answer.
- **PgBouncer**: replication connections can't go through the built-in pooler, and even Mode A's temp slot is session-bound — connect **direct to 5432**, not 6432.
- **Slot budget**: every concurrent TUI is one slot and one WAL sender. Azure's sizing guidance counts these against `max_replication_slots` / `max_wal_senders` alongside HA (which needs four of each) and one per read replica ([Azure HA docs](https://learn.microsoft.com/en-us/azure/postgresql/high-availability/concepts-high-availability)) — the default of 10 covers a few TUIs on an HA server with a replica, but bump it before rolling this out to a whole team.

---

## Part 4 — One slot, many listeners: Option 1 resurrected without the trigger

If TUI count grows (decode cost × N) or other consumers appear, invert it: a tiny **bridge daemon** owns one slot (persistent or temporary — temporary + refetch-on-restart keeps the same safety story), applies the predicate centrally, and re-broadcasts compact events via `SELECT pg_notify('versaai_events', payload)` — keys and action only, payloads are capped at ~8 KB anyway.

The TUIs then just `LISTEN versaai_events` **on their existing `pio_reader` connection**. This is the quietly great part: LISTEN/NOTIFY require no table privileges at all, and per the hot-standby docs, in normal operation even *read-only transactions* are allowed to use LISTEN and NOTIFY ([hot standby](https://www.postgresql.org/docs/current/hot-standby.html)) — so the TUI ends up *more* self-contained than today: pure SELECT + LISTEN, zero elevated privileges, push latency. The REPLICATION grant concentrates in one small service instead of every laptop. This is exactly the "pio-api emits these events for its own purposes" future from the original doc — except nothing in pio-api changes; the WAL is the emitter. (One limit: LISTEN/NOTIFY don't work on hot-standby read replicas, so listeners connect to the primary.)

One 16-vs-17 wrinkle if the bridge uses a **persistent** slot on an HA-enabled server: on PG16 the slot simply doesn't exist on the new primary after a failover unless the `pg_failover_slots` extension is configured, while PG17 supports slot synchronization natively — create the slot with `failover => true` and enable `sync_replication_slots` + `hot_standby_feedback` on the standby, and it survives failover with no extension ([logical decoding concepts](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html), [Azure HA docs](https://learn.microsoft.com/en-us/azure/postgresql/high-availability/concepts-high-availability)). Azure additionally exposes a preview metric, `logical_replication_slot_sync_status`, to alert when slots aren't failover-ready. A *temporary* bridge slot sidesteps all of it: the slot dies with the failover, the bridge reconnects and recreates it, and listeners get one "full refresh" nudge — which is the same story the TUIs already handle.

---

## Part 5 — Decode from the read replica (unlocked by the 16+ baseline)

Before 16, creating a logical slot on a standby failed with `logical decoding cannot be used while in recovery` ([pgpedia](https://pgpedia.info/p/pg_create_logical_replication_slot.html)); from 16, logical slots — including temporary ones, consumed via either mode — can be created and used on a hot standby ([logical decoding concepts](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html)). For a monitoring tool this is the natural home: point the TUI's slot *and* its window queries at an Azure read replica, and the primary carries zero monitoring load — no extra walsender, no decode CPU, no extra connections. What it takes, and where it can bite:

- `wal_level = logical` still has to be set **on the primary** — the standby decodes the primary's WAL stream, and standby slots are invalidated the instant the primary's `wal_level` drops below logical. So the one restart from Part 3 doesn't go away; the replica just relieves the primary of the *runtime* cost.
- Set `hot_standby_feedback = on` on the replica so VACUUM on the primary doesn't remove catalog rows the slot still needs; if required rows are removed anyway, the slot is **invalidated** rather than blocking the primary forever. Invalidation shows up in `pg_replication_slots.conflicting` — treat it exactly like slot loss on the ladder: drop, recreate, full refresh. Upstream recommends backing feedback with a physical slot between primary and standby (otherwise feedback only holds while the walreceiver connection is alive); on Azure that's the default anyway, since read replicas use streaming replication with replication slots as the standard operating mode ([Azure read replicas](https://learn.microsoft.com/en-us/azure/postgresql/read-replica/concepts-read-replicas)). Usual tradeoff applies: feedback delays vacuum on the primary, i.e. some bloat pressure.
- Slot creation on a standby waits for a running-transactions record from the primary, so on an *idle* system it can stall; `SELECT pg_log_standby_snapshot()` on the primary unsticks it. A warehouse pick database is rarely idle — this is mostly a test-environment quirk, but it's baffling the first time you hit it.
- Events arrive behind replication lag, and window queries on the replica see the *same* lag — the TUI stays self-consistent, just not primary-fresh. Fine for a human-facing monitor; worth a sentence in the README.
- LISTEN/NOTIFY still don't run in recovery, so in the bridge variant the decoder can live on the replica while its `pg_notify` fan-out (and the TUIs' LISTEN connections) target the primary.
- **Honesty flag (inference, not a retrieved guarantee):** the engine supports all of this on 16+, and Azure read replicas are ordinary hot standbys with user-settable parameters, but I found no Azure doc explicitly blessing logical decoding *on* Flexible Server read replicas. Verify on a scratch replica before depending on it.

---

## Part 6 — Client implementation notes

**Mode A** needs nothing beyond your existing driver — it's two queries. **Mode B** needs replication-protocol support: Go has `jackc/pglogrepl`; Python has `psycopg2`'s `LogicalReplicationConnection` (check current psycopg 3 status — the replication protocol historically lived in psycopg2); Node has `pg-logical-replication`; Rust has replication-mode support in the `postgres`/`tokio-postgres` ecosystem (e.g. the `postgres-replication` / Supabase `etl`, ex-`pg_replicate`, crates); and `pg_recvlogical` is the zero-code prototype — point it at prod with `--slot ... --create-slot -P wal2json` on a scratch temp slot and watch the events before writing a line of TUI code.

**Footnote:** if pio-api ever *does* want to emit explicit app-level events, `pg_logical_emit_message()` drops them into this same pipe with no tables involved.

---

## Part 7 — Decision matrix and recommended path

| Approach | Latency | Privileges beyond SELECT | Persistent objects | Predicate lives | Risk to writers |
|---|---|---|---|---|---|
| Sentinel (today) | ~0.2 s | none | none | fingerprint ≈ implicit | none |
| Temp slot, SQL poll (Mode A) | ~0.2 s, no false positives | REPLICATION + `wal_level` change | none | client, full data | none |
| Temp slot, streaming (Mode B) | ~ms | same as A | none | client, full data | none |
| Temp slot on read replica (16+) | ~ms + replication lag | same as A, pointed at replica | none | client, full data | none — primary untouched at runtime |
| Bridge + LISTEN/NOTIFY | ~ms, TUI needs nothing | REPLICATION on bridge only | one daemon | bridge | none |
| Trigger + NOTIFY (perm.) | ~ms | TRIGGER grant / migration | trigger + function | trigger WHEN | lock blips, write-path coupling |
| pgoutput + row-filter publication | ~ms | REPLICATION + CREATE + ownership | publication | server (RI-columns only!) | may force REPLICA IDENTITY FULL |

**Recommendation.** Keep the sentinel as the permanent bottom rung of a degradation ladder. Next step is **Mode A**: it's the smallest possible delta (swap the sentinel query for a slot poll), needs the one-time admin change (`wal_level` + role), kills both the false-positive refetches and the old-session blind spot, and — the actual point — gives the predicate function real rows to evaluate. Structure the code as `source → predicate → debounce → refetch` with sources being `stream | slot-poll | sentinel`, falling back down the ladder when slot setup fails (no privilege, wal_level not set, etc.), so the TUI keeps working everywhere. Move to Mode B only if push latency earns its client complexity; stand up the bridge when TUI count or new consumers make per-client decoding wasteful — at which point the TUI itself drops back to zero special privileges. And since the baseline is now 16+, seriously consider pointing the whole thing — slot and window queries alike — at a read replica from day one (after the scratch-replica verification in Part 5): the primary then pays only the one-time `wal_level` change, and monitoring load lands where read load belongs. Slot invalidation on the replica slots into the ladder as just another flavor of slot loss.

---

## Sources

- Azure Database for PostgreSQL Flexible Server — [Logical replication and logical decoding](https://learn.microsoft.com/en-us/azure/postgresql/configure-maintain/concepts-logical) (prerequisites, `ALTER ROLE ... WITH REPLICATION`, plugins, HA/failover slot behavior, auto-drop of unused slots, monitoring)
- PostgreSQL docs — [System administration functions §Replication](https://www.postgresql.org/docs/current/functions-admin.html) (slot functions restricted to superuser/REPLICATION), [CREATE ROLE](https://www.postgresql.org/docs/current/sql-createrole.html) (REPLICATION attribute semantics), [Explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html) (CREATE TRIGGER → SHARE ROW EXCLUSIVE), [Row filters](https://www.postgresql.org/docs/current/logical-replication-row-filter.html) (replica-identity restriction), [Logical replication security](https://www.postgresql.org/docs/current/logical-replication-security.html) (publication privileges), [Hot standby](https://www.postgresql.org/docs/current/hot-standby.html) (LISTEN/NOTIFY allowed in read-only txns, disallowed in recovery)
- [wal2json README](https://github.com/eulerto/wal2json) (format-version 2, `add-tables`, `actions`, replica-identity dependence of old tuples, SQL vs streaming consumption)
- [pgpedia: pg_create_logical_replication_slot](https://pgpedia.info/p/pg_create_logical_replication_slot.html) (`temporary` parameter since PG10, standby support since PG16)
- PostgreSQL docs — [Logical decoding concepts, PG16](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html) / [PG17](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) (slots on hot standby, `hot_standby_feedback`, invalidation semantics, `pg_log_standby_snapshot`; PG17 failover-slot synchronization via `failover` + `sync_replication_slots`)
- Azure — [Read replicas](https://learn.microsoft.com/en-us/azure/postgresql/read-replica/concepts-read-replicas) (physical streaming with replication slots as default mode), [High availability](https://learn.microsoft.com/en-us/azure/postgresql/high-availability/concepts-high-availability) (slot/WAL-sender sizing incl. HA's four, PG16 `pg_failover_slots` vs PG17 native slot sync, `logical_replication_slot_sync_status` metric)
