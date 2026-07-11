# Managed platforms

pgnudge has **not** been integration-tested against any managed service. The
`WalFeed` column below reflects each vendor's own documentation (linked at the
bottom); verify against your plan and region, and let `pgnudge doctor` confirm
the live handshake.

Short version: every platform here documents a `WalFeed` path. Flip
`wal_level = logical` and grant a REPLICATION-capable role. Whether `RawFeed`
works (it needs external `START_REPLICATION PHYSICAL` to a non-managed standby)
is **untested** on most of them, and mostly undocumented; pgnudge makes no claim
either way. The one confirmed data point is **Azure Flexible Server, which
blocks it** (see caveats). If you confirm it works, or that a platform blocks
it, open an issue and this table gets updated.

| Platform                | `WalFeed` (documented) | `RawFeed` | Enable `wal_level = logical`                                  |
|-------------------------|------------------------|-----------|--------------------------------------------------------------|
| AWS RDS PostgreSQL      | yes                    | untested  | `rds.logical_replication=1`, grant `rds_replication`         |
| AWS Aurora PostgreSQL   | yes                    | untested  | `rds.logical_replication=1` (cluster parameter group)        |
| Google Cloud SQL        | yes                    | untested  | flag `cloudsql.logical_decoding=on`, user `WITH REPLICATION` |
| Azure Flexible Server   | yes                    | **no**    | `wal_level=logical`, `ALTER ROLE ... WITH REPLICATION`       |
| Supabase                | yes\*                  | untested  | role `WITH REPLICATION`; **direct** connection only          |
| Neon                    | yes\*                  | untested  | enabling logical repl flips `wal_level` project-wide         |

`\*` Supabase and Neon require a **direct** connection, not their pooler
(Supavisor / PgBouncer), the same rule pgnudge already states for any pooler.
The `RawFeed` column is left **untested**: these vendors document logical
decoding as the external replication path and do not document an external
physical-streaming endpoint, but we have neither confirmed nor ruled one out.
Reports welcome.

## Caveats worth a pre-flight `pgnudge doctor`

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

## Sources (vendor docs)

[RDS logical replication][rds-lr], [RDS/Aurora to self-managed][rds-selfmanaged],
[Aurora logical replication][aurora-lr], [Cloud SQL logical replication][gcp-lr],
[Cloud SQL external server][gcp-ext], [Azure logical][azure-lr],
[Supabase external replication][supa-lr], [Neon logical replication][neon-lr],
[Neon connection pooling][neon-pool].

[rds-lr]: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html
[rds-selfmanaged]: https://aws.amazon.com/blogs/database/using-logical-replication-to-replicate-managed-amazon-rds-for-postgresql-and-amazon-aurora-to-self-managed-postgresql/
[aurora-lr]: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.Configure.html
[gcp-lr]: https://docs.cloud.google.com/sql/docs/postgres/replication/configure-logical-replication
[gcp-ext]: https://docs.cloud.google.com/sql/docs/postgres/replication/external-server
[azure-lr]: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical
[supa-lr]: https://supabase.com/docs/guides/database/postgres/setup-replication-external
[neon-lr]: https://neon.com/docs/guides/logical-replication-neon
[neon-pool]: https://neon.com/docs/connect/connection-pooling
