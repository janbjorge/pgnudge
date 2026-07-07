#!/bin/bash
# The official image writes only `host all all all <auth>`. Replication
# connections use the special `replication` pseudo-db, which `host all` does
# NOT match, so both transports (WalFeed logical + RawFeed physical) get
# rejected without this. Runs after initdb; the edit survives to final start.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
