#!/usr/bin/env bash
set -euo pipefail

HOST="127.0.0.1"
PORT="55432"
USER="postgres"
DBNAME="postgres_key"
CLIENTS=100
DURATION=300

export PGPASSWORD="${PGUPSTREAM_PASSWORD:-}"

cat > /tmp/pgbench_test.sql <<EOF
SELECT * FROM test WHERE id = 100;
EOF

echo "Running pgbench: clients=$CLIENTS duration=${DURATION}s"
pgbench \
  -h "$HOST" \
  -p "$PORT" \
  -U "$USER" \
  -d "$DBNAME" \
  -c "$CLIENTS" \
  -T "$DURATION" \
  -f /tmp/pgbench_test.sql \
  -n \
  -r \
  --progress=5

rm /tmp/pgbench_test.sql