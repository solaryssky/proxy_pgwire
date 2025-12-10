#!/usr/bin/env bash
# filepath: /home/dima/github/proxy_pgwire/test_select.sh
set -euo pipefail

HOST="127.0.0.1"
PORT="55432"
USER="postgres"
DBKEY="postgres_key"
QUERY='select * from test where id = 100;'

TOTAL=100
CONCURRENCY=100

ENV_SH="$(dirname "$0")/env.sh"
if [[ -f "$ENV_SH" ]]; then
  source "$ENV_SH"
fi

# Важно: чтобы psql не спрашивал пароль
export PGPASSWORD="${PGUPSTREAM_PASSWORD:-}"

run_psql() {
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DBKEY" -w -c "$QUERY" >/dev/null
}

export -f run_psql
export HOST PORT USER DBKEY QUERY

seq "$TOTAL" | xargs -P"$CONCURRENCY" -I{} bash -c 'run_psql' 
echo "Dispatched $TOTAL psql requests with concurrency=$CONCURRENCY"