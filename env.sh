#!/usr/bin/env bash
# filepath: /home/dima/github/proxy_pgwire/env.sh
export LOG4RS_CONFIG="/home/dima/github/proxy_pgwire/log4rs.yaml"

export PGUPSTREAM_HOST="127.0.0.1"
export PGUPSTREAM_PORT="5432"
export PGUPSTREAM_DB="postgres"
export PGUPSTREAM_USER="postgres"
export PGUPSTREAM_PASSWORD="123ASDasd"
export PGUPSTREAM_POOL_SIZE="512"
export PGUPSTREAM_POOL_WAIT_MS="2000"
export PGWIRE_LISTEN="0.0.0.0:55432"
export PGWIRE_MAX_CONCURRENCY="1024"


export PGWIRE_USERS='{"postgres":["postgres","123ASDasd"]}'
export PGWIRE_KEYS='{"postgres":"postgres_key"}'
export PGWIRE_RATE_LIMITS='{"postgres":1000000000}' # 100k rpm = ~1666 RPS