#!/usr/bin/env bash
# One-command local stack: kafka + minio + sharded builders + sharded servers.
#
# Usage:
#   scripts/local.sh up         # full stack: compose + produce + builders + servers
#   scripts/local.sh down       # stop builders/servers + docker compose
#   scripts/local.sh status     # show what's running
#   scripts/local.sh logs <n>   # tail a log file (e.g. builder-0, server-2, producer)
#   scripts/local.sh clean      # remove data/, logs/, pids/ (after `down`)
#   scripts/local.sh restart    # down + up
#
# Tunables (env vars, all optional):
#   SHARD_COUNT=3
#   TOTAL_RECORDS=200000
#   KEY_CARDINALITY=50000
#   CHECKPOINT_INTERVAL_MS=15000
#   CHECKPOINT_MAX_RECORDS=50000
#   WAIT_FOR_CHECKPOINT_SECS=120
#   KEEP_DOCKER=1               # `down` keeps kafka/minio running

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOGS_DIR="$ROOT/logs"
PIDS_DIR="$ROOT/pids"
DATA_DIR="$ROOT/data"
mkdir -p "$LOGS_DIR" "$PIDS_DIR" "$DATA_DIR"

SHARD_COUNT="${SHARD_COUNT:-3}"
TOTAL_RECORDS="${TOTAL_RECORDS:-200000}"
KEY_CARDINALITY="${KEY_CARDINALITY:-50000}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
CHECKPOINT_MAX_RECORDS="${CHECKPOINT_MAX_RECORDS:-50000}"
WAIT_FOR_CHECKPOINT_SECS="${WAIT_FOR_CHECKPOINT_SECS:-120}"

BUILDER_JAR="state-builder/target/state-builder-0.1.0-SNAPSHOT.jar"
PRODUCER_JAR="bench-producer/target/bench-producer-0.1.0-SNAPSHOT.jar"
SERVER_JAR="feature-server/target/feature-server-0.1.0-SNAPSHOT.jar"

log()  { printf '[%s] %s\n' "$1" "$2"; }
fail() { printf '[error] %s\n' "$1" >&2; exit 1; }

cmd_build() {
  if [[ -f "$BUILDER_JAR" && -f "$PRODUCER_JAR" && -f "$SERVER_JAR" ]]; then
    log build "jars present, skipping"
    return
  fi
  log build "mvn package..."
  mvn -q -DskipTests package
}

cmd_compose_up() {
  log compose "starting kafka + minio..."
  docker compose up -d
  log compose "waiting for kafka topic feature-events..."
  for _ in $(seq 1 90); do
    if docker exec rocksdb-checkpoint-kafka /opt/kafka/bin/kafka-topics.sh \
         --bootstrap-server localhost:19092 --list 2>/dev/null \
         | grep -qx 'feature-events'; then
      log compose "kafka ready"
      return
    fi
    sleep 1
  done
  fail "timed out waiting for kafka topic"
}

cmd_produce() {
  log produce "generating $TOTAL_RECORDS records (cardinality $KEY_CARDINALITY)..."
  TOTAL_RECORDS="$TOTAL_RECORDS" KEY_CARDINALITY="$KEY_CARDINALITY" \
    java -jar "$PRODUCER_JAR" 2>&1 | tee "$LOGS_DIR/producer.log"
}

is_running() {
  local pidfile="$1"
  [[ -f "$pidfile" ]] || return 1
  local pid
  pid="$(cat "$pidfile")"
  kill -0 "$pid" 2>/dev/null
}

start_builders() {
  for i in $(seq 0 $((SHARD_COUNT - 1))); do
    local pidfile="$PIDS_DIR/builder-$i.pid"
    if is_running "$pidfile"; then
      log "builder-$i" "already running pid=$(cat "$pidfile")"
      continue
    fi
    : > "$LOGS_DIR/builder-$i.log"
    SHARD_COUNT="$SHARD_COUNT" \
    SHARD_ID="$i" \
    HOSTNAME="state-builder-$i" \
    KAFKA_CLIENT_ID="state-builder-$i" \
    ROCKSDB_DATA_DIR="$DATA_DIR/builder-$i/rocksdb" \
    CHECKPOINT_STAGING_DIR="$DATA_DIR/builder-$i/staging" \
    CHECKPOINT_INTERVAL_MS="$CHECKPOINT_INTERVAL_MS" \
    CHECKPOINT_MAX_RECORDS="$CHECKPOINT_MAX_RECORDS" \
    nohup java -jar "$BUILDER_JAR" \
      >"$LOGS_DIR/builder-$i.log" 2>&1 &
    echo $! > "$pidfile"
    log "builder-$i" "started pid=$(cat "$pidfile")"
  done
}

wait_for_first_checkpoint() {
  for i in $(seq 0 $((SHARD_COUNT - 1))); do
    log "builder-$i" "waiting up to ${WAIT_FOR_CHECKPOINT_SECS}s for first checkpoint..."
    for _ in $(seq 1 "$WAIT_FOR_CHECKPOINT_SECS"); do
      if grep -q 'checkpoint v[0-9][0-9]* committed' "$LOGS_DIR/builder-$i.log" 2>/dev/null; then
        log "builder-$i" "checkpoint committed"
        continue 2
      fi
      sleep 1
    done
    fail "builder-$i did not commit a checkpoint within ${WAIT_FOR_CHECKPOINT_SECS}s — see $LOGS_DIR/builder-$i.log"
  done
}

start_servers() {
  for i in $(seq 0 $((SHARD_COUNT - 1))); do
    local pidfile="$PIDS_DIR/server-$i.pid"
    if is_running "$pidfile"; then
      log "server-$i" "already running pid=$(cat "$pidfile")"
      continue
    fi
    : > "$LOGS_DIR/server-$i.log"
    local port=$((8080 + i))
    SHARD_COUNT="$SHARD_COUNT" \
    SHARD_ID="$i" \
    HOSTNAME="feature-server-$i" \
    KAFKA_CLIENT_ID="feature-server-$i" \
    RESTORE_DIR="$DATA_DIR/server-$i/restored" \
    HTTP_PORT="$port" \
    nohup java -jar "$SERVER_JAR" \
      >"$LOGS_DIR/server-$i.log" 2>&1 &
    echo $! > "$pidfile"
    log "server-$i" "started pid=$(cat "$pidfile") port=$port"
  done
}

stop_pid() {
  local pidfile="$1"
  [[ -f "$pidfile" ]] || return 0
  local pid
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" || true
    fi
  fi
  rm -f "$pidfile"
}

cmd_up() {
  cmd_build
  cmd_compose_up
  cmd_produce
  start_builders
  wait_for_first_checkpoint
  start_servers
  echo
  cmd_status
  echo
  echo "feature-server endpoints:"
  for i in $(seq 0 $((SHARD_COUNT - 1))); do
    echo "  shard $i: curl -s http://localhost:$((8080 + i))/manifest | jq ."
  done
}

cmd_down() {
  log down "stopping builders/servers..."
  for f in "$PIDS_DIR"/*.pid; do
    [[ -f "$f" ]] || continue
    stop_pid "$f"
  done
  if [[ "${KEEP_DOCKER:-0}" != "1" ]]; then
    log down "stopping docker compose..."
    docker compose down 2>/dev/null || true
  else
    log down "KEEP_DOCKER=1, leaving compose up"
  fi
}

cmd_status() {
  echo "[status] docker:"
  docker compose ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null \
    || echo "  (compose not running)"
  echo "[status] java processes:"
  local found=0
  for f in "$PIDS_DIR"/*.pid; do
    [[ -f "$f" ]] || continue
    found=1
    local name pid state
    name="$(basename "$f" .pid)"
    pid="$(cat "$f")"
    if kill -0 "$pid" 2>/dev/null; then state="running"; else state="DEAD"; fi
    printf "  %-12s pid=%-7s %s\n" "$name" "$pid" "$state"
  done
  [[ "$found" -eq 1 ]] || echo "  (none)"
}

cmd_logs() {
  local name="${1:-}"
  [[ -n "$name" ]] || fail "usage: scripts/local.sh logs <name>"
  local f="$LOGS_DIR/$name.log"
  [[ -f "$f" ]] || fail "no log: $f"
  exec tail -f "$f"
}

cmd_clean() {
  rm -rf "$DATA_DIR" "$LOGS_DIR" "$PIDS_DIR"
  mkdir -p "$DATA_DIR" "$LOGS_DIR" "$PIDS_DIR"
  log clean "removed data/ logs/ pids/"
}

case "${1:-up}" in
  up)      cmd_up ;;
  down)    cmd_down ;;
  status)  cmd_status ;;
  logs)    shift; cmd_logs "$@" ;;
  build)   cmd_build ;;
  produce) cmd_compose_up; cmd_produce ;;
  clean)   cmd_clean ;;
  restart) cmd_down; cmd_up ;;
  *)
    echo "usage: scripts/local.sh {up|down|status|logs <name>|build|produce|clean|restart}"
    exit 2 ;;
esac
