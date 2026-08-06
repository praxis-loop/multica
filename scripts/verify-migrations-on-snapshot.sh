#!/usr/bin/env bash
# Verify pending migrations against a read-only snapshot of production.
#
# Production is NEVER written to: the only thing this does against the live
# stack is `pg_dump`. Everything else happens in a throwaway pgvector container
# that is created with a timestamped name and destroyed on exit (including on
# failure, via trap).
#
# Two databases are exercised:
#   fresh — empty, proves the migration chain works on a new install
#   snap  — the production snapshot, proves the chain is safe on THIS deployment
#
# Each gets `migrate up` twice; the second pass must be a complete no-op, which
# is what "idempotent" means operationally.
set -euo pipefail

# Defaults to the checkout this script lives in, so the migrations it verifies
# are always the ones sitting next to it.
REPO=${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
SERVER=${SERVER:-ubuntu@192.168.15.53}

usage() {
  cat <<'USAGE'
Usage: scripts/verify-migrations-on-snapshot.sh [--repo PATH] [--server USER@HOST] [--out DIR]

Verifies the repo's migrations against a read-only snapshot of production.
Production is only ever pg_dump'ed; every write happens in a throwaway container.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --repo)   REPO=$2; shift 2 ;;
    --server) SERVER=$2; shift 2 ;;
    --out)    OUT=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

STAMP=$(date +%Y%m%d-%H%M%S)
CT=multica-migverify-$STAMP
RPORT=55432                       # port on the server's loopback
LPORT=55432                       # local end of the tunnel
DUMP=/tmp/multica-migverify-$STAMP.dump
SOCK=/tmp/mtun-$STAMP.sock        # MUST stay short: unix sockets cap at ~104 bytes
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT"

say() { printf '\n=== %s\n' "$*"; }
# All remote work multiplexes over ONE connection. Opening a fresh ssh per query
# trips sshd's rate limiter partway through the run ("Connection reset by peer").
ssh_() { ssh -S "$SOCK" "$SERVER" "$@"; }
# psql against the throwaway container, unaligned/tuples-only for clean capture
tsql() { ssh_ "docker exec $CT psql -U multica -d $1 -X -t -A -c \"$2\""; }

cleanup() {
  local rc=$?
  say "cleanup"
  ssh -S "$SOCK" -O exit "$SERVER" 2>/dev/null || true
  ssh_ "docker rm -f $CT >/dev/null 2>&1; rm -f $DUMP" 2>/dev/null || true
  echo "throwaway container and dump removed (artifacts kept in $OUT)"
  exit $rc
}
trap cleanup EXIT

say "0. open shared SSH control connection"
ssh -o BatchMode=yes -o ConnectTimeout=10 -M -S "$SOCK" -fnNT "$SERVER"
ssh -S "$SOCK" -O check "$SERVER"

say "1. read-only dump of production"
ssh_ "docker exec multica-postgres-1 pg_dump -U multica -d multica --no-owner --no-acl -Fc -f /tmp/$(basename "$DUMP") && docker cp multica-postgres-1:/tmp/$(basename "$DUMP") $DUMP && docker exec multica-postgres-1 rm -f /tmp/$(basename "$DUMP") && ls -lh $DUMP"

say "2. start throwaway pgvector container ($CT) on 127.0.0.1:$RPORT"
ssh_ "docker run -d --name $CT -e POSTGRES_USER=multica -e POSTGRES_PASSWORD=multica -e POSTGRES_DB=postgres -p 127.0.0.1:$RPORT:5432 pgvector/pgvector:pg17 >/dev/null"
for i in $(seq 1 60); do
  if ssh_ "docker exec $CT pg_isready -U multica -q" 2>/dev/null; then break; fi
  sleep 2
  [ "$i" = 60 ] && { echo "container never became ready"; exit 1; }
done
echo "ready"

say "3. create fresh (empty) + snap (production snapshot)"
ssh_ "docker exec $CT psql -U multica -d postgres -X -q -c 'CREATE DATABASE fresh' -c 'CREATE DATABASE snap'"
ssh_ "docker cp $DUMP $CT:/tmp/snap.dump && docker exec $CT pg_restore -U multica -d snap --no-owner --no-acl /tmp/snap.dump" 2>&1 | tail -5 || true
echo "snap restored; ledger rows: $(tsql snap 'SELECT count(*) FROM schema_migrations')"
echo "snap ledger max: $(tsql snap 'SELECT max(version) FROM schema_migrations')"

say "4. add port-forward localhost:$LPORT -> $CT over the existing connection"
ssh -S "$SOCK" -O forward -L 127.0.0.1:$LPORT:127.0.0.1:$RPORT "$SERVER"
sleep 1
echo "tunnel up"

say "5. baseline: per-table row counts + outbox status (snap, BEFORE migrating)"
tsql snap "SELECT relname||' '||n_live_tup FROM pg_stat_user_tables ORDER BY relname" > "$OUT/rows.before" || true
# n_live_tup is an estimate; take exact counts for the tables our migrations touch
EXACT="channel_project_binding channel_issue_topic_binding channel_notification_outbox issue agent_task_queue"
: > "$OUT/exact.before"
for t in $EXACT; do
  c=$(tsql snap "SELECT count(*) FROM $t" 2>/dev/null || echo "MISSING")
  echo "$t $c" >> "$OUT/exact.before"
done
cat "$OUT/exact.before"
tsql snap "SELECT status||' '||count(*) FROM channel_notification_outbox GROUP BY status ORDER BY status" > "$OUT/outbox.before" 2>/dev/null || true
echo "--- outbox before ---"; cat "$OUT/outbox.before"

run_migrate() {   # $1=db  $2=pass label
  ( cd "$REPO/server" && DATABASE_URL="postgres://multica:multica@127.0.0.1:$LPORT/$1?sslmode=disable" \
      go run ./cmd/migrate up ) > "$OUT/$1.$2.log" 2>&1
  local rc=$?
  echo "  $1 pass$2 exit=$rc  applied=$(grep -c '^  up  ' "$OUT/$1.$2.log" || true)  skipped=$(grep -c '^  skip ' "$OUT/$1.$2.log" || true)"
  return $rc
}

say "6. migrate fresh (twice)"
run_migrate fresh 1
run_migrate fresh 2

say "7. migrate snap (twice)"
run_migrate snap 1
run_migrate snap 2

say "8. integrity checks"
FAIL=0
chk() { if [ "$2" = "$3" ]; then echo "  PASS  $1: $2"; else echo "  FAIL  $1: got '$2' want '$3'"; FAIL=1; fi; }

# a. idempotency — second pass must apply nothing
chk "fresh pass2 applied nothing" "$(grep -c '^  up  ' "$OUT/fresh.2.log" || true)" "0"
chk "snap  pass2 applied nothing" "$(grep -c '^  up  ' "$OUT/snap.2.log" || true)" "0"

# b. our 12 migrations really ran on snap (they were renumbered, so they re-run)
echo "  -- our 319-330 on snap pass1:"
grep -E '^  (up|skip) +3(19|2[0-9]|30)_' "$OUT/snap.1.log" | sed 's/^/     /' || echo "     (none)"

# c. row counts unchanged on snap
: > "$OUT/exact.after"
for t in $EXACT; do
  c=$(tsql snap "SELECT count(*) FROM $t" 2>/dev/null || echo "MISSING")
  echo "$t $c" >> "$OUT/exact.after"
done
if diff -q "$OUT/exact.before" "$OUT/exact.after" >/dev/null; then
  echo "  PASS  snap row counts unchanged"; cat "$OUT/exact.after" | sed 's/^/     /'
else
  echo "  FAIL  snap row counts changed:"; diff "$OUT/exact.before" "$OUT/exact.after" | sed 's/^/     /' || true; FAIL=1
fi

# d. no invalid indexes (an interrupted CREATE INDEX CONCURRENTLY leaves one,
#    and an INVALID unique index does not enforce uniqueness)
for db in fresh snap; do
  chk "$db invalid indexes" "$(tsql $db "SELECT count(*) FROM pg_index WHERE NOT indisvalid")" "0"
done

# e. every index our migrations build exists AND is valid, on both databases
IDX="uq_channel_project_binding_bind_token uq_channel_project_binding_project uq_channel_project_binding_workspace_project uq_channel_project_binding_bot_group uq_channel_issue_topic_issue uq_channel_issue_topic_workspace_issue uq_channel_issue_topic_route uq_channel_notification_outbox_event idx_channel_notification_outbox_pending idx_channel_notification_outbox_issue_order idx_channel_notification_outbox_issue_event_order"
for db in fresh snap; do
  miss=0
  for i in $IDX; do
    v=$(tsql $db "SELECT count(*) FROM pg_class c JOIN pg_index x ON x.indexrelid=c.oid WHERE c.relname='$i' AND x.indisvalid")
    [ "$v" = "1" ] || { echo "  FAIL  $db index $i valid=$v"; miss=1; FAIL=1; }
  done
  [ $miss = 0 ] && echo "  PASS  $db: all 11 lark indexes present and valid"
done

# f. outbox must not regress sent -> pending/sending
tsql snap "SELECT status||' '||count(*) FROM channel_notification_outbox GROUP BY status ORDER BY status" > "$OUT/outbox.after" 2>/dev/null || true
echo "  -- outbox after ---"; sed 's/^/     /' "$OUT/outbox.after"
SB=$(awk '$1=="sent"{print $2}' "$OUT/outbox.before"); SB=${SB:-0}
SA=$(awk '$1=="sent"{print $2}' "$OUT/outbox.after");  SA=${SA:-0}
if [ "$SA" -ge "$SB" ]; then echo "  PASS  outbox sent did not regress ($SB -> $SA)"; else echo "  FAIL  outbox sent regressed ($SB -> $SA)"; FAIL=1; fi

# g. fresh and snap must converge on the same schema
for what in "SELECT count(*) FROM pg_index x JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'|indexes" \
            "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public'|constraints" \
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'|tables"; do
  q=${what%|*}; label=${what#*|}
  f=$(tsql fresh "$q"); s=$(tsql snap "$q")
  if [ "$f" = "$s" ]; then echo "  PASS  fresh/snap $label match: $f"; else echo "  WARN  fresh/snap $label differ: fresh=$f snap=$s"; fi
done

# g2. name-level diff so any count mismatch above is explained, not just counted
CONQ="SELECT c.conrelid::regclass::text||'.'||c.conname||' ('||c.contype::text||')' FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public' ORDER BY 1"
tsql fresh "$CONQ" | sort > "$OUT/con.fresh"
tsql snap  "$CONQ" | sort > "$OUT/con.snap"
if diff -q "$OUT/con.fresh" "$OUT/con.snap" >/dev/null; then
  echo "  PASS  fresh/snap constraint names identical"
else
  echo "  -- constraint name diff (< only in fresh, > only in snap):"
  diff "$OUT/con.fresh" "$OUT/con.snap" | grep -E '^[<>]' | sed 's/^/     /' || true
fi
IDXQ="SELECT c2.relname FROM pg_index x JOIN pg_class c2 ON c2.oid=x.indexrelid JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY 1"
tsql fresh "$IDXQ" | sort > "$OUT/idx.fresh"
tsql snap  "$IDXQ" | sort > "$OUT/idx.snap"
if diff -q "$OUT/idx.fresh" "$OUT/idx.snap" >/dev/null; then
  echo "  PASS  fresh/snap index names identical"
else
  echo "  -- index name diff:"; diff "$OUT/idx.fresh" "$OUT/idx.snap" | grep -E '^[<>]' | sed 's/^/     /' || true
fi

# h. ledger: both databases must end at the same max version
chk "fresh/snap same ledger max" "$(tsql fresh 'SELECT max(version) FROM schema_migrations')" "$(tsql snap 'SELECT max(version) FROM schema_migrations')"
echo "  final max version: $(tsql snap 'SELECT max(version) FROM schema_migrations')"
echo "  snap ledger rows now: $(tsql snap 'SELECT count(*) FROM schema_migrations')"

say "RESULT"
if [ $FAIL = 0 ]; then echo "ALL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
echo "logs: $OUT"
exit $FAIL
