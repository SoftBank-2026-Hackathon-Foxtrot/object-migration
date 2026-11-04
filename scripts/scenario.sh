#!/bin/bash
# Runs one migration through switch and finalize (or rollback) while
# simulated apps write, read and delete, then checks the bucket kept.
#
#   scripts/scenario.sh body.json [switch|rollback]
#
# body.json is the POST /v1/migrations request. The server must be running
# (SERVER, default http://127.0.0.1:8080); bin/appsim must be built. App
# credentials: APP_REF for s3 and gcs (default "default"), SEAWEED_APP_REF
# for seaweedfs (default env:SEAWEEDAPP). Set AWS_PROFILE for AWS.
set -u
BODY=$1
MODE=${2:-switch}
SERVER=${SERVER:-http://127.0.0.1:8080}
OUT=${OUT:-$(mktemp -d)}
B=$SERVER/v1/migrations
APPSIM=${APPSIM:-$(dirname "$0")/../bin/appsim}

ts() { date +%T; }
cmd() {
	echo "$(ts) $1"
	curl -sf -X POST "$B/$ID/$1" >/dev/null || { echo "$(ts) $1 refused"; return 1; }
}
waitfor() {
	for _ in $(seq 1 "${2:-300}"); do
		R=$(curl -s "$B/$ID")
		if echo "$R" | jq -e "$1" >/dev/null; then
			echo "$(ts)   $(echo "$R" | jq -c '{phase, state, route: "\(.routing.generation):\(.routing.write)"}')"
			return 0
		fi
		sleep 1
	done
	echo "$(ts) TIMEOUT waiting for $1: $(echo "$R" | jq -c '{phase, state, lastError, cutover: [.readiness.cutover.checks[]? | select(.ok|not) | .message]}')"
	return 1
}
fail() { kill -INT "$APPS" 2>/dev/null; wait "$APPS"; exit 1; }

echo "$(ts) preflight: $(curl -s -X POST "$SERVER/v1/preflight" -d @"$BODY" | jq -c '{ok, failed: [.checks[] | select(.ok|not) | "\(.name): \(.message)"]}')"
ID=$(curl -s -X POST "$B" -d @"$BODY" | jq -r .id)
echo "$(ts) migration $ID"
waitfor '.phase=="READY"' 600 || exit 1

"$APPSIM" -server "$SERVER" -migration "$ID" -apps 3 -for 30m -out "$OUT" \
	-ref "${APP_REF:-default}" -seaweedfs-ref "${SEAWEED_APP_REF:-env:SEAWEEDAPP}" > "$OUT/appsim.log" 2>&1 &
APPS=$!
sleep 20
cmd freeze && waitfor '.phase=="FROZEN" and .pendingCommand==null' 60 || fail
cmd switch && waitfor '.phase=="SWITCHED" and .pendingCommand==null' 120 || fail
sleep 20
if [ "$MODE" = rollback ]; then
	cmd rollback && waitfor '.phase=="ROLLED_BACK"' 600 || fail
else
	cmd resume && waitfor '.phase=="VERIFYING" and .state=="succeeded"' 600 || fail
	sleep 20
	cmd finalize && waitfor '.phase=="COMPLETED"' 120 || fail
fi
sleep 15
kill -INT "$APPS"
wait "$APPS"
STATUS=$?
grep -E "^run=|verify:|FAIL" "$OUT/appsim.log" | head -8
echo "$(ts) appsim exit $STATUS (logs in $OUT)"
exit $STATUS
