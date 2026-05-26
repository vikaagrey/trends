#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
REQUESTS="${REQUESTS:-300}"
CONCURRENCY="${CONCURRENCY:-30}"

echo "Load test GET /api/v1/top?n=10"
echo "BASE_URL=$BASE_URL REQUESTS=$REQUESTS CONCURRENCY=$CONCURRENCY"

curl -fsS "$BASE_URL/healthz" >/dev/null

export BASE_URL
start="$(date +%s)"
codes="$(
  seq "$REQUESTS" | xargs -n1 -P "$CONCURRENCY" sh -c \
    'curl -sS -o /dev/null -w "%{http_code}\n" "$BASE_URL/api/v1/top?n=10"'
)"
finish="$(date +%s)"

failed="$(printf "%s\n" "$codes" | grep -vc '^200$' || true)"
duration="$((finish - start))"
if [ "$duration" -le 0 ]; then
  duration=1
fi
rps="$((REQUESTS / duration))"

echo "requests=$REQUESTS failed=$failed duration=${duration}s rps~$rps"

if [ "$failed" != "0" ]; then
  echo "FAIL: есть ответы не 200"
  exit 1
fi

echo "PASS: нагрузочный тест завершён"
