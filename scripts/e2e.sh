#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
BROKERS="${BROKERS:-localhost:29092}"
TOPIC="${TOPIC:-search.events}"
QUERY="${QUERY:-e2e-query}"

body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT

request() {
  curl -sS -o "$body_file" -w "%{http_code}" "$@"
}

expect_code() {
  local expected="$1"
  local actual="$2"
  local name="$3"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL: $name, HTTP $actual, body: $(cat "$body_file")"
    exit 1
  fi
  echo "PASS: $name"
}

wait_top_contains() {
  local pattern="\"query\":\"$QUERY\""
  for _ in $(seq 1 20); do
    curl -sS "$BASE_URL/api/v1/top?n=20" > "$body_file"
    if grep -Fq "$pattern" "$body_file"; then
      echo "PASS: query появился в топе"
      return
    fi
    sleep 1
  done
  echo "FAIL: query не появился в топе, body: $(cat "$body_file")"
  exit 1
}

wait_top_absent() {
  local pattern="\"query\":\"$QUERY\""
  for _ in $(seq 1 10); do
    curl -sS "$BASE_URL/api/v1/top?n=20" > "$body_file"
    if ! grep -Fq "$pattern" "$body_file"; then
      echo "PASS: stoplist убрал query из топа"
      return
    fi
    sleep 1
  done
  echo "FAIL: query остался в топе после stoplist, body: $(cat "$body_file")"
  exit 1
}

echo "E2E trends"
echo "BASE_URL=$BASE_URL"
echo "BROKERS=$BROKERS"
echo "TOPIC=$TOPIC"

code="$(request "$BASE_URL/healthz")"
expect_code "200" "$code" "healthz"

request -X DELETE "$BASE_URL/api/v1/stoplist/$QUERY" >/dev/null || true

go run ./scripts/producer -brokers "$BROKERS" -topic "$TOPIC" -query "$QUERY" -n 40 -sources 40 >/dev/null
wait_top_contains

code="$(request "$BASE_URL/api/v1/top?n=bad")"
expect_code "400" "$code" "валидация параметра n"

code="$(request -X POST "$BASE_URL/api/v1/stoplist" \
  -H "Content-Type: application/json" \
  -d "{\"word\":\"$QUERY\"}")"
expect_code "200" "$code" "добавление stopword"
wait_top_absent

curl -sS "$BASE_URL/api/v1/stoplist" > "$body_file"
if ! grep -Fq "\"$QUERY\"" "$body_file"; then
  echo "FAIL: stopword не найден в stoplist, body: $(cat "$body_file")"
  exit 1
fi
echo "PASS: stopword виден в stoplist"

code="$(request -X DELETE "$BASE_URL/api/v1/stoplist/$QUERY")"
expect_code "200" "$code" "удаление stopword"

echo "PASS: E2E завершён"
