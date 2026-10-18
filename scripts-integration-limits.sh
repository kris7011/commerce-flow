#!/usr/bin/env bash
# INTEGRATION BRANCH ONLY: checks the Order API limits from PR #3 against
# the containerised stack from PR #4. Not part of any PR.
set -euo pipefail

url="http://127.0.0.1:3001/orders"
failures=0

check() {
    local description="$1" expected="$2" body="$3"
    local status
    status=$(curl -s -o /tmp/limits-response.json -w '%{http_code}' \
        -H 'content-type: application/json' \
        -H "x-correlation-id: limits-${GITHUB_RUN_ID:-local}" \
        -d "$body" "$url")
    if [[ "$status" == "$expected" ]]; then
        echo "PASS  $description -> HTTP $status"
    else
        echo "FAIL  $description -> HTTP $status (expected $expected): $(cat /tmp/limits-response.json)"
        failures=$((failures + 1))
    fi
}

id200=$(printf 'c%.0s' $(seq 1 200))
id201=$(printf 'c%.0s' $(seq 1 201))
item='{"productId":"dishwasher-01","quantity":1,"unitPrice":1}'
items100=$(printf "${item},%.0s" $(seq 1 100)); items100="[${items100%,}]"
items101=$(printf "${item},%.0s" $(seq 1 101)); items101="[${items101%,}]"

check "customerId with 200 characters" 201 "{\"customerId\":\"$id200\",\"items\":[$item]}"
check "customerId with 201 characters" 400 "{\"customerId\":\"$id201\",\"items\":[$item]}"
check "productId with 201 characters" 400 "{\"customerId\":\"c\",\"items\":[{\"productId\":\"$id201\",\"quantity\":1,\"unitPrice\":1}]}"
check "quantity 2147483647 (INTEGER max)" 201 '{"customerId":"c","items":[{"productId":"dryer-01","quantity":2147483647,"unitPrice":1}]}'
check "quantity 2147483648" 400 '{"customerId":"c","items":[{"productId":"dryer-01","quantity":2147483648,"unitPrice":1}]}'
check "100 items" 201 "{\"customerId\":\"c\",\"items\":$items100}"
check "101 items" 400 "{\"customerId\":\"c\",\"items\":$items101}"
check "total overflowing to Infinity" 400 '{"customerId":"c","items":[{"productId":"dryer-01","quantity":2147483647,"unitPrice":1e308}]}'

# The accepted boundary orders must not end up in a dead-letter queue.
sleep 5
npm run --silent dlq

if [[ "$failures" -gt 0 ]]; then
    echo "$failures limit check(s) failed"
    exit 1
fi
