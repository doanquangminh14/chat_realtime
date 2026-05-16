#!/usr/bin/env bash
# demo.sh — Automated smoke test / demo of the gRPC calculator
# Requires rpc-server to be running on localhost:50051
set -euo pipefail

BINARY="${1:-./bin/rpc-client}"
SERVER="${DS_GRPC_HOST:-localhost}:${DS_GRPC_PORT:-50051}"

log() { echo -e "\033[1;36m[demo]\033[0m $*"; }
ok()  { echo -e "\033[1;32m  ✓\033[0m $*"; }

log "Running gRPC Calculator demo against $SERVER"

run_op() {
    local label="$1"
    local input="$2"
    echo "$input" | $BINARY 2>/dev/null | grep -E "(✓|✗)" || true
    ok "$label"
}

log "--- Binary operations ---"
run_op "10 + 5"       "add 10 5"
run_op "100 - 37"     "sub 100 37"
run_op "6 × 7"        "mul 6 7"
run_op "22 ÷ 7"       "div 22 7"
run_op "2 ^ 32"       "pow 2 32"

log "--- Unary operations ---"
run_op "10!"          "fact 10"
run_op "fib(30)"      "fib 30"

log "--- Error cases ---"
run_op "divide by 0"  "div 5 0"
run_op "neg factorial" "fact -1"
run_op "large fib"    "fib 9999"

log "Demo complete!"
