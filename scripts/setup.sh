#!/usr/bin/env bash
# setup.sh — Bootstrap the distributed-systems project locally
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

log() { echo -e "\033[1;34m[setup]\033[0m $*"; }
ok()  { echo -e "\033[1;32m[ok]\033[0m $*"; }
err() { echo -e "\033[1;31m[err]\033[0m $*" >&2; exit 1; }

# --- Prerequisites check ---

log "Checking prerequisites..."

command -v go     >/dev/null 2>&1 || err "Go not found. Install from https://go.dev/dl/"
command -v docker >/dev/null 2>&1 || err "Docker not found. Install from https://docs.docker.com/get-docker/"
command -v make   >/dev/null 2>&1 || err "make not found"

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
REQUIRED="1.22"
if [[ "$(printf '%s\n' "$REQUIRED" "$GO_VERSION" | sort -V | head -n1)" != "$REQUIRED" ]]; then
    err "Go $REQUIRED+ required (found $GO_VERSION)"
fi

ok "Go $GO_VERSION ✓"
ok "Docker $(docker --version | awk '{print $3}' | tr -d ',') ✓"

# --- Dependencies ---

log "Downloading Go modules..."
cd "$ROOT_DIR"
go mod download
go mod tidy
ok "Modules ready ✓"

# --- Build ---

log "Building all binaries..."
mkdir -p bin
make build
ok "Binaries built in ./bin/ ✓"

# --- Start RabbitMQ ---

log "Starting RabbitMQ..."
if docker ps --format '{{.Names}}' | grep -q "^ds-rabbitmq$"; then
    ok "RabbitMQ already running ✓"
else
    docker run -d \
        --name ds-rabbitmq \
        -p 5672:5672 \
        -p 15672:15672 \
        -e RABBITMQ_DEFAULT_USER=guest \
        -e RABBITMQ_DEFAULT_PASS=guest \
        rabbitmq:3.13-management-alpine

    log "Waiting for RabbitMQ to be ready..."
    for i in {1..15}; do
        if docker exec ds-rabbitmq rabbitmq-diagnostics ping >/dev/null 2>&1; then
            ok "RabbitMQ ready ✓"
            break
        fi
        sleep 2
        if [[ $i -eq 15 ]]; then
            err "RabbitMQ failed to start"
        fi
    done
fi

# --- Summary ---

echo ""
echo "=========================================="
echo "  Setup complete! Run components with:"
echo ""
echo "  Terminal 1: ./bin/rpc-server"
echo "  Terminal 2: ./bin/messaging-worker"
echo "  Terminal 3: ./bin/rpc-client"
echo "  Terminal 4: ./bin/chat-server"
echo "  Terminal 5: ./bin/chat-client"
echo ""
echo "  RabbitMQ UI: http://localhost:15672"
echo "  (user: guest / pass: guest)"
echo "=========================================="
