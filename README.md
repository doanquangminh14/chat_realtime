# Distributed Systems — gRPC · Messaging · Chat

> Production-level distributed systems project in Go demonstrating RPC communication, asynchronous messaging, and real-time chat — built with clean architecture principles.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Prerequisites](#setup--prerequisites)
- [Running the System](#running-the-system)
- [RPC Flow](#rpc-flow)
- [Messaging Flow](#messaging-flow)
- [Chat Flow](#chat-flow)
- [Sequence Diagrams](#sequence-diagrams)
- [Distributed Systems Concepts](#distributed-systems-concepts)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Client Tier                                   │
│   ┌───────────────┐           ┌───────────────┐                     │
│   │  rpc-client   │           │  chat-client  │                     │
│   │  (gRPC CLI)   │           │  (TCP CLI)    │                     │
│   └───────┬───────┘           └───────┬───────┘                     │
└───────────┼───────────────────────────┼─────────────────────────────┘
            │ gRPC (protobuf)           │ TCP (JSON)
┌───────────▼───────────────────────────▼─────────────────────────────┐
│                        Service Tier                                   │
│   ┌───────────────┐           ┌───────────────┐                     │
│   │  rpc-server   │           │  chat-server  │                     │
│   │  :50051       │           │  :8080        │                     │
│   │               │           │               │                     │
│   │ ┌───────────┐ │           │ ┌───────────┐ │                     │
│   │ │ handler   │ │           │ │  manager  │ │                     │
│   │ │ service   │ │           │ │  rooms    │ │                     │
│   │ │ middleware│ │           │ │  history  │ │                     │
│   │ └───────────┘ │           │ └───────────┘ │                     │
│   └───────┬───────┘           └───────────────┘                     │
└───────────┼─────────────────────────────────────────────────────────┘
            │ AMQP publish (async, fire-and-forget)
┌───────────▼─────────────────────────────────────────────────────────┐
│                      Messaging Tier                                   │
│   ┌───────────────────────────────────────────┐                     │
│   │          RabbitMQ  :5672                  │                     │
│   │                                           │                     │
│   │  Exchange: calculator_events (topic)      │                     │
│   │  Queue:    calculation_results            │                     │
│   │  DLQ:      calculation_results_dlq        │                     │
│   └───────────────────────┬───────────────────┘                     │
│                           │ AMQP consume                            │
│   ┌───────────────────────▼───────────────────┐                     │
│   │  messaging-worker (3 concurrent consumers) │                    │
│   └───────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer       | Technology                          |
|-------------|-------------------------------------|
| Language    | Go 1.22                             |
| RPC         | gRPC + Protocol Buffers             |
| Messaging   | RabbitMQ (AMQP 0-9-1)              |
| Chat        | TCP Sockets (JSON protocol)         |
| Config      | Viper (YAML + env vars)             |
| Logging     | Uber Zap (structured JSON)          |
| Containers  | Docker + Docker Compose             |

---

## Project Structure

```
distributed-systems/
│
├── cmd/
│   ├── rpc-server/         # gRPC server entry point
│   ├── rpc-client/         # Interactive gRPC CLI client
│   ├── chat-server/        # TCP chat server entry point
│   ├── chat-client/        # Interactive chat CLI client
│   └── messaging-worker/   # RabbitMQ consumer worker
│
├── internal/
│   ├── config/             # Viper-based config management
│   ├── logger/             # Zap structured logger wrapper
│   ├── middleware/         # gRPC unary interceptors (logging, recovery, metrics)
│   │
│   ├── rpc/
│   │   ├── handler/        # gRPC handler (transport layer)
│   │   ├── service/        # Calculator business logic
│   │   └── proto/          # Protobuf definitions + generated code
│   │
│   ├── messaging/
│   │   ├── broker/         # RabbitMQ connection + topology management
│   │   ├── producer/       # Event publisher
│   │   └── consumer/       # Event consumer with retry + DLQ
│   │
│   └── chat/
│       ├── server/         # TCP server + client connection handling
│       ├── client/         # Chat client CLI
│       ├── manager/        # Room management + broadcast
│       └── model/          # Message types and structs
│
├── configs/
│   └── config.yaml         # Default configuration
│
├── deployments/
│   ├── Dockerfile.rpc-server
│   ├── Dockerfile.chat-server
│   └── Dockerfile.messaging-worker
│
├── docs/
│   └── report.md           # Academic technical report
│
├── Makefile
├── docker-compose.yml
└── go.mod
```

---

## Setup & Prerequisites

### Local Development

```bash
# Prerequisites
go 1.22+
docker + docker-compose
make

# Clone and install dependencies
git clone <repo>
cd distributed-systems
make deps
```

### Environment Variables

All configuration can be overridden via env vars with `DS_` prefix:

```bash
DS_GRPC_HOST=localhost
DS_GRPC_PORT=50051
DS_RABBITMQ_URL=amqp://guest:guest@localhost:5672/
DS_CHAT_HOST=0.0.0.0
DS_CHAT_PORT=8080
DS_LOG_LEVEL=info
DS_LOG_FORMAT=json
```

---

## Running the System

### Option A: Docker Compose (Recommended)

```bash
# Start everything (RabbitMQ + rpc-server + messaging-worker + chat-server)
make docker-up

# Follow logs
make docker-logs

# Stop
make docker-down
```

### Option B: Local (manual)

**Terminal 1 — RabbitMQ:**
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

**Terminal 2 — gRPC Server:**
```bash
make run-server
# or: go run ./cmd/rpc-server
```

**Terminal 3 — Messaging Worker:**
```bash
make run-worker
# or: go run ./cmd/messaging-worker
```

**Terminal 4 — gRPC Client:**
```bash
make run-client
# Interactive prompt:
# > add 10 5
# > fib 30
# > fact 12
# > div 10 0
```

**Terminal 5 — Chat Server:**
```bash
make run-chat-server
```

**Terminal 6+ — Chat Clients:**
```bash
# Open multiple terminals
make run-chat-client
# Enter username, then:
# /join general
# Hello everyone!
# /msg alice Hey!
# /rooms
# /quit
```

### Available Make Targets

```bash
make build          # Build all binaries to ./bin/
make test           # Run all tests with race detection
make proto          # Regenerate protobuf code
make docker         # Build Docker images
make docker-up      # Start docker-compose stack
make docker-down    # Tear down stack
make lint           # Run golangci-lint
make clean          # Remove build artifacts
```

---

## RPC Flow

```
Client                        Server
  │                              │
  │── Add(BinaryRequest) ───────>│
  │   request_id: uuid           │
  │   operand_a: 10.0            │
  │   operand_b: 5.0             │
  │                              │
  │                       [handler]
  │                       validate request
  │                       [service]
  │                       result = 10 + 5 = 15
  │                       publishEvent() ──> RabbitMQ (async)
  │                              │
  │<── CalculationResponse ──────│
  │    result: 15.0              │
  │    status: success           │
  │    computation_time_ns: ...  │
```

---

## Messaging Flow

```
rpc-server                RabbitMQ                 messaging-worker
    │                         │                          │
    │── Publish(event) ──────>│                          │
    │   {                     │                          │
    │     operation: "add"    │── deliver ─────────────> │
    │     result: 15.0        │   (to 3 workers)         │
    │     request_id: uuid    │                    [worker 1] process
    │   }                     │                    log to history
    │                         │                    ack message
    │                         │                          │
    │                    [on error]                      │
    │                    retry (max 3)                   │
    │                    → DLQ if exhausted              │
```

**Key concepts demonstrated:**
- **Async decoupling**: RPC server doesn't wait for worker to finish
- **Dead-letter queue**: Failed messages route to DLQ after 3 retries
- **Concurrent consumers**: 3 goroutines share the same queue fairly (QoS prefetch)
- **Message persistence**: `DeliveryMode: Persistent` survives broker restart

---

## Chat Flow

```
Client A          Server            Client B         Client C
  │                  │                  │                 │
  │── /join general >│                  │                 │
  │                  │── "A joined" ───>│                 │
  │                  │── "A joined" ────────────────────> │
  │<─ history ───────│                  │                 │
  │                  │                  │                 │
  │── "Hello!" ─────>│                  │                 │
  │                  │── "Hello!" ─────>│                 │
  │                  │── "Hello!" ────────────────────── >│
  │                  │                  │                 │
  │── /msg B Hey! ──>│                  │                 │
  │                  │── [PM] Hey! ────>│                 │
  │<─ [PM echo] ─────│                  │                 │
```

---

## Sequence Diagrams

### gRPC Calculator Sequence

```
┌──────────┐       ┌──────────────┐      ┌────────────────┐    ┌──────────┐
│  Client  │       │  Middleware  │      │  CalcHandler   │    │  Broker  │
└────┬─────┘       └──────┬───────┘      └───────┬────────┘    └────┬─────┘
     │  gRPC Add()        │                      │                  │
     │──────────────────> │                      │                  │
     │                    │ RecoveryInterceptor   │                  │
     │                    │ LoggingInterceptor    │                  │
     │                    │ MetricsInterceptor    │                  │
     │                    │──────────────────────>│                  │
     │                    │                       │ validate()       │
     │                    │                       │ svc.Add()        │
     │                    │                       │ publishEvent() ──>│
     │                    │                       │ (goroutine)      │
     │                    │<──────────────────────│                  │
     │<───────────────────│                       │                  │
     │  CalculationResponse                       │                  │
```

---

## Distributed Systems Concepts

| Concept              | Implementation                                              |
|----------------------|-------------------------------------------------------------|
| RPC                  | gRPC with Protocol Buffers for type-safe remote calls       |
| Async Messaging      | RabbitMQ topic exchange, fire-and-forget from RPC server    |
| Fault Tolerance      | Consumer retry (x3), dead-letter queue, server recovery     |
| Concurrency          | Goroutines per connection, sync.Mutex for shared state      |
| Context Propagation  | `context.Context` through all layers for timeout/cancel     |
| Graceful Shutdown    | `os.Signal` → cancel context → GracefulStop()               |
| Config Management    | Viper: YAML file + env var override (12-factor app)         |
| Structured Logging   | Uber Zap JSON logs with request_id, component, duration     |
| Service Discovery    | Env-var based address resolution (K8s-ready)                |
| Health Checks        | gRPC HealthCheck RPC + Docker healthcheck                   |
