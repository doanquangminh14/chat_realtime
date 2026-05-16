# BÁO CÁO KỸ THUẬT: HỆ THỐNG PHÂN TÁN

**Môn học:** Hệ Thống Phân Tán  
**Đề tài:** Xây dựng hệ thống phân tán với RPC, Messaging và Chat  
**Ngôn ngữ:** Go (Golang)  
**Ngày:** 2026  

---

## Mục Lục

1. [Tổng Quan Hệ Thống Phân Tán](#1-tổng-quan-hệ-thống-phân-tán)
2. [Giao Tiếp RPC](#2-giao-tiếp-rpc)
3. [Kiến Trúc Messaging](#3-kiến-trúc-messaging)
4. [Synchronous vs Asynchronous](#4-synchronous-vs-asynchronous)
5. [Fault Tolerance](#5-fault-tolerance)
6. [Xử Lý Concurrency](#6-xử-lý-concurrency)
7. [Khả Năng Mở Rộng](#7-khả-năng-mở-rộng)
8. [Ưu và Nhược Điểm](#8-ưu-và-nhược-điểm)
9. [Cải Tiến Tương Lai](#9-cải-tiến-tương-lai)

---

## 1. Tổng Quan Hệ Thống Phân Tán

### 1.1 Định Nghĩa

Hệ thống phân tán (Distributed System) là tập hợp các chương trình máy tính chạy đồng thời trên nhiều máy tính riêng biệt trong một mạng lưới, phối hợp với nhau để đạt được mục tiêu chung. Từ góc độ người dùng, hệ thống này hoạt động như một thực thể duy nhất.

**Đặc điểm cơ bản:**
- **Concurrency**: Các thành phần chạy đồng thời
- **No global clock**: Không có đồng hồ chung — phải dùng logical clock hoặc timestamp
- **Independent failures**: Thành phần có thể lỗi độc lập
- **Message passing**: Giao tiếp qua mạng (không dùng shared memory)

### 1.2 Kiến Trúc Dự Án

Dự án này triển khai 3 subsystem độc lập, mỗi cái thể hiện một mô hình giao tiếp khác nhau:

```
┌────────────────────────────────────────────────┐
│          Distributed Systems Platform           │
│                                                │
│  ┌──────────────┐   ┌────────────────────────┐│
│  │  Calculator  │   │   Messaging System     ││
│  │  RPC Service │──>│   (RabbitMQ)           ││
│  │  (gRPC)      │   │   async event log      ││
│  └──────────────┘   └────────────────────────┘│
│                                                │
│  ┌──────────────────────────────────────────┐ │
│  │           Chat System (TCP)              │ │
│  │   rooms · private msg · history         │ │
│  └──────────────────────────────────────────┘ │
└────────────────────────────────────────────────┘
```

---

## 2. Giao Tiếp RPC

### 2.1 Remote Procedure Call là gì?

RPC (Remote Procedure Call) là mô hình cho phép một chương trình gọi thủ tục/hàm trên máy tính khác giống như gọi hàm cục bộ. Client không cần biết chi tiết về network, serialization hay transport.

**Luồng hoạt động của RPC:**

```
Client Side                        Server Side
┌─────────────────────┐           ┌─────────────────────┐
│ 1. Call procedure   │           │                     │
│ 2. Marshal args     │           │ 5. Unmarshal args   │
│ 3. Send over net ──────────────>│ 6. Execute function │
│ 4. Wait for reply   │           │ 7. Marshal result   │
│ 8. Unmarshal result │<──────────── 8. Send result     │
│ 9. Return to caller │           │                     │
└─────────────────────┘           └─────────────────────┘
```

### 2.2 gRPC và Protocol Buffers

Dự án dùng **gRPC** — framework RPC hiện đại của Google — trên nền **HTTP/2**:

| Tính năng             | gRPC                              | REST HTTP/1.1           |
|-----------------------|-----------------------------------|-------------------------|
| Protocol              | HTTP/2 (multiplexing)             | HTTP/1.1 (sequential)   |
| Serialization         | Protocol Buffers (binary)         | JSON (text)             |
| Type safety           | Có (schema từ .proto)             | Không (runtime error)   |
| Code generation       | Tự động từ .proto                 | Manual                  |
| Streaming             | Bi-directional streaming          | Không native            |
| Performance           | ~7-10x faster than JSON REST      | Baseline                |

**Protocol Buffer schema của Calculator Service:**

```protobuf
service CalculatorService {
  rpc Add(BinaryRequest)      returns (CalculationResponse);
  rpc Subtract(BinaryRequest) returns (CalculationResponse);
  rpc Multiply(BinaryRequest) returns (CalculationResponse);
  rpc Divide(BinaryRequest)   returns (CalculationResponse);
  rpc Power(BinaryRequest)    returns (CalculationResponse);
  rpc Factorial(UnaryRequest) returns (CalculationResponse);
  rpc Fibonacci(UnaryRequest) returns (CalculationResponse);
}
```

### 2.3 Clean Architecture trong RPC Layer

Dự án áp dụng **separation of concerns** với 3 lớp riêng biệt:

```
Request ──> [Handler Layer]  ──> [Service Layer]  ──> [EventPublisher]
             (transport)         (business logic)      (side effects)
             validate           compute                publish async
             buildResponse      error handling
```

- **Handler**: Chỉ biết về gRPC protocol, request/response format
- **Service**: Chỉ biết về business logic (math operations)
- **EventPublisher**: Interface — service không biết là RabbitMQ hay Kafka

### 2.4 Interceptor Pattern (Middleware)

gRPC interceptors hoạt động như middleware pipeline:

```go
grpc.ChainUnaryInterceptor(
    RecoveryInterceptor,   // panic recovery — outermost
    LoggingInterceptor,    // log every request/response
    MetricsInterceptor,    // record latency, status codes
)
```

Mỗi interceptor có thể:
- Chặn request (access control)
- Transform request/response
- Ghi log, metrics
- Recovery từ panic

---

## 3. Kiến Trúc Messaging

### 3.1 Message Queue là gì?

Message Queue (MQ) là middleware cho phép các service giao tiếp **bất đồng bộ** thông qua messages được lưu trữ tạm thời trong một queue. Producer gửi message vào queue, consumer lấy ra và xử lý.

```
Producer ──[publish]──> [Exchange] ──[route]──> [Queue] ──[consume]──> Consumer
```

### 3.2 RabbitMQ — AMQP Model

Dự án dùng **RabbitMQ** với **AMQP 0-9-1**. Các thành phần:

| Thành phần | Vai trò                                              |
|------------|------------------------------------------------------|
| Exchange   | Nhận messages từ producer, route theo routing key    |
| Queue      | Lưu trữ messages, durable (tồn tại khi broker restart)|
| Binding    | Kết nối Exchange với Queue theo pattern              |
| Consumer   | Đọc messages từ Queue và xử lý                      |

**Topology trong dự án:**

```
calculator_events (topic exchange)
         │
         ├── routing_key: "calculation.result"
         │          │
         │          └──> calculation_results (queue)
         │                      │
         │                      ├── worker-1 (goroutine)
         │                      ├── worker-2 (goroutine)
         │                      └── worker-3 (goroutine)
         │
         └── [on failure after 3 retries]
                    │
                    └──> calculator_events.dlx (exchange)
                                   │
                                   └──> calculation_results_dlq
```

### 3.3 Dead Letter Queue (DLQ)

DLQ là pattern xử lý "poison messages" — messages không thể xử lý được sau nhiều lần retry:

```go
// Sau maxRetries = 3 lần thất bại
if retryCount >= maxRetries {
    d.Nack(false, false) // requeue=false → route to DLX → DLQ
}
```

Lợi ích:
- Messages không bị mất
- Có thể inspect và reprocess thủ công
- Tránh vòng lặp retry vô hạn làm chậm queue

### 3.4 Event Schema

Mỗi phép tính RPC phát ra một event có cấu trúc:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "operation": "add",
  "operands": [10.0, 5.0],
  "result": 15.0,
  "timestamp": "2026-05-16T10:30:00Z",
  "computation_time_ns": 1250,
  "status": "success"
}
```

---

## 4. Synchronous vs Asynchronous

### 4.1 Synchronous (RPC)

Trong mô hình **đồng bộ**, client chờ (block) cho đến khi nhận được response:

```
Client                    Server
  │──── gRPC Call() ─────>│
  │  (blocked, waiting)    │  processing...
  │<──── Response ─────────│
  │  (unblocked)           │
```

**Đặc điểm:**
- ✓ Simple programming model — giống gọi hàm local
- ✓ Immediate error feedback
- ✗ Client blocked trong suốt thời gian server xử lý
- ✗ Coupling — nếu server chậm, client bị ảnh hưởng
- ✗ Cascading failures — một service chậm lan sang toàn bộ chain

### 4.2 Asynchronous (Messaging)

Trong mô hình **bất đồng bộ**, producer không chờ consumer xử lý xong:

```
Producer              Broker (Queue)         Consumer
  │──── publish() ──>│                          │
  │  (immediate)     │──── deliver ────────────>│
  │  continues...    │                    processing...
```

**Đặc điểm:**
- ✓ Decoupling — producer và consumer không biết nhau
- ✓ Resilience — consumer offline không ảnh hưởng producer
- ✓ Load leveling — queue absorbs traffic spikes
- ✓ Scalability — thêm consumers không cần thay đổi producer
- ✗ Eventual consistency — không có immediate response
- ✗ Harder to debug (distributed trace cần tooling)

### 4.3 Khi Nào Dùng Gì?

| Tình huống                          | Mô hình       |
|-------------------------------------|---------------|
| Cần kết quả ngay lập tức            | Synchronous   |
| Xử lý background (email, log, audit)| Asynchronous  |
| Service phụ thuộc vào response       | Synchronous   |
| Fanout (thông báo nhiều service)    | Asynchronous  |
| Cần ACID transaction                | Synchronous   |
| High-throughput event processing    | Asynchronous  |

**Trong dự án này:**
- Calculator kết quả: **Synchronous** (client cần biết ngay)
- Audit log, analytics: **Asynchronous** (không ảnh hưởng latency)

---

## 5. Fault Tolerance

### 5.1 Các Loại Lỗi Phân Tán

```
Network Failures:       │  Service Failures:
- Packet loss           │  - Crash
- Latency spikes        │  - Memory leak
- Partition             │  - Deadlock
- Timeout               │  - Bug in handler
```

### 5.2 Strategies Trong Dự Án

**a) Retry với Exponential Backoff (RPC Client)**

```go
for i := 0; i <= cfg.GRPC.MaxRetries; i++ {
    conn, err := grpc.Dial(address, opts...)
    if err == nil {
        return conn, nil
    }
    time.Sleep(cfg.GRPC.RetryWaitMin)  // 1s, 2s, 5s...
}
```

**b) gRPC Keepalive**

```go
grpc.KeepaliveParams(keepalive.ServerParameters{
    MaxConnectionIdle: 5 * time.Minute,
    Time:              30 * time.Second,
    Timeout:           10 * time.Second,
})
```

Keepalive phát hiện và ngắt kết nối "zombie" (network đứt nhưng không báo).

**c) RabbitMQ Auto-reconnect**

```go
func (b *RabbitMQBroker) watchConnection() {
    for {
        select {
        case <-b.reconnectCh:
            for {
                time.Sleep(b.cfg.ReconnectDelay)
                if err := b.connect(); err == nil {
                    break
                }
            }
        }
    }
}
```

**d) Panic Recovery**

```go
defer func() {
    if r := recover(); r != nil {
        log.Error("handler panic recovered", ...)
        err = status.Errorf(codes.Internal, "internal server error")
    }
}()
```

Ngăn server crash toàn bộ khi một handler panic.

**e) Context Timeout & Cancellation**

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Fibonacci kiểm tra ctx ở mỗi iteration
select {
case <-ctx.Done():
    return 0, ctx.Err()
default:
}
```

---

## 6. Xử Lý Concurrency

### 6.1 Goroutines

Go dùng **goroutines** — lightweight threads với stack bắt đầu từ 2KB (so với OS thread ~1MB):

```go
// Chat server: mỗi client connection chạy goroutine riêng
go s.handleConn(ctx, conn)

// Messaging: 3 worker goroutines chia sẻ một delivery channel
for i := 0; i < numWorkers; i++ {
    go c.worker(ctx, i, deliveries)
}
```

### 6.2 Mutex cho Shared State

Chat manager dùng `sync.RWMutex` bảo vệ shared maps:

```go
type Manager struct {
    mu      sync.RWMutex
    clients map[string]ClientConn   // shared state
    rooms   map[string]map[string]ClientConn
}

// Read operations: RLock (nhiều goroutine đọc cùng lúc)
func (m *Manager) ClientCount() int {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return len(m.clients)
}

// Write operations: Lock (exclusive)
func (m *Manager) Register(conn ClientConn) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.clients[conn.ID()] = conn
}
```

**Pattern quan trọng:** Lock → modify → Unlock, snapshot trước khi release lock:

```go
// Snapshot để tránh giữ lock khi send (có thể block lâu)
m.mu.RLock()
targets := make([]ClientConn, 0, len(members))
for id, conn := range members {
    targets = append(targets, conn)
}
m.mu.RUnlock()

// Send ngoài lock scope
for _, conn := range targets {
    conn.Send(msg)
}
```

### 6.3 Context Propagation

`context.Context` truyền qua toàn bộ call stack:

```
main() → context.WithCancel()
  │
  ├──> chatServer.Start(ctx)
  │        │
  │        └──> handleConn(ctx, conn)
  │                 │
  │                 └──> heartbeat goroutine
  │                          │
  │                          select <-ctx.Done(): return
  │
  └──> Signal received → cancel() → ctx.Done() closes
```

---

## 7. Khả Năng Mở Rộng

### 7.1 Horizontal Scaling

**gRPC Server:**

```
Load Balancer (envoy/nginx)
      │
      ├──> rpc-server:50051 (instance 1)
      ├──> rpc-server:50052 (instance 2)
      └──> rpc-server:50053 (instance 3)
```

Vì service là **stateless**, có thể scale theo chiều ngang dễ dàng.

**Messaging Workers:**

```
RabbitMQ Queue (Fair dispatch với QoS prefetch=10)
      │
      ├──> messaging-worker (pod 1, 3 goroutines)
      ├──> messaging-worker (pod 2, 3 goroutines)
      └──> messaging-worker (pod 3, 3 goroutines)
```

Thêm worker pods không cần code thay đổi — RabbitMQ tự phân phối.

**Chat Server:**

Chat server có **stateful** (rooms, client list) nên cần:
- Sticky sessions (client cố định với một server)
- Hoặc shared state (Redis Pub/Sub cho broadcast cross-server)

### 7.2 Bottlenecks và Giải Pháp

| Bottleneck          | Giải Pháp                              |
|---------------------|----------------------------------------|
| gRPC server overload| Horizontal scale + load balancer       |
| RabbitMQ throughput | Clustering, sharded queues             |
| Chat state sync     | Redis Pub/Sub hoặc Kafka               |
| DB writes (history) | Async write + batch insert             |

---

## 8. Ưu và Nhược Điểm

### 8.1 gRPC RPC

**Ưu điểm:**
- Type-safe interface từ .proto schema
- Binary serialization (nhỏ và nhanh hơn JSON ~3-10x)
- Bi-directional streaming
- Code generation tự động cho nhiều ngôn ngữ
- HTTP/2 multiplexing (nhiều stream trên một connection)

**Nhược điểm:**
- Không readable như REST JSON
- Cần tooling để debug (grpcurl, grpc-gateway)
- Browser support hạn chế (cần grpc-web)
- Schema thay đổi cần regenerate code

### 8.2 RabbitMQ Messaging

**Ưu điểm:**
- Mature và battle-tested
- Flexible routing (fanout, topic, direct)
- Dead-letter queue built-in
- Management UI tiện lợi
- Message persistence (durable queues)

**Nhược điểm:**
- Single broker = single point of failure (cần clustering)
- Not designed for high-throughput event streaming (Kafka tốt hơn)
- Message ordering chỉ guaranteed trong single queue

### 8.3 TCP Chat

**Ưu điểm:**
- Latency thấp nhất có thể
- Full control over protocol
- Lightweight (không overhead của HTTP)

**Nhược điểm:**
- Browser không support raw TCP (cần WebSocket)
- NAT traversal phức tạp
- Không có built-in reconnect

---

## 9. Cải Tiến Tương Lai

### 9.1 Ngắn Hạn

- [ ] **Authentication & Authorization**: JWT token trong gRPC metadata
- [ ] **TLS/mTLS**: Encrypt all service-to-service communication
- [ ] **WebSocket Chat**: Thay TCP bằng WebSocket cho browser support
- [ ] **Unit & Integration Tests**: >80% coverage với testify
- [ ] **Metrics với Prometheus**: Export latency, error rate, throughput

### 9.2 Trung Hạn

- [ ] **Distributed Tracing**: OpenTelemetry + Jaeger cho end-to-end trace
- [ ] **Service Mesh**: Istio hoặc Linkerd cho traffic management
- [ ] **Database**: PostgreSQL cho calculation history (thay in-memory log)
- [ ] **Redis Pub/Sub**: Cho chat broadcast cross-server
- [ ] **Kubernetes Deployment**: Helm charts, HPA, PodDisruptionBudget

### 9.3 Dài Hạn

- [ ] **Event Sourcing**: Toàn bộ state rebuild từ events
- [ ] **CQRS**: Separate read/write models cho calculator history
- [ ] **Kafka Migration**: Cho high-throughput event streaming (>100k msg/s)
- [ ] **Circuit Breaker**: Hystrix pattern cho resilience
- [ ] **Multi-region**: Geo-distributed deployment với eventual consistency

---

## Tổng Kết

Dự án đã triển khai thành công ba mô hình giao tiếp cơ bản trong hệ thống phân tán:

1. **RPC (gRPC)**: Giao tiếp đồng bộ, type-safe, high-performance giữa calculator client và server với đầy đủ interceptor middleware, error handling, và timeout management.

2. **Messaging (RabbitMQ)**: Giao tiếp bất đồng bộ, decoupled với pub/sub pattern, retry mechanism, dead-letter queue, và concurrent consumers.

3. **Chat (TCP)**: Real-time distributed chat với goroutine-per-connection model, mutex-protected shared state, room management, và graceful shutdown.

Toàn bộ dự án được tổ chức theo **Clean Architecture** với clear separation of concerns, dependency injection, và production-ready patterns: structured logging, context propagation, graceful shutdown, và Docker deployment.
