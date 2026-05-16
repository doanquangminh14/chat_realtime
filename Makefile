.PHONY: all build test run proto docker clean lint help

# Variables
MODULE     = github.com/distributed-systems
GO         = go
GOFLAGS    = -ldflags="-s -w"
BUILD_DIR  = ./bin
CONFIG_DIR = ./configs

PROTO_DIR       = ./internal/rpc/proto
PROTO_OUT       = ./internal/rpc/proto
PROTOC          = protoc
PROTOC_GEN_GO   = protoc-gen-go
PROTOC_GEN_GRPC = protoc-gen-go-grpc

## help: Show this help message
help:
	@echo "Usage: make <target>"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sort

## build: Build all binaries
build: build-rpc-server build-rpc-client build-chat-server build-chat-client build-worker

build-rpc-server:
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/rpc-server ./cmd/rpc-server

build-rpc-client:
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/rpc-client ./cmd/rpc-client

build-chat-server:
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/chat-server ./cmd/chat-server

build-chat-client:
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/chat-client ./cmd/chat-client

build-worker:
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/messaging-worker ./cmd/messaging-worker

## proto: Regenerate protobuf + gRPC code
proto:
	$(PROTOC) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/calculator.proto
	@echo "Protobuf code generated"

## run-server: Run gRPC server locally
run-server:
	$(GO) run ./cmd/rpc-server

## run-client: Run gRPC client locally
run-client:
	$(GO) run ./cmd/rpc-client

## run-chat-server: Run chat server locally
run-chat-server:
	$(GO) run ./cmd/chat-server

## run-chat-client: Run chat client locally
run-chat-client:
	$(GO) run ./cmd/chat-client

## run-worker: Run messaging worker locally
run-worker:
	$(GO) run ./cmd/messaging-worker

## test: Run all tests
test:
	$(GO) test -race -cover ./...

## test-verbose: Run tests with verbose output
test-verbose:
	$(GO) test -race -cover -v ./...

## lint: Run golangci-lint
lint:
	golangci-lint run ./...

## docker: Build all Docker images
docker:
	docker build -t ds-rpc-server -f deployments/Dockerfile.rpc-server .
	docker build -t ds-chat-server -f deployments/Dockerfile.chat-server .
	docker build -t ds-messaging-worker -f deployments/Dockerfile.messaging-worker .

## docker-up: Start all services via docker-compose
docker-up:
	docker-compose up --build -d

## docker-down: Stop all services
docker-down:
	docker-compose down -v

## docker-logs: Follow logs for all services
docker-logs:
	docker-compose logs -f

## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR)
	$(GO) clean ./...

## deps: Download module dependencies
deps:
	$(GO) mod download
	$(GO) mod tidy

## vet: Run go vet
vet:
	$(GO) vet ./...
