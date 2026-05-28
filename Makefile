# Makefile for relayer-server
# Usage:
#   make build          - build the relayer binary into ./bin/relayer
#   make client         - build the client binary into ./bin/client
#   make loadtest       - build the loadtest binary into ./bin/loadtest
#   make stress         - build then run the loadtest binary (use ARGS to pass args)
#   make run            - run the relayer from source (go run)
#   make start          - build then run binary in foreground
#   make test           - run go tests
#   make fmt            - run go fmt on the module
#   make vet            - run go vet on the module
#   make lint           - run golangci-lint if available
#   make deps           - download modules (go mod download)
#   make clean          - remove build artifacts

BINARY=bin/relayer
CLIENT_BIN=bin/client
LOADTEST_BIN=bin/loadtest
PKG=./...

.PHONY: all build client loadtest stress run start test fmt vet lint deps clean churntest churn
all: build

build: deps | bin
	@echo "Building $(BINARY)..."
	go build -o $(BINARY) ./cmd/relayer
	@echo "Built $(BINARY)"

client: deps | bin
	@echo "Building client $(CLIENT_BIN)..."
	go build -o $(CLIENT_BIN) ./cmd/client
	@echo "Built $(CLIENT_BIN)"

loadtest: deps | bin
	@echo "Building loadtest $(LOADTEST_BIN)..."
	go build -o $(LOADTEST_BIN) ./cmd/loadtest
	@echo "Built $(LOADTEST_BIN)"

churntest: deps | bin
	@echo "Building churntest $(BIN)/churntest..."
	go build -o $(LOADTEST_BIN) ./cmd/loadtest
	go build -o bin/churntest ./cmd/churntest
	@echo "Built bin/churntest"

stress: loadtest
	@echo "Starting loadtest (stress)..."
	# Use ARGS to pass custom flags, e.g. make stress ARGS="-n 1000 -m 20 -addr localhost:8080"
	./$(LOADTEST_BIN) $(ARGS)

churn: churntest
	@echo "Starting churntest..."
	# Use ARGS to pass custom flags, e.g. make churn ARGS="-n 100 -m 20 -addr localhost:8080"
	./bin/churntest $(ARGS)

bin:
	@mkdir -p bin

run:
	@echo "Running relayer (go run ./cmd/relayer)..."
	go run ./cmd/relayer

start: build
	@echo "Starting $(BINARY)"
	./$(BINARY)

test:
	@echo "Running tests..."
	go test -v $(PKG)

fmt:
	@echo "Formatting..."
	go fmt $(PKG)

vet:
	@echo "Running go vet..."
	go vet $(PKG)

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed; install with \"curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.60.0\""; exit 1; }
	golangci-lint run

deps:
	@echo "Downloading modules..."
	go mod download

clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin
	@echo "done"
