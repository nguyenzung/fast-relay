# Makefile for relayer-server
# Usage:
#   make setup-jemalloc - install libjemalloc-dev (required for Linux builds)
#   make check-jemalloc - verify jemalloc is available
#   make build          - build the relayer binary into ./bin/relayer
#   make build-aarch64  - build the relayer binary for linux/arm64 into ./bin/relayer-aarch64
#   make client         - build the client binary into ./bin/client
#   make loadtest       - build the loadtest binary into ./bin/loadtest
#   make stress         - build then run the loadtest binary (use ARGS to pass args)
#   make run            - run the relayer from source (go run)
#   make start          - build then run binary in foreground
#   make test           - run go tests (-short, skips the heavy e2e churn test)
#   make test-unit      - run only the unit tests (internal/..., race-enabled; excludes e2e/ by package path)
#   make test-e2e       - run the e2e churn correctness test, output saved to test-result/e2e_churn_correctness.log
#   make fmt            - run go fmt on the module
#   make vet            - run go vet on the module
#   make lint           - run golangci-lint if available
#   make deps           - download modules (go mod download)
#   make clean          - remove build artifacts

BINARY=bin/relayer
CLIENT_BIN=bin/client
LOADTEST_BIN=bin/loadtest
PKG=./...
# Package path exclusion (not just -short) so a future e2e test that forgets
# testing.Short() can't sneak a multi-minute run into test-unit.
UNIT_PKG=$(shell go list ./... | grep -v '/e2e$$')

.PHONY: all build build-aarch64 client loadtest stress run start test test-unit test-e2e fmt vet lint deps clean churntest churn metrics setup-jemalloc check-jemalloc
all: build

setup-jemalloc:
	@echo "Setting up jemalloc..."
	./scripts/setup_jemalloc.sh

check-jemalloc:
	@pkg-config --libs jemalloc >/dev/null 2>&1 \
		&& echo "jemalloc $(shell pkg-config --modversion jemalloc 2>/dev/null) found." \
		|| (echo "ERROR: libjemalloc-dev not found. Run 'make setup-jemalloc' first." >&2 && exit 1)

metrics:
	@echo "Starting metrics collection..."
	./scripts/collect_metrics.sh

build: check-jemalloc deps | bin
	@echo "Building $(BINARY)..."
	go build -o $(BINARY) ./cmd/relayer
	@echo "Built $(BINARY)"

build-aarch64: deps | bin
	@echo "Building $(BINARY) for aarch64 (linux/arm64)..."
	GOOS=linux GOARCH=arm64 go build -o bin/relayer-aarch64 ./cmd/relayer
	@echo "Built bin/relayer-aarch64"

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
	@echo "Running tests (-short, skips the heavy e2e churn test)..."
	go test -short -v $(PKG)

test-unit:
	@echo "Running unit tests (-race, excludes e2e/)..."
	go test -race -v $(UNIT_PKG)

test-e2e:
	@echo "Running e2e churn correctness test (see e2e/churn_correctness_test.go for client count/duration)..."
	@mkdir -p test-result
	@echo "Full output: test-result/e2e_churn_correctness.log"
	bash -o pipefail -c 'go test ./e2e/... -run TestChurnCorrectness -v -timeout 5m 2>&1 | tee test-result/e2e_churn_correctness.log'

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
