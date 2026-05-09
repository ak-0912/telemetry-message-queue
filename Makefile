.PHONY: build run stop clean-data proto vet lint test test-coverage generate

BIN_DIR       := bin
SERVER_BIN    := $(BIN_DIR)/mq-server
CSV_BIN       := $(BIN_DIR)/csv-streamer
COLL_BIN      := $(BIN_DIR)/telemetry-collector
PID_FILE      := .mq-server.pid
DATA_DIR      := ./data/mq
GRPC_PORT     ?= 50051
HTTP_PORT     ?= 8080

vet:
	go vet ./...

# Uses staticcheck via go run (no global install required).
lint:
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

build: vet lint
	mkdir -p $(BIN_DIR)
	go build -o $(SERVER_BIN) ./cmd/server
	go build -o $(CSV_BIN) ./cmd/csv-streamer
	go build -o $(COLL_BIN) ./cmd/telemetry-collector

proto:
	go generate ./...

generate: proto

$(SERVER_BIN):
	mkdir -p $(BIN_DIR)
	go build -o $(SERVER_BIN) ./cmd/server

# Starts mq-server in the background; PID is written to $(PID_FILE). Use `make stop` to terminate.
run: $(SERVER_BIN)
	@mkdir -p $(DATA_DIR)
	@if [ -f $(PID_FILE) ]; then \
		echo "already running (pid $$(cat $(PID_FILE))); run make stop first"; \
		exit 1; \
	fi
	@MQ_DATA_DIR=$(DATA_DIR) MQ_GRPC_PORT=$(GRPC_PORT) MQ_HTTP_PORT=$(HTTP_PORT) $(SERVER_BIN) & echo $$! > $(PID_FILE)
	@echo "mq-server pid $$(cat $(PID_FILE))  grpc=$(GRPC_PORT) http=$(HTTP_PORT) data=$(DATA_DIR)"

stop:
	@if [ ! -f $(PID_FILE) ]; then echo "not running ($(PID_FILE) missing)"; exit 0; fi
	@kill $$(cat $(PID_FILE)) 2>/dev/null && rm -f $(PID_FILE) && echo stopped || \
		(rm -f $(PID_FILE); echo "process gone; removed stale $(PID_FILE)")

# Remove persisted queue state (partition WALs + offset JSON). Stop mq-server first
# (make stop, or Ctrl+C if you started it with go run); otherwise the process may error or hold stale fds.
clean-data:
	@if [ -f $(PID_FILE) ]; then echo "Stopping mq-server (from $(PID_FILE))..."; $(MAKE) stop; fi
	rm -rf $(DATA_DIR)
	@mkdir -p $(DATA_DIR)
	@echo "Cleaned $(DATA_DIR). Restart mq-server for an empty queue."

test:
	go test ./...

# Statement coverage for application code under ./internal/... and ./pkg/... (excludes cmd/, generated proto).
# Fails if total coverage is below COVERAGE_MIN percent.
COVERAGE_PKGS := ./internal/... ./pkg/...
COVERAGE_MIN    ?= 80

test-coverage:
	go test $(COVERAGE_PKGS) -coverprofile=coverage.out -covermode=atomic
	@go tool cover -func=coverage.out | grep '^total:'
	@pct=$$(go tool cover -func=coverage.out | grep '^total:' | awk '{print $$NF}' | tr -d '%'); \
	awk -v p="$$pct" -v m="$(COVERAGE_MIN)" 'BEGIN { \
		if (p+0 < m) { printf "error: coverage %.1f%% < required %.1f%%\n", p, m; exit 1 } \
		printf "ok: coverage %.1f%% (minimum %.1f%%)\n", p, m \
	}'
