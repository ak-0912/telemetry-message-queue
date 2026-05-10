# Telemetry message queue — multi-stage build (mq-server only).
# Build:  docker build -t telemetry-message-queue:latest .
# Run:    docker run --rm -p 50051:50051 -p 8080:8080 -v mqdata:/data/mq telemetry-message-queue:latest

FROM golang:1.25-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /src

# Dependencies first for layer cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary, no CGO (matches distroless/static runtime).
# GOARCH follows the builder image (use docker buildx for other platforms).
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/mq-server ./cmd/server

FROM gcr.io/distroless/static:nonroot

# Default data dir (override with -e MQ_DATA_DIR=... or Helm env).
ENV MQ_DATA_DIR=/data/mq \
    MQ_GRPC_PORT=50051 \
    MQ_HTTP_PORT=8080

COPY --from=builder /out/mq-server /mq-server

USER nonroot:nonroot

EXPOSE 50051/tcp 8080/tcp

ENTRYPOINT ["/mq-server"]
