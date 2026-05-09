FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mq-server ./cmd/server

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/mq-server /mq-server
EXPOSE 50051 8080
USER nonroot:nonroot
ENTRYPOINT ["/mq-server"]
