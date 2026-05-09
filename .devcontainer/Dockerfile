# Development image for telemetry-message-queue (Go 1.25 — match go.mod and ./Dockerfile).
FROM mcr.microsoft.com/devcontainers/go:1.25
USER root
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER vscode
