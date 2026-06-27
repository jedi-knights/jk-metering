# syntax=docker/dockerfile:1.6
#
# Single Dockerfile builds two binaries from cmd/worker and cmd/ingest.
# Each Fly app picks the right ENTRYPOINT via fly.toml or fly.ingest.toml.

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jk-metering ./cmd/worker
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jk-metering-ingest ./cmd/ingest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/jk-metering /jk-metering
COPY --from=builder /out/jk-metering-ingest /jk-metering-ingest
# Default ENTRYPOINT runs the worker; fly.ingest.toml overrides via
# processes or a custom CMD when deploying the ingest service.
ENTRYPOINT ["/jk-metering"]
