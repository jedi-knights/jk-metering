# syntax=docker/dockerfile:1.6
#
# Single Dockerfile builds two binaries from cmd/worker and cmd/ingest.
# Each Fly app picks the right binary via the [processes] block in
# fly.toml (worker default) or fly.ingest.toml (ingest override).

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
# Default CMD runs the worker. Fly's [processes] block sets CMD, so
# fly.ingest.toml can fully override to /jk-metering-ingest. Do not
# switch to ENTRYPOINT — ENTRYPOINT prepends, and [processes] would
# become an argv, not a binary swap.
CMD ["/jk-metering"]
