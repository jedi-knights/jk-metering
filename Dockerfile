# syntax=docker/dockerfile:1.6

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jk-metering ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/jk-metering /jk-metering
ENTRYPOINT ["/jk-metering"]
