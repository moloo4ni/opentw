# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/opentw ./cmd/opentw

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 opentw
WORKDIR /app
COPY --from=build /out/opentw /app/opentw
COPY config.example.yaml /app/config.example.yaml
# the named volume inherits this directory's owner on first initialization
RUN mkdir -p /data && chown opentw:opentw /data
USER opentw
# Monitoring liveness: the heartbeat is updated at the end of a successful
# cycle (and at the start of a cycle following a successful one). The app
# writes the threshold to /data/opentw.heartbeat.maxage from the actual
# interval — the image needs no rebuild when the config changes; the 2820s
# fallback is the formula's maxage for the shipped config (2×15m + worst
# cycle + one minute); startup coverage below.
#
# Worst first cycle of the shipped config (config.example.yaml: 25 models,
# concurrency 4, http_timeout 120s): catalog request + ceil(25/4)=7 waves
# = (1+7)×120s = 960s — that is how long the heartbeat may be missing on a
# healthy start. start-period 1200s = 960s + 25% headroom: checks before it
# do not count as failures, no false unhealthy.
# IMPORTANT: with a substantially larger N/concurrency/http_timeout set your
# own start-period in the container run config (Quadlet: HealthStartPeriod=;
# cover ceil(N/K)+1 timeout waves) — the image value cannot guess an
# arbitrary config.
HEALTHCHECK --interval=60s --timeout=5s --start-period=1200s --retries=3 \
  CMD test $(( $(date +%s) - $(stat -c %Y /data/opentw.heartbeat) )) -lt $(cat /data/opentw.heartbeat.maxage 2>/dev/null || echo 2820)
ENV TZ=Europe/Moscow
VOLUME ["/data"]
ENTRYPOINT ["/app/opentw"]
CMD ["-config", "/app/config.yaml"]