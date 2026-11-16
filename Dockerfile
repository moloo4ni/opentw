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
# named volume наследует владелца этой директории при первой инициализации
RUN mkdir -p /data && chown opentw:opentw /data
USER opentw
# Живость мониторинга: heartbeat обновляется в конце успешного цикла
# (и в начале цикла после успешного предыдущего). Порог приложение пишет в
# /data/opentw.heartbeat.maxage из фактического interval — образ не нужно
# пересобирать при смене конфигурации; фолбэк 1860с покрывает старт.
HEALTHCHECK --interval=60s --timeout=5s --start-period=90s --retries=3 \
  CMD test $(( $(date +%s) - $(stat -c %Y /data/opentw.heartbeat) )) -lt $(cat /data/opentw.heartbeat.maxage 2>/dev/null || echo 1860)
ENV TZ=Europe/Moscow
VOLUME ["/data"]
ENTRYPOINT ["/app/opentw"]
CMD ["-config", "/app/config.yaml"]